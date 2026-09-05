use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::{Json, Router, routing::get};
use crab_remote_git::{
    Commit, Error, GitPath, HistoryTraversal, OperationKind, OperationLimits, PageCursor,
    PageRequest, RemoteGitRepository, RemoteGitRuntime, RepositoryIdentity, RepositoryOptions,
    Revision, RevisionError,
};
use crab_storage::{StorageProviderKind, Store, StoreLayout, build_static_env_store};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct Server {
    store: Store,
    layout: StoreLayout<Store>,
    identity: RepositoryIdentity,
    options: RepositoryOptions,
    repository: RemoteGitRepository,
    cursor_key: [u8; 32],
    admission: Semaphore,
    cancellation: CancellationToken,
    port: u16,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Action {
    Refs,
    Commit,
    Commits,
    Tree,
    Blob,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CacheMode {
    Cold,
    #[default]
    Warm,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Parameters {
    mode: CacheMode,
    rev: Option<String>,
    path: Option<String>,
    path_hex: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error("{0}")]
    Input(&'static str),
    #[error("remote Git operation failed")]
    Remote(#[from] Error),
    #[error("repository generation changed; restart the server before comparing timings")]
    Changed,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::Input(message) => (StatusCode::BAD_REQUEST, *message),
            Self::Changed => (
                StatusCode::CONFLICT,
                "Repository changed; restart the server",
            ),
            Self::Remote(error) => match error {
                Error::PathNotFound
                | Error::EmptyRepository
                | Error::Revision {
                    reason: RevisionError::NotFound | RevisionError::NotReachable,
                } => (StatusCode::NOT_FOUND, "Path or revision not found"),
                Error::InvalidPath { .. }
                | Error::InvalidCursor { .. }
                | Error::InvalidLimit { .. }
                | Error::Revision { .. }
                | Error::EntryNotBlob { .. }
                | Error::PathComponentNotTree { .. } => (
                    StatusCode::BAD_REQUEST,
                    "Invalid path, revision, cursor, or entry kind",
                ),
                Error::LimitExceeded { .. } => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Read exceeds the example's operation limits",
                ),
                Error::Timeout { .. } => (StatusCode::GATEWAY_TIMEOUT, "Remote read timed out"),
                Error::Cancelled => (StatusCode::SERVICE_UNAVAILABLE, "Remote read cancelled"),
                Error::RepositoryIndexing { .. } => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Repository is indexing; run the metadata owner from the uploader",
                ),
                _ => (
                    StatusCode::BAD_GATEWAY,
                    "Remote Git read failed; check storage and repository health",
                ),
            },
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: browse_http <bucket> <repository-prefix> [port=8787]";
    let bucket = args.next().ok_or_else(|| io::Error::other(usage))?;
    let prefix = args.next().ok_or_else(|| io::Error::other(usage))?;
    let port: u16 = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(8787);
    if args.next().is_some() {
        return Err(io::Error::other(usage).into());
    }
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let port = listener.local_addr()?.port();
    let store = build_static_env_store(&bucket, StorageProviderKind::S3)?;
    let layout = StoreLayout::new(store.clone(), prefix.clone());
    let identity = RepositoryIdentity::new(format!("s3:{bucket}"), prefix, 1)?;
    let options = RepositoryOptions::new(
        Default::default(),
        OperationLimits {
            max_duration: Duration::from_secs(30),
            max_response_bytes: 8 * 1024 * 1024,
            ..Default::default()
        },
    )?;
    let runtime = Arc::new(RemoteGitRuntime::default());
    let cancellation = CancellationToken::new();
    let started = Instant::now();
    let result = async {
        let repository = RemoteGitRepository::open(
            store.clone(),
            layout.clone(),
            identity.clone(),
            Arc::clone(&runtime),
            options,
            &cancellation,
        )
        .await?;
        println!(
            "Opened generation {} in {:.3} ms",
            repository.generation(),
            milliseconds(started)
        );
        let server = Arc::new(Server {
            store,
            layout,
            identity,
            options,
            repository,
            cursor_key: rand::random(),
            admission: Semaphore::new(4),
            cancellation: cancellation.clone(),
            port,
        });
        let router = Router::new()
            .route(
                "/",
                get(|| async { Html(include_str!("browse_http.html")) }),
            )
            .route("/api/{action}", get(read))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&server),
                local_only,
            ))
            .with_state(server);
        println!("Browse http://127.0.0.1:{port} (Ctrl-C to stop)");
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                if tokio::signal::ctrl_c().await.is_err() {
                    eprintln!("Unable to listen for Ctrl-C; shutting down");
                }
                cancellation.cancel();
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    runtime.shutdown().await;
    result
}

async fn local_only(State(server): State<Arc<Server>>, request: Request, next: Next) -> Response {
    let host = request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok());
    let allowed = [
        format!("127.0.0.1:{}", server.port),
        format!("localhost:{}", server.port),
    ];
    // A loopback listener alone does not prevent browser DNS rebinding.
    if !allowed.iter().any(|value| Some(value.as_str()) == host) {
        return StatusCode::FORBIDDEN.into_response();
    }
    (
        [
            ("cache-control", "no-store"),
            ("x-content-type-options", "nosniff"),
            ("content-security-policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"),
        ],
        next.run(request).await,
    ).into_response()
}

async fn read(
    State(server): State<Arc<Server>>,
    Path(action): Path<Action>,
    Query(params): Query<Parameters>,
) -> Response {
    let started = Instant::now();
    let Ok(_permit) = server.admission.try_acquire() else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "Four reads already in flight; retry after they finish"})),
        )
            .into_response();
    };
    let cancellation = server.cancellation.child_token();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let mut open_ms = 0.0;
    let mut read_ms = 0.0;
    let mut shutdown_ms = 0.0;
    let cold_runtime =
        matches!(params.mode, CacheMode::Cold).then(|| Arc::new(RemoteGitRuntime::default()));
    let result = async {
        let limit = params.limit.unwrap_or(50);
        if !(1..=200).contains(&limit) {
            return Err(ApiError::Input("limit must be between 1 and 200"));
        }
        let path = match (&params.path, &params.path_hex) {
            (Some(_), Some(_)) => return Err(ApiError::Input("use path or path_hex, not both")),
            (_, Some(value)) => GitPath::new(decode_hex(value)?)?,
            (Some(value), _) => GitPath::new(value.as_bytes().to_vec())?,
            _ => GitPath::root(),
        };
        let cursor = params
            .cursor
            .as_deref()
            .map(|value| server.decode_cursor(value))
            .transpose()?;
        let page = PageRequest::new(limit, cursor)?;
        let repository = match &cold_runtime {
            Some(runtime) => {
                let timer = Instant::now();
                let opened = RemoteGitRepository::open(
                    server.store.clone(),
                    server.layout.clone(),
                    server.identity.clone(),
                    Arc::clone(runtime),
                    server.options,
                    &cancellation,
                )
                .await;
                open_ms = milliseconds(timer);
                let repository = opened?;
                if repository.generation() != server.repository.generation() {
                    return Err(ApiError::Changed);
                }
                repository
            }
            None => server.repository.clone(),
        };
        let timer = Instant::now();
        let response = execute(
            &server,
            &repository,
            action,
            params.rev.as_deref(),
            path,
            page,
            &cancellation,
        )
        .await;
        read_ms = milliseconds(timer);
        response.map_err(ApiError::from)
    }
    .await;
    // Cold runtimes own locator cleanup tasks too; drain them on success and failure.
    if let Some(runtime) = cold_runtime {
        let timer = Instant::now();
        runtime.shutdown().await;
        shutdown_ms = milliseconds(timer);
    }
    let mode = match params.mode {
        CacheMode::Cold => "cold",
        CacheMode::Warm => "warm",
    };
    let response = match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    (
        [
            ("server-timing", format!("open;dur={open_ms:.3}, read;dur={read_ms:.3}, shutdown;dur={shutdown_ms:.3}, total;dur={:.3}", milliseconds(started))),
            ("x-crab-cache-mode", mode.to_owned()),
            ("x-crab-generation", server.repository.generation().to_string()),
        ],
        response,
    ).into_response()
}

async fn execute(
    server: &Server,
    repository: &RemoteGitRepository,
    action: Action,
    revision: Option<&str>,
    path: GitPath,
    page: PageRequest,
    cancellation: &CancellationToken,
) -> crab_remote_git::Result<Response> {
    if matches!(action, Action::Refs) {
        let refs = repository.refs();
        return Ok(Json(json!({
            "generation": repository.generation(), "packs": repository.pack_count(),
            "head": refs.head.as_ref().map(|head| json!({"name": head.name, "oid": head.target.to_string()})),
            "refs": refs.entries.iter().map(|entry| json!({
                "name": entry.name, "oid": entry.target.to_string(), "peeled": entry.peeled.map(|oid| oid.to_string()),
            })).collect::<Vec<_>>(),
        })).into_response());
    }
    let revision = match revision {
        Some(value) => Revision::parse(value)?,
        None => Revision::Reference(
            repository
                .refs()
                .head
                .as_ref()
                .ok_or(Error::EmptyRepository)?
                .name
                .clone(),
        ),
    };
    let kind = match action {
        Action::Refs => OperationKind::Repository,
        Action::Commit => OperationKind::Commit,
        Action::Commits => OperationKind::History,
        Action::Tree => OperationKind::Tree,
        Action::Blob => OperationKind::Content,
    };
    let operation = repository.operation(kind, cancellation).await?;
    let result = async {
        let snapshot = repository.snapshot(&revision, &operation).await?;
        let response = match action {
            Action::Refs | Action::Commit => {
                Json(commit_json(&snapshot.commit(&operation).await?)).into_response()
            }
            Action::Commits => {
                let result = snapshot
                    .history(HistoryTraversal::FirstParent, &page, &operation)
                    .await?;
                Json(json!({
                    "items": result.items.iter().map(commit_json).collect::<Vec<_>>(),
                    "next": result.next.map(|cursor| server.encode_cursor(cursor)),
                }))
                .into_response()
            }
            Action::Tree => {
                let result = snapshot.list_directory(&path, &page, &operation).await?;
                Json(json!({
                    "items": result.items.iter().map(|entry| json!({
                        "path": String::from_utf8_lossy(entry.path.as_bytes()),
                        "path_hex": encode_hex(entry.path.as_bytes()),
                        "oid": entry.oid.to_string(), "mode": format!("{:06o}", entry.mode.raw()),
                        "kind": format!("{:?}", entry.kind),
                    })).collect::<Vec<_>>(),
                    "next": result.next.map(|cursor| server.encode_cursor(cursor)),
                }))
                .into_response()
            }
            Action::Blob => {
                let blob = snapshot.read_blob(&path, &operation).await?;
                (
                    [
                        ("content-type", "application/octet-stream".to_owned()),
                        ("content-disposition", "attachment".to_owned()),
                        ("x-crab-blob-oid", blob.metadata.oid.to_string()),
                        (
                            "x-crab-content-class",
                            format!("{:?}", blob.metadata.classification),
                        ),
                    ],
                    blob.bytes,
                )
                    .into_response()
            }
        };
        Ok((
            [("x-crab-commit", snapshot.commit_oid().to_string())],
            response,
        )
            .into_response())
    }
    .await;
    operation.finish(result).await
}

fn commit_json(commit: &Commit) -> Value {
    json!({
        "oid": commit.oid.to_string(), "tree": commit.tree.to_string(),
        "parents": commit.parents.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "author": String::from_utf8_lossy(&commit.author.name),
        "author_seconds": commit.author.seconds,
        "message": String::from_utf8_lossy(&commit.message),
        "message_hex": encode_hex(&commit.message),
    })
}

impl Server {
    fn encode_cursor(&self, cursor: PageCursor) -> String {
        let bytes = cursor.as_bytes();
        format!(
            "{}.{}",
            encode_hex(bytes),
            blake3::keyed_hash(&self.cursor_key, bytes).to_hex()
        )
    }

    fn decode_cursor(&self, value: &str) -> Result<PageCursor, ApiError> {
        let (payload, signature) = value
            .split_once('.')
            .ok_or(ApiError::Input("Invalid cursor"))?;
        let bytes = decode_hex(payload)?;
        let signature = decode_hex(signature)?;
        // Compare Hash values with Blake3's constant-time equality implementation.
        if blake3::keyed_hash(&self.cursor_key, &bytes) != *signature.as_slice() {
            return Err(ApiError::Input("Invalid cursor signature"));
        }
        Ok(PageCursor::from_bytes(bytes)?)
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(value: &str) -> Result<Vec<u8>, ApiError> {
    if !value.len().is_multiple_of(2)
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.len() > 128 * 1024
    {
        return Err(ApiError::Input("Invalid hex bytes"));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| ApiError::Input("Invalid hex bytes"))
        })
        .collect()
}

fn milliseconds(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}
