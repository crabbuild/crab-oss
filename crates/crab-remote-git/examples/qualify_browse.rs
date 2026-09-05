//! Emit remote-only browsing evidence for comparison with an independent Git oracle.
use std::error::Error as StdError;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crab_remote_git::{
    EntryKind, Error, GitPath, HistoryTraversal, ObjectLimits, OperationKind, OperationLimits,
    PageRequest, RemoteGitRepository, RemoteGitRuntime, RepositoryIdentity, RepositoryOptions,
    Revision,
};
use crab_storage::{StorageProviderKind, StoreLayout, build_static_env_store};
use futures_util::TryStreamExt;
use serde_json::json;
use sha1::{Digest, Sha1};
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [bucket, prefix] = args.as_slice() else {
        return Err(io::Error::other("usage: qualify_browse <bucket> <prefix>").into());
    };
    let runtime = Arc::new(RemoteGitRuntime::default());
    let result = qualify(bucket, prefix, Arc::clone(&runtime)).await;
    runtime.shutdown().await;
    result
}

async fn qualify(bucket: &str, prefix: &str, runtime: Arc<RemoteGitRuntime>) -> Result<()> {
    let started = Instant::now();
    let cancellation = CancellationToken::new();
    let store = build_static_env_store(bucket, StorageProviderKind::S3)?;
    // A complete large snapshot needs a larger aggregate archive budget than
    // interactive browsing. Each object remains subject to the default bound.
    let options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_duration: Duration::from_secs(3600),
            max_logical_objects: 200_000,
            max_storage_requests: 500_000,
            max_fetched_bytes: 4 * 1024 * 1024 * 1024,
            max_inflated_bytes: 4 * 1024 * 1024 * 1024,
            ..OperationLimits::default()
        },
    )?;
    let repository = RemoteGitRepository::open(
        store.clone(),
        StoreLayout::new(store, prefix.to_owned()),
        RepositoryIdentity::new(format!("s3:{bucket}"), prefix, 1)?,
        runtime,
        options,
        &cancellation,
    )
    .await?;
    for reference in &repository.refs().entries {
        println!(
            "{}",
            json!({"kind":"ref", "name":reference.name, "oid":reference.target.to_string()})
        );
    }
    let head = repository
        .refs()
        .head
        .as_ref()
        .ok_or_else(|| io::Error::other("repository has no HEAD"))?;
    let operation = repository
        .operation(OperationKind::Snapshot, &cancellation)
        .await?;
    let result = repository
        .snapshot(&Revision::Reference(head.name.clone()), &operation)
        .await;
    let snapshot = operation.finish(result).await?;
    println!(
        "{}",
        json!({"kind":"snapshot", "commit":snapshot.commit_oid().to_string(),
        "tree":snapshot.root_tree_oid().to_string(), "generation":repository.generation(),
        "packs":repository.pack_count()})
    );

    let mut cursor = None;
    for _ in 0..10 {
        let operation = repository
            .operation(OperationKind::History, &cancellation)
            .await?;
        let request = PageRequest::new(100, cursor)?;
        let result = snapshot
            .history(HistoryTraversal::FirstParent, &request, &operation)
            .await;
        let page = operation.finish(result).await?;
        for commit in page.items {
            println!(
                "{}",
                json!({"kind":"commit", "oid":commit.oid.to_string(),
                "tree":commit.tree.to_string(), "parents":commit.parents.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "message":commit.message.as_ref(), "author_name":commit.author.name.as_ref(),
                "author_email":commit.author.email.as_ref(), "author_seconds":commit.author.seconds,
                "committer_seconds":commit.committer.seconds})
            );
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }

    let mut pending = vec![GitPath::root()];
    let mut files = Vec::new();
    let mut directories = 0usize;
    while let Some(path) = pending.pop() {
        let mut cursor = None;
        loop {
            let operation = repository
                .operation(OperationKind::Tree, &cancellation)
                .await?;
            let request = PageRequest::new(17, cursor)?;
            let result = snapshot.list_directory(&path, &request, &operation).await;
            let page = operation.finish(result).await?;
            for entry in page.items {
                println!(
                    "{}",
                    json!({"kind":"entry", "path":entry.path.as_bytes(),
                    "oid":entry.oid.to_string(), "mode":entry.mode.raw()})
                );
                if entry.kind == EntryKind::Tree {
                    pending.push(entry.path);
                } else if entry.kind != EntryKind::Submodule {
                    files.push(entry.path);
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        directories += 1;
        if directories.is_multiple_of(500) {
            eprintln!("directories={directories}");
        }
    }

    files.sort();
    let stride = (files.len() / 128).max(1);
    let mut samples = 0usize;
    for path in files.iter().step_by(stride).take(128) {
        let operation = repository
            .operation(OperationKind::Content, &cancellation)
            .await?;
        let result = snapshot.read_blob(path, &operation).await;
        let blob = operation.finish(result).await?;
        let operation = repository
            .operation(OperationKind::Content, &cancellation)
            .await?;
        let result = snapshot.read_blob(path, &operation).await;
        let warm = operation.finish(result).await?;
        if blob != warm {
            return Err(io::Error::other("cold/warm blob mismatch").into());
        }
        println!(
            "{}",
            json!({"kind":"blob", "path":path.as_bytes(),
            "oid":blob.metadata.oid.to_string(), "bytes":blob.bytes.len(),
            "computed_oid":blob_oid(&blob.bytes)})
        );
        samples += 1;
    }

    let operation = repository
        .operation(OperationKind::Content, &cancellation)
        .await?;
    let result = snapshot.read_blob(&GitPath::root(), &operation).await;
    if !matches!(
        operation.finish(result).await,
        Err(Error::EntryNotBlob { .. })
    ) {
        return Err(
            io::Error::other("directory-as-blob did not fail with the expected type").into(),
        );
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    if !matches!(
        repository.operation(OperationKind::Tree, &cancelled).await,
        Err(Error::Cancelled)
    ) {
        return Err(
            io::Error::other("cancelled operation did not fail with the expected type").into(),
        );
    }
    println!(
        "{}",
        json!({"kind":"errors", "directory_as_blob":"EntryNotBlob", "cancelled":"Cancelled"})
    );

    let operation = repository
        .operation(OperationKind::Archive, &cancellation)
        .await?;
    let mut archive = snapshot.archive_stream(operation)?;
    let mut archive_entries = 0usize;
    while let Some(entry) = archive.try_next().await? {
        println!(
            "{}",
            json!({"kind":"archive", "path":entry.path.as_bytes(),
            "mode":entry.mode.raw(), "bytes":entry.bytes.as_ref().map(|bytes| bytes.len()),
            "computed_oid":entry.bytes.as_ref().map(|bytes| blob_oid(bytes))})
        );
        archive_entries += 1;
        if archive_entries.is_multiple_of(5000) {
            eprintln!("archive_entries={archive_entries}");
        }
    }
    println!(
        "{}",
        json!({"kind":"complete", "directories":directories,
        "blob_samples":samples, "archive_entries":archive_entries,
        "elapsed_ms":started.elapsed().as_millis()})
    );
    Ok(())
}

fn blob_oid(bytes: &[u8]) -> String {
    let mut hash = Sha1::new();
    hash.update(format!("blob {}\0", bytes.len()));
    hash.update(bytes);
    format!("{:x}", hash.finalize())
}
