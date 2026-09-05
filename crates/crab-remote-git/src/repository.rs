use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crab_metadata::git_object_locator::{
    GitLocatorCoverage, GitObjectLocatorSession, GitObjectLookup, GitPackInventoryEntry,
};
use crab_metadata::manifest_store::{read_bulk_pack_list, read_manifest};
use crab_metadata::ref_journal::{list_active_transactions, materialize_ref_journal};
use crab_storage::{Store, StoreLayout};
use crab_xet::hash::MerkleHash;
use gix_hash::ObjectId;
use tokio_util::sync::CancellationToken;

use crate::commit_graph::CommitGraphIndex;
use crate::operation::{TrackedLocatorSession, finish_with_close};
use crate::reader::{ReaderLimits, RemoteGitReader};
use crate::state::RepositoryState;
use crate::{
    Error, HeadReference, OperationContext, OperationKind, RemoteGitRuntime, RemoteGitSnapshot,
    RepositoryRef, RepositoryRefs, RepositoryStateError, ResolvedRevision, Result, Revision,
    RevisionError,
};

const MAX_IDENTITY_COMPONENT_BYTES: usize = 1_024;

/// Immutable logical and placement identity used to isolate repository state.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RepositoryIdentity {
    provider_namespace: Arc<str>,
    repository_namespace: Arc<str>,
    placement_generation: u64,
    snapshot_digest: Option<Arc<str>>,
}

impl RepositoryIdentity {
    /// Validate identity components used by cache and single-flight keys.
    ///
    /// Values are intentionally omitted from `Debug` and errors because they
    /// may encode tenant or physical placement information.
    pub fn new(
        provider_namespace: impl Into<Arc<str>>,
        repository_namespace: impl Into<Arc<str>>,
        placement_generation: u64,
    ) -> Result<Self> {
        let provider_namespace = provider_namespace.into();
        let repository_namespace = repository_namespace.into();
        validate_identity_component("provider namespace", &provider_namespace)?;
        validate_identity_component("repository namespace", &repository_namespace)?;
        if placement_generation == 0 {
            return Err(Error::InvalidRepositoryIdentity {
                component: "placement generation",
            });
        }
        Ok(Self {
            provider_namespace,
            repository_namespace,
            placement_generation,
            snapshot_digest: None,
        })
    }

    /// Return the catalog placement generation included in cache identity.
    #[must_use]
    pub const fn placement_generation(&self) -> u64 {
        self.placement_generation
    }

    pub(crate) fn hash_cache_identity(&self, hash: &mut blake3::Hasher) {
        for component in [
            self.provider_namespace.as_bytes(),
            self.repository_namespace.as_bytes(),
        ] {
            hash.update(&(component.len() as u64).to_be_bytes());
            hash.update(component);
        }
        hash.update(&self.placement_generation.to_be_bytes());
        if let Some(digest) = &self.snapshot_digest {
            hash.update(digest.as_bytes());
        }
    }
}

impl fmt::Debug for RepositoryIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepositoryIdentity")
            .field("provider_namespace", &"<redacted>")
            .field("repository_namespace", &"<redacted>")
            .field("placement_generation", &self.placement_generation)
            .finish()
    }
}

fn validate_identity_component(component: &'static str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_IDENTITY_COMPONENT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::InvalidRepositoryIdentity { component });
    }
    Ok(())
}

/// Limits applied independently to each decoded Git object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectLimits {
    pub max_packed_entry_bytes: u64,
    pub max_inflated_entry_bytes: u64,
    pub max_object_bytes: u64,
    pub max_pack_index_bytes: u64,
    pub max_commit_graph_bytes: u64,
    pub max_delta_depth: usize,
    pub max_tag_depth: usize,
}

impl Default for ObjectLimits {
    fn default() -> Self {
        Self {
            max_packed_entry_bytes: 64 * 1024 * 1024,
            max_inflated_entry_bytes: 64 * 1024 * 1024,
            max_object_bytes: 64 * 1024 * 1024,
            max_pack_index_bytes: 128 * 1024 * 1024,
            max_commit_graph_bytes:
                crab_metadata::split_commit_graph::DEFAULT_MAX_SPLIT_COMMIT_GRAPH_BYTES,
            max_delta_depth: 128,
            max_tag_depth: 32,
        }
    }
}

/// Aggregate limits charged across one repository operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationLimits {
    /// Maximum wall time from locator open through semantic completion.
    pub max_duration: Duration,
    pub max_logical_objects: u64,
    pub max_storage_requests: u64,
    pub max_fetched_bytes: u64,
    pub max_inflated_bytes: u64,
    pub max_depth: u64,
    pub max_entries: u64,
    pub max_history_commits: u64,
    pub max_diff_input_bytes: u64,
    pub max_diff_output_bytes: u64,
    pub max_blame_lines: u64,
    /// Maximum dynamic-programming cells used by line attribution.
    pub max_blame_comparison_cells: u64,
    pub max_archive_entries: u64,
    pub max_archive_bytes: u64,
    pub max_response_bytes: u64,
}

impl Default for OperationLimits {
    fn default() -> Self {
        Self {
            max_duration: Duration::from_secs(5 * 60),
            max_logical_objects: 10_000,
            max_storage_requests: 20_000,
            max_fetched_bytes: 512 * 1024 * 1024,
            max_inflated_bytes: 512 * 1024 * 1024,
            max_depth: 1_024,
            max_entries: 100_000,
            max_history_commits: 10_000,
            max_diff_input_bytes: 32 * 1024 * 1024,
            max_diff_output_bytes: 8 * 1024 * 1024,
            max_blame_lines: 100_000,
            max_blame_comparison_cells: 4_000_000,
            max_archive_entries: 100_000,
            max_archive_bytes: 2 * 1024 * 1024 * 1024,
            max_response_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

/// Validated behavior and resource limits for opening a repository.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RepositoryOptions {
    object: ObjectLimits,
    operation: OperationLimits,
}

impl RepositoryOptions {
    /// Validate all non-zero object and aggregate bounds.
    pub fn new(object: ObjectLimits, operation: OperationLimits) -> Result<Self> {
        validate_object_limits(object)?;
        validate_operation_limits(operation)?;
        Ok(Self { object, operation })
    }

    /// Return per-object resource limits.
    #[must_use]
    pub const fn object_limits(&self) -> ObjectLimits {
        self.object
    }

    /// Return aggregate operation limits.
    #[must_use]
    pub const fn operation_limits(&self) -> OperationLimits {
        self.operation
    }
}

fn validate_object_limits(limits: ObjectLimits) -> Result<()> {
    for (name, value) in [
        ("max_packed_entry_bytes", limits.max_packed_entry_bytes),
        ("max_inflated_entry_bytes", limits.max_inflated_entry_bytes),
        ("max_object_bytes", limits.max_object_bytes),
        ("max_pack_index_bytes", limits.max_pack_index_bytes),
        ("max_commit_graph_bytes", limits.max_commit_graph_bytes),
        ("max_delta_depth", limits.max_delta_depth as u64),
        ("max_tag_depth", limits.max_tag_depth as u64),
    ] {
        validate_non_zero(name, value)?;
    }
    Ok(())
}

fn validate_operation_limits(limits: OperationLimits) -> Result<()> {
    if limits.max_duration.is_zero()
        || limits
            .max_duration
            .checked_add(Duration::from_secs(1))
            .and_then(|duration| duration.checked_mul(2))
            .is_none()
    {
        return Err(Error::InvalidLimit {
            name: "operation duration",
        });
    }
    for (name, value) in [
        ("max_logical_objects", limits.max_logical_objects),
        ("max_storage_requests", limits.max_storage_requests),
        ("max_fetched_bytes", limits.max_fetched_bytes),
        ("max_inflated_bytes", limits.max_inflated_bytes),
        ("max_depth", limits.max_depth),
        ("max_entries", limits.max_entries),
        ("max_history_commits", limits.max_history_commits),
        ("max_diff_input_bytes", limits.max_diff_input_bytes),
        ("max_diff_output_bytes", limits.max_diff_output_bytes),
        ("max_blame_lines", limits.max_blame_lines),
        (
            "max_blame_comparison_cells",
            limits.max_blame_comparison_cells,
        ),
        ("max_archive_entries", limits.max_archive_entries),
        ("max_archive_bytes", limits.max_archive_bytes),
        ("max_response_bytes", limits.max_response_bytes),
    ] {
        validate_non_zero(name, value)?;
    }
    Ok(())
}

fn validate_non_zero(name: &'static str, value: u64) -> Result<()> {
    if value == 0 {
        return Err(Error::InvalidLimit { name });
    }
    Ok(())
}

/// A generation-pinned, filesystem-free view of one Crab Git repository.
///
/// `open` reads and validates the canonical manifest, its complete immutable
/// pack inventory, and matching exact-object locator coverage. It never clones,
/// creates a local Git object database, scans complete pack bodies, or accepts
/// caller-constructed coverage. Every later operation is pinned to this state.
#[derive(Clone)]
pub struct RemoteGitRepository {
    pub(crate) state: Arc<RepositoryState>,
    pub(crate) generated_pack_lease_provider: Option<Arc<dyn crate::GeneratedPackLeaseProvider>>,
}

impl fmt::Debug for RemoteGitRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteGitRepository")
            .field("identity", &self.state.identity)
            .field("generation", &self.state.generation)
            .field("pack_count", &self.state.inventory.len())
            .finish()
    }
}

impl OperationContext {
    /// Read canonical immutable packs from a caller-pinned repository snapshot.
    ///
    /// No locator, lease, repair, or generated-pack publication is opened. The
    /// caller must authorize this store and snapshot together, arrange object
    /// retention, and revalidate snapshot freshness before using its result.
    /// Finish the returned operation on both success and failure.
    pub async fn from_snapshot(
        layout: StoreLayout<Store>,
        snapshot: &crab_metadata::manifest_store::RepositorySnapshot,
        mut identity: RepositoryIdentity,
        runtime: Arc<RemoteGitRuntime>,
        options: RepositoryOptions,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        RepositoryOptions::new(options.object_limits(), options.operation_limits())?;
        check_cancelled(cancellation)?;
        check_cancelled(&runtime.background_cancellation())?;
        let entries = snapshot
            .journal
            .packs
            .len()
            .saturating_add(snapshot.journal.refs.len()) as u64;
        if entries > options.operation_limits().max_entries {
            return Err(Error::LimitExceeded {
                limit: "snapshot entries",
                actual: entries,
                maximum: options.operation_limits().max_entries,
            });
        }
        // Journal commits can change inventory without incrementing the base
        // generation. In particular, an old cached miss must not hide a new pack.
        identity.snapshot_digest = Some(Arc::from(snapshot.digest()?));
        let manifest = snapshot.materialized_manifest();
        let refs = RepositoryRefs::try_from(&manifest)?;
        let inventory = parse_inventory(&snapshot.journal.packs)?;
        let reader = RemoteGitReader::from_pinned(
            layout.store().clone(),
            layout.repo_prefix(),
            inventory.values().copied(),
            ReaderLimits::from_options(options),
            Arc::clone(&runtime),
            identity.clone(),
            manifest.generation,
        )?;
        let state = RepositoryState {
            store: layout.store().clone(),
            layout,
            runtime,
            identity,
            options,
            generation: manifest.generation,
            git_validation_digest: Arc::from(manifest.git_validation_digest.as_str()),
            manifest_etag: snapshot.manifest_etag.clone(),
            coverage: None,
            inventory,
            refs,
            reader: Some(Arc::new(reader)),
            commit_graph: None,
            shallow_closure: None,
        };
        Self::open(Arc::new(state), OperationKind::Repository, cancellation).await
    }
}

impl RemoteGitRepository {
    /// Open one consistent repository generation from an authenticated store.
    ///
    /// The supplied cancellation token is checked around metadata I/O. Empty
    /// repositories open successfully but snapshot operations return
    /// [`Error::EmptyRepository`]. Locator publication lag returns the retryable
    /// [`Error::RepositoryIndexing`]; malformed metadata, provider failures, and
    /// close failures retain their typed source errors.
    pub async fn open(
        store: Store,
        layout: StoreLayout<Store>,
        identity: RepositoryIdentity,
        runtime: Arc<RemoteGitRuntime>,
        options: RepositoryOptions,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        RepositoryOptions::new(options.object_limits(), options.operation_limits())?;
        let _task_token = runtime.operation_token();
        let runtime_cancellation = runtime.background_cancellation();

        for attempt in 0..2 {
            check_cancelled(cancellation)?;
            check_cancelled(&runtime_cancellation)?;
            let active_transactions = list_active_transactions(&store, &layout)
                .await
                .map_err(Error::Metadata)?;
            let (manifest, manifest_etag) = load_manifest(
                &store,
                &layout,
                &runtime,
                &identity,
                cancellation,
                &runtime_cancellation,
            )
            .await?;
            let base_packs = if manifest.pack_index_hash.is_empty() {
                Vec::new()
            } else {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(Error::Cancelled),
                    () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
                    result = read_bulk_pack_list(&store, &layout, &manifest.pack_index_hash) => {
                        result.map_err(|source| Error::Inventory { source })?
                    }
                }
            };
            let journal = materialize_ref_journal(
                &store,
                &layout,
                &manifest,
                &base_packs,
                &[],
                &active_transactions,
            )
            .await
            .map_err(Error::Metadata)?;
            if !journal.transactions.is_empty() {
                return Err(Error::RepositoryIndexing {
                    observed: Some(manifest.generation),
                    required: manifest.generation.saturating_add(1),
                });
            }
            check_cancelled(cancellation)?;
            check_cancelled(&runtime_cancellation)?;
            let refs = RepositoryRefs::try_from(&manifest)?;
            if refs.is_empty() {
                let state = RepositoryState {
                    store,
                    layout,
                    runtime,
                    identity,
                    options,
                    generation: manifest.generation,
                    git_validation_digest: Arc::from(manifest.git_validation_digest.as_str()),
                    manifest_etag,
                    coverage: None,
                    inventory: std::collections::HashMap::new(),
                    refs,
                    reader: None,
                    commit_graph: None,
                    shallow_closure: None,
                };
                return Ok(Self {
                    state: Arc::new(state),
                    generated_pack_lease_provider: None,
                });
            }

            let pack_index_hash = parse_merkle_hash(&manifest.pack_index_hash)?;
            let inventory = match runtime.cached_inventory(&identity, pack_index_hash).await {
                Some(inventory) => inventory.as_ref().clone(),
                None => {
                    check_cancelled(cancellation)?;
                    check_cancelled(&runtime_cancellation)?;
                    let inventory = parse_inventory(&journal.packs)?;
                    runtime
                        .insert_inventory(
                            identity.clone(),
                            pack_index_hash,
                            Arc::new(inventory.clone()),
                        )
                        .await;
                    inventory
                }
            };
            let coverage = GitLocatorCoverage {
                generation: manifest.generation,
                pack_index_hash,
            };
            let session = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(Error::Cancelled),
                () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
                session = GitObjectLocatorSession::open_for_operation(
                    Arc::clone(store.inner()),
                    layout.repo_prefix(),
                    options.operation_limits().max_duration,
                ) => session?,
            };
            let session = TrackedLocatorSession::new(session, Arc::clone(&runtime));
            let observed = session.coverage();
            if observed == Some(coverage) {
                session.close().await.map_err(Error::Metadata)?;
                let reader = RemoteGitReader::from_pinned(
                    store.clone(),
                    layout.repo_prefix(),
                    inventory.values().copied(),
                    ReaderLimits::from_options(options),
                    Arc::clone(&runtime),
                    identity.clone(),
                    manifest.generation,
                )?;
                let commit_graph = match CommitGraphIndex::load(
                    &store,
                    &layout,
                    manifest.commit_graph_hash.as_deref(),
                    manifest.generation,
                    &manifest.pack_index_hash,
                    &manifest.git_validation_digest,
                    &refs
                        .entries
                        .iter()
                        .map(|entry| entry.peeled.unwrap_or(entry.target))
                        .collect::<Vec<_>>(),
                    options.object_limits().max_commit_graph_bytes,
                    cancellation,
                    &runtime_cancellation,
                )
                .await
                {
                    Ok(index) => index.map(Arc::new),
                    Err(Error::Cancelled) => return Err(Error::Cancelled),
                    Err(_) => {
                        runtime.metrics().record(crate::MetricObservation {
                            kind: crate::MetricKind::Metadata,
                            value: 1,
                            duration: None,
                            outcome: Some(crate::MetricOutcome::Error),
                            cache: None,
                        });
                        None
                    }
                };
                let shallow_closure = match tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(Error::Cancelled),
                    () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
                    result = crab_metadata::shallow_closure::load_shallow_closure_descriptor(
                        &store,
                        &layout,
                        &manifest.git_validation_digest,
                        manifest.generation,
                        &manifest.pack_index_hash,
                        crab_metadata::shallow_closure::DEFAULT_MAX_SHALLOW_CLOSURE_DESCRIPTOR_BYTES,
                    ) => result.map_err(Error::Metadata),
                } {
                    Ok(index) => index.map(Arc::new),
                    Err(error) => {
                        tracing::warn!(error = %error, "shallow closure index unavailable; using bounded traversal");
                        runtime.metrics().record(crate::MetricObservation {
                            kind: crate::MetricKind::Metadata,
                            value: 1,
                            duration: None,
                            outcome: Some(crate::MetricOutcome::Error),
                            cache: None,
                        });
                        None
                    }
                };
                let state = RepositoryState {
                    store,
                    layout,
                    runtime,
                    identity,
                    options,
                    generation: manifest.generation,
                    git_validation_digest: Arc::from(manifest.git_validation_digest.as_str()),
                    manifest_etag,
                    coverage: Some(coverage),
                    inventory,
                    refs,
                    reader: Some(Arc::new(reader)),
                    commit_graph,
                    shallow_closure,
                };
                return Ok(Self {
                    state: Arc::new(state),
                    generated_pack_lease_provider: None,
                });
            }

            let observed_generation = observed.map(|value| value.generation);
            if observed_generation.is_none_or(|value| value < manifest.generation) {
                runtime.metrics().record(crate::MetricObservation {
                    kind: crate::MetricKind::PublicationLag,
                    value: manifest
                        .generation
                        .saturating_sub(observed_generation.unwrap_or_default()),
                    duration: None,
                    outcome: None,
                    cache: None,
                });
                let operation = Error::RepositoryIndexing {
                    observed: observed_generation,
                    required: manifest.generation,
                };
                return finish_locator_validation(session, Err(operation)).await;
            }
            if attempt == 1 {
                let operation = Error::RepositoryState {
                    reason: RepositoryStateError::InconsistentGeneration,
                };
                return finish_locator_validation(session, Err(operation)).await;
            }
            session.close().await.map_err(Error::Metadata)?;
        }
        Err(Error::RepositoryState {
            reason: RepositoryStateError::InconsistentGeneration,
        })
    }

    /// Return the immutable manifest generation pinned by this handle.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.state.generation
    }

    pub(crate) fn git_validation_digest(&self) -> &str {
        &self.state.git_validation_digest
    }

    /// Return complete references from the pinned manifest.
    #[must_use]
    pub fn refs(&self) -> &RepositoryRefs {
        &self.state.refs
    }

    /// Compare the pinned immutable packs with a captured metadata inventory.
    ///
    /// Ordering and metadata-generation changes do not change pack identity.
    /// Malformed identities or duplicate packs return an error.
    pub fn matches_pack_inventory(
        &self,
        packs: &[crab_metadata::manifests::PackManifestEntry],
    ) -> Result<bool> {
        Ok(self.state.inventory == parse_inventory(packs)?)
    }

    /// Return the redaction-safe physical identity used by runtime caches.
    #[must_use]
    pub fn identity(&self) -> &RepositoryIdentity {
        &self.state.identity
    }

    /// Install product-owned coordination for generated response-pack misses.
    ///
    /// The provider must protect this repository's object-store namespace.
    #[must_use]
    pub fn with_generated_pack_lease_provider(
        mut self,
        provider: Arc<dyn crate::GeneratedPackLeaseProvider>,
    ) -> Self {
        self.generated_pack_lease_provider = Some(provider);
        self
    }

    /// Return the number of immutable packs in the pinned inventory.
    #[must_use]
    pub fn pack_count(&self) -> usize {
        self.state.inventory.len()
    }

    pub(crate) fn single_pack_inventory(&self) -> Option<GitPackInventoryEntry> {
        if self.state.inventory.len() != 1 {
            return None;
        }
        self.state.inventory.values().copied().next()
    }

    /// Check the current catalog-bound visibility proof without loading its object dictionary.
    pub async fn catalog_visibility_available(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        let runtime_cancellation = self.state.runtime.background_cancellation();
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        let Some(coverage) = self.state.coverage else {
            return Ok(self.state.refs.is_empty());
        };
        let pack_index_hash = coverage.pack_index_hash.to_string();
        let available = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(Error::Cancelled),
            () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
            result = crab_metadata::git_visibility::catalog_bound_available(
                &self.state.store,
                &self.state.layout,
                self.state.generation,
                &pack_index_hash,
                &self.state.git_validation_digest,
            ) => result.map_err(Error::Metadata)?,
        };
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        Ok(available)
    }

    /// Read and validate a catalog-bound v1 proof without materializing its OID dictionary.
    ///
    /// Ref tips are checked with small exact catalog lookups. Later upload-pack
    /// planning resolves only the ordinals selected by the request through its
    /// own generation-pinned operation session.
    pub async fn catalog_visibility_index(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<crab_metadata::git_visibility::GitCatalogVisibilityIndex> {
        let runtime_cancellation = self.state.runtime.background_cancellation();
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        let coverage = self.state.coverage.ok_or_else(|| {
            if self.state.refs.is_empty() {
                Error::EmptyRepository
            } else {
                Error::RepositoryState {
                    reason: RepositoryStateError::VisibilityProofMismatch,
                }
            }
        })?;
        let pack_index_hash = coverage.pack_index_hash.to_string();
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(Error::Cancelled),
            () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
            result = crab_metadata::git_visibility::read_catalog_with_format(
                &self.state.store,
                &self.state.layout,
                self.state.generation,
                &pack_index_hash,
                &self.state.git_validation_digest,
            ) => result.map_err(Error::Metadata)?,
        };
        if read.format != crab_metadata::git_visibility::GitVisibilityFormat::CatalogV1 {
            return Err(Error::RepositoryState {
                reason: RepositoryStateError::VisibilityProofMismatch,
            });
        }
        let index = read.index;
        if index.ref_count() != self.state.refs.entries.len()
            || self
                .state
                .refs
                .entries
                .iter()
                .any(|reference| !index.contains_ref(&reference.name))
        {
            return Err(Error::RepositoryState {
                reason: RepositoryStateError::VisibilityProofMismatch,
            });
        }
        let expected = self
            .state
            .refs
            .entries
            .iter()
            .flat_map(|reference| {
                [
                    Some((reference.name.as_str(), reference.target)),
                    reference.peeled.map(|oid| (reference.name.as_str(), oid)),
                ]
                .into_iter()
                .flatten()
            })
            .map(|(name, oid)| {
                oid.as_bytes()
                    .try_into()
                    .map(|oid| (name.to_owned(), oid))
                    .map_err(|_| Error::RepositoryState {
                        reason: RepositoryStateError::VisibilityProofMismatch,
                    })
            })
            .collect::<Result<Vec<(String, [u8; 20])>>>()?;
        let object_ids = expected
            .iter()
            .map(|(_, object_id)| *object_id)
            .collect::<Vec<_>>();
        let identity = index.catalog_identity().map_err(Error::Metadata)?;
        let session = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(Error::Cancelled),
            () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
            result = GitObjectLocatorSession::open_for_catalog(
                Arc::clone(self.state.store.inner()),
                self.state.layout.repo_prefix(),
                identity,
                Duration::from_secs(60 * 60),
            ) => result.map_err(Error::Metadata)?,
        };
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(Error::Cancelled),
            () = runtime_cancellation.cancelled() => Err(Error::Cancelled),
            lookups = session.lookup_batch(&object_ids, &self.state.inventory) => {
                let lookups = lookups.map_err(Error::Metadata)?;
                if lookups.len() != expected.len()
                    || lookups.iter().zip(&expected).any(|(lookup, (name, _))| {
                        !matches!(lookup, GitObjectLookup::Hit(locator)
                            if index.contains_ordinal_in_ref(name, locator.ordinal))
                    })
                {
                    Err(Error::RepositoryState {
                        reason: RepositoryStateError::VisibilityProofMismatch,
                    })
                } else {
                    Ok(index)
                }
            }
        };
        finish_with_close(result, session.close().await)
    }

    /// Read the immutable object-visibility proof for this pinned generation.
    pub async fn visibility_index(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<crab_metadata::git_visibility::GitVisibilityIndex> {
        let runtime_cancellation = self.state.runtime.background_cancellation();
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        let index = if let Some(coverage) = self.state.coverage {
            let pack_index_hash = coverage.pack_index_hash.to_string();
            let read = crab_metadata::git_visibility::read_with_format(
                &self.state.store,
                &self.state.layout,
                self.state.generation,
                &pack_index_hash,
                &self.state.git_validation_digest,
            );
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(Error::Cancelled),
                () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
                result = read => {
                    let read = result.map_err(Error::Metadata)?;
                    if read.format != crab_metadata::git_visibility::GitVisibilityFormat::CatalogV1 {
                        return Err(Error::RepositoryState {
                            reason: RepositoryStateError::VisibilityProofMismatch,
                        });
                    }
                    read.index
                },
            }
        } else if self.state.refs.is_empty() {
            // An empty repository has no pack index or locator coverage to
            // bind. Its empty closure is still a complete snapshot proof.
            crab_metadata::git_visibility::GitVisibilityIndex::new(
                self.state.generation,
                String::new(),
                self.state.git_validation_digest.as_ref(),
                std::collections::BTreeMap::new(),
            )
            .map_err(Error::Metadata)?
        } else {
            return Err(Error::EmptyRepository);
        };

        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;

        if index.ref_count() != self.state.refs.entries.len()
            || self.state.refs.entries.iter().any(|reference| {
                if !index.contains_ref(&reference.name) {
                    return true;
                }
                let Ok(target) = reference.target.as_bytes().try_into() else {
                    return true;
                };
                !index.contains_in_ref(&reference.name, &target)
                    || reference.peeled.is_some_and(|peeled| {
                        let Ok(peeled) = peeled.as_bytes().try_into() else {
                            return true;
                        };
                        !index.contains_in_ref(&reference.name, &peeled)
                    })
            })
        {
            return Err(Error::RepositoryState {
                reason: RepositoryStateError::VisibilityProofMismatch,
            });
        }
        Ok(index)
    }

    /// Rebuild complete ref visibility from this locator-pinned generation.
    ///
    /// This does not read or trust an existing visibility proof. Canonical
    /// commit, tree, and tag objects are fetched once, while the resulting
    /// per-ref closures share one distinct-object operation bound.
    pub async fn rebuild_visibility_index(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<crab_metadata::git_visibility::GitVisibilityIndex> {
        let pack_index_hash = self
            .state
            .coverage
            .map(|coverage| coverage.pack_index_hash.to_string())
            .unwrap_or_default();
        crate::visibility::rebuild(self, pack_index_hash, cancellation).await
    }

    /// Check whether the canonical manifest still names this pinned generation.
    ///
    /// This performs one metadata-only provider request and never mutates the
    /// handle. Callers may reuse the handle when it returns `true`; `false`
    /// requires a fresh [`Self::open`] handshake. Cancellation and provider
    /// failures remain structured errors.
    pub async fn is_current(&self, cancellation: &CancellationToken) -> Result<bool> {
        let _task_token = self.state.runtime.operation_token();
        let runtime_cancellation = self.state.runtime.background_cancellation();
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        let manifest_path = self.state.layout.manifest_path();
        let current = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(Error::Cancelled),
            () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
            current = self.state.store.head(&manifest_path) => current?,
        };
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        Ok(current.e_tag.as_deref().unwrap_or_default() == self.state.manifest_etag)
    }

    /// Start one named bounded operation with a close-guaranteed locator session.
    ///
    /// The caller must invoke [`OperationContext::finish`] with the semantic
    /// result before completing its response. `kind` is a bounded telemetry
    /// label and cannot contain repository or request data. Cancellation
    /// prevents new work; object, traversal, storage, and response limits are
    /// shared by all calls made with this context.
    pub async fn operation(
        &self,
        kind: OperationKind,
        cancellation: &CancellationToken,
    ) -> Result<OperationContext> {
        OperationContext::open(Arc::clone(&self.state), kind, cancellation).await
    }

    /// Prove which candidate commits are reachable from any pinned graph root.
    ///
    /// Returns `None` when the pinned repository has no complete commit graph.
    /// This metadata-only proof never opens the object-locator database.
    pub fn commits_reachable_from(
        &self,
        candidates: &[ObjectId],
        roots: &[ObjectId],
        cancellation: &CancellationToken,
    ) -> Result<Option<Vec<bool>>> {
        let _task_token = self.state.runtime.operation_token();
        let runtime_cancellation = self.state.runtime.background_cancellation();
        check_cancelled(cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        let Some(graph) = self.state.commit_graph.as_ref() else {
            return Ok(None);
        };
        let reachable = graph.reachable_from_roots(candidates, roots, cancellation)?;
        check_cancelled(&runtime_cancellation)?;
        Ok(Some(reachable))
    }

    /// Resolve a reference or reachable full commit ID against pinned refs.
    ///
    /// Reference names are resolved deterministically. Unqualified names are
    /// rejected when both a branch and tag match. The returned commit is
    /// immutable and suitable for creating a snapshot in the same operation.
    pub async fn resolve(
        &self,
        revision: &Revision,
        operation: &OperationContext,
    ) -> Result<ResolvedRevision> {
        ensure_operation(operation, &self.state)?;
        let (reference_name, start, expected_peeled) = match revision {
            Revision::Reference(name) => {
                let reference = select_reference(&self.state.refs, name)?;
                (
                    Some(reference.name.clone()),
                    reference.target,
                    reference.peeled,
                )
            }
            Revision::Commit(commit) => {
                if !prove_reachable(*commit, &self.state.refs, operation).await? {
                    return Err(Error::Revision {
                        reason: RevisionError::NotReachable,
                    });
                }
                (None, *commit, Some(*commit))
            }
        };
        let (commit, tags) = peel_to_commit(start, operation).await?;
        if expected_peeled.is_some_and(|expected| expected != commit) {
            return Err(Error::Corrupt {
                stage: crate::CorruptionStage::Tag,
            });
        }
        Ok(ResolvedRevision {
            requested: revision.clone(),
            reference: reference_name,
            commit,
            tags,
        })
    }

    /// Resolve a revision and create an immutable commit snapshot.
    ///
    /// The commit object is fetched through the same operation, verified by
    /// object ID, kind-checked, and parsed before the root tree is exposed.
    /// Limits, cancellation, provider, parse, and locator-close failures remain
    /// structured and must be finalized through [`OperationContext::finish`].
    pub async fn snapshot(
        &self,
        revision: &Revision,
        operation: &OperationContext,
    ) -> Result<RemoteGitSnapshot> {
        let resolved = self.resolve(revision, operation).await?;
        let commit = operation.read_commit(resolved.commit).await?;
        let coverage = self.state.coverage.ok_or(Error::EmptyRepository)?;
        Ok(RemoteGitSnapshot {
            generation: self.state.generation,
            pack_index_hash: coverage.pack_index_hash,
            commit_oid: commit.oid,
            root_tree_oid: commit.tree,
            repository: Arc::clone(&self.state),
            commit_graph: self
                .state
                .commit_graph
                .as_ref()
                .filter(|index| index.parents_match(commit.oid, &commit.parents))
                .cloned(),
        })
    }
}

async fn load_manifest(
    store: &Store,
    layout: &StoreLayout<Store>,
    runtime: &RemoteGitRuntime,
    identity: &RepositoryIdentity,
    cancellation: &CancellationToken,
    runtime_cancellation: &CancellationToken,
) -> Result<(crab_metadata::manifests::Manifest, String)> {
    if let Some((manifest, etag)) = runtime.cached_manifest(identity).await {
        let manifest_path = layout.manifest_path();
        let current = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(Error::Cancelled),
            () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
            current = store.head(&manifest_path) => current?,
        };
        if current.e_tag.unwrap_or_default() == etag {
            return Ok((manifest, etag));
        }
    }
    let (manifest, etag) = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(Error::Cancelled),
        () = runtime_cancellation.cancelled() => return Err(Error::Cancelled),
        result = read_manifest(store, layout) => {
            result.map_err(|source| Error::Manifest { source })?
        }
    };
    runtime
        .insert_manifest(identity.clone(), manifest.clone(), etag.clone())
        .await;
    Ok((manifest, etag))
}

async fn finish_locator_validation<T>(
    session: TrackedLocatorSession,
    operation: Result<T>,
) -> Result<T> {
    finish_with_close(operation, session.close().await)
}

/// Parse references from a validated manifest without opening object storage.
///
/// The caller validates the manifest envelope and Git-state digest. This
/// conversion checks ref names, targets, peeling associations and HEAD.
impl TryFrom<&crab_metadata::manifests::Manifest> for RepositoryRefs {
    type Error = Error;

    fn try_from(manifest: &crab_metadata::manifests::Manifest) -> Result<Self> {
        if crab_git::validate_push_refname(&manifest.head).is_err()
            || !manifest.head.starts_with("refs/")
        {
            return Err(Error::RepositoryState {
                reason: RepositoryStateError::InvalidReference,
            });
        }
        if manifest
            .peeled_refs
            .keys()
            .any(|name| !manifest.refs.contains_key(name))
        {
            return Err(Error::RepositoryState {
                reason: RepositoryStateError::OrphanPeeledReference,
            });
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(manifest.refs.len())
            .map_err(|source| Error::Allocation {
                requested: manifest
                    .refs
                    .len()
                    .saturating_mul(std::mem::size_of::<RepositoryRef>()),
                source,
            })?;
        for (name, value) in &manifest.refs {
            if !name.starts_with("refs/") || crab_git::validate_push_refname(name).is_err() {
                return Err(Error::RepositoryState {
                    reason: RepositoryStateError::InvalidReference,
                });
            }
            let target = parse_oid(value)?;
            let peeled = manifest
                .peeled_refs
                .get(name)
                .map(|value| parse_oid(value))
                .transpose()?;
            entries.push(RepositoryRef {
                name: name.clone(),
                target,
                peeled,
            });
        }
        let (head, unborn_head) = if entries.is_empty() {
            (None, Some(manifest.head.clone()))
        } else {
            let target = manifest
                .refs
                .get(&manifest.head)
                .ok_or(Error::RepositoryState {
                    reason: RepositoryStateError::HeadDoesNotResolve,
                })?;
            (
                Some(HeadReference {
                    name: manifest.head.clone(),
                    target: parse_oid(target)?,
                }),
                None,
            )
        };
        Ok(RepositoryRefs {
            head,
            unborn_head,
            entries,
        })
    }
}

fn parse_inventory(
    packs: &[crab_metadata::manifests::PackManifestEntry],
) -> Result<std::collections::HashMap<MerkleHash, GitPackInventoryEntry>> {
    let mut inventory = std::collections::HashMap::new();
    inventory
        .try_reserve(packs.len())
        .map_err(|source| Error::Allocation {
            requested: packs
                .len()
                .saturating_mul(std::mem::size_of::<GitPackInventoryEntry>()),
            source,
        })?;
    for pack in packs {
        let pack_id = parse_merkle_hash(&pack.pack_id)?;
        let entry = GitPackInventoryEntry {
            pack_id,
            object_count: pack.object_count,
            pack_size: pack.size,
        };
        if inventory.insert(pack_id, entry).is_some() {
            return Err(Error::RepositoryState {
                reason: RepositoryStateError::DuplicatePack,
            });
        }
    }
    Ok(inventory)
}

fn parse_merkle_hash(value: &str) -> Result<MerkleHash> {
    MerkleHash::from_hex(value).map_err(|_| Error::RepositoryState {
        reason: RepositoryStateError::InvalidContentIdentity,
    })
}

fn parse_oid(value: &str) -> Result<ObjectId> {
    ObjectId::from_hex(value.as_bytes()).map_err(|_| Error::RepositoryState {
        reason: RepositoryStateError::InvalidContentIdentity,
    })
}

fn select_reference<'a>(refs: &'a RepositoryRefs, name: &str) -> Result<&'a RepositoryRef> {
    if crab_git::validate_push_refname(name).is_err() {
        return Err(Error::Revision {
            reason: RevisionError::InvalidReference,
        });
    }
    let reference = if name.starts_with("refs/") {
        refs.find(name)
    } else {
        let branch = format!("refs/heads/{name}");
        let tag = format!("refs/tags/{name}");
        match (refs.find(&branch), refs.find(&tag)) {
            (Some(_), Some(_)) => {
                return Err(Error::Revision {
                    reason: RevisionError::AmbiguousReference,
                });
            }
            (Some(reference), None) | (None, Some(reference)) => Some(reference),
            (None, None) => None,
        }
    }
    .ok_or_else(|| {
        if name.len() < 40 && name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Error::Revision {
                reason: RevisionError::AbbreviatedObjectId,
            }
        } else if name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            Error::UnsupportedObjectFormat
        } else {
            Error::Revision {
                reason: RevisionError::NotFound,
            }
        }
    })?;
    Ok(reference)
}

async fn peel_to_commit(
    start: ObjectId,
    operation: &OperationContext,
) -> Result<(ObjectId, Vec<crate::AnnotatedTag>)> {
    let mut current = start;
    let mut state = TagPeelState::new(operation.object_limits().max_tag_depth);
    loop {
        state.visit(current)?;
        let object = operation.read_object(current).await?;
        match object.kind {
            gix_object::Kind::Commit => return Ok((current, state.tags)),
            gix_object::Kind::Tag => {
                let tag = operation.parse_tag_object(&object).await?;
                current = tag.target;
                state.push(tag)?;
            }
            _ => {
                return Err(Error::Revision {
                    reason: RevisionError::NotCommit,
                });
            }
        }
    }
}

struct TagPeelState {
    maximum: usize,
    visited: std::collections::HashSet<ObjectId>,
    tags: Vec<crate::AnnotatedTag>,
}

impl TagPeelState {
    fn new(maximum: usize) -> Self {
        Self {
            maximum,
            visited: std::collections::HashSet::new(),
            tags: Vec::new(),
        }
    }

    fn visit(&mut self, oid: ObjectId) -> Result<()> {
        if self.visited.insert(oid) {
            Ok(())
        } else {
            Err(Error::Revision {
                reason: RevisionError::TagCycle,
            })
        }
    }

    fn push(&mut self, tag: crate::AnnotatedTag) -> Result<()> {
        if self.tags.len() >= self.maximum {
            return Err(Error::Revision {
                reason: RevisionError::TagDepth,
            });
        }
        self.tags.push(tag);
        Ok(())
    }
}

async fn prove_reachable(
    target: ObjectId,
    refs: &RepositoryRefs,
    operation: &OperationContext,
) -> Result<bool> {
    // Visit nearby commits before descending either side of a merge. A depth-first
    // walk can exhaust the read budget before checking even HEAD's first parent.
    let mut pending = std::collections::VecDeque::new();
    for reference in &refs.entries {
        match peel_to_commit(reference.target, operation).await {
            Ok((commit, _)) => {
                if reference.peeled.is_some_and(|expected| expected != commit) {
                    return Err(Error::Corrupt {
                        stage: crate::CorruptionStage::Tag,
                    });
                }
                pending.push_back(commit);
            }
            Err(Error::Revision {
                reason: RevisionError::NotCommit,
            }) if reference.name.starts_with("refs/tags/") => {}
            Err(error) => return Err(error),
        }
    }
    let mut visited = std::collections::HashSet::new();
    while let Some(commit_oid) = pending.pop_front() {
        if !visited.insert(commit_oid) {
            continue;
        }
        operation
            .charge(crate::BudgetDimension::HistoryCommits, 1)
            .await?;
        if commit_oid == target {
            return Ok(true);
        }
        let commit = operation.read_commit(commit_oid).await?;
        pending.extend(commit.parents);
    }
    Ok(false)
}

fn ensure_operation(operation: &OperationContext, state: &Arc<RepositoryState>) -> Result<()> {
    if operation.belongs_to(state) {
        Ok(())
    } else {
        Err(Error::InternalInvariant {
            invariant: "operation belongs to another repository generation",
        })
    }
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use crab_metadata::git_object_locator::{GitObjectLocatorWriter, GitPackLocatorRecord};
    use crab_metadata::manifest_store::{
        create_manifest, upload_segmented_bulk, write_manifest_cas,
    };
    use crab_metadata::manifests::{
        BulkData, Manifest, PackManifestEntry, compact_pack_index, compact_shard_index,
    };
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use proptest::prelude::*;

    use super::*;

    #[derive(Debug)]
    struct OpenTestStore {
        inner: Arc<dyn ObjectStore>,
        manifest_path: String,
        manifest_gets: AtomicUsize,
        manifest_heads: AtomicUsize,
        short_manifest: AtomicBool,
        block_manifest: AtomicBool,
        manifest_started: tokio::sync::Notify,
        manifest_release: tokio::sync::Notify,
        block_fragment: std::sync::Mutex<Option<String>>,
        request_started: tokio::sync::Notify,
        request_release: tokio::sync::Notify,
    }

    impl OpenTestStore {
        fn new(repo_prefix: &str) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                manifest_path: format!("{repo_prefix}/manifest"),
                manifest_gets: AtomicUsize::new(0),
                manifest_heads: AtomicUsize::new(0),
                short_manifest: AtomicBool::new(false),
                block_manifest: AtomicBool::new(false),
                manifest_started: tokio::sync::Notify::new(),
                manifest_release: tokio::sync::Notify::new(),
                block_fragment: std::sync::Mutex::new(None),
                request_started: tokio::sync::Notify::new(),
                request_release: tokio::sync::Notify::new(),
            }
        }

        fn block_path_containing(&self, fragment: &str) {
            *self.block_fragment.lock().expect("block-fragment lock") = Some(fragment.to_owned());
        }
    }

    impl fmt::Display for OpenTestStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("remote-git-open-test-store")
        }
    }

    #[async_trait]
    impl ObjectStore for OpenTestStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let should_block = self
                .block_fragment
                .lock()
                .expect("block-fragment lock")
                .as_ref()
                .is_some_and(|fragment| location.as_ref().contains(fragment));
            if should_block {
                self.request_started.notify_one();
                self.request_release.notified().await;
            }
            let is_head = options.head;
            if location.as_ref() == self.manifest_path {
                if is_head {
                    self.manifest_heads.fetch_add(1, Ordering::SeqCst);
                } else {
                    self.manifest_gets.fetch_add(1, Ordering::SeqCst);
                    self.manifest_started.notify_one();
                    if self.block_manifest.load(Ordering::SeqCst) {
                        self.manifest_release.notified().await;
                    }
                }
            }
            let mut result = self.inner.get_opts(location, options).await?;
            if location.as_ref() == self.manifest_path && self.short_manifest.load(Ordering::SeqCst)
            {
                result.meta.size = result.meta.size.saturating_add(1);
                result.range.end = result.range.end.saturating_add(1);
            }
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct OpenFixture {
        backend: Arc<OpenTestStore>,
        store: Store,
        layout: StoreLayout<Store>,
        manifest: Manifest,
        pack_id: MerkleHash,
    }

    async fn open_fixture(
        manifest_generation: u64,
        locator_generation: Option<u64>,
    ) -> OpenFixture {
        let backend = Arc::new(OpenTestStore::new("org/repo"));
        let object_store: Arc<dyn ObjectStore> = backend.clone();
        let store = Store::new(Arc::clone(&object_store));
        let layout = StoreLayout::new(store.clone(), "org/repo".to_owned());
        let pack_id = crab_xet::hash::compute_data_hash(b"repository-open-pack");
        let pack = PackManifestEntry {
            pack_id: pack_id.to_string(),
            size: 128,
            content_hash: pack_id.to_string(),
            ref_tips: vec!["1111111111111111111111111111111111111111".to_owned()],
            object_count: 1,
        };
        let (pack_index_hash, _, pack_index) = compact_pack_index(1, &[pack]).expect("pack index");
        let (shard_index_hash, _, shard_index) = compact_shard_index(1, &[]).expect("shard index");
        upload_segmented_bulk(
            &store,
            &layout,
            &BulkData {
                shard_index,
                pack_index,
            },
        )
        .await
        .expect("upload metadata");

        let mut manifest = Manifest::default_for_repo("refs/heads/main");
        manifest.generation = manifest_generation;
        manifest.refs.insert(
            "refs/heads/main".to_owned(),
            "1111111111111111111111111111111111111111".to_owned(),
        );
        manifest.pack_index_hash = pack_index_hash;
        manifest.shard_index_hash = shard_index_hash;
        manifest.seal_git_validation();
        create_manifest(&store, &layout, &manifest)
            .await
            .expect("create manifest");

        if let Some(locator_generation) = locator_generation {
            let hash = MerkleHash::from_hex(&manifest.pack_index_hash).expect("inventory hash");
            let mut writer = GitObjectLocatorWriter::open(object_store, "org/repo")
                .await
                .expect("open locator");
            writer
                .bind_packs(&[GitPackLocatorRecord {
                    pack_id,
                    committed_generation: 1,
                    pack_index_hash: hash,
                    object_count: 1,
                    pack_size: 128,
                }])
                .await
                .expect("bind pack");
            writer
                .set_coverage(GitLocatorCoverage {
                    generation: locator_generation,
                    pack_index_hash: hash,
                })
                .await
                .expect("set coverage");
            writer.close().await.expect("close locator");
        }
        backend.manifest_gets.store(0, Ordering::SeqCst);
        backend.manifest_heads.store(0, Ordering::SeqCst);
        OpenFixture {
            backend,
            store,
            layout,
            manifest,
            pack_id,
        }
    }

    async fn open(fixture: &OpenFixture) -> Result<RemoteGitRepository> {
        RemoteGitRepository::open(
            fixture.store.clone(),
            fixture.layout.clone(),
            RepositoryIdentity::new("memory", fixture.layout.repo_prefix(), 1)?,
            Arc::new(RemoteGitRuntime::default()),
            RepositoryOptions::default(),
            &CancellationToken::new(),
        )
        .await
    }

    #[test]
    fn identity_is_exact_for_equality_but_redacted_for_debug() {
        let first = RepositoryIdentity::new("provider-a", "repository-a", 1).expect("identity");
        let different = RepositoryIdentity::new("provider-a", "repository-a", 2).expect("identity");
        assert_ne!(first, different);
        let debug = format!("{first:?}");
        assert!(!debug.contains("provider-a"));
        assert!(!debug.contains("repository-a"));
    }

    #[test]
    fn repository_options_reject_each_zero_bound() {
        let object = ObjectLimits {
            max_object_bytes: 0,
            ..ObjectLimits::default()
        };
        assert!(matches!(
            RepositoryOptions::new(object, OperationLimits::default()),
            Err(Error::InvalidLimit {
                name: "max_object_bytes"
            })
        ));

        let operation = OperationLimits {
            max_storage_requests: 0,
            ..OperationLimits::default()
        };
        assert!(matches!(
            RepositoryOptions::new(ObjectLimits::default(), operation),
            Err(Error::InvalidLimit {
                name: "max_storage_requests"
            })
        ));

        let operation = OperationLimits {
            max_duration: Duration::ZERO,
            ..OperationLimits::default()
        };
        assert!(matches!(
            RepositoryOptions::new(ObjectLimits::default(), operation),
            Err(Error::InvalidLimit {
                name: "operation duration"
            })
        ));
    }

    #[tokio::test]
    async fn empty_manifest_opens_without_inventing_refs_or_locator_state() {
        let backend = Arc::new(OpenTestStore::new("org/empty"));
        let object_store: Arc<dyn ObjectStore> = backend;
        let store = Store::new(object_store);
        let layout = StoreLayout::new(store.clone(), "org/empty".to_owned());
        create_manifest(
            &store,
            &layout,
            &Manifest::default_for_repo("refs/heads/main"),
        )
        .await
        .expect("create empty manifest");
        let repository = RemoteGitRepository::open(
            store,
            layout,
            RepositoryIdentity::new("memory", "org/empty", 1).expect("identity"),
            Arc::new(RemoteGitRuntime::default()),
            RepositoryOptions::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("open empty repository");
        assert!(repository.refs().is_empty());
        assert_eq!(repository.pack_count(), 0);
        assert!(
            repository
                .catalog_visibility_available(&CancellationToken::new())
                .await
                .expect("empty repository visibility proof")
        );
    }

    #[tokio::test]
    async fn consistent_manifest_inventory_and_locator_open_one_generation() {
        let fixture = open_fixture(1, Some(1)).await;
        let repository = open(&fixture).await.expect("open repository");
        assert_eq!(repository.generation(), 1);
        assert_eq!(repository.pack_count(), 1);
        assert!(
            !repository
                .catalog_visibility_available(&CancellationToken::new())
                .await
                .expect("missing visibility proof")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_shutdown_cancels_and_drains_repository_open() {
        let fixture = open_fixture(1, Some(1)).await;
        fixture.backend.block_manifest.store(true, Ordering::SeqCst);
        let manifest_started = fixture.backend.manifest_started.notified();
        let runtime = Arc::new(RemoteGitRuntime::default());
        let open_runtime = Arc::clone(&runtime);
        let store = fixture.store.clone();
        let layout = fixture.layout.clone();
        let open = tokio::spawn(async move {
            RemoteGitRepository::open(
                store,
                layout,
                RepositoryIdentity::new("memory", "org/repo", 1).expect("identity"),
                open_runtime,
                RepositoryOptions::default(),
                &CancellationToken::new(),
            )
            .await
        });
        manifest_started.await;
        let shutdown_runtime = Arc::clone(&runtime);
        let shutdown = tokio::spawn(async move {
            shutdown_runtime.shutdown().await;
        });

        assert!(matches!(
            open.await.expect("open task"),
            Err(Error::Cancelled)
        ));
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown drains repository open")
            .expect("shutdown task");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn caller_cancellation_interrupts_each_repository_open_io_phase() {
        for fragment in [
            "org/repo/manifest",
            "metadata/pack/",
            "git_object_catalog_db/",
        ] {
            let fixture = open_fixture(1, Some(1)).await;
            fixture.backend.block_path_containing(fragment);
            let request_started = fixture.backend.request_started.notified();
            let cancellation = CancellationToken::new();
            let open_cancellation = cancellation.clone();
            let runtime = Arc::new(RemoteGitRuntime::default());
            let open_runtime = Arc::clone(&runtime);
            let store = fixture.store.clone();
            let layout = fixture.layout.clone();
            let open = tokio::spawn(async move {
                RemoteGitRepository::open(
                    store,
                    layout,
                    RepositoryIdentity::new("memory", "org/repo", 1).expect("identity"),
                    open_runtime,
                    RepositoryOptions::default(),
                    &open_cancellation,
                )
                .await
            });

            tokio::time::timeout(Duration::from_secs(1), request_started)
                .await
                .unwrap_or_else(|_| panic!("selected I/O phase {fragment} starts"));
            cancellation.cancel();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), open)
                    .await
                    .expect("cancelled open returns")
                    .expect("open task joins"),
                Err(Error::Cancelled)
            ));
            runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn warm_manifest_is_revalidated_and_changed_etag_never_serves_stale_refs() {
        let fixture = open_fixture(1, Some(1)).await;
        let runtime = Arc::new(RemoteGitRuntime::default());
        let first_cancellation = CancellationToken::new();
        let first = RemoteGitRepository::open(
            fixture.store.clone(),
            fixture.layout.clone(),
            RepositoryIdentity::new("memory", fixture.layout.repo_prefix(), 1).expect("identity"),
            Arc::clone(&runtime),
            RepositoryOptions::default(),
            &first_cancellation,
        )
        .await
        .expect("cold open");
        assert_eq!(first.generation(), 1);
        assert_eq!(fixture.backend.manifest_gets.load(Ordering::SeqCst), 1);

        let second_cancellation = CancellationToken::new();
        let second = RemoteGitRepository::open(
            fixture.store.clone(),
            fixture.layout.clone(),
            RepositoryIdentity::new("memory", fixture.layout.repo_prefix(), 1).expect("identity"),
            Arc::clone(&runtime),
            RepositoryOptions::default(),
            &second_cancellation,
        )
        .await
        .expect("warm open");
        assert_eq!(second.generation(), 1);
        assert_eq!(fixture.backend.manifest_gets.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.backend.manifest_heads.load(Ordering::SeqCst), 1);

        let mut next = fixture.manifest.clone();
        next.generation = 2;
        next.refs.insert(
            "refs/heads/main".to_owned(),
            "2222222222222222222222222222222222222222".to_owned(),
        );
        next.seal_git_validation();
        let (_, etag) =
            crab_metadata::manifest_store::read_manifest(&fixture.store, &fixture.layout)
                .await
                .expect("read current manifest");
        write_manifest_cas(&fixture.store, &fixture.layout, &next, &etag)
            .await
            .expect("publish changed manifest");
        let before = fixture.backend.manifest_gets.load(Ordering::SeqCst);
        let changed_cancellation = CancellationToken::new();
        let error = RemoteGitRepository::open(
            fixture.store.clone(),
            fixture.layout.clone(),
            RepositoryIdentity::new("memory", fixture.layout.repo_prefix(), 1).expect("identity"),
            Arc::clone(&runtime),
            RepositoryOptions::default(),
            &changed_cancellation,
        )
        .await
        .expect_err("new manifest must not reuse stale refs");
        assert!(matches!(
            error,
            Error::RepositoryIndexing {
                observed: Some(1),
                required: 2
            }
        ));
        assert!(fixture.backend.manifest_gets.load(Ordering::SeqCst) > before);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn stale_and_absent_locator_states_are_retryable_and_diagnostic() {
        let stale = open_fixture(2, Some(1)).await;
        let stale_error = open(&stale).await.expect_err("stale locator must fail");
        assert!(matches!(
            stale_error,
            Error::RepositoryIndexing {
                observed: Some(1),
                required: 2
            }
        ));
        assert_eq!(
            stale_error.repository_diagnostic(),
            Some(crate::RepositoryDiagnostic::LocatorPublicationInProgress)
        );

        let absent = open_fixture(1, None).await;
        let absent_error = open(&absent).await.expect_err("absent locator must fail");
        assert!(matches!(
            absent_error,
            Error::RepositoryIndexing {
                observed: None,
                required: 1
            }
        ));
        assert_eq!(
            absent_error.repository_diagnostic(),
            Some(crate::RepositoryDiagnostic::LocatorRebuildRequired)
        );
    }

    #[tokio::test]
    async fn newer_locator_retries_the_full_handshake_once_then_fails_consistency() {
        let fixture = open_fixture(1, Some(2)).await;
        let error = open(&fixture)
            .await
            .expect_err("stable newer coverage must fail");
        assert!(matches!(
            error,
            Error::RepositoryState {
                reason: RepositoryStateError::InconsistentGeneration
            }
        ));
        assert_eq!(fixture.backend.manifest_gets.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.backend.manifest_heads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn corrupt_inventory_and_unavailable_origin_have_distinct_diagnostics() {
        let corrupt = open_fixture(1, Some(1)).await;
        let index_path = corrupt.layout.repo_path(&format!(
            "metadata/pack/indexes/{}.json",
            corrupt.manifest.pack_index_hash
        ));
        corrupt
            .backend
            .put(&index_path, Bytes::from_static(b"corrupt").into())
            .await
            .expect("corrupt inventory");
        let corrupt_error = open(&corrupt).await.expect_err("inventory must fail");
        assert_eq!(
            corrupt_error.repository_diagnostic(),
            Some(crate::RepositoryDiagnostic::CorruptInventory)
        );

        let missing = open_fixture(1, Some(1)).await;
        missing
            .backend
            .delete(&missing.layout.manifest_path())
            .await
            .expect("remove manifest");
        let missing_error = open(&missing).await.expect_err("origin must fail");
        assert_eq!(
            missing_error.repository_diagnostic(),
            Some(crate::RepositoryDiagnostic::OriginUnavailable)
        );
    }

    #[tokio::test]
    async fn short_manifest_body_is_rejected_before_repository_state_is_exposed() {
        let fixture = open_fixture(1, Some(1)).await;
        fixture.backend.short_manifest.store(true, Ordering::SeqCst);
        let error = open(&fixture).await.expect_err("short body must fail");
        assert!(matches!(
            error,
            Error::Manifest {
                source: crab_metadata::error::MetadataError::Storage {
                    source: crab_storage::StorageError::CorruptObject { .. }
                }
            }
        ));
    }

    #[tokio::test]
    async fn repository_handle_never_mutates_to_a_concurrently_published_generation() {
        let fixture = open_fixture(1, Some(1)).await;
        let repository = open(&fixture).await.expect("open generation one");
        let mut next = fixture.manifest.clone();
        next.generation = 2;
        next.refs.insert(
            "refs/heads/main".to_owned(),
            "2222222222222222222222222222222222222222".to_owned(),
        );
        next.seal_git_validation();
        let (_, etag) =
            crab_metadata::manifest_store::read_manifest(&fixture.store, &fixture.layout)
                .await
                .expect("read manifest");
        write_manifest_cas(&fixture.store, &fixture.layout, &next, &etag)
            .await
            .expect("publish manifest");
        let object_store: Arc<dyn ObjectStore> = fixture.backend.clone();
        let mut writer = GitObjectLocatorWriter::open(object_store, "org/repo")
            .await
            .expect("open locator");
        writer
            .set_coverage(GitLocatorCoverage {
                generation: 2,
                pack_index_hash: MerkleHash::from_hex(&next.pack_index_hash)
                    .expect("inventory hash"),
            })
            .await
            .expect("publish coverage");
        writer.close().await.expect("close locator");

        assert_eq!(repository.generation(), 1);
        assert_eq!(
            repository.refs().head.as_ref().map(|head| head.target),
            Some(parse_oid("1111111111111111111111111111111111111111").expect("OID"))
        );
        assert!(matches!(
            repository
                .operation(OperationKind::Repository, &CancellationToken::new())
                .await,
            Err(Error::RepositoryIndexing {
                observed: Some(2),
                required: 1
            })
        ));

        let current = open(&fixture).await.expect("open generation two");
        assert_eq!(current.generation(), 2);
        assert_eq!(
            current.refs().head.as_ref().map(|head| head.target),
            Some(parse_oid("2222222222222222222222222222222222222222").expect("OID"))
        );
        assert_eq!(current.pack_count(), 1);
        assert_eq!(fixture.pack_id.to_string().len(), 64);
    }

    #[test]
    fn manifest_refs_reject_invalid_names_and_orphan_peeled_targets() {
        let mut invalid = Manifest::default_for_repo("refs/heads/main");
        invalid.refs.insert(
            "refs/heads/bad..name".to_owned(),
            "1111111111111111111111111111111111111111".to_owned(),
        );
        assert!(matches!(
            RepositoryRefs::try_from(&invalid),
            Err(Error::RepositoryState {
                reason: RepositoryStateError::InvalidReference
            })
        ));

        let mut orphan = Manifest::default_for_repo("refs/heads/main");
        orphan.peeled_refs.insert(
            "refs/tags/missing".to_owned(),
            "1111111111111111111111111111111111111111".to_owned(),
        );
        assert!(matches!(
            RepositoryRefs::try_from(&orphan),
            Err(Error::RepositoryState {
                reason: RepositoryStateError::OrphanPeeledReference
            })
        ));
    }

    #[tokio::test]
    async fn pinned_inventory_comparison_rejects_changed_or_duplicate_packs() {
        let fixture = open_fixture(1, Some(1)).await;
        let repository = open(&fixture).await.unwrap();
        let packs = read_bulk_pack_list(
            &fixture.store,
            &fixture.layout,
            &fixture.manifest.pack_index_hash,
        )
        .await
        .unwrap();
        assert!(repository.matches_pack_inventory(&packs).unwrap());
        let mut changed = packs.clone();
        changed[0].size += 1;
        assert!(!repository.matches_pack_inventory(&changed).unwrap());
        changed = packs.clone();
        changed[0].object_count += 1;
        assert!(!repository.matches_pack_inventory(&changed).unwrap());
        changed = packs.clone();
        changed[0].pack_id = "f".repeat(64);
        assert!(!repository.matches_pack_inventory(&changed).unwrap());
        assert!(!repository.matches_pack_inventory(&[]).unwrap());
        let duplicated = [packs[0].clone(), packs[0].clone()];
        assert!(matches!(
            repository.matches_pack_inventory(&duplicated),
            Err(Error::RepositoryState {
                reason: RepositoryStateError::DuplicatePack
            })
        ));
    }

    #[test]
    fn short_ref_resolution_rejects_ambiguity_and_abbreviated_oid_fallback() {
        let oid = parse_oid("1111111111111111111111111111111111111111").expect("OID");
        let refs = RepositoryRefs {
            head: None,
            unborn_head: None,
            entries: vec![
                RepositoryRef {
                    name: "refs/heads/release".to_owned(),
                    target: oid,
                    peeled: None,
                },
                RepositoryRef {
                    name: "refs/tags/release".to_owned(),
                    target: oid,
                    peeled: None,
                },
            ],
        };
        assert!(matches!(
            select_reference(&refs, "release"),
            Err(Error::Revision {
                reason: RevisionError::AmbiguousReference
            })
        ));
        assert!(matches!(
            select_reference(&RepositoryRefs::default(), "deadbeef"),
            Err(Error::Revision {
                reason: RevisionError::AbbreviatedObjectId
            })
        ));
    }

    fn tag(oid: ObjectId, target: ObjectId) -> crate::AnnotatedTag {
        crate::AnnotatedTag {
            oid,
            target,
            target_kind: gix_object::Kind::Tag,
            name: Bytes::from_static(b"tag"),
            tagger: None,
            message: Bytes::new(),
            signature: None,
        }
    }

    proptest! {
        #[test]
        fn tag_depth_limit_is_exact(maximum in 0usize..32) {
            let mut state = TagPeelState::new(maximum);
            for index in 0..maximum {
                let oid = ObjectId::from([index as u8 + 1; 20]);
                let target = ObjectId::from([index as u8 + 2; 20]);
                state.visit(oid).expect("unique tag");
                state.push(tag(oid, target)).expect("within depth");
            }
            let oid = ObjectId::from([200; 20]);
            let rejected = matches!(
                state.push(tag(oid, ObjectId::from([201; 20]))),
                Err(Error::Revision { reason: RevisionError::TagDepth })
            );
            prop_assert!(rejected);
        }

        #[test]
        fn repeated_tag_oid_is_always_a_cycle(bytes in proptest::array::uniform20(any::<u8>())) {
            let oid = ObjectId::from(bytes);
            let mut state = TagPeelState::new(8);
            state.visit(oid).expect("first visit");
            let rejected = matches!(
                state.visit(oid),
                Err(Error::Revision { reason: RevisionError::TagCycle })
            );
            prop_assert!(rejected);
        }
    }
}
