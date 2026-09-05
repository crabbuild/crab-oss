use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use crab_xet::hash::MerkleHash;
use gix_hash::ObjectId;

use crate::commit_graph::CommitGraphIndex;
use crate::objects::parse_blob;
use crate::state::RepositoryState;
use crate::{
    ArchiveEntry, ArchiveStream, Blame, BlameRange, BlameUnsupportedReason, Blob, BlobMetadata,
    BudgetDimension, ChangeKind, Commit, Comparison, ContentClassification, CorruptionStage,
    CursorError, Diff, DiffClassification, DiffHunk, DirectoryMetadata, EntryKind, EntryMode,
    Error, GitPath, HistoryTraversal, OperationContext, Page, PageCursor, PageRequest,
    PathHistoryEntry, Result, Submodule, Symlink, TreeChange, TreeEntry,
};

/// Immutable commit snapshot used by all repository browsing operations.
///
/// A snapshot is pinned to one validated manifest generation, pack inventory,
/// commit, and root tree. Moving refs and later pushes cannot mutate it.
#[derive(Clone)]
pub struct RemoteGitSnapshot {
    pub(crate) generation: u64,
    pub(crate) pack_index_hash: MerkleHash,
    pub(crate) commit_oid: ObjectId,
    pub(crate) root_tree_oid: ObjectId,
    pub(crate) repository: Arc<RepositoryState>,
    pub(crate) commit_graph: Option<Arc<CommitGraphIndex>>,
}

impl RemoteGitSnapshot {
    /// Return the pinned manifest generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Return the immutable pack-inventory identity.
    #[must_use]
    pub const fn pack_index_hash(&self) -> MerkleHash {
        self.pack_index_hash
    }

    /// Return the resolved immutable commit ID.
    #[must_use]
    pub const fn commit_oid(&self) -> ObjectId {
        self.commit_oid
    }

    /// Return the root tree ID parsed from the verified commit.
    #[must_use]
    pub const fn root_tree_oid(&self) -> ObjectId {
        self.root_tree_oid
    }

    /// Read and parse the snapshot commit through the supplied operation.
    pub async fn commit(&self, operation: &OperationContext) -> Result<Commit> {
        self.ensure_operation(operation)?;
        operation.read_commit(self.commit_oid).await
    }

    /// Return one deterministic bounded page of commits reachable from this snapshot.
    ///
    /// Continuations bind the immutable start commit and traversal policy. A
    /// continuation may rewalk a bounded prefix after cache loss and therefore
    /// requires no server-side history database.
    pub async fn history(
        &self,
        traversal: HistoryTraversal,
        page: &PageRequest,
        operation: &OperationContext,
    ) -> Result<Page<Commit>> {
        self.ensure_operation(operation)?;
        let skip = match page.after() {
            Some(cursor) => {
                let decoded = cursor.decode_history()?;
                if decoded.start != self.commit_oid || decoded.mode != traversal {
                    return Err(Error::InvalidCursor {
                        reason: CursorError::ContextMismatch,
                    });
                }
                decoded.skip
            }
            None => 0,
        };
        let mut pending = vec![self.commit_oid];
        let mut visited = HashSet::new();
        let mut traversed = 0u64;
        let mut items = Vec::new();
        while let Some(oid) = pending.pop() {
            operation.ensure_active()?;
            if visited.contains(&oid) {
                continue;
            }
            if traversed >= skip && items.len() == page.limit() {
                pending.push(oid);
                break;
            }
            visited.insert(oid);
            operation.charge(BudgetDimension::HistoryCommits, 1).await?;
            let commit = operation.read_commit(oid).await?;
            match traversal {
                HistoryTraversal::FirstParent => {
                    if let Some(parent) = commit.parents.first() {
                        pending.push(*parent);
                    }
                }
                HistoryTraversal::AllParents => {
                    queue_all_parents(&mut pending, &commit, self.commit_graph.as_deref())?;
                }
            }
            if traversed >= skip {
                operation
                    .charge(
                        BudgetDimension::ResponseBytes,
                        commit_response_bytes(&commit),
                    )
                    .await?;
                items.try_reserve(1).map_err(|source| Error::Allocation {
                    requested: mem::size_of::<Commit>(),
                    source,
                })?;
                items.push(commit);
            }
            traversed = traversed.checked_add(1).ok_or(Error::LimitExceeded {
                limit: "history commits",
                actual: u64::MAX,
                maximum: u64::MAX,
            })?;
        }
        while pending.last().is_some_and(|oid| visited.contains(oid)) {
            pending.pop();
        }
        let next = if pending.is_empty() {
            None
        } else {
            let next_skip = skip
                .checked_add(items.len() as u64)
                .ok_or(Error::LimitExceeded {
                    limit: "history commits",
                    actual: u64::MAX,
                    maximum: u64::MAX,
                })?;
            Some(PageCursor::history(self.commit_oid, traversal, next_skip))
        };
        Ok(Page { items, next })
    }

    /// Return commits that changed one exact path under a parent policy.
    ///
    /// Content, mode, type, addition, and deletion changes are detected from
    /// raw tree entries. Rename similarity is intentionally not inferred.
    pub async fn path_history(
        &self,
        path: &GitPath,
        traversal: HistoryTraversal,
        page: &PageRequest,
        operation: &OperationContext,
    ) -> Result<Page<PathHistoryEntry>> {
        self.ensure_operation(operation)?;
        let skip = match page.after() {
            Some(cursor) => {
                let decoded = cursor.decode_path_history()?;
                if decoded.start != self.commit_oid
                    || decoded.mode != traversal
                    || decoded.path != path.as_bytes()
                {
                    return Err(Error::InvalidCursor {
                        reason: CursorError::ContextMismatch,
                    });
                }
                decoded.skip
            }
            None => 0,
        };
        let mut pending = vec![self.commit_oid];
        let mut visited = HashSet::new();
        let mut traversed = 0u64;
        let mut items = Vec::new();
        let mut next_skip = None;
        while let Some(oid) = pending.pop() {
            operation.ensure_active()?;
            if !visited.insert(oid) {
                continue;
            }
            operation.charge(BudgetDimension::HistoryCommits, 1).await?;
            let commit = operation.read_commit(oid).await?;
            match traversal {
                HistoryTraversal::FirstParent => {
                    if let Some(parent) = commit.parents.first() {
                        pending.push(*parent);
                    }
                }
                HistoryTraversal::AllParents => {
                    queue_all_parents(&mut pending, &commit, self.commit_graph.as_deref())?;
                }
            }
            let mut filled_page = false;
            if traversed >= skip
                && let Some(kind) = path_change(self, &commit, path, traversal, operation).await?
            {
                operation
                    .charge(
                        BudgetDimension::ResponseBytes,
                        commit_response_bytes(&commit)
                            .saturating_add(mem::size_of::<PathHistoryEntry>() as u64),
                    )
                    .await?;
                items.try_reserve(1).map_err(|source| Error::Allocation {
                    requested: mem::size_of::<PathHistoryEntry>(),
                    source,
                })?;
                items.push(PathHistoryEntry { commit, kind });
                filled_page = items.len() == page.limit();
            }
            traversed = traversed.checked_add(1).ok_or(Error::LimitExceeded {
                limit: "history commits",
                actual: u64::MAX,
                maximum: u64::MAX,
            })?;
            if filled_page {
                while pending.last().is_some_and(|oid| visited.contains(oid)) {
                    pending.pop();
                }
                if !pending.is_empty() {
                    next_skip = Some(traversed);
                }
                break;
            }
        }
        let next = next_skip
            .map(|skip| PageCursor::path_history(self.commit_oid, traversal, path, skip))
            .transpose()?;
        Ok(Page { items, next })
    }

    /// Recursively compare this snapshot with an older snapshot.
    ///
    /// Equal tree IDs prune complete subtrees. Results preserve raw Git path
    /// ordering and fail with a limit error rather than returning an
    /// unmarked partial comparison.
    pub async fn compare(&self, base: &Self, operation: &OperationContext) -> Result<Comparison> {
        self.ensure_operation(operation)?;
        base.ensure_operation(operation)?;
        if !Arc::ptr_eq(&self.repository, &base.repository) {
            return Err(Error::InternalInvariant {
                invariant: "comparison snapshots belong to different repository generations",
            });
        }
        let mut changes = Vec::new();
        compare_trees(
            base.root_tree_oid,
            self.root_tree_oid,
            &GitPath::root(),
            0,
            &mut changes,
            operation,
        )
        .await?;
        changes.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(Comparison {
            base: base.commit_oid,
            head: self.commit_oid,
            changes,
        })
    }

    /// Produce a deterministic bounded textual diff for one exact path.
    ///
    /// Binary, pointer, oversized, and unsupported-encoding inputs return an
    /// explicit classification with no hunks. This implementation emits one
    /// complete replacement hunk and performs no rename similarity analysis.
    pub async fn diff(
        &self,
        base: &Self,
        path: &GitPath,
        operation: &OperationContext,
    ) -> Result<Diff> {
        self.ensure_operation(operation)?;
        base.ensure_operation(operation)?;
        if !Arc::ptr_eq(&self.repository, &base.repository) {
            return Err(Error::InternalInvariant {
                invariant: "diff snapshots belong to different repository generations",
            });
        }
        let old = read_optional_blob(base, path, operation).await?;
        let new = read_optional_blob(self, path, operation).await?;
        let old_bytes = old.as_ref().map_or(&[][..], |blob| blob.bytes.as_ref());
        let new_bytes = new.as_ref().map_or(&[][..], |blob| blob.bytes.as_ref());
        let input_bytes = (old_bytes.len() as u64).saturating_add(new_bytes.len() as u64);
        if let Err(error) = operation
            .charge(BudgetDimension::DiffInputBytes, input_bytes)
            .await
        {
            return limit_as_diff(path, error);
        }
        let classification = classify_diff(old.as_ref(), new.as_ref(), operation.cancellation())?;
        if classification != DiffClassification::Text {
            return Ok(Diff {
                path: path.clone(),
                classification,
                hunks: Vec::new(),
            });
        }
        if old_bytes == new_bytes {
            return Ok(Diff {
                path: path.clone(),
                classification,
                hunks: Vec::new(),
            });
        }
        let old_lines = line_count(old_bytes, operation.cancellation())?;
        let new_lines = line_count(new_bytes, operation.cancellation())?;
        let output_size = old_bytes
            .len()
            .saturating_add(new_bytes.len())
            .saturating_add(old_lines)
            .saturating_add(new_lines);
        if let Err(error) = operation
            .charge(BudgetDimension::DiffOutputBytes, output_size as u64)
            .await
        {
            return limit_as_diff(path, error);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(output_size)
            .map_err(|source| Error::Allocation {
                requested: output_size,
                source,
            })?;
        append_diff_lines(&mut bytes, b'-', old_bytes, operation.cancellation())?;
        append_diff_lines(&mut bytes, b'+', new_bytes, operation.cancellation())?;
        operation
            .charge(BudgetDimension::ResponseBytes, bytes.len() as u64)
            .await?;
        Ok(Diff {
            path: path.clone(),
            classification,
            hunks: vec![DiffHunk {
                old_start: 1,
                old_lines: old_lines as u64,
                new_start: 1,
                new_lines: new_lines as u64,
                bytes: bytes::Bytes::from(bytes),
            }],
        })
    }

    /// Attribute every line in one ordinary UTF-8 file through first parents.
    ///
    /// The result is complete or fails with a resource limit. Rename following,
    /// merge-parent attribution, binary blobs, and pointer materialization are
    /// intentionally outside this bounded object-store reader operation.
    pub async fn blame(&self, path: &GitPath, operation: &OperationContext) -> Result<Blame> {
        self.ensure_operation(operation)?;
        operation.ensure_active()?;
        if let Some(cached) = operation.cached_blame(self.commit_oid, path).await {
            operation.charge_cached(cached.usage).await?;
            return Ok(cached.value.as_ref().clone());
        }
        let budget_before = operation.budget_usage().await;
        let head_commit = operation.read_commit(self.commit_oid).await?;
        let head_blob = read_blame_blob(self, path, operation).await?;
        let mut current_oid = head_blob.metadata.oid;
        let mut current_bytes = head_blob.bytes;
        let current_line_count = line_count(&current_bytes, operation.cancellation())?;
        operation
            .charge(BudgetDimension::BlameLines, current_line_count as u64)
            .await?;
        let mut current_lines =
            line_ranges(&current_bytes, current_line_count, operation.cancellation())?;

        let mut origins = Vec::new();
        origins
            .try_reserve_exact(current_lines.len())
            .map_err(|source| Error::Allocation {
                requested: current_lines
                    .len()
                    .saturating_mul(mem::size_of::<ObjectId>()),
                source,
            })?;
        origins.resize(current_lines.len(), self.commit_oid);
        let mut tracked = Vec::new();
        tracked
            .try_reserve_exact(current_lines.len())
            .map_err(|source| Error::Allocation {
                requested: current_lines
                    .len()
                    .saturating_mul(mem::size_of::<Option<usize>>()),
                source,
            })?;
        tracked.extend((0..current_lines.len()).map(Some));
        let mut commits = HashMap::new();
        commits.insert(head_commit.oid, head_commit.clone());
        let mut current_commit = head_commit;
        let mut visited = HashSet::new();
        visited.insert(current_commit.oid);

        while let Some(parent_oid) = current_commit.parents.first().copied() {
            operation.ensure_active()?;
            if !visited.insert(parent_oid) {
                return Err(Error::Corrupt {
                    stage: CorruptionStage::Commit,
                });
            }
            operation.charge(BudgetDimension::HistoryCommits, 1).await?;
            let parent_commit = operation.read_commit(parent_oid).await?;
            let Some(parent_blob) = read_optional_blame_blob(
                self,
                parent_commit.tree,
                parent_commit.oid,
                path,
                operation,
            )
            .await?
            else {
                break;
            };
            if parent_blob.metadata.oid == current_oid {
                let mut attributed = false;
                for final_line in tracked.iter().flatten() {
                    origins[*final_line] = parent_commit.oid;
                    attributed = true;
                }
                if attributed {
                    commits.insert(parent_commit.oid, parent_commit.clone());
                }
                current_commit = parent_commit;
                continue;
            }
            let parent_bytes = parent_blob.bytes;
            let parent_line_count = line_count(&parent_bytes, operation.cancellation())?;
            let parent_lines =
                line_ranges(&parent_bytes, parent_line_count, operation.cancellation())?;
            let (equal_prefix, equal_suffix) = matching_ends(
                &parent_bytes,
                &parent_lines,
                &current_bytes,
                &current_lines,
                operation.cancellation(),
            )?;
            let comparison_cells = comparison_cells(
                parent_line_count - equal_prefix - equal_suffix,
                current_lines.len() - equal_prefix - equal_suffix,
            )?;
            operation
                .charge(BudgetDimension::BlameComparisons, comparison_cells)
                .await?;
            let matches = lcs_matches(
                operation.runtime(),
                parent_bytes.clone(),
                parent_lines.clone(),
                current_bytes.clone(),
                current_lines,
                equal_prefix,
                equal_suffix,
                operation.cancellation().clone(),
            )
            .await?;
            let mut parent_tracked = Vec::new();
            parent_tracked
                .try_reserve_exact(parent_lines.len())
                .map_err(|source| Error::Allocation {
                    requested: parent_lines
                        .len()
                        .saturating_mul(mem::size_of::<Option<usize>>()),
                    source,
                })?;
            parent_tracked.resize(parent_lines.len(), None);
            let mut attributed = false;
            for (parent_line, current_line) in matches {
                if let Some(final_line) = tracked[current_line] {
                    origins[final_line] = parent_commit.oid;
                    parent_tracked[parent_line] = Some(final_line);
                    attributed = true;
                }
            }
            if attributed {
                commits.insert(parent_commit.oid, parent_commit.clone());
            }
            if parent_tracked.iter().all(Option::is_none) {
                break;
            }
            current_commit = parent_commit;
            current_oid = parent_blob.metadata.oid;
            current_bytes = parent_bytes;
            current_lines = parent_lines;
            tracked = parent_tracked;
        }

        let ranges = blame_ranges(path, &origins, &commits, operation.cancellation())?;
        let response_bytes = ranges
            .iter()
            .fold(path.as_bytes().len() as u64, |total, range| {
                total
                    .saturating_add(commit_response_bytes(&range.commit))
                    .saturating_add(range.source_path.as_bytes().len() as u64)
                    .saturating_add(mem::size_of::<BlameRange>() as u64)
            });
        operation
            .charge(BudgetDimension::ResponseBytes, response_bytes)
            .await?;
        let blame = Arc::new(Blame {
            commit: self.commit_oid,
            path: path.clone(),
            ranges,
        });
        let depth = u64::try_from(path.components().count()).map_err(|_| Error::LimitExceeded {
            limit: "traversal depth",
            actual: u64::MAX,
            maximum: u64::MAX,
        })?;
        let usage = operation
            .budget_usage()
            .await
            .semantic_delta(budget_before, depth);
        operation
            .insert_blame(self.commit_oid, path.clone(), Arc::clone(&blame), usage)
            .await;
        Ok(blame.as_ref().clone())
    }

    /// Traverse the snapshot into bounded archive entries without a checkout.
    ///
    /// Tree and submodule entries carry metadata only. Blob and symlink entries
    /// contain verified Git-representation bytes. Results use canonical raw
    /// path order and cancellation is checked throughout traversal.
    pub async fn archive(&self, operation: &OperationContext) -> Result<Vec<ArchiveEntry>> {
        self.ensure_operation(operation)?;
        let mut pending = vec![(self.root_tree_oid, GitPath::root(), 0u64)];
        let mut entries = Vec::new();
        while let Some((tree_oid, parent, depth)) = pending.pop() {
            operation.ensure_active()?;
            let tree = operation.read_tree(tree_oid, &parent).await?;
            for entry in tree.into_iter().rev() {
                operation.ensure_active()?;
                let entry_depth = next_depth(depth)?;
                operation
                    .charge(BudgetDimension::Depth, entry_depth)
                    .await?;
                operation.charge(BudgetDimension::ArchiveEntries, 1).await?;
                let bytes = if matches!(entry.kind, EntryKind::Blob | EntryKind::Symlink) {
                    let blob = parse_blob(operation.read_object(entry.oid).await?, entry.mode)?;
                    operation
                        .charge(BudgetDimension::ArchiveBytes, blob.bytes.len() as u64)
                        .await?;
                    Some(blob.bytes)
                } else {
                    None
                };
                entries.try_reserve(1).map_err(|source| Error::Allocation {
                    requested: mem::size_of::<ArchiveEntry>(),
                    source,
                })?;
                if entry.kind == EntryKind::Tree {
                    pending.push((entry.oid, entry.path.clone(), entry_depth));
                }
                entries.push(ArchiveEntry {
                    path: entry.path,
                    oid: entry.oid,
                    mode: entry.mode,
                    kind: entry.kind,
                    bytes,
                });
            }
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let response_bytes = entries.iter().fold(0u64, |total, entry| {
            total
                .saturating_add(entry.path.as_bytes().len() as u64)
                .saturating_add(entry.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64))
        });
        operation
            .charge(BudgetDimension::ResponseBytes, response_bytes)
            .await?;
        Ok(entries)
    }

    /// Stream bounded archive entries while owning and finalizing the operation.
    ///
    /// The stream reads no descendants until polled. Dropping it triggers the
    /// operation's tracked cleanup fallback; normal completion reports locator
    /// close failures as the terminal stream error.
    pub fn archive_stream(&self, operation: OperationContext) -> Result<ArchiveStream> {
        self.ensure_operation(&operation)?;
        let state = ArchiveStreamState {
            operation: Some(operation),
            pending: vec![ArchiveWork::Tree {
                oid: self.root_tree_oid,
                parent: GitPath::root(),
                depth: 0,
            }],
        };
        Ok(Box::pin(futures_util::stream::try_unfold(
            state,
            |mut state| async move {
                match state.next_entry().await {
                    Ok(Some(entry)) => Ok(Some((entry, state))),
                    Ok(None) => {
                        let operation = state.operation.take().ok_or(Error::InternalInvariant {
                            invariant: "archive stream completed without an operation",
                        })?;
                        operation.finish(Ok(())).await?;
                        Ok(None)
                    }
                    Err(error) => {
                        let operation = state.operation.take().ok_or(Error::InternalInvariant {
                            invariant: "archive stream failed without an operation",
                        })?;
                        match operation.finish::<()>(Err(error)).await {
                            Err(error) => Err(error),
                            Ok(()) => Err(Error::InternalInvariant {
                                invariant: "failed archive stream finalized successfully",
                            }),
                        }
                    }
                }
            },
        )))
    }

    /// Resolve one exact byte path without following symlinks.
    ///
    /// Traversal reads at most one dependent tree per path component. The root
    /// is returned as a synthetic tree entry. Missing final entries return
    /// `None`; a non-tree intermediate component returns a structured error.
    pub async fn entry(
        &self,
        path: &GitPath,
        operation: &OperationContext,
    ) -> Result<Option<TreeEntry>> {
        self.ensure_operation(operation)?;
        if path.is_root() {
            return Ok(Some(TreeEntry {
                path: GitPath::root(),
                oid: self.root_tree_oid,
                mode: EntryMode::Tree,
                kind: EntryKind::Tree,
                size: None,
            }));
        }

        let mut tree_oid = self.root_tree_oid;
        let mut parent = GitPath::root();
        let mut components = path.components().peekable();
        let mut depth = 0u64;
        while let Some(component) = components.next() {
            operation.ensure_active()?;
            depth = depth.checked_add(1).ok_or(Error::LimitExceeded {
                limit: "traversal depth",
                actual: u64::MAX,
                maximum: u64::MAX,
            })?;
            operation.charge(BudgetDimension::Depth, depth).await?;
            let entries = operation.read_tree(tree_oid, &parent).await?;
            operation
                .charge(BudgetDimension::Entries, entries.len() as u64)
                .await?;
            let found = entries
                .into_iter()
                .find(|entry| entry.path.file_name() == Some(component));
            let Some(entry) = found else {
                return Ok(None);
            };
            if components.peek().is_none() {
                return Ok(Some(entry));
            }
            if entry.kind != EntryKind::Tree {
                return Err(Error::PathComponentNotTree { actual: entry.kind });
            }
            tree_oid = entry.oid;
            parent = entry.path;
        }
        Err(Error::InternalInvariant {
            invariant: "non-root path traversal had no components",
        })
    }

    /// List immediate children of one directory in canonical Git order.
    ///
    /// This method never recursively reads descendants and leaves child blob
    /// sizes absent. Pagination is based on exact raw names from the pinned
    /// tree; the HTTP boundary is responsible for signing repository-bound
    /// cursors before exposing them to clients.
    pub async fn list_directory(
        &self,
        path: &GitPath,
        page: &PageRequest,
        operation: &OperationContext,
    ) -> Result<Page<TreeEntry>> {
        self.list_directory_with_metadata(path, page, DirectoryMetadata::None, operation)
            .await
    }

    /// List immediate children with an explicit bounded metadata policy.
    ///
    /// `BlobSizes` reads only blob-backed entries contained in the returned
    /// page and performs one batched exact-locator lookup for those objects.
    pub async fn list_directory_with_metadata(
        &self,
        path: &GitPath,
        page: &PageRequest,
        metadata: DirectoryMetadata,
        operation: &OperationContext,
    ) -> Result<Page<TreeEntry>> {
        self.ensure_operation(operation)?;
        let directory = self
            .entry(path, operation)
            .await?
            .ok_or(Error::PathNotFound)?;
        if directory.kind != EntryKind::Tree {
            return Err(Error::PathComponentNotTree {
                actual: directory.kind,
            });
        }
        let entries = operation.read_tree(directory.oid, path).await?;
        let mut after = match page.after() {
            Some(cursor) => {
                let decoded = cursor.decode_directory()?;
                let expected_limit =
                    u64::try_from(page.limit()).map_err(|_| Error::InvalidCursor {
                        reason: crate::CursorError::ContextMismatch,
                    })?;
                if decoded.commit != self.commit_oid
                    || decoded.tree != directory.oid
                    || decoded.limit != expected_limit
                    || decoded.path != path.as_bytes()
                {
                    return Err(Error::InvalidCursor {
                        reason: crate::CursorError::ContextMismatch,
                    });
                }
                Some(decoded.last_name)
            }
            None => None,
        };
        operation
            .charge(BudgetDimension::Entries, page.limit() as u64)
            .await?;
        let mut items = Vec::new();
        items
            .try_reserve_exact(page.limit())
            .map_err(|source| Error::Allocation {
                requested: page
                    .limit()
                    .saturating_mul(std::mem::size_of::<TreeEntry>()),
                source,
            })?;
        let mut has_more = false;
        for (position, entry) in entries.into_iter().enumerate() {
            if position % 256 == 0 {
                operation.ensure_active()?;
            }
            // The cursor pins this exact tree. Resume after its named entry:
            // Git sorts directories with a trailing slash, so plain name
            // comparisons can skip directories or repeat neighboring files.
            if let Some(name) = after {
                if entry.path.file_name() == Some(name) {
                    after = None;
                }
                continue;
            }
            if items.len() == page.limit() {
                has_more = true;
                break;
            }
            items.push(entry);
        }
        if after.is_some() {
            return Err(Error::InvalidCursor {
                reason: crate::CursorError::ContextMismatch,
            });
        }
        if metadata == DirectoryMetadata::BlobSizes {
            let positions_and_oids = items
                .iter()
                .enumerate()
                .filter(|(_, entry)| matches!(entry.kind, EntryKind::Blob | EntryKind::Symlink))
                .map(|(position, entry)| (position, entry.oid))
                .collect::<Vec<_>>();
            let oids = positions_and_oids
                .iter()
                .map(|(_, oid)| *oid)
                .collect::<Vec<_>>();
            let objects = operation.read_objects(&oids).await?;
            for ((position, _), object) in positions_and_oids.into_iter().zip(objects) {
                let mode = items[position].mode;
                let blob = parse_blob(object, mode)?;
                items[position].size = Some(blob.metadata.git_size);
            }
        }
        let next = if has_more {
            items
                .last()
                .and_then(|entry| entry.path.file_name())
                .map(|name| {
                    PageCursor::directory(self.commit_oid, directory.oid, path, page.limit(), name)
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page { items, next })
    }

    /// Read and verify blob-backed content at one exact path.
    ///
    /// Ordinary and symlink entries return their Git representation bytes.
    /// Trees and submodules return [`Error::EntryNotBlob`]. Object and aggregate
    /// byte limits plus cancellation are enforced by the shared operation.
    pub async fn read_blob(&self, path: &GitPath, operation: &OperationContext) -> Result<Blob> {
        self.ensure_operation(operation)?;
        let entry = self
            .entry(path, operation)
            .await?
            .ok_or(Error::PathNotFound)?;
        if !matches!(entry.kind, EntryKind::Blob | EntryKind::Symlink) {
            return Err(Error::EntryNotBlob { actual: entry.kind });
        }
        let blob = parse_blob(operation.read_object(entry.oid).await?, entry.mode)?;
        operation
            .charge(BudgetDimension::ResponseBytes, blob.bytes.len() as u64)
            .await?;
        Ok(blob)
    }

    /// Read metadata for blob-backed content without materializing pointer targets.
    pub async fn blob_metadata(
        &self,
        path: &GitPath,
        operation: &OperationContext,
    ) -> Result<BlobMetadata> {
        self.ensure_operation(operation)?;
        let entry = self
            .entry(path, operation)
            .await?
            .ok_or(Error::PathNotFound)?;
        if !matches!(entry.kind, EntryKind::Blob | EntryKind::Symlink) {
            return Err(Error::EntryNotBlob { actual: entry.kind });
        }
        let metadata = operation.read_object_metadata(entry.oid).await?;
        if metadata.kind != gix_object::Kind::Blob {
            return Err(Error::ObjectKind {
                oid: metadata.oid,
                expected: gix_object::Kind::Blob,
                actual: metadata.kind,
            });
        }
        if metadata.size <= crab_git::MAX_LFS_POINTER_SIZE as u64 {
            return parse_blob(
                operation.read_small_metadata_object(entry.oid).await?,
                entry.mode,
            )
            .map(|blob| blob.metadata);
        }
        Ok(BlobMetadata {
            oid: metadata.oid,
            git_size: metadata.size,
            logical_size: Some(metadata.size),
            mode: entry.mode,
            kind: entry.kind,
            classification: crate::ContentClassification::OrdinaryGit,
        })
    }

    /// Read a symbolic link's exact target bytes without following it.
    pub async fn symlink(&self, path: &GitPath, operation: &OperationContext) -> Result<Symlink> {
        self.ensure_operation(operation)?;
        let entry = self
            .entry(path, operation)
            .await?
            .ok_or(Error::PathNotFound)?;
        if entry.kind != EntryKind::Symlink {
            return Err(Error::EntryNotSymlink { actual: entry.kind });
        }
        let blob = parse_blob(operation.read_object(entry.oid).await?, entry.mode)?;
        operation
            .charge(BudgetDimension::ResponseBytes, blob.bytes.len() as u64)
            .await?;
        Ok(Symlink {
            entry,
            target: blob.bytes,
        })
    }

    /// Return a submodule's recorded gitlink commit without network access.
    pub async fn submodule(
        &self,
        path: &GitPath,
        operation: &OperationContext,
    ) -> Result<Submodule> {
        self.ensure_operation(operation)?;
        let entry = self
            .entry(path, operation)
            .await?
            .ok_or(Error::PathNotFound)?;
        if entry.kind != EntryKind::Submodule {
            return Err(Error::EntryNotSubmodule { actual: entry.kind });
        }
        Ok(Submodule {
            commit: entry.oid,
            entry,
        })
    }

    fn ensure_operation(&self, operation: &OperationContext) -> Result<()> {
        if operation.belongs_to(&self.repository) {
            Ok(())
        } else {
            Err(Error::InternalInvariant {
                invariant: "operation belongs to another repository generation",
            })
        }
    }
}

enum ArchiveWork {
    Tree {
        oid: ObjectId,
        parent: GitPath,
        depth: u64,
    },
    Entry {
        entry: TreeEntry,
        depth: u64,
    },
}

struct ArchiveStreamState {
    operation: Option<OperationContext>,
    pending: Vec<ArchiveWork>,
}

impl ArchiveStreamState {
    async fn next_entry(&mut self) -> Result<Option<ArchiveEntry>> {
        loop {
            let Some(work) = self.pending.pop() else {
                return Ok(None);
            };
            let operation = self.operation.as_ref().ok_or(Error::InternalInvariant {
                invariant: "archive stream has no operation",
            })?;
            operation.ensure_active()?;
            match work {
                ArchiveWork::Tree { oid, parent, depth } => {
                    let tree = operation.read_tree(oid, &parent).await?;
                    let entry_depth = next_depth(depth)?;
                    // One pending level comes from one verified tree object,
                    // so queue memory is bounded by the per-object byte limit.
                    self.pending
                        .try_reserve(tree.len())
                        .map_err(|source| Error::Allocation {
                            requested: tree.len().saturating_mul(mem::size_of::<ArchiveWork>()),
                            source,
                        })?;
                    self.pending
                        .extend(tree.into_iter().rev().map(|entry| ArchiveWork::Entry {
                            entry,
                            depth: entry_depth,
                        }));
                }
                ArchiveWork::Entry { entry, depth } => {
                    operation.charge(BudgetDimension::Depth, depth).await?;
                    operation.charge(BudgetDimension::ArchiveEntries, 1).await?;
                    let bytes = if matches!(entry.kind, EntryKind::Blob | EntryKind::Symlink) {
                        let blob = parse_blob(operation.read_object(entry.oid).await?, entry.mode)?;
                        operation
                            .charge(BudgetDimension::ArchiveBytes, blob.bytes.len() as u64)
                            .await?;
                        Some(blob.bytes)
                    } else {
                        None
                    };
                    if entry.kind == EntryKind::Tree {
                        self.pending.push(ArchiveWork::Tree {
                            oid: entry.oid,
                            parent: entry.path.clone(),
                            depth,
                        });
                    }
                    let response_bytes = (entry.path.as_bytes().len() as u64)
                        .saturating_add(bytes.as_ref().map_or(0, |bytes| bytes.len() as u64));
                    operation
                        .charge(BudgetDimension::ResponseBytes, response_bytes)
                        .await?;
                    return Ok(Some(ArchiveEntry {
                        path: entry.path,
                        oid: entry.oid,
                        mode: entry.mode,
                        kind: entry.kind,
                        bytes,
                    }));
                }
            }
        }
    }
}

async fn read_optional_blob(
    snapshot: &RemoteGitSnapshot,
    path: &GitPath,
    operation: &OperationContext,
) -> Result<Option<Blob>> {
    let Some(entry) = snapshot.entry(path, operation).await? else {
        return Ok(None);
    };
    if !matches!(entry.kind, EntryKind::Blob | EntryKind::Symlink) {
        return Err(Error::EntryNotBlob { actual: entry.kind });
    }
    parse_blob(operation.read_object(entry.oid).await?, entry.mode).map(Some)
}

async fn read_blame_blob(
    snapshot: &RemoteGitSnapshot,
    path: &GitPath,
    operation: &OperationContext,
) -> Result<Blob> {
    let entry = snapshot
        .entry(path, operation)
        .await?
        .ok_or(Error::PathNotFound)?;
    blame_blob_for_entry(entry, operation).await
}

async fn read_optional_blame_blob(
    source: &RemoteGitSnapshot,
    tree: ObjectId,
    commit: ObjectId,
    path: &GitPath,
    operation: &OperationContext,
) -> Result<Option<Blob>> {
    let Some(entry) = entry_at_tree(source, tree, commit, path, operation).await? else {
        return Ok(None);
    };
    if entry.kind != EntryKind::Blob {
        return Ok(None);
    }
    let blob = parse_blob(operation.read_object(entry.oid).await?, entry.mode)?;
    Ok(blame_unsupported_reason(&blob).is_none().then_some(blob))
}

async fn blame_blob_for_entry(entry: TreeEntry, operation: &OperationContext) -> Result<Blob> {
    if entry.kind != EntryKind::Blob {
        return Err(Error::BlameUnsupported {
            reason: BlameUnsupportedReason::EntryKind,
        });
    }
    let blob = parse_blob(operation.read_object(entry.oid).await?, entry.mode)?;
    if let Some(reason) = blame_unsupported_reason(&blob) {
        return Err(Error::BlameUnsupported { reason });
    }
    Ok(blob)
}

fn blame_unsupported_reason(blob: &Blob) -> Option<BlameUnsupportedReason> {
    match blob.metadata.classification {
        ContentClassification::CrabPointer => return Some(BlameUnsupportedReason::CrabPointer),
        ContentClassification::LfsPointer => return Some(BlameUnsupportedReason::LfsPointer),
        ContentClassification::OrdinaryGit => {}
    }
    if blob.bytes.contains(&0) {
        return Some(BlameUnsupportedReason::Binary);
    }
    std::str::from_utf8(&blob.bytes)
        .is_err()
        .then_some(BlameUnsupportedReason::UnsupportedEncoding)
}

fn line_ranges(
    bytes: &[u8],
    expected: usize,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<Vec<Range<usize>>> {
    check_cpu_cancellation(cancellation)?;
    let mut start = 0;
    let mut ranges = Vec::new();
    ranges
        .try_reserve_exact(expected)
        .map_err(|source| Error::Allocation {
            requested: expected.saturating_mul(mem::size_of::<Range<usize>>()),
            source,
        })?;
    for (position, byte) in bytes.iter().enumerate() {
        if position % CPU_CANCELLATION_INTERVAL == 0 && cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if *byte == b'\n' {
            ranges.push(start..position + 1);
            start = position + 1;
        }
    }
    if start < bytes.len() {
        ranges.push(start..bytes.len());
    }
    if ranges.len() != expected {
        return Err(Error::InternalInvariant {
            invariant: "line count changed while building ranges",
        });
    }
    Ok(ranges)
}

fn comparison_cells(parent_lines: usize, current_lines: usize) -> Result<u64> {
    let rows = u64::try_from(parent_lines)
        .ok()
        .and_then(|value| value.checked_add(1));
    let columns = u64::try_from(current_lines)
        .ok()
        .and_then(|value| value.checked_add(1));
    rows.zip(columns)
        .and_then(|(rows, columns)| rows.checked_mul(columns))
        .ok_or(Error::LimitExceeded {
            limit: "blame comparison cells",
            actual: u64::MAX,
            maximum: u64::MAX,
        })
}

fn matching_ends(
    parent: &[u8],
    parent_lines: &[Range<usize>],
    current: &[u8],
    current_lines: &[Range<usize>],
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<(usize, usize)> {
    let shared = parent_lines.len().min(current_lines.len());
    let mut prefix = 0;
    while prefix < shared
        && parent[parent_lines[prefix].clone()] == current[current_lines[prefix].clone()]
    {
        if prefix % CPU_CANCELLATION_INTERVAL == 0 {
            check_cpu_cancellation(cancellation)?;
        }
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < shared - prefix
        && parent[parent_lines[parent_lines.len() - suffix - 1].clone()]
            == current[current_lines[current_lines.len() - suffix - 1].clone()]
    {
        if suffix % CPU_CANCELLATION_INTERVAL == 0 {
            check_cpu_cancellation(cancellation)?;
        }
        suffix += 1;
    }
    Ok((prefix, suffix))
}

async fn lcs_matches(
    runtime: &crate::RemoteGitRuntime,
    parent: bytes::Bytes,
    parent_lines: Vec<Range<usize>>,
    current: bytes::Bytes,
    current_lines: Vec<Range<usize>>,
    equal_prefix: usize,
    equal_suffix: usize,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<Vec<(usize, usize)>> {
    runtime
        .spawn_blocking(move || {
            check_cpu_cancellation(&cancellation)?;
            let parent_middle_end = parent_lines.len() - equal_suffix;
            let current_middle_end = current_lines.len() - equal_suffix;
            let rows = parent_middle_end
                .saturating_sub(equal_prefix)
                .checked_add(1)
                .ok_or(Error::LimitExceeded {
                    limit: "blame comparison cells",
                    actual: u64::MAX,
                    maximum: u64::MAX,
                })?;
            let columns = current_middle_end
                .saturating_sub(equal_prefix)
                .checked_add(1)
                .ok_or(Error::LimitExceeded {
                    limit: "blame comparison cells",
                    actual: u64::MAX,
                    maximum: u64::MAX,
                })?;
            let cells = rows.checked_mul(columns).ok_or(Error::LimitExceeded {
                limit: "blame comparison cells",
                actual: u64::MAX,
                maximum: u64::MAX,
            })?;
            let requested =
                cells
                    .checked_mul(mem::size_of::<u32>())
                    .ok_or(Error::LimitExceeded {
                        limit: "blame comparison cells",
                        actual: u64::MAX,
                        maximum: u64::MAX,
                    })?;
            let mut matrix = Vec::new();
            matrix
                .try_reserve_exact(cells)
                .map_err(|source| Error::Allocation { requested, source })?;
            matrix.resize(cells, 0u32);
            for row in 1..rows {
                check_cpu_cancellation(&cancellation)?;
                for column in 1..columns {
                    let parent_line = &parent[parent_lines[equal_prefix + row - 1].clone()];
                    let current_line = &current[current_lines[equal_prefix + column - 1].clone()];
                    let index = row * columns + column;
                    matrix[index] = if parent_line == current_line {
                        matrix[(row - 1) * columns + column - 1].saturating_add(1)
                    } else {
                        matrix[(row - 1) * columns + column].max(matrix[row * columns + column - 1])
                    };
                }
            }
            let capacity = matrix[rows * columns - 1] as usize;
            let mut matches = Vec::new();
            matches
                .try_reserve_exact(
                    equal_prefix
                        .saturating_add(capacity)
                        .saturating_add(equal_suffix),
                )
                .map_err(|source| Error::Allocation {
                    requested: equal_prefix
                        .saturating_add(capacity)
                        .saturating_add(equal_suffix)
                        .saturating_mul(mem::size_of::<(usize, usize)>()),
                    source,
                })?;
            matches.extend((0..equal_prefix).map(|line| (line, line)));
            let middle_start = matches.len();
            let (mut row, mut column) = (rows - 1, columns - 1);
            let mut backtrace_steps = 0usize;
            while row > 0 && column > 0 {
                if backtrace_steps.is_multiple_of(CPU_CANCELLATION_INTERVAL) {
                    check_cpu_cancellation(&cancellation)?;
                }
                if parent[parent_lines[equal_prefix + row - 1].clone()]
                    == current[current_lines[equal_prefix + column - 1].clone()]
                {
                    matches.push((equal_prefix + row - 1, equal_prefix + column - 1));
                    row -= 1;
                    column -= 1;
                } else if matrix[(row - 1) * columns + column] >= matrix[row * columns + column - 1]
                {
                    row -= 1;
                } else {
                    column -= 1;
                }
                backtrace_steps = backtrace_steps.saturating_add(1);
            }
            matches[middle_start..].reverse();
            matches.extend((0..equal_suffix).map(|line| {
                (
                    parent_lines.len() - equal_suffix + line,
                    current_lines.len() - equal_suffix + line,
                )
            }));
            Ok(matches)
        })
        .await
        .map_err(|source| Error::DecodeTask { source })?
}

fn blame_ranges(
    path: &GitPath,
    origins: &[ObjectId],
    commits: &HashMap<ObjectId, Commit>,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<Vec<BlameRange>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < origins.len() {
        if start % CPU_CANCELLATION_INTERVAL == 0 {
            check_cpu_cancellation(cancellation)?;
        }
        let oid = origins[start];
        let mut end = start + 1;
        while origins.get(end) == Some(&oid) {
            end += 1;
        }
        let commit = commits.get(&oid).ok_or(Error::InternalInvariant {
            invariant: "blame origin commit metadata is absent",
        })?;
        ranges.try_reserve(1).map_err(|source| Error::Allocation {
            requested: mem::size_of::<BlameRange>(),
            source,
        })?;
        ranges.push(BlameRange {
            start: start as u64 + 1,
            lines: (end - start) as u64,
            commit: commit.clone(),
            source_path: path.clone(),
        });
        start = end;
    }
    Ok(ranges)
}

async fn path_change(
    snapshot: &RemoteGitSnapshot,
    commit: &Commit,
    path: &GitPath,
    traversal: HistoryTraversal,
    operation: &OperationContext,
) -> Result<Option<ChangeKind>> {
    let current = entry_at_tree(snapshot, commit.tree, commit.oid, path, operation).await?;
    let parent_oids: Vec<ObjectId> = match traversal {
        HistoryTraversal::FirstParent => commit.parents.first().copied().into_iter().collect(),
        HistoryTraversal::AllParents => commit.parents.clone(),
    };
    let mut parents = Vec::new();
    for oid in parent_oids {
        let parent = operation.read_commit(oid).await?;
        parents.push(entry_at_tree(snapshot, parent.tree, parent.oid, path, operation).await?);
    }
    if parents.is_empty() {
        return Ok(current.map(|_| ChangeKind::Added));
    }
    match current {
        None => Ok(parents
            .iter()
            .any(Option::is_some)
            .then_some(ChangeKind::Deleted)),
        Some(current) => {
            if parents.iter().all(Option::is_none) {
                return Ok(Some(ChangeKind::Added));
            }
            if parents
                .iter()
                .flatten()
                .any(|parent| parent.kind != current.kind)
            {
                return Ok(Some(ChangeKind::TypeChanged));
            }
            if parents
                .iter()
                .flatten()
                .any(|parent| parent.oid != current.oid)
            {
                return Ok(Some(ChangeKind::Modified));
            }
            if parents
                .iter()
                .flatten()
                .any(|parent| parent.mode != current.mode)
            {
                return Ok(Some(ChangeKind::ModeChanged));
            }
            Ok(None)
        }
    }
}

fn queue_all_parents(
    pending: &mut Vec<ObjectId>,
    commit: &Commit,
    commit_graph: Option<&CommitGraphIndex>,
) -> Result<()> {
    pending
        .try_reserve(commit.parents.len())
        .map_err(|source| Error::Allocation {
            requested: commit
                .parents
                .len()
                .saturating_mul(mem::size_of::<ObjectId>()),
            source,
        })?;
    let Some(commit_graph) =
        commit_graph.filter(|index| index.parents_match(commit.oid, &commit.parents))
    else {
        pending.extend(commit.parents.iter().rev().copied());
        return Ok(());
    };
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(commit.parents.len())
        .map_err(|source| Error::Allocation {
            requested: commit
                .parents
                .len()
                .saturating_mul(mem::size_of::<ObjectId>()),
            source,
        })?;
    ordered.extend(commit.parents.iter().copied());
    ordered.sort_unstable_by(|left, right| {
        commit_graph
            .generation(right)
            .cmp(&commit_graph.generation(left))
            .then_with(|| left.cmp(right))
    });
    pending.extend(ordered.into_iter().rev());
    Ok(())
}

async fn entry_at_tree(
    source: &RemoteGitSnapshot,
    tree: ObjectId,
    commit: ObjectId,
    path: &GitPath,
    operation: &OperationContext,
) -> Result<Option<TreeEntry>> {
    let snapshot = RemoteGitSnapshot {
        generation: source.generation,
        pack_index_hash: source.pack_index_hash,
        commit_oid: commit,
        root_tree_oid: tree,
        repository: Arc::clone(&source.repository),
        commit_graph: source.commit_graph.clone(),
    };
    snapshot.entry(path, operation).await
}

const CPU_CANCELLATION_INTERVAL: usize = 4 * 1024;

fn classify_diff(
    old: Option<&Blob>,
    new: Option<&Blob>,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<DiffClassification> {
    for blob in [old, new].into_iter().flatten() {
        match blob.metadata.classification {
            ContentClassification::CrabPointer => return Ok(DiffClassification::CrabPointer),
            ContentClassification::LfsPointer => return Ok(DiffClassification::LfsPointer),
            ContentClassification::OrdinaryGit => {}
        }
        for chunk in blob.bytes.chunks(CPU_CANCELLATION_INTERVAL) {
            check_cpu_cancellation(cancellation)?;
            if chunk.contains(&0) {
                return Ok(DiffClassification::Binary);
            }
        }
        check_cpu_cancellation(cancellation)?;
        if std::str::from_utf8(&blob.bytes).is_err() {
            return Ok(DiffClassification::UnsupportedEncoding);
        }
    }
    Ok(DiffClassification::Text)
}

fn limit_as_diff(path: &GitPath, error: Error) -> Result<Diff> {
    match error {
        Error::LimitExceeded { .. } => Ok(Diff {
            path: path.clone(),
            classification: DiffClassification::TooLarge,
            hunks: Vec::new(),
        }),
        error => Err(error),
    }
}

fn line_count(bytes: &[u8], cancellation: &tokio_util::sync::CancellationToken) -> Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut lines = 0usize;
    for chunk in bytes.chunks(CPU_CANCELLATION_INTERVAL) {
        check_cpu_cancellation(cancellation)?;
        lines = lines
            .checked_add(chunk.iter().filter(|byte| **byte == b'\n').count())
            .ok_or(Error::LimitExceeded {
                limit: "line count",
                actual: u64::MAX,
                maximum: u64::MAX,
            })?;
    }
    lines
        .checked_add(usize::from(!bytes.ends_with(b"\n")))
        .ok_or(Error::LimitExceeded {
            limit: "line count",
            actual: u64::MAX,
            maximum: u64::MAX,
        })
}

fn append_diff_lines(
    output: &mut Vec<u8>,
    prefix: u8,
    bytes: &[u8],
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    let mut start = 0;
    for (position, byte) in bytes.iter().enumerate() {
        if position % CPU_CANCELLATION_INTERVAL == 0 {
            check_cpu_cancellation(cancellation)?;
        }
        if *byte == b'\n' {
            output.push(prefix);
            output.extend_from_slice(&bytes[start..=position]);
            start = position + 1;
        }
    }
    if start < bytes.len() {
        output.push(prefix);
        output.extend_from_slice(&bytes[start..]);
    }
    Ok(())
}

fn check_cpu_cancellation(cancellation: &tokio_util::sync::CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

fn next_depth(depth: u64) -> Result<u64> {
    depth.checked_add(1).ok_or(Error::LimitExceeded {
        limit: "traversal depth",
        actual: u64::MAX,
        maximum: u64::MAX,
    })
}

fn commit_response_bytes(commit: &Commit) -> u64 {
    let bytes = mem::size_of::<Commit>()
        .saturating_add(
            commit
                .parents
                .len()
                .saturating_mul(mem::size_of::<ObjectId>()),
        )
        .saturating_add(commit.author.name.len())
        .saturating_add(commit.author.email.len())
        .saturating_add(commit.committer.name.len())
        .saturating_add(commit.committer.email.len())
        .saturating_add(commit.encoding.as_ref().map_or(0, bytes::Bytes::len))
        .saturating_add(commit.message.len())
        .saturating_add(
            commit
                .signature_headers
                .iter()
                .map(|header| header.name.len().saturating_add(header.value.len()))
                .sum::<usize>(),
        );
    bytes as u64
}

fn compare_trees<'a>(
    base_oid: ObjectId,
    head_oid: ObjectId,
    parent: &'a GitPath,
    depth: u64,
    changes: &'a mut Vec<TreeChange>,
    operation: &'a OperationContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        operation.ensure_active()?;
        if base_oid == head_oid {
            return Ok(());
        }
        operation.charge(BudgetDimension::Depth, depth).await?;
        let base = operation.read_tree(base_oid, parent).await?;
        let head = operation.read_tree(head_oid, parent).await?;
        let child_depth = next_depth(depth)?;
        let mut base_position = 0;
        let mut head_position = 0;
        while base_position < base.len() || head_position < head.len() {
            operation.ensure_active()?;
            let old = base.get(base_position);
            let new = head.get(head_position);
            match (old, new) {
                (Some(old), Some(new)) => match old.path.cmp(&new.path) {
                    std::cmp::Ordering::Less => {
                        collect_subtree(
                            old.clone(),
                            ChangeKind::Deleted,
                            child_depth,
                            changes,
                            operation,
                        )
                        .await?;
                        base_position += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        collect_subtree(
                            new.clone(),
                            ChangeKind::Added,
                            child_depth,
                            changes,
                            operation,
                        )
                        .await?;
                        head_position += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        if old.kind != new.kind {
                            push_change(
                                changes,
                                TreeChange {
                                    path: old.path.clone(),
                                    kind: ChangeKind::TypeChanged,
                                    old: Some(old.clone()),
                                    new: Some(new.clone()),
                                },
                                operation,
                            )
                            .await?;
                        } else if old.kind == EntryKind::Tree {
                            compare_trees(
                                old.oid,
                                new.oid,
                                &old.path,
                                child_depth,
                                changes,
                                operation,
                            )
                            .await?;
                        } else if old.oid != new.oid {
                            push_change(
                                changes,
                                TreeChange {
                                    path: old.path.clone(),
                                    kind: ChangeKind::Modified,
                                    old: Some(old.clone()),
                                    new: Some(new.clone()),
                                },
                                operation,
                            )
                            .await?;
                        } else if old.mode != new.mode {
                            push_change(
                                changes,
                                TreeChange {
                                    path: old.path.clone(),
                                    kind: ChangeKind::ModeChanged,
                                    old: Some(old.clone()),
                                    new: Some(new.clone()),
                                },
                                operation,
                            )
                            .await?;
                        }
                        base_position += 1;
                        head_position += 1;
                    }
                },
                (Some(old), None) => {
                    collect_subtree(
                        old.clone(),
                        ChangeKind::Deleted,
                        child_depth,
                        changes,
                        operation,
                    )
                    .await?;
                    base_position += 1;
                }
                (None, Some(new)) => {
                    collect_subtree(
                        new.clone(),
                        ChangeKind::Added,
                        child_depth,
                        changes,
                        operation,
                    )
                    .await?;
                    head_position += 1;
                }
                (None, None) => break,
            }
        }
        Ok(())
    })
}

fn collect_subtree<'a>(
    entry: TreeEntry,
    kind: ChangeKind,
    depth: u64,
    changes: &'a mut Vec<TreeChange>,
    operation: &'a OperationContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        operation.charge(BudgetDimension::Depth, depth).await?;
        if entry.kind != EntryKind::Tree {
            let (old, new) = match kind {
                ChangeKind::Added => (None, Some(entry.clone())),
                ChangeKind::Deleted => (Some(entry.clone()), None),
                _ => {
                    return Err(Error::InternalInvariant {
                        invariant: "subtree collection requires add or delete",
                    });
                }
            };
            return push_change(
                changes,
                TreeChange {
                    path: entry.path,
                    kind,
                    old,
                    new,
                },
                operation,
            )
            .await;
        }
        for child in operation.read_tree(entry.oid, &entry.path).await? {
            collect_subtree(child, kind, next_depth(depth)?, changes, operation).await?;
        }
        Ok(())
    })
}

async fn push_change(
    changes: &mut Vec<TreeChange>,
    change: TreeChange,
    operation: &OperationContext,
) -> Result<()> {
    operation.charge(BudgetDimension::Entries, 1).await?;
    operation
        .charge(
            BudgetDimension::ResponseBytes,
            change.path.as_bytes().len() as u64 + mem::size_of::<TreeChange>() as u64,
        )
        .await?;
    changes.try_reserve(1).map_err(|source| Error::Allocation {
        requested: mem::size_of::<TreeChange>(),
        source,
    })?;
    changes.push(change);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_cpu_helpers_preserve_lines_and_honor_cancellation() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let bytes = b"first\nsecond";
        assert_eq!(line_count(bytes, &cancellation).expect("line count"), 2);
        let mut output = Vec::new();
        append_diff_lines(&mut output, b'+', bytes, &cancellation).expect("diff lines");
        assert_eq!(output, b"+first\n+second");

        cancellation.cancel();
        assert!(matches!(
            line_count(bytes, &cancellation),
            Err(Error::Cancelled)
        ));
        assert!(matches!(
            line_ranges(bytes, usize::MAX, &cancellation),
            Err(Error::Cancelled)
        ));
    }

    #[tokio::test]
    async fn blame_matrix_stops_when_cancelled() {
        let runtime = crate::RemoteGitRuntime::default();
        let cancellation = tokio_util::sync::CancellationToken::new();
        cancellation.cancel();
        let error = lcs_matches(
            &runtime,
            bytes::Bytes::from_static(b"parent\n"),
            std::iter::once(0..7).collect(),
            bytes::Bytes::from_static(b"current\n"),
            std::iter::once(0..8).collect(),
            0,
            0,
            cancellation,
        )
        .await
        .expect_err("cancelled blame matrix");
        assert!(matches!(error, Error::Cancelled));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn blame_matrix_trims_equal_prefix_and_suffix() {
        let runtime = crate::RemoteGitRuntime::default();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let parent = bytes::Bytes::from_static(b"same\nold\ntail\n");
        let current = bytes::Bytes::from_static(b"same\nnew\ntail\n");
        let parent_lines = vec![0..5, 5..9, 9..14];
        let current_lines = vec![0..5, 5..9, 9..14];
        let ends = matching_ends(
            &parent,
            &parent_lines,
            &current,
            &current_lines,
            &cancellation,
        )
        .expect("matching ends");
        assert_eq!(ends, (1, 1));
        assert_eq!(
            lcs_matches(
                &runtime,
                parent,
                parent_lines,
                current,
                current_lines,
                ends.0,
                ends.1,
                cancellation,
            )
            .await
            .expect("line matches"),
            vec![(0, 0), (2, 2)]
        );
        runtime.shutdown().await;
    }
}
