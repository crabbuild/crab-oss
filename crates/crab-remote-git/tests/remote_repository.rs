use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crab_git::{PackLocationIter, write_pack_reverse_index};
use crab_metadata::commit_graph::{CommitEntry, CommitGraphSummary};
use crab_metadata::git_object_locator::{
    GitLocatorCoverage, GitObjectLocation, GitObjectLocatorEntry, GitObjectLocatorWriter,
    GitPackLocatorRecord,
};
use crab_metadata::manifest_store::{
    create_manifest, read_manifest, upload_segmented_bulk, write_manifest_cas,
};
use crab_metadata::manifests::{
    BulkData, Manifest, PackManifestEntry, compact_pack_index, compact_shard_index,
};
use crab_remote_git::{
    BlameUnsupportedReason, ChangeKind, ContentClassification, CursorError, DiffClassification,
    DirectoryMetadata, EntryKind, EntryMode, Error, GeneratedPackLease, GeneratedPackLeaseAttempt,
    GeneratedPackLeaseError, GeneratedPackLeaseProvider, GitPath, HistoryTraversal, ObjectLimits,
    OperationKind, OperationLimits, PageCursor, PageRequest, RemoteGitRepository, RemoteGitRuntime,
    RepositoryOptions, Revision, RuntimeOptions,
};
use crab_storage::{Store, StoreLayout};
use crab_xet::hash::MerkleHash;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use gix_pack::data::entry::Header;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sha1::{Digest as _, Sha1};
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum DeltaKind {
    Ref,
    RefShared,
    Ofs,
}

struct PackFixture {
    _temp: tempfile::TempDir,
    pack: PathBuf,
    index: PathBuf,
    reverse: PathBuf,
    commit: gix_hash::ObjectId,
    root_commit: gix_hash::ObjectId,
    side_commit: gix_hash::ObjectId,
    semantic_base_commit: gix_hash::ObjectId,
    semantic_head_commit: gix_hash::ObjectId,
    nested_tag: gix_hash::ObjectId,
    blob_tag: gix_hash::ObjectId,
    unreachable: gix_hash::ObjectId,
    target: gix_hash::ObjectId,
    target_path: String,
    expected: Vec<u8>,
    base_path: String,
    base_offset: u64,
    base_expected: Vec<u8>,
    blob_paths: Vec<(String, Vec<u8>)>,
    shared_paths: Option<[(String, Vec<u8>); 2]>,
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<InMemory>,
    mutations: AtomicUsize,
    pack_gets: AtomicUsize,
    generated_pack_descriptor_gets: AtomicUsize,
    generated_pack_descriptor_puts: AtomicUsize,
    throttled_generated_pack_descriptor_gets: AtomicUsize,
    block_next_pack_get: AtomicBool,
    block_pack_offset: AtomicU64,
    pack_get_entered: Semaphore,
    release_pack_get: Notify,
    slow_pack_gets: AtomicBool,
    active_pack_gets: AtomicUsize,
    max_active_pack_gets: AtomicUsize,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            mutations: AtomicUsize::new(0),
            pack_gets: AtomicUsize::new(0),
            generated_pack_descriptor_gets: AtomicUsize::new(0),
            generated_pack_descriptor_puts: AtomicUsize::new(0),
            throttled_generated_pack_descriptor_gets: AtomicUsize::new(0),
            block_next_pack_get: AtomicBool::new(false),
            block_pack_offset: AtomicU64::new(u64::MAX),
            pack_get_entered: Semaphore::new(0),
            release_pack_get: Notify::new(),
            slow_pack_gets: AtomicBool::new(false),
            active_pack_gets: AtomicUsize::new(0),
            max_active_pack_gets: AtomicUsize::new(0),
        }
    }

    fn reset_pack_gets(&self) {
        self.pack_gets.store(0, Ordering::SeqCst);
    }

    fn reset_pack_activity(&self) {
        assert_eq!(self.active_pack_gets.load(Ordering::SeqCst), 0);
        self.max_active_pack_gets.store(0, Ordering::SeqCst);
    }

    fn pack_gets(&self) -> usize {
        self.pack_gets.load(Ordering::SeqCst)
    }

    fn generated_pack_descriptor_puts(&self) -> usize {
        self.generated_pack_descriptor_puts.load(Ordering::SeqCst)
    }

    fn reset_generated_pack_descriptor_gets(&self) {
        self.generated_pack_descriptor_gets
            .store(0, Ordering::SeqCst);
    }

    fn generated_pack_descriptor_gets(&self) -> usize {
        self.generated_pack_descriptor_gets.load(Ordering::SeqCst)
    }

    fn throttle_generated_pack_descriptor_gets(&self, attempts: usize) {
        self.throttled_generated_pack_descriptor_gets
            .store(attempts, Ordering::SeqCst);
    }

    fn block_next_pack_get(&self) {
        self.block_next_pack_get.store(true, Ordering::SeqCst);
    }

    fn block_pack_get_at(&self, pack_offset: u64) {
        self.block_pack_offset.store(pack_offset, Ordering::SeqCst);
    }

    async fn wait_for_blocked_pack_get(&self) {
        let permit = self
            .pack_get_entered
            .acquire()
            .await
            .expect("test semaphore remains open");
        permit.forget();
    }

    fn release_blocked_pack_get(&self) {
        self.release_pack_get.notify_one();
    }

    fn slow_pack_gets(&self) {
        self.slow_pack_gets.store(true, Ordering::SeqCst);
    }

    fn max_active_pack_gets(&self) -> usize {
        self.max_active_pack_gets.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("remote-git-counting-store")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        if location.as_ref().contains("/generated-packs/v1/requests/") {
            self.generated_pack_descriptor_puts
                .fetch_add(1, Ordering::SeqCst);
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if location.as_ref().contains("/generated-packs/v1/requests/") {
            self.generated_pack_descriptor_gets
                .fetch_add(1, Ordering::SeqCst);
            if self
                .throttled_generated_pack_descriptor_gets
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(object_store::Error::Generic {
                    store: "remote-git-counting-store",
                    source: Box::new(std::io::Error::other(
                        "injected generated-pack descriptor throttling",
                    )),
                });
            }
        }
        if !options.head && location.as_ref().ends_with(".pack") {
            self.pack_gets.fetch_add(1, Ordering::SeqCst);
            let active = self.active_pack_gets.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active_pack_gets
                .fetch_max(active, Ordering::SeqCst);
            let range_start = options.range.as_ref().and_then(|range| match range {
                GetRange::Bounded(range) => Some(range.start),
                GetRange::Offset(offset) => Some(*offset),
                GetRange::Suffix(_) => None,
            });
            let block_pack_offset = self.block_pack_offset.load(Ordering::SeqCst);
            let block_base = range_start.is_some_and(|offset| {
                offset == block_pack_offset
                    && self
                        .block_pack_offset
                        .compare_exchange(
                            block_pack_offset,
                            u64::MAX,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
            });
            if self.block_next_pack_get.swap(false, Ordering::SeqCst) || block_base {
                self.pack_get_entered.add_permits(1);
                self.release_pack_get.notified().await;
            }
            if self.slow_pack_gets.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let result = self.inner.get_opts(location, options).await;
            self.active_pack_gets.fetch_sub(1, Ordering::SeqCst);
            return result;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
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
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.copy_opts(from, to, options).await
    }
}

impl PackFixture {
    fn new(delta_kind: DeltaKind) -> Self {
        let temp = tempfile::tempdir().expect("temporary pack fixture");
        let git_dir = temp.path().join("repo.git");
        git(&["init", "--bare", path(&git_dir)], None);

        let mut blobs = Vec::new();
        for object_index in 0..32usize {
            let mut data = vec![b'a'; 64 * 1024];
            let start = object_index * 97;
            for (offset, byte) in data[start..start + 512].iter_mut().enumerate() {
                *byte = b'A' + ((object_index + offset) % 26) as u8;
            }
            data.extend_from_slice(format!("\nobject-{object_index:02}\n").as_bytes());
            let oid = hash_object(&git_dir, &data);
            blobs.push((format!("file-{object_index:02}.txt"), oid, data));
        }
        let empty_blob = hash_object(&git_dir, b"");
        let executable_blob = hash_object(&git_dir, b"#!/bin/sh\necho crab\n");
        let deep_blob = hash_object(&git_dir, b"deep content\n");
        let symlink_blob = hash_object(&git_dir, b"dir/nested/deep.txt");
        let non_utf8_blob = hash_object(&git_dir, b"raw name\n");
        let crab_pointer = b"version https://crab.dev/spec/v1\nfile-hash 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\nsize 4294967296\n";
        let crab_pointer_blob = hash_object(&git_dir, crab_pointer);
        let lfs_pointer = b"version https://git-lfs.github.com/spec/v1\noid sha256:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\nsize 8589934592\n";
        let lfs_pointer_blob = hash_object(&git_dir, lfs_pointer);
        let semantic_text_base = hash_object(&git_dir, b"before\n");
        let semantic_text_head = hash_object(&git_dir, b"after\n");
        let semantic_binary_base = hash_object(&git_dir, b"before\0binary\n");
        let semantic_binary_head = hash_object(&git_dir, b"after\0binary\n");
        let semantic_pointer_head = hash_object(
            &git_dir,
            b"version https://crab.dev/spec/v1\nfile-hash 101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f\nsize 4294967297\n",
        );

        let nested_tree = make_tree(&git_dir, &[(0o100644, "blob", deep_blob, b"deep.txt")]);
        let directory_tree = make_tree(&git_dir, &[(0o040000, "tree", nested_tree, b"nested")]);
        let submodule_tree = make_tree(&git_dir, &[(0o100644, "blob", empty_blob, b"README")]);
        let raw_name_tree = make_tree(
            &git_dir,
            &[
                (0o100644, "blob", non_utf8_blob, b"\xfe"),
                (0o100644, "blob", non_utf8_blob, b"\xff"),
                (0o100644, "blob", non_utf8_blob, b"\xffx"),
            ],
        );
        let ordered_tree = make_tree(
            &git_dir,
            &[
                (0o100644, "blob", deep_blob, b"item.ext"),
                (0o040000, "tree", nested_tree, b"item"),
                (0o100644, "blob", deep_blob, b"item0"),
                (0o100644, "blob", non_utf8_blob, b"\xfe.ext"),
                (0o040000, "tree", nested_tree, b"\xfe"),
                (0o100644, "blob", non_utf8_blob, b"\xfe0"),
            ],
        );
        let submodule_commit = parse_oid(&git(
            &[
                "--git-dir",
                path(&git_dir),
                "commit-tree",
                &submodule_tree.to_string(),
                "-m",
                "submodule fixture",
            ],
            None,
        ));

        let mut root_entries = blobs
            .iter()
            .map(|(name, oid, _)| (0o100644, "blob", *oid, name.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        for index in 0..1_024usize {
            root_entries.push((
                0o100644,
                "blob",
                empty_blob,
                format!("flat-{index:04}.txt").into_bytes(),
            ));
        }
        root_entries.extend([
            (0o100644, "blob", empty_blob, b"..".to_vec()),
            (0o100644, "blob", empty_blob, b".git".to_vec()),
            (0o100644, "blob", empty_blob, b"empty".to_vec()),
            (0o100755, "blob", executable_blob, b"run.sh".to_vec()),
            (0o120000, "blob", symlink_blob, b"link".to_vec()),
            (
                0o100644,
                "blob",
                crab_pointer_blob,
                b"crab.pointer".to_vec(),
            ),
            (0o100644, "blob", lfs_pointer_blob, b"lfs.pointer".to_vec()),
            (0o100644, "blob", non_utf8_blob, b"\xff.txt".to_vec()),
            (0o040000, "tree", directory_tree, b"dir".to_vec()),
            (0o040000, "tree", raw_name_tree, b"raw".to_vec()),
            (0o040000, "tree", ordered_tree, b"ordered".to_vec()),
            (0o160000, "commit", submodule_commit, b"module".to_vec()),
        ]);
        let tree = make_tree_owned(&git_dir, &root_entries);
        let semantic_type_tree = make_tree(
            &git_dir,
            &[(0o100644, "blob", semantic_text_head, b"child")],
        );
        let semantic_base_tree = make_tree(
            &git_dir,
            &[
                (0o100644, "blob", semantic_binary_base, b"binary.dat"),
                (0o100644, "blob", semantic_text_base, b"mode.txt"),
                (0o100644, "blob", semantic_text_base, b"old-name.txt"),
                (0o100644, "blob", crab_pointer_blob, b"pointer.crab"),
                (0o040000, "tree", nested_tree, b"same"),
                (0o100644, "blob", semantic_text_base, b"text.txt"),
                (0o100644, "blob", semantic_text_base, b"type"),
            ],
        );
        let semantic_head_tree = make_tree(
            &git_dir,
            &[
                (0o100644, "blob", semantic_binary_head, b"binary.dat"),
                (0o100755, "blob", semantic_text_base, b"mode.txt"),
                (0o100644, "blob", semantic_text_base, b"new-name.txt"),
                (0o100644, "blob", semantic_pointer_head, b"pointer.crab"),
                (0o040000, "tree", nested_tree, b"same"),
                (0o100644, "blob", semantic_text_head, b"text.txt"),
                (0o040000, "tree", semantic_type_tree, b"type"),
            ],
        );
        let semantic_base_commit = parse_oid(&git(
            &[
                "--git-dir",
                path(&git_dir),
                "commit-tree",
                &semantic_base_tree.to_string(),
                "-m",
                "semantic base fixture",
            ],
            None,
        ));
        let semantic_head_commit = parse_oid(&git(
            &[
                "--git-dir",
                path(&git_dir),
                "commit-tree",
                &semantic_head_tree.to_string(),
                "-p",
                &semantic_base_commit.to_string(),
                "-m",
                "semantic head fixture",
            ],
            None,
        ));
        let root_commit = parse_oid(&git(
            &[
                "--git-dir",
                path(&git_dir),
                "commit-tree",
                &tree.to_string(),
                "-m",
                "root fixture",
            ],
            None,
        ));
        let side_commit = parse_oid(&git(
            &[
                "--git-dir",
                path(&git_dir),
                "commit-tree",
                &tree.to_string(),
                "-p",
                &root_commit.to_string(),
                "-m",
                "side fixture",
            ],
            None,
        ));
        let signed_commit = format!(
            "tree {tree}\nparent {root_commit}\nparent {side_commit}\nauthor Crab Test <test@crab.invalid> 1700000001 +0000\ncommitter Crab Test <test@crab.invalid> 1700000002 +0000\nencoding UTF-8\ngpgsig -----BEGIN PGP SIGNATURE-----\n test-signature\n -----END PGP SIGNATURE-----\n\nsigned merge fixture\n"
        );
        let commit = hash_typed_object(&git_dir, "commit", signed_commit.as_bytes());
        let unreachable = parse_oid(&git(
            &[
                "--git-dir",
                path(&git_dir),
                "commit-tree",
                &tree.to_string(),
                "-m",
                "unreachable fixture",
            ],
            None,
        ));
        let tag_one = format!(
            "object {commit}\ntype commit\ntag v1\ntagger Crab Test <test@crab.invalid> 1700000003 +0000\n\nfirst tag\n"
        );
        let tag_one = hash_typed_object(&git_dir, "tag", tag_one.as_bytes());
        let tag_two = format!(
            "object {tag_one}\ntype tag\ntag v2\ntagger Crab Test <test@crab.invalid> 1700000004 +0000\n\nnested tag\n-----BEGIN PGP SIGNATURE-----\ntest-signature\n-----END PGP SIGNATURE-----\n"
        );
        let nested_tag = hash_typed_object(&git_dir, "tag", tag_two.as_bytes());
        let blob_tag_body = format!(
            "object {}\ntype blob\ntag blob\ntagger Crab Test <test@crab.invalid> 1700000005 +0000\n\nblob tag\n",
            blobs[0].1
        );
        let blob_tag = hash_typed_object(&git_dir, "tag", blob_tag_body.as_bytes());

        let object_list = blobs
            .iter()
            .map(|(_, oid, _)| oid.to_string())
            .chain([
                empty_blob.to_string(),
                executable_blob.to_string(),
                deep_blob.to_string(),
                symlink_blob.to_string(),
                non_utf8_blob.to_string(),
                crab_pointer_blob.to_string(),
                lfs_pointer_blob.to_string(),
                semantic_text_base.to_string(),
                semantic_text_head.to_string(),
                semantic_binary_base.to_string(),
                semantic_binary_head.to_string(),
                semantic_pointer_head.to_string(),
                nested_tree.to_string(),
                directory_tree.to_string(),
                submodule_tree.to_string(),
                raw_name_tree.to_string(),
                ordered_tree.to_string(),
                submodule_commit.to_string(),
                tree.to_string(),
                semantic_type_tree.to_string(),
                semantic_base_tree.to_string(),
                semantic_head_tree.to_string(),
                semantic_base_commit.to_string(),
                semantic_head_commit.to_string(),
                root_commit.to_string(),
                side_commit.to_string(),
                commit.to_string(),
                unreachable.to_string(),
                tag_one.to_string(),
                nested_tag.to_string(),
                blob_tag.to_string(),
            ])
            .collect::<Vec<_>>()
            .join("\n");
        let base = temp.path().join("fixture");
        let depth = if matches!(delta_kind, DeltaKind::RefShared) {
            "1"
        } else {
            "32"
        };
        let mut args = vec![
            "--git-dir",
            path(&git_dir),
            "pack-objects",
            "--index-version=2",
            "--window=32",
            "--depth",
            depth,
            "--no-reuse-delta",
            "--no-reuse-object",
        ];
        if matches!(delta_kind, DeltaKind::Ofs) {
            args.push("--delta-base-offset");
        }
        args.push(path(&base));
        let pack_hash = String::from_utf8(git(&args, Some(format!("{object_list}\n").as_bytes())))
            .expect("pack hash UTF-8")
            .trim()
            .to_owned();
        let pack = temp.path().join(format!("fixture-{pack_hash}.pack"));
        let index = temp.path().join(format!("fixture-{pack_hash}.idx"));
        let reverse = temp.path().join(format!("fixture-{pack_hash}.rev"));
        write_pack_reverse_index(&index, &reverse).expect("write reverse index");

        let pack_bytes = fs::read(&pack).expect("read pack");
        let locations = PackLocationIter::open(
            &index,
            &reverse,
            fs::metadata(&pack).expect("pack metadata").len(),
        )
        .expect("open pack locations");
        let target = locations
            .filter_map(|location| location.ok())
            .find_map(|location| {
                if !blobs.iter().any(|(_, oid, _)| *oid == location.oid) {
                    return None;
                }
                let start = location.pack_offset as usize;
                let end = start + location.entry_len as usize;
                let entry = gix_pack::data::Entry::from_bytes(
                    &pack_bytes[start..end],
                    location.pack_offset,
                    20,
                )
                .ok()?;
                let requested = match delta_kind {
                    DeltaKind::Ref | DeltaKind::RefShared => {
                        matches!(entry.header, Header::RefDelta { .. })
                    }
                    DeltaKind::Ofs => matches!(entry.header, Header::OfsDelta { .. }),
                };
                requested.then_some(location.oid)
            })
            .expect("fixture must contain the requested delta kind");
        let (base, base_offset) = PackLocationIter::open(
            &index,
            &reverse,
            fs::metadata(&pack).expect("pack metadata").len(),
        )
        .expect("open pack locations")
        .filter_map(|location| location.ok())
        .find_map(|location| {
            if !blobs.iter().any(|(_, oid, _)| *oid == location.oid) {
                return None;
            }
            let start = location.pack_offset as usize;
            let end = start + location.entry_len as usize;
            let entry = gix_pack::data::Entry::from_bytes(
                &pack_bytes[start..end],
                location.pack_offset,
                20,
            )
            .ok()?;
            matches!(entry.header, Header::Blob).then_some((location.oid, location.pack_offset))
        })
        .expect("fixture must contain a base blob");
        let (target_path, _, expected) = blobs
            .iter()
            .find(|(_, oid, _)| *oid == target)
            .cloned()
            .expect("target blob metadata");
        let (base_path, _, base_expected) = blobs
            .iter()
            .find(|(_, oid, _)| *oid == base)
            .cloned()
            .expect("base blob metadata");
        let blob_paths = blobs
            .iter()
            .map(|(path, _, data)| (path.clone(), data.clone()))
            .collect();
        let shared_paths = if matches!(delta_kind, DeltaKind::RefShared) {
            let mut by_base = std::collections::HashMap::new();
            let blob_oids = blobs
                .iter()
                .map(|(_, oid, _)| *oid)
                .collect::<std::collections::HashSet<_>>();
            for location in PackLocationIter::open(
                &index,
                &reverse,
                fs::metadata(&pack).expect("pack metadata").len(),
            )
            .expect("open pack locations")
            {
                let location = location.expect("pack location");
                let start = location.pack_offset as usize;
                let end = start + location.entry_len as usize;
                let entry = gix_pack::data::Entry::from_bytes(
                    &pack_bytes[start..end],
                    location.pack_offset,
                    20,
                )
                .expect("pack entry");
                if blob_oids.contains(&location.oid)
                    && let Header::RefDelta { base_id } = entry.header
                {
                    by_base
                        .entry(base_id)
                        .or_insert_with(Vec::new)
                        .push(location.oid);
                }
            }
            let targets = by_base
                .values()
                .find(|targets| targets.len() >= 2)
                .expect("depth-one fixture must contain a shared delta base");
            Some(std::array::from_fn(|index| {
                let oid = targets[index];
                let (path, _, data) = blobs
                    .iter()
                    .find(|(_, candidate, _)| *candidate == oid)
                    .expect("shared target blob");
                (path.clone(), data.clone())
            }))
        } else {
            None
        };
        Self {
            _temp: temp,
            pack,
            index,
            reverse,
            commit,
            root_commit,
            side_commit,
            semantic_base_commit,
            semantic_head_commit,
            nested_tag,
            blob_tag,
            unreachable,
            target,
            target_path,
            expected,
            base_path,
            base_offset,
            base_expected,
            blob_paths,
            shared_paths,
        }
    }
}

struct PublishedFixture {
    _source: tempfile::TempDir,
    source_git_dir: PathBuf,
    repository: RemoteGitRepository,
    runtime: Arc<RemoteGitRuntime>,
    backend: Arc<CountingStore>,
    store: Store,
    layout: StoreLayout<Store>,
    target: gix_hash::ObjectId,
    target_path: GitPath,
    expected: Vec<u8>,
    base_path: GitPath,
    base_offset: u64,
    base_expected: Vec<u8>,
    root_commit: gix_hash::ObjectId,
    side_commit: gix_hash::ObjectId,
    semantic_base_commit: gix_hash::ObjectId,
    semantic_head_commit: gix_hash::ObjectId,
    unreachable: gix_hash::ObjectId,
    blob_paths: Vec<(GitPath, Vec<u8>)>,
    shared_paths: Option<[(GitPath, Vec<u8>); 2]>,
}

async fn publish(
    delta_kind: DeltaKind,
    corrupt_target_crc: bool,
    options: RepositoryOptions,
) -> PublishedFixture {
    publish_with_runtime(
        delta_kind,
        corrupt_target_crc,
        options,
        RuntimeOptions::default(),
    )
    .await
}

async fn publish_with_runtime(
    delta_kind: DeltaKind,
    corrupt_target_crc: bool,
    options: RepositoryOptions,
    runtime_options: RuntimeOptions,
) -> PublishedFixture {
    publish_with_runtime_and_summary(
        delta_kind,
        corrupt_target_crc,
        options,
        runtime_options,
        false,
    )
    .await
}

async fn publish_with_summary(
    delta_kind: DeltaKind,
    options: RepositoryOptions,
) -> PublishedFixture {
    publish_with_runtime_and_summary(delta_kind, false, options, RuntimeOptions::default(), true)
        .await
}

async fn publish_with_runtime_and_summary(
    delta_kind: DeltaKind,
    corrupt_target_crc: bool,
    options: RepositoryOptions,
    runtime_options: RuntimeOptions,
    include_incomplete_commit_graph: bool,
) -> PublishedFixture {
    let fixture = PackFixture::new(delta_kind);
    let pack_bytes = Bytes::from(fs::read(&fixture.pack).expect("read fixture pack"));
    let index_bytes = Bytes::from(fs::read(&fixture.index).expect("read fixture index"));
    let reverse_index_bytes =
        Bytes::from(fs::read(&fixture.reverse).expect("read fixture reverse index"));
    let pack_id = MerkleHash::from_hex(blake3::hash(&pack_bytes).to_hex().as_str())
        .expect("raw BLAKE3 pack identity");
    let pack_size = pack_bytes.len() as u64;
    let backend = Arc::new(CountingStore::new());
    let inner: Arc<dyn ObjectStore> = backend.clone();
    let store = Store::new(Arc::clone(&inner));
    let layout = StoreLayout::new(store.clone(), "org/repo".to_owned());
    inner
        .put(&layout.pack_path(&pack_id), pack_bytes.into())
        .await
        .expect("upload pack");
    inner
        .put(&layout.pack_index_path(&pack_id), index_bytes.into())
        .await
        .expect("upload index");
    inner
        .put(
            &layout.pack_reverse_index_path(&pack_id),
            reverse_index_bytes.into(),
        )
        .await
        .expect("upload reverse index");

    let locations = PackLocationIter::open(&fixture.index, &fixture.reverse, pack_size)
        .expect("open fixture locations")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect fixture locations");
    let object_count = locations.len() as u64;
    let pack_manifest = PackManifestEntry {
        pack_id: pack_id.to_string(),
        size: pack_size,
        content_hash: pack_id.to_string(),
        ref_tips: vec![
            fixture.commit.to_string(),
            fixture.semantic_head_commit.to_string(),
        ],
        object_count,
    };
    let (pack_index_hash, _, pack_index) =
        compact_pack_index(1, &[pack_manifest]).expect("pack inventory");
    let (shard_index_hash, _, shard_index) =
        compact_shard_index(1, &[]).expect("empty shard inventory");
    upload_segmented_bulk(
        &store,
        &layout,
        &BulkData {
            shard_index,
            pack_index,
        },
    )
    .await
    .expect("upload inventories");

    let mut manifest = Manifest::default_for_repo("refs/heads/main");
    manifest.generation = 1;
    manifest.refs = BTreeMap::from([
        ("refs/heads/main".to_owned(), fixture.commit.to_string()),
        (
            "refs/heads/semantic-base".to_owned(),
            fixture.semantic_base_commit.to_string(),
        ),
        (
            "refs/heads/semantic-head".to_owned(),
            fixture.semantic_head_commit.to_string(),
        ),
        (
            "refs/heads/root".to_owned(),
            fixture.root_commit.to_string(),
        ),
        ("refs/tags/blob".to_owned(), fixture.blob_tag.to_string()),
        ("refs/tags/v2".to_owned(), fixture.nested_tag.to_string()),
    ]);
    manifest
        .peeled_refs
        .insert("refs/tags/v2".to_owned(), fixture.commit.to_string());
    manifest.pack_index_hash = pack_index_hash;
    manifest.shard_index_hash = shard_index_hash;
    if include_incomplete_commit_graph {
        let summary = CommitGraphSummary {
            generation: 1,
            commits: vec![
                CommitEntry {
                    oid: fixture.commit.to_string(),
                    gen_number: 10,
                    parents: vec![
                        fixture.root_commit.to_string(),
                        fixture.side_commit.to_string(),
                    ],
                },
                CommitEntry {
                    oid: fixture.side_commit.to_string(),
                    gen_number: 9,
                    parents: vec![fixture.root_commit.to_string()],
                },
            ],
        };
        let bytes = serde_json::to_vec(&summary).expect("serialize commit graph summary");
        let hash = blake3::hash(&bytes).to_hex().to_string();
        inner
            .put(
                &layout.bulk_manifest_path("commit-graph", &hash),
                Bytes::from(bytes).into(),
            )
            .await
            .expect("upload commit graph summary");
        manifest.commit_graph_hash = Some(hash);
    }
    manifest.seal_git_validation();
    create_manifest(&store, &layout, &manifest)
        .await
        .expect("create manifest");

    let coverage = GitLocatorCoverage {
        generation: 1,
        pack_index_hash: crab_xet::hash::MerkleHash::from_hex(&manifest.pack_index_hash)
            .expect("pack index hash"),
    };
    let record = GitPackLocatorRecord {
        pack_id,
        committed_generation: 1,
        pack_index_hash: coverage.pack_index_hash,
        object_count,
        pack_size,
    };
    let mut writer = GitObjectLocatorWriter::open(Arc::clone(&inner), "org/repo")
        .await
        .expect("open locator writer");
    let binding = writer
        .bind_packs(&[record])
        .await
        .expect("bind fixture pack")[0];
    let entries = locations
        .into_iter()
        .map(|location| GitObjectLocatorEntry {
            oid: location.oid.as_bytes().try_into().expect("SHA-1 object ID"),
            location: GitObjectLocation {
                pack_offset: location.pack_offset,
                entry_len: location.entry_len,
                crc32: if corrupt_target_crc && location.oid == fixture.target {
                    location.crc32 ^ 1
                } else {
                    location.crc32
                },
            },
            metadata: Default::default(),
        })
        .collect::<Vec<_>>();
    writer
        .write_locations(binding, &entries)
        .await
        .expect("write fixture locations");
    writer
        .set_coverage(coverage)
        .await
        .expect("publish locator coverage");
    writer.close().await.expect("close locator writer");

    let cancellation = CancellationToken::new();
    let runtime = Arc::new(
        RemoteGitRuntime::new(runtime_options, Arc::new(crab_remote_git::NoopMetrics))
            .expect("runtime"),
    );
    let repository = RemoteGitRepository::open(
        store.clone(),
        layout.clone(),
        crab_remote_git::RepositoryIdentity::new("memory", "org/repo", 1).expect("identity"),
        Arc::clone(&runtime),
        options,
        &cancellation,
    )
    .await
    .expect("open repository");
    PublishedFixture {
        source_git_dir: fixture._temp.path().join("repo.git"),
        _source: fixture._temp,
        repository,
        runtime,
        backend,
        store,
        layout,
        target: fixture.target,
        target_path: GitPath::new(Bytes::from(fixture.target_path)).expect("target path"),
        expected: fixture.expected,
        base_path: GitPath::new(Bytes::from(fixture.base_path)).expect("base path"),
        base_offset: fixture.base_offset,
        base_expected: fixture.base_expected,
        root_commit: fixture.root_commit,
        side_commit: fixture.side_commit,
        semantic_base_commit: fixture.semantic_base_commit,
        semantic_head_commit: fixture.semantic_head_commit,
        unreachable: fixture.unreachable,
        blob_paths: fixture
            .blob_paths
            .into_iter()
            .map(|(path, bytes)| (GitPath::new(Bytes::from(path)).expect("blob path"), bytes))
            .collect(),
        shared_paths: fixture.shared_paths.map(|paths| {
            paths
                .map(|(path, bytes)| (GitPath::new(Bytes::from(path)).expect("shared path"), bytes))
        }),
    }
}

async fn read_target(fixture: &PublishedFixture) -> crab_remote_git::Result<Bytes> {
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await?;
    let result = async {
        let revision = Revision::Reference("refs/heads/main".to_owned());
        let snapshot = fixture.repository.snapshot(&revision, &operation).await?;
        snapshot
            .read_blob(&fixture.target_path, &operation)
            .await
            .map(|blob| blob.bytes)
    }
    .await;
    operation.finish(result).await
}

async fn reopen_fixture(
    fixture: &PublishedFixture,
) -> (RemoteGitRepository, Arc<RemoteGitRuntime>) {
    let runtime = Arc::new(
        RemoteGitRuntime::new(
            RuntimeOptions::default(),
            Arc::new(crab_remote_git::NoopMetrics),
        )
        .expect("runtime"),
    );
    let repository = RemoteGitRepository::open(
        fixture.store.clone(),
        fixture.layout.clone(),
        crab_remote_git::RepositoryIdentity::new("memory", "org/repo", 1).expect("identity"),
        Arc::clone(&runtime),
        RepositoryOptions::default(),
        &CancellationToken::new(),
    )
    .await
    .expect("reopen fixture");
    (repository, runtime)
}

#[tokio::test]
async fn canonical_snapshot_reads_deltas_without_catalog_or_remote_mutation() {
    for kind in [DeltaKind::Ref, DeltaKind::Ofs, DeltaKind::RefShared] {
        let fixture = publish(kind, false, RepositoryOptions::default()).await;
        crab_metadata::layout_descriptor::ensure_canonical_layout(&fixture.store, &fixture.layout)
            .await
            .expect("canonical descriptor");
        let snapshot = crab_metadata::manifest_store::read_repository_snapshot(
            &fixture.store,
            &fixture.layout,
        )
        .await
        .expect("pinned snapshot");
        let catalog = ObjectPath::from(crab_metadata::git_object_locator::git_object_locator_path(
            fixture.layout.repo_prefix(),
        ));
        let entries = fixture
            .backend
            .inner
            .list(Some(&catalog))
            .collect::<Vec<_>>()
            .await;
        for entry in entries {
            fixture
                .backend
                .inner
                .delete(&entry.expect("catalog entry").location)
                .await
                .expect("remove test catalog");
        }
        let runtime = Arc::new(RemoteGitRuntime::default());
        fixture.backend.mutations.store(0, Ordering::SeqCst);
        let operation = crab_remote_git::OperationContext::from_snapshot(
            fixture.layout.clone(),
            &snapshot,
            fixture.repository.identity().clone(),
            Arc::clone(&runtime),
            RepositoryOptions::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("open without catalog");
        let result = operation.read_object(fixture.target).await;
        let object = operation
            .finish(result)
            .await
            .expect("canonical delta read");
        runtime.shutdown().await;
        assert_eq!(
            (
                object.data.as_ref(),
                fixture.backend.mutations.load(Ordering::SeqCst)
            ),
            (fixture.expected.as_slice(), 0)
        );
        fixture.runtime.shutdown().await;
    }
}

#[tokio::test]
async fn canonical_snapshot_does_not_reuse_misses_from_another_inventory() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    crab_metadata::layout_descriptor::ensure_canonical_layout(&fixture.store, &fixture.layout)
        .await
        .expect("canonical descriptor");
    let current =
        crab_metadata::manifest_store::read_repository_snapshot(&fixture.store, &fixture.layout)
            .await
            .expect("current snapshot");
    let mut empty = Manifest::default_for_repo("refs/heads/main");
    empty.generation = current.manifest.generation;
    empty.seal_git_validation();
    let journal = crab_metadata::ref_journal::materialize_ref_journal(
        &fixture.store,
        &fixture.layout,
        &empty,
        &[],
        &[],
        &BTreeSet::new(),
    )
    .await
    .expect("empty journal projection");
    let before = crab_metadata::manifest_store::RepositorySnapshot {
        layout: current.layout.clone(),
        manifest: empty,
        manifest_etag: current.manifest_etag.clone(),
        journal,
    };
    let runtime = Arc::new(RemoteGitRuntime::default());
    let cancellation = CancellationToken::new();
    let old = crab_remote_git::OperationContext::from_snapshot(
        fixture.layout.clone(),
        &before,
        fixture.repository.identity().clone(),
        Arc::clone(&runtime),
        RepositoryOptions::default(),
        &cancellation,
    )
    .await
    .expect("old snapshot");
    let missing = old.read_object(fixture.target).await;
    assert!(matches!(
        old.finish(missing).await,
        Err(Error::ObjectNotFound { .. })
    ));
    let new = crab_remote_git::OperationContext::from_snapshot(
        fixture.layout.clone(),
        &current,
        fixture.repository.identity().clone(),
        Arc::clone(&runtime),
        RepositoryOptions::default(),
        &cancellation,
    )
    .await
    .expect("new snapshot with the same base generation");
    let found = new.read_object(fixture.target).await;
    assert_eq!(
        new.finish(found)
            .await
            .expect("new inventory object")
            .data
            .as_ref(),
        fixture.expected
    );
    runtime.shutdown().await;
    fixture.runtime.shutdown().await;
}

fn contains_limit_exceeded(error: &Error, expected_limit: &str) -> bool {
    match error {
        Error::LimitExceeded { limit, .. } => *limit == expected_limit,
        Error::SharedRead { source } => contains_limit_exceeded(source, expected_limit),
        Error::CloseAfterFailure { operation, .. } => {
            contains_limit_exceeded(operation, expected_limit)
        }
        _ => false,
    }
}

fn contains_cancelled(error: &Error) -> bool {
    match error {
        Error::Cancelled => true,
        Error::SharedRead { source } => contains_cancelled(source),
        Error::CloseAfterFailure { operation, .. } => contains_cancelled(operation),
        _ => false,
    }
}

fn contains_corruption(error: &Error, expected_stage: crab_remote_git::CorruptionStage) -> bool {
    match error {
        Error::Corrupt { stage } => *stage == expected_stage,
        Error::SharedRead { source } => contains_corruption(source, expected_stage),
        Error::CloseAfterFailure { operation, .. } => {
            contains_corruption(operation, expected_stage)
        }
        _ => false,
    }
}

fn fixture_object_ids(fixture: &PublishedFixture) -> Vec<gix_hash::ObjectId> {
    let output = git(
        &[
            "--git-dir",
            path(&fixture.source_git_dir),
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
        None,
    );
    String::from_utf8(output)
        .expect("object list is UTF-8")
        .lines()
        .map(|oid| gix_hash::ObjectId::from_hex(oid.as_bytes()).expect("full SHA-1"))
        .collect()
}

fn fixture_pack_path(fixture: &PublishedFixture, extension: &str) -> PathBuf {
    fs::read_dir(fixture._source.path())
        .expect("read source pack directory")
        .map(|entry| entry.expect("source pack entry").path())
        .find(|path| {
            path.extension()
                .is_some_and(|candidate| candidate == extension)
        })
        .expect("source pack artifact")
}

fn fixture_pack_id(pack: &[u8]) -> MerkleHash {
    MerkleHash::from_hex(blake3::hash(pack).to_hex().as_str()).expect("raw BLAKE3 pack identity")
}

fn strict_pack_objects(pack_path: &Path, source_git_dir: &Path) -> (BTreeSet<String>, usize) {
    let repository = tempfile::tempdir().expect("temporary strict-pack repository");
    git(&["init", "--bare", path(repository.path())], None);
    let pack = fs::File::open(pack_path).expect("open generated pack");
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(repository.path())
        .args(["index-pack", "--stdin"])
        .stdin(Stdio::from(pack))
        .output()
        .expect("run structural index-pack");
    assert!(
        output.status.success(),
        "structural index-pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let checksum_output = String::from_utf8(output.stdout).expect("index-pack checksum is UTF-8");
    let checksum = checksum_output
        .split_whitespace()
        .next_back()
        .expect("index-pack returns a checksum");
    let index = repository
        .path()
        .join("objects/pack")
        .join(format!("pack-{checksum}.idx"));
    let output = Command::new("git")
        .args(["verify-pack", "-v"])
        .arg(index)
        .output()
        .expect("run verify-pack");
    assert!(
        output.status.success(),
        "verify-pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let verify_output = String::from_utf8(output.stdout).expect("verify-pack output is UTF-8");
    let objects = verify_output
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map(str::to_owned)
        .collect();
    let deltas = verify_output
        .lines()
        .filter(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            fields.first().is_some_and(|value| value.len() == 40) && fields.len() >= 7
        })
        .count();

    let strict_repository = tempfile::tempdir().expect("temporary strict fsck repository");
    git(&["init", "--bare", path(strict_repository.path())], None);
    let alternates = strict_repository.path().join("objects/info/alternates");
    fs::write(
        alternates,
        format!("{}\n", source_git_dir.join("objects").display()),
    )
    .expect("write source object alternate");
    let pack = fs::File::open(pack_path).expect("reopen generated pack");
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(strict_repository.path())
        .args([
            "index-pack",
            "--strict=hasDotdot=ignore,hasDotgit=ignore",
            "--stdin",
        ])
        .stdin(Stdio::from(pack))
        .output()
        .expect("run strict index-pack");
    assert!(
        output.status.success(),
        "strict index-pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (objects, deltas)
}

fn fixture_ref_delta_base(fixture: &PublishedFixture) -> gix_hash::ObjectId {
    let pack = fs::read(fixture_pack_path(fixture, "pack")).expect("read fixture pack");
    let index = fixture_pack_path(fixture, "idx");
    let reverse = fixture_pack_path(fixture, "rev");
    PackLocationIter::open(&index, &reverse, pack.len() as u64)
        .expect("open fixture locations")
        .map(|location| location.expect("fixture location"))
        .find_map(|location| {
            if location.oid != fixture.target {
                return None;
            }
            let start = location.pack_offset as usize;
            let end = start + location.entry_len as usize;
            let entry =
                gix_pack::data::Entry::from_bytes(&pack[start..end], location.pack_offset, 20)
                    .expect("target pack entry");
            match entry.header {
                Header::RefDelta { base_id } => Some(base_id),
                _ => None,
            }
        })
        .expect("target must be a REF delta")
}

fn strict_thin_pack(pack_path: &Path, source_git_dir: &Path) {
    let repository = tempfile::tempdir().expect("temporary thin-pack repository");
    git(&["init", "--bare", path(repository.path())], None);
    fs::write(
        repository.path().join("objects/info/alternates"),
        format!("{}\n", source_git_dir.join("objects").display()),
    )
    .expect("write source object alternate");
    let pack = fs::File::open(pack_path).expect("open generated thin pack");
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(repository.path())
        .args([
            "index-pack",
            "--fix-thin",
            "--strict=hasDotdot=ignore,hasDotgit=ignore",
            "--stdin",
        ])
        .stdin(Stdio::from(pack))
        .output()
        .expect("run strict thin index-pack");
    assert!(
        output.status.success(),
        "strict thin index-pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn assert_runtime_is_within_configured_bounds(
    runtime: &RemoteGitRuntime,
    options: RuntimeOptions,
) {
    let occupancy = runtime.snapshot().await;
    assert_eq!(occupancy.active_object_flights, 0);
    assert_eq!(occupancy.active_pack_index_flights, 0);
    assert_eq!(occupancy.active_generated_pack_flights, 0);
    assert!(occupancy.object_entries <= options.max_object_cache_entries);
    assert!(occupancy.object_bytes <= options.max_object_cache_bytes);
    assert!(occupancy.pack_index_entries <= options.max_pack_index_cache_entries);
    assert!(occupancy.pack_index_bytes <= options.max_pack_index_cache_bytes);
    assert!(occupancy.parsed_entries <= options.max_parsed_cache_entries);
    assert!(occupancy.parsed_bytes <= options.max_parsed_cache_bytes);
    assert!(occupancy.blame_entries <= options.max_blame_cache_entries);
    assert!(occupancy.blame_bytes <= options.max_blame_cache_bytes);
    assert!(occupancy.manifest_entries <= options.max_manifest_cache_entries);
    assert!(occupancy.manifest_bytes <= options.max_manifest_cache_bytes);
    assert!(occupancy.inventory_entries <= options.max_inventory_cache_entries);
    assert!(occupancy.inventory_bytes <= options.max_inventory_cache_bytes);
    assert!(occupancy.negative_entries <= options.max_negative_cache_entries);
    assert!(occupancy.negative_bytes <= options.max_negative_cache_bytes);
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_single_pack_closure_reuses_the_verified_canonical_pack() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let object_ids = fixture_object_ids(&fixture);
    let source_bytes = fs::read(fixture_pack_path(&fixture, "pack")).expect("read canonical pack");

    fixture.backend.reset_pack_gets();
    let generated = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect("generate exact pack");

    assert_eq!(fixture.backend.pack_gets(), 1);
    assert_eq!(
        fs::read(generated.path()).expect("read generated pack"),
        source_bytes
    );
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn single_pack_reuse_rejects_a_valid_index_for_another_pack() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let object_ids = fixture_object_ids(&fixture);
    let pack = fs::read(fixture_pack_path(&fixture, "pack")).expect("read canonical pack");
    let pack_id = fixture_pack_id(&pack);
    let mut index = fs::read(fixture_pack_path(&fixture, "idx")).expect("read canonical index");
    let pack_checksum = index.len() - 40;
    index[pack_checksum] ^= 1;
    let index_checksum = index.len() - 20;
    let checksum = Sha1::digest(&index[..index_checksum]);
    index[index_checksum..].copy_from_slice(&checksum);
    fixture
        .backend
        .inner
        .put(
            &fixture.layout.pack_index_path(&pack_id),
            Bytes::from(index).into(),
        )
        .await
        .expect("replace pack index");

    fixture.backend.reset_pack_gets();
    let error = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect_err("mismatched pack checksum must reject canonical reuse");

    assert!(contains_corruption(
        &error,
        crab_remote_git::CorruptionStage::PackEntry
    ));
    assert_eq!(fixture.backend.pack_gets(), 1);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn single_pack_reuse_rejects_corrupt_pack_content() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let object_ids = fixture_object_ids(&fixture);
    let mut pack = fs::read(fixture_pack_path(&fixture, "pack")).expect("read canonical pack");
    let pack_id = fixture_pack_id(&pack);
    pack[12] ^= 1;
    fixture
        .backend
        .inner
        .put(
            &fixture.layout.pack_path(&pack_id),
            Bytes::from(pack).into(),
        )
        .await
        .expect("replace pack");

    let error = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect_err("corrupt pack must reject canonical reuse");

    assert!(contains_corruption(
        &error,
        crab_remote_git::CorruptionStage::PackEntry
    ));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn single_pack_reuse_checks_response_limit_before_pack_download() {
    let options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_response_bytes: 1,
            ..OperationLimits::default()
        },
    )
    .expect("repository options");
    let fixture = publish(DeltaKind::Ofs, false, options).await;
    let object_ids = fixture_object_ids(&fixture);

    fixture.backend.reset_pack_gets();
    let error = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect_err("oversized pack must fail before download");

    assert!(contains_limit_exceeded(&error, "pack response bytes"));
    assert_eq!(fixture.backend.pack_gets(), 0);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_single_pack_reuse_stops_a_blocked_download() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let object_ids = fixture_object_ids(&fixture);
    let repository = fixture.repository.clone();
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();

    fixture.backend.block_next_pack_get();
    let task = tokio::spawn(async move {
        repository
            .generate_pack(&object_ids, &task_cancellation)
            .await
    });
    fixture.backend.wait_for_blocked_pack_get().await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cancelled pack download returns promptly")
        .expect("pack task joins")
        .expect_err("cancelled pack download fails");

    assert!(contains_cancelled(&error));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subset_pack_generation_does_not_reuse_the_canonical_pack() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    let omitted = object_ids.pop().expect("fixture has objects");
    let source_bytes = fs::read(fixture_pack_path(&fixture, "pack")).expect("read canonical pack");

    fixture.backend.reset_pack_gets();
    let generated = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect("generate subset pack");

    assert_eq!(generated.object_count() as usize, object_ids.len());
    assert_ne!(
        fs::read(generated.path()).expect("read generated pack"),
        source_bytes,
        "subset response must not disclose the complete canonical pack containing {omitted}"
    );
    let (packed_objects, deltas) = strict_pack_objects(generated.path(), &fixture.source_git_dir);
    assert_eq!(
        packed_objects,
        object_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
    );
    assert!(deltas > 0, "OFS delta payloads should be preserved");
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shallow_fetch_pack_uses_pinned_inventory_and_exact_boundary() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let want = fixture
        .repository
        .refs()
        .entries
        .iter()
        .find(|reference| reference.name == "refs/heads/main")
        .expect("main reference")
        .target;

    fixture.backend.reset_pack_gets();
    let generated = fixture
        .repository
        .generate_shallow_fetch_pack(
            &[want],
            &[fixture.root_commit],
            &[fixture.root_commit],
            &[],
            &CancellationToken::new(),
        )
        .await
        .expect("generate shallow fetch pack");

    assert_eq!(fixture.backend.pack_gets(), 1);
    let (packed_objects, _) = strict_pack_objects(generated.path(), &fixture.source_git_dir);
    assert!(packed_objects.contains(&want.to_string()));
    assert!(packed_objects.contains(&fixture.side_commit.to_string()));
    assert!(!packed_objects.contains(&fixture.root_commit.to_string()));
    assert!(!packed_objects.contains(&fixture.unreachable.to_string()));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ref_delta_subset_pack_is_strict_and_contains_exactly_selected_objects() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.remove(0);

    let generated = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect("generate ref-delta subset pack");

    let (packed_objects, deltas) = strict_pack_objects(generated.path(), &fixture.source_git_dir);
    assert_eq!(
        packed_objects,
        object_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
    );
    assert!(deltas > 0, "REF delta payloads should be preserved");
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subset_pack_generation_honors_response_limit() {
    let options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_response_bytes: 1,
            ..OperationLimits::default()
        },
    )
    .expect("repository options");
    let fixture = publish(DeltaKind::Ofs, false, options).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");

    let error = fixture
        .repository
        .generate_pack(&object_ids, &CancellationToken::new())
        .await
        .expect_err("oversized subset pack must fail");

    assert!(contains_limit_exceeded(&error, "pack response bytes"));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_subset_pack_generation_stops_a_blocked_range_read() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");
    let repository = fixture.repository.clone();
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();

    fixture.backend.block_next_pack_get();
    let task = tokio::spawn(async move {
        repository
            .generate_pack(&object_ids, &task_cancellation)
            .await
    });
    fixture.backend.wait_for_blocked_pack_get().await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cancelled subset generation returns promptly")
        .expect("pack task joins")
        .expect_err("cancelled subset generation fails");

    assert!(contains_cancelled(&error));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subset_pack_generation_rejects_a_corrupt_packed_entry() {
    let fixture = publish(DeltaKind::Ref, true, RepositoryOptions::default()).await;

    let error = fixture
        .repository
        .generate_pack(&[fixture.target], &CancellationToken::new())
        .await
        .expect_err("corrupt selected entry must fail");

    assert!(matches!(error, Error::PackedEntryCrcMismatch { .. }));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn thin_subset_pack_uses_only_client_proven_delta_bases() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let base = fixture_ref_delta_base(&fixture);

    let generated = fixture
        .repository
        .generate_pack_with_bases(&[fixture.target], &[base], &CancellationToken::new())
        .await
        .expect("generate thin subset pack");

    assert_eq!(generated.object_count(), 1);
    strict_thin_pack(generated.path(), &fixture.source_git_dir);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_pack_cache_reuses_one_verified_immutable_artifact() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");
    let key = fixture
        .repository
        .generated_pack_cache_key([3; 32], &object_ids, false);

    let cold = fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect("generate and publish cached pack");
    fixture.backend.reset_pack_gets();
    fixture.backend.reset_generated_pack_descriptor_gets();
    let warm = fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect("load cached pack");

    assert_eq!(fixture.backend.generated_pack_descriptor_puts(), 1);
    assert_eq!(fixture.backend.pack_gets(), 1);
    assert_eq!(fixture.backend.generated_pack_descriptor_gets(), 1);
    assert_eq!(
        fs::read(cold.path()).expect("read cold pack"),
        fs::read(warm.path()).expect("read warm pack")
    );
    let (packed_objects, _) = strict_pack_objects(warm.path(), &fixture.source_git_dir);
    assert_eq!(
        packed_objects,
        object_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
    );
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_pack_cache_rejects_corrupt_artifact_bytes() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");
    let key = fixture
        .repository
        .generated_pack_cache_key([7; 32], &object_ids, false);
    fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect("publish cached pack");
    let prefix = fixture.layout.repo_path("generated-packs/v1/artifacts");
    let artifacts = fixture
        .backend
        .inner
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await;
    let artifact = artifacts
        .into_iter()
        .next()
        .expect("generated artifact")
        .expect("artifact metadata")
        .location;
    let mut bytes = fixture
        .backend
        .inner
        .get(&artifact)
        .await
        .expect("read artifact")
        .bytes()
        .await
        .expect("collect artifact")
        .to_vec();
    bytes[12] ^= 1;
    fixture
        .backend
        .inner
        .put(&artifact, Bytes::from(bytes).into())
        .await
        .expect("corrupt artifact");

    let error = fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect_err("corrupt cached pack must fail closed");
    assert!(contains_corruption(
        &error,
        crab_remote_git::CorruptionStage::PackEntry
    ));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_pack_cache_rejects_an_oversized_artifact() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");
    let key = fixture
        .repository
        .generated_pack_cache_key([7; 32], &object_ids, false);
    fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect("publish cached pack");
    let prefix = fixture.layout.repo_path("generated-packs/v1/artifacts");
    let artifact = fixture
        .backend
        .inner
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .next()
        .expect("generated artifact")
        .expect("artifact metadata")
        .location;
    let mut bytes = fixture
        .backend
        .inner
        .get(&artifact)
        .await
        .expect("read artifact")
        .bytes()
        .await
        .expect("collect artifact")
        .to_vec();
    bytes.extend_from_slice(b"oversized");
    fixture
        .backend
        .inner
        .put(&artifact, Bytes::from(bytes).into())
        .await
        .expect("replace artifact");

    let error = fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect_err("oversized cached pack must fail closed");
    assert!(matches!(
        error,
        Error::Storage(crab_storage::StorageError::CorruptObject { .. })
    ));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_pack_cache_rejects_corrupt_request_descriptor() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");
    let key = fixture
        .repository
        .generated_pack_cache_key([7; 32], &object_ids, false);
    fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect("publish cached pack");
    let prefix = fixture.layout.repo_path("generated-packs/v1/requests");
    let descriptors = fixture
        .backend
        .inner
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await;
    let descriptor = descriptors
        .into_iter()
        .next()
        .expect("generated descriptor")
        .expect("descriptor metadata")
        .location;
    fixture
        .backend
        .inner
        .put(&descriptor, Bytes::from_static(b"{}").into())
        .await
        .expect("corrupt descriptor");

    let error = fixture
        .repository
        .generate_pack_cached(&object_ids, key, &CancellationToken::new())
        .await
        .expect_err("corrupt descriptor must fail closed");
    assert!(contains_corruption(
        &error,
        crab_remote_git::CorruptionStage::PackEntry
    ));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_pack_cache_coalesces_runtimes_and_survives_waiter_cancellation() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let (second_repository, second_runtime) = reopen_fixture(&fixture).await;
    let lease_provider = Arc::new(TestGeneratedPackLeaseProvider::default());
    let first_repository = fixture
        .repository
        .clone()
        .with_generated_pack_lease_provider(lease_provider.clone());
    let second_repository = second_repository.with_generated_pack_lease_provider(lease_provider);
    let mut object_ids = fixture_object_ids(&fixture);
    object_ids.pop().expect("fixture has objects");
    let key = fixture
        .repository
        .generated_pack_cache_key([5; 32], &object_ids, false);
    let cancelled = CancellationToken::new();
    let first_objects = object_ids.clone();
    let first_cancellation = cancelled.clone();

    fixture.backend.block_next_pack_get();
    let first = tokio::spawn(async move {
        first_repository
            .generate_pack_cached(&first_objects, key, &first_cancellation)
            .await
    });
    fixture.backend.wait_for_blocked_pack_get().await;
    let second_objects = object_ids.clone();
    let second = tokio::spawn(async move {
        second_repository
            .generate_pack_cached(&second_objects, key, &CancellationToken::new())
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancelled.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .expect("cancelled waiter returns promptly")
        .expect("cancelled waiter joins")
        .expect_err("cancelled waiter fails");
    assert!(contains_cancelled(&error));

    fixture.backend.release_blocked_pack_get();
    let generated = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("coalesced runtime completes")
        .expect("coalesced runtime joins")
        .expect("coalesced runtime receives pack");
    assert_eq!(generated.object_count() as usize, object_ids.len());
    assert_eq!(fixture.backend.generated_pack_descriptor_puts(), 1);
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .await
            .active_generated_pack_flights,
        0
    );
    assert_eq!(
        second_runtime
            .snapshot()
            .await
            .active_generated_pack_flights,
        0
    );
    fixture.runtime.shutdown().await;
    second_runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn request_bound_generated_pack_cache_plans_once_across_runtimes() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let (second_repository, second_runtime) = reopen_fixture(&fixture).await;
    let lease_provider = Arc::new(TestGeneratedPackLeaseProvider::default());
    let first_repository = fixture
        .repository
        .clone()
        .with_generated_pack_lease_provider(lease_provider.clone());
    let second_repository =
        second_repository.with_generated_pack_lease_provider(lease_provider.clone());
    let object_ids = fixture_object_ids(&fixture);
    let key = fixture
        .repository
        .generated_pack_request_cache_key([11; 32], [12; 32]);
    let producer_polls = Arc::new(AtomicUsize::new(0));
    let producer_started = Arc::new(Notify::new());
    let release_producer = Arc::new(Notify::new());

    let first_objects = object_ids.clone();
    let first_polls = Arc::clone(&producer_polls);
    let first_producer_repository = first_repository.clone();
    let first_started = Arc::clone(&producer_started);
    let first_release = Arc::clone(&release_producer);
    let warm_repository = first_repository.clone();
    let first = tokio::spawn(async move {
        first_repository
            .generate_pack_request_cached(
                key,
                async move {
                    first_polls.fetch_add(1, Ordering::SeqCst);
                    first_started.notify_one();
                    first_release.notified().await;
                    first_producer_repository
                        .generate_pack(&first_objects, &CancellationToken::new())
                        .await
                },
                &CancellationToken::new(),
            )
            .await
    });
    producer_started.notified().await;

    let second_objects = object_ids.clone();
    let second_polls = Arc::clone(&producer_polls);
    let second_producer_repository = second_repository.clone();
    let second = tokio::spawn(async move {
        second_repository
            .generate_pack_request_cached(
                key,
                async move {
                    second_polls.fetch_add(1, Ordering::SeqCst);
                    second_producer_repository
                        .generate_pack(&second_objects, &CancellationToken::new())
                        .await
                },
                &CancellationToken::new(),
            )
            .await
    });
    lease_provider.wait_for_held_attempt().await;
    assert_eq!(producer_polls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lease_provider.held_attempts.load(Ordering::SeqCst), 1);

    release_producer.notify_one();
    let first_pack = first
        .await
        .expect("first request joins")
        .expect("first request receives pack");
    let second_pack = second
        .await
        .expect("second request joins")
        .expect("second request receives pack");

    assert_eq!(producer_polls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.backend.generated_pack_descriptor_puts(), 1);
    assert_eq!(first_pack.object_count(), second_pack.object_count());
    assert_eq!(
        fs::read(first_pack.path()).expect("read first pack"),
        fs::read(second_pack.path()).expect("read second pack")
    );
    // Exhaust the storage helper's retry budget once. The generated-pack wait
    // boundary must keep retrying instead of failing the whole fanout request.
    fixture.backend.throttle_generated_pack_descriptor_gets(6);
    let warm_pack = warm_repository
        .generate_pack_request_cached(
            key,
            async {
                Err::<crab_remote_git::GeneratedPack, _>("warm cache hit polled its producer")
            },
            &CancellationToken::new(),
        )
        .await
        .expect("warm request receives cached pack");
    assert_eq!(warm_pack.object_count(), first_pack.object_count());
    assert_eq!(lease_provider.read_admissions.load(Ordering::SeqCst), 2);
    assert_eq!(lease_provider.read_releases.load(Ordering::SeqCst), 2);
    fixture.runtime.shutdown().await;
    second_runtime.shutdown().await;
}

#[derive(Default)]
struct TestGeneratedPackLeaseProvider {
    lease: Arc<Mutex<()>>,
    held_attempts: AtomicUsize,
    held_notify: Notify,
    read_admissions: AtomicUsize,
    read_releases: Arc<AtomicUsize>,
}

impl TestGeneratedPackLeaseProvider {
    async fn wait_for_held_attempt(&self) {
        loop {
            let notified = self.held_notify.notified();
            if self.held_attempts.load(Ordering::SeqCst) > 0 {
                return;
            }
            notified.await;
        }
    }
}

struct TestGeneratedPackLease {
    _guard: OwnedMutexGuard<()>,
}

impl GeneratedPackLeaseProvider for TestGeneratedPackLeaseProvider {
    fn try_acquire<'a>(
        &'a self,
        _resource: &'a str,
        _ttl: Duration,
    ) -> futures_util::future::BoxFuture<
        'a,
        std::result::Result<GeneratedPackLeaseAttempt, GeneratedPackLeaseError>,
    > {
        let lease = Arc::clone(&self.lease);
        Box::pin(async move {
            Ok(match lease.try_lock_owned() {
                Ok(guard) => {
                    GeneratedPackLeaseAttempt::Acquired(Box::new(TestGeneratedPackLease {
                        _guard: guard,
                    }))
                }
                Err(_) => {
                    self.held_attempts.fetch_add(1, Ordering::SeqCst);
                    self.held_notify.notify_waiters();
                    GeneratedPackLeaseAttempt::Held {
                        retry_after: Duration::from_secs(1),
                    }
                }
            })
        })
    }

    fn acquire_read<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
        _max_wait: Duration,
    ) -> futures_util::future::BoxFuture<
        'a,
        std::result::Result<Box<dyn GeneratedPackLease>, GeneratedPackLeaseError>,
    > {
        Box::pin(async {
            self.read_admissions.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(TestGeneratedPackReadPermit {
                releases: Arc::clone(&self.read_releases),
            }) as Box<dyn GeneratedPackLease>)
        })
    }
}

struct TestGeneratedPackReadPermit {
    releases: Arc<AtomicUsize>,
}

impl GeneratedPackLease for TestGeneratedPackReadPermit {
    fn renew(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, std::result::Result<(), GeneratedPackLeaseError>> {
        Box::pin(async { Ok(()) })
    }

    fn release(
        self: Box<Self>,
    ) -> futures_util::future::BoxFuture<'static, std::result::Result<(), GeneratedPackLeaseError>>
    {
        Box::pin(async move {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

impl GeneratedPackLease for TestGeneratedPackLease {
    fn renew(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, std::result::Result<(), GeneratedPackLeaseError>> {
        Box::pin(async { Ok(()) })
    }

    fn release(
        self: Box<Self>,
    ) -> futures_util::future::BoxFuture<'static, std::result::Result<(), GeneratedPackLeaseError>>
    {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn generated_pack_cache_key_binds_authorization_selection_and_pack_mode() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let first = fixture.target;
    let second = fixture.root_commit;
    let key = |authorization, objects: &[gix_hash::ObjectId], thin_pack| {
        fixture
            .repository
            .generated_pack_cache_key(authorization, objects, thin_pack)
    };

    let base = key([1; 32], &[first], false);
    assert_ne!(base, key([9; 32], &[first], false));
    assert_ne!(base, key([1; 32], &[first], true));
    assert_ne!(base, key([1; 32], &[first, second], false));
    let error = fixture
        .repository
        .generate_pack_cached(&[second], base, &CancellationToken::new())
        .await
        .expect_err("cache key cannot be reused for another selection");
    assert!(matches!(error, Error::InternalInvariant { .. }));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn public_api_opens_resolves_snapshots_lists_and_reads_without_a_filesystem() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let revision = Revision::Reference("main".to_owned());
        let resolved = fixture.repository.resolve(&revision, &operation).await?;
        let snapshot = fixture.repository.snapshot(&revision, &operation).await?;
        let page = snapshot
            .list_directory(&GitPath::root(), &PageRequest::new(10, None)?, &operation)
            .await?;
        assert_eq!(resolved.commit, snapshot.commit_oid());
        assert_eq!(page.items.len(), 10);
        let blob = snapshot.read_blob(&fixture.target_path, &operation).await?;
        assert_eq!(blob.bytes.as_ref(), fixture.expected);
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn visibility_rebuild_batches_remote_object_reads() {
    let options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_storage_requests: 30,
            ..OperationLimits::default()
        },
    )
    .expect("repository options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    fixture.backend.reset_pack_gets();

    let index = fixture
        .repository
        .rebuild_visibility_index(&CancellationToken::new())
        .await
        .expect("rebuild visibility");

    assert_eq!(index.ref_count(), fixture.repository.refs().entries.len());
    let pack_gets = fixture.backend.pack_gets();
    assert!(
        pack_gets <= 14,
        "visibility reconstruction performed {pack_gets} pack reads"
    );
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repository_handle_revalidation_detects_manifest_replacement() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    assert!(
        fixture
            .repository
            .is_current(&cancellation)
            .await
            .expect("revalidate current manifest")
    );

    let (mut manifest, etag) = read_manifest(&fixture.store, &fixture.layout)
        .await
        .expect("read fixture manifest");
    manifest.generation += 1;
    manifest.seal_git_validation();
    write_manifest_cas(&fixture.store, &fixture.layout, &manifest, &etag)
        .await
        .expect("replace fixture manifest");

    assert!(
        !fixture
            .repository
            .is_current(&cancellation)
            .await
            .expect("revalidate replaced manifest")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_paths_preserve_modes_and_never_follow_links_or_gitlinks() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;

        let root = snapshot
            .entry(&GitPath::root(), &operation)
            .await?
            .expect("root entry");
        assert_eq!((root.mode, root.kind), (EntryMode::Tree, EntryKind::Tree));

        let executable_path = GitPath::new(Bytes::from_static(b"run.sh"))?;
        let executable = snapshot
            .entry(&executable_path, &operation)
            .await?
            .expect("executable entry");
        assert_eq!(executable.mode, EntryMode::Executable);
        assert_eq!(
            snapshot
                .read_blob(&executable_path, &operation)
                .await?
                .bytes,
            Bytes::from_static(b"#!/bin/sh\necho crab\n")
        );

        let symlink_path = GitPath::new(Bytes::from_static(b"link"))?;
        let symlink = snapshot.symlink(&symlink_path, &operation).await?;
        assert_eq!(symlink.entry.mode, EntryMode::Symlink);
        assert_eq!(symlink.target, Bytes::from_static(b"dir/nested/deep.txt"));

        let module_path = GitPath::new(Bytes::from_static(b"module"))?;
        let module = snapshot.submodule(&module_path, &operation).await?;
        assert_eq!(module.entry.mode, EntryMode::Submodule);
        assert_eq!(module.commit, module.entry.oid);

        let deep_path = GitPath::new(Bytes::from_static(b"dir/nested/deep.txt"))?;
        assert_eq!(
            snapshot.read_blob(&deep_path, &operation).await?.bytes,
            Bytes::from_static(b"deep content\n")
        );
        let non_utf8_path = GitPath::new(Bytes::from_static(b"\xff.txt"))?;
        assert_eq!(
            snapshot
                .entry(&non_utf8_path, &operation)
                .await?
                .expect("non-UTF-8 entry")
                .path
                .as_bytes(),
            b"\xff.txt"
        );
        let dot_path = GitPath::new(Bytes::from_static(b".."))?;
        assert!(
            snapshot
                .read_blob(&dot_path, &operation)
                .await?
                .bytes
                .is_empty()
        );
        assert!(
            snapshot
                .entry(&GitPath::new(Bytes::from_static(b"missing"))?, &operation)
                .await?
                .is_none()
        );
        let invalid_deep = GitPath::new(Bytes::from_static(b"run.sh/child"))?;
        assert!(matches!(
            snapshot.entry(&invalid_deep, &operation).await,
            Err(Error::PathComponentNotTree {
                actual: EntryKind::Blob
            })
        ));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_pages_preserve_git_order_for_shared_file_and_tree_prefixes() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Tree, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let revision = format!("{}:ordered", snapshot.commit_oid());
        let expected = git(
            &[
                "--git-dir",
                path(&fixture.source_git_dir),
                "ls-tree",
                "--name-only",
                "-z",
                &revision,
            ],
            None,
        )
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
        let directory = GitPath::new(Bytes::from_static(b"ordered"))?;
        for metadata in [DirectoryMetadata::None, DirectoryMetadata::BlobSizes] {
            for limit in [1, 2, 3] {
                let mut cursor = None;
                let mut names = Vec::new();
                for _ in 0..=expected.len() {
                    let request = PageRequest::new(limit, cursor.take())?;
                    let page = snapshot
                        .list_directory_with_metadata(&directory, &request, metadata, &operation)
                        .await?;
                    names.extend(
                        page.items
                            .iter()
                            .map(|entry| entry.path.file_name().expect("child name").to_vec()),
                    );
                    cursor = page.next;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(
                    (&names, cursor.is_none()),
                    (&expected, true),
                    "metadata={metadata:?}, page size={limit}"
                );
            }
        }
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_cursor_binds_raw_path_tree_commit_and_page_shape() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let raw_path = GitPath::new(Bytes::from_static(b"raw"))?;
        let first = snapshot
            .list_directory(&raw_path, &PageRequest::new(1, None)?, &operation)
            .await?;
        assert_eq!(first.items[0].path.file_name(), Some(b"\xfe".as_slice()));
        let encoded = first.next.expect("continuation cursor");
        let decoded = PageCursor::from_bytes(Bytes::copy_from_slice(encoded.as_bytes()))?;
        let second = snapshot
            .list_directory(
                &raw_path,
                &PageRequest::new(1, Some(decoded.clone()))?,
                &operation,
            )
            .await?;
        assert_eq!(second.items[0].path.file_name(), Some(b"\xff".as_slice()));

        let mut absent_name = encoded.as_bytes().to_vec();
        *absent_name.last_mut().expect("cursor name") = 0xfd;
        let absent_name = PageCursor::from_bytes(Bytes::from(absent_name))?;
        let absent_entry = snapshot
            .list_directory(
                &raw_path,
                &PageRequest::new(1, Some(absent_name))?,
                &operation,
            )
            .await;
        assert!(matches!(
            absent_entry,
            Err(Error::InvalidCursor {
                reason: CursorError::ContextMismatch
            })
        ));

        let wrong_shape = snapshot
            .list_directory(
                &raw_path,
                &PageRequest::new(2, Some(decoded.clone()))?,
                &operation,
            )
            .await;
        assert!(matches!(
            wrong_shape,
            Err(Error::InvalidCursor {
                reason: CursorError::ContextMismatch
            })
        ));
        let wrong_path = snapshot
            .list_directory(
                &GitPath::root(),
                &PageRequest::new(1, Some(decoded))?,
                &operation,
            )
            .await;
        assert!(matches!(
            wrong_path,
            Err(Error::InvalidCursor {
                reason: CursorError::ContextMismatch
            })
        ));
        let malformed = PageCursor::from_bytes(Bytes::from_static(b"not-a-cursor"));
        assert!(matches!(
            malformed,
            Err(Error::InvalidCursor {
                reason: CursorError::Malformed
            })
        ));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn content_metadata_distinguishes_git_and_logical_representations() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let crab = snapshot
            .blob_metadata(
                &GitPath::new(Bytes::from_static(b"crab.pointer"))?,
                &operation,
            )
            .await?;
        assert_eq!(crab.classification, ContentClassification::CrabPointer);
        assert_eq!(crab.logical_size, Some(4_294_967_296));
        assert!(crab.git_size < 256);
        assert_eq!(
            (crab.mode, crab.kind),
            (EntryMode::Regular, EntryKind::Blob)
        );

        let lfs = snapshot
            .blob_metadata(
                &GitPath::new(Bytes::from_static(b"lfs.pointer"))?,
                &operation,
            )
            .await?;
        assert_eq!(lfs.classification, ContentClassification::LfsPointer);
        assert_eq!(lfs.logical_size, Some(8_589_934_592));

        let empty = snapshot
            .blob_metadata(&GitPath::new(Bytes::from_static(b"empty"))?, &operation)
            .await?;
        assert_eq!(empty.classification, ContentClassification::OrdinaryGit);
        assert_eq!((empty.git_size, empty.logical_size), (0, Some(0)));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_reads_are_immediate_and_metadata_is_page_bounded() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        fixture.backend.reset_pack_gets();
        let root = snapshot
            .list_directory(
                &GitPath::root(),
                &PageRequest::new(1_000, None)?,
                &operation,
            )
            .await?;
        let root_reads = fixture.backend.pack_gets();
        assert_eq!(root.items.len(), 1_000);
        assert!(root.next.is_some());
        assert!(root.items.iter().all(|entry| entry.size.is_none()));
        assert!(root_reads < 16, "listing recursively fetched descendants");

        let raw_path = GitPath::new(Bytes::from_static(b"raw"))?;
        fixture.backend.reset_pack_gets();
        let plain = snapshot
            .list_directory(&raw_path, &PageRequest::new(3, None)?, &operation)
            .await?;
        let plain_reads = fixture.backend.pack_gets();
        assert!(plain.items.iter().all(|entry| entry.size.is_none()));

        fixture.backend.reset_pack_gets();
        let metadata = snapshot
            .list_directory_with_metadata(
                &raw_path,
                &PageRequest::new(3, None)?,
                DirectoryMetadata::BlobSizes,
                &operation,
            )
            .await?;
        let metadata_reads = fixture.backend.pack_gets();
        assert!(metadata.items.iter().all(|entry| entry.size == Some(9)));
        assert!(plain_reads > 0);
        assert_eq!(metadata_reads, 1);
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_batch_concurrency_is_bounded_by_aggregate_byte_limits() {
    let object_limits = ObjectLimits::default();
    let operation_limits = OperationLimits {
        max_fetched_bytes: object_limits.max_packed_entry_bytes * 2,
        max_inflated_bytes: object_limits.max_inflated_entry_bytes * 2,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(object_limits, operation_limits).expect("valid limits");
    let runtime_options = RuntimeOptions {
        max_origin_concurrency: 64,
        ..RuntimeOptions::default()
    };
    let fixture = publish_with_runtime(DeltaKind::Ref, false, options, runtime_options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        snapshot
            .list_directory(&GitPath::root(), &PageRequest::new(40, None)?, &operation)
            .await?;

        fixture.backend.reset_pack_gets();
        fixture.backend.reset_pack_activity();
        fixture.backend.slow_pack_gets();
        let page = snapshot
            .list_directory_with_metadata(
                &GitPath::root(),
                &PageRequest::new(40, None)?,
                DirectoryMetadata::BlobSizes,
                &operation,
            )
            .await?;

        assert!(
            page.items
                .iter()
                .filter(|entry| entry.size.is_some())
                .count()
                > 16
        );
        // Coalescing can complete the page through one range lane; the
        // aggregate byte limit still caps the number of concurrent lanes.
        assert!((1..=2).contains(&fixture.backend.max_active_pack_gets()));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
    assert_runtime_is_within_configured_bounds(&fixture.runtime, runtime_options).await;
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_ref_delta_from_object_store_ranges() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let bytes = read_target(&fixture).await.expect("read REF delta");
    assert_eq!(bytes.as_ref(), fixture.expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_blob_reads_are_single_flight_and_warm_reads_hit_cache() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let setup_cancellation = CancellationToken::new();
    let setup_operation = fixture
        .repository
        .operation(OperationKind::Repository, &setup_cancellation)
        .await
        .expect("setup operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &setup_operation)
        .await
        .expect("snapshot");
    snapshot
        .entry(&fixture.base_path, &setup_operation)
        .await
        .expect("warm base path");
    setup_operation.finish(Ok(())).await.expect("finish setup");

    // A verified base blob isolates packed-entry single-flight. Delta reads
    // finish reconstruction after the shared packed entry is released.
    let mut operations = Vec::new();
    for _ in 0..16 {
        operations.push(
            fixture
                .repository
                .operation(OperationKind::Repository, &CancellationToken::new())
                .await
                .expect("concurrent operation"),
        );
    }
    fixture.backend.reset_pack_gets();
    let concurrent_snapshot = snapshot.clone();
    let concurrent_path = fixture.base_path.clone();
    let concurrent_expected = fixture.base_expected.clone();
    let reads_path = concurrent_path.clone();
    let reads = operations.into_iter().map(move |operation| {
        let snapshot = concurrent_snapshot.clone();
        let path = reads_path.clone();
        async move {
            let result = snapshot
                .read_blob(&path, &operation)
                .await
                .map(|blob| blob.bytes);
            operation.finish(result).await
        }
    });
    // Hold the first origin read open so every waiter observes the same
    // in-flight entry instead of relying on scheduler timing for coalescing.
    fixture.backend.block_next_pack_get();
    let reads_task = tokio::spawn(async move { futures_util::future::join_all(reads).await });
    fixture.backend.wait_for_blocked_pack_get().await;
    fixture.backend.release_blocked_pack_get();
    let results = reads_task.await.expect("concurrent reads join");
    for result in results {
        assert_eq!(
            result.expect("concurrent read").as_ref(),
            concurrent_expected
        );
    }
    let cold_reads = fixture.backend.pack_gets();
    assert!(cold_reads > 0);
    assert!(cold_reads < 16, "cold reads were not coalesced");

    fixture.backend.reset_pack_gets();
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("warm operation");
    let warm_path = concurrent_path.clone();
    let warm = snapshot
        .read_blob(&warm_path, &operation)
        .await
        .map(|blob| blob.bytes);
    assert_eq!(
        operation.finish(warm).await.expect("warm read").as_ref(),
        concurrent_expected
    );
    assert_eq!(fixture.backend.pack_gets(), 0);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_caches_preserve_canonical_read_results() {
    let runtime_options = RuntimeOptions {
        max_object_cache_entries: 0,
        max_object_cache_bytes: 0,
        max_pack_index_cache_entries: 0,
        max_pack_index_cache_bytes: 0,
        max_parsed_cache_entries: 0,
        max_parsed_cache_bytes: 0,
        max_manifest_cache_entries: 0,
        max_manifest_cache_bytes: 0,
        manifest_cache_ttl: Duration::ZERO,
        max_inventory_cache_entries: 0,
        max_inventory_cache_bytes: 0,
        max_negative_cache_entries: 0,
        max_negative_cache_bytes: 0,
        negative_cache_ttl: Duration::ZERO,
        ..RuntimeOptions::default()
    };
    let fixture = publish_with_runtime(
        DeltaKind::Ref,
        false,
        RepositoryOptions::default(),
        runtime_options,
    )
    .await;
    assert_eq!(
        read_target(&fixture).await.expect("first read").as_ref(),
        fixture.expected
    );
    fixture.backend.reset_pack_gets();
    assert_eq!(
        read_target(&fixture).await.expect("second read").as_ref(),
        fixture.expected
    );
    assert!(fixture.backend.pack_gets() > 0);
    assert_runtime_is_within_configured_bounds(&fixture.runtime, runtime_options).await;
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn object_cache_eviction_reconstructs_the_same_verified_bytes() {
    let runtime_options = RuntimeOptions {
        max_object_cache_entries: 1,
        max_object_cache_bytes: 128 * 1024,
        ..RuntimeOptions::default()
    };
    let fixture = publish_with_runtime(
        DeltaKind::Ref,
        false,
        RepositoryOptions::default(),
        runtime_options,
    )
    .await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &operation)
        .await
        .expect("snapshot");
    let first_path = &fixture.blob_paths[0].0;
    let first_expected = &fixture.blob_paths[0].1;
    let second_path = &fixture.blob_paths[1].0;
    assert_eq!(
        snapshot
            .read_blob(first_path, &operation)
            .await
            .expect("first blob")
            .bytes
            .as_ref(),
        first_expected
    );
    snapshot
        .read_blob(second_path, &operation)
        .await
        .expect("evict with second blob");
    fixture.backend.reset_pack_gets();
    assert_eq!(
        snapshot
            .read_blob(first_path, &operation)
            .await
            .expect("reconstructed first blob")
            .bytes
            .as_ref(),
        first_expected
    );
    assert!(fixture.backend.pack_gets() > 0);
    operation.finish(Ok(())).await.expect("finish operation");
    assert_runtime_is_within_configured_bounds(&fixture.runtime, runtime_options).await;
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_delta_objects_share_one_cold_base_fetch() {
    let fixture = publish(DeltaKind::RefShared, false, RepositoryOptions::default()).await;
    let [(first_path, first_expected), (second_path, second_expected)] =
        fixture.shared_paths.clone().expect("shared-base paths");
    let setup_cancellation = CancellationToken::new();
    let setup = fixture
        .repository
        .operation(OperationKind::Repository, &setup_cancellation)
        .await
        .expect("setup operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &setup)
        .await
        .expect("snapshot");
    snapshot
        .entry(&first_path, &setup)
        .await
        .expect("warm root tree");
    setup.finish(Ok(())).await.expect("finish setup");

    let first_cancellation = CancellationToken::new();
    let first_operation = fixture
        .repository
        .operation(OperationKind::Repository, &first_cancellation)
        .await
        .expect("first operation");
    let second_cancellation = CancellationToken::new();
    let second_operation = fixture
        .repository
        .operation(OperationKind::Repository, &second_cancellation)
        .await
        .expect("second operation");
    fixture.backend.reset_pack_gets();
    fixture.backend.block_pack_get_at(fixture.base_offset);
    let first_read = snapshot.read_blob(&first_path, &first_operation);
    let second_read = snapshot.read_blob(&second_path, &second_operation);
    let (first, second) = {
        let reads = async { tokio::join!(first_read, second_read) };
        tokio::pin!(reads);
        tokio::select! {
            _ = fixture.backend.wait_for_blocked_pack_get() => {}
            _ = &mut reads => panic!("first delta read completed before the shared base was blocked"),
        }
        tokio::task::yield_now().await;
        fixture.backend.release_blocked_pack_get();
        reads.as_mut().await
    };
    assert_eq!(first.expect("first blob").bytes.as_ref(), first_expected);
    assert_eq!(second.expect("second blob").bytes.as_ref(), second_expected);
    first_operation.finish(Ok(())).await.expect("finish first");
    second_operation
        .finish(Ok(()))
        .await
        .expect("finish second");
    assert_eq!(
        fixture.backend.pack_gets(),
        3,
        "two deltas must share their one full base fetch"
    );
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_distinct_reads_never_exceed_origin_admission_bound() {
    let runtime_options = RuntimeOptions {
        max_origin_concurrency: 2,
        ..RuntimeOptions::default()
    };
    let fixture = publish_with_runtime(
        DeltaKind::Ref,
        false,
        RepositoryOptions::default(),
        runtime_options,
    )
    .await;
    let setup_cancellation = CancellationToken::new();
    let setup = fixture
        .repository
        .operation(OperationKind::Repository, &setup_cancellation)
        .await
        .expect("setup operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &setup)
        .await
        .expect("snapshot");
    snapshot
        .entry(&fixture.blob_paths[0].0, &setup)
        .await
        .expect("warm root tree");
    setup.finish(Ok(())).await.expect("finish setup");

    let mut operations = Vec::new();
    for _ in 0..16 {
        operations.push(
            fixture
                .repository
                .operation(OperationKind::Repository, &CancellationToken::new())
                .await
                .expect("operation"),
        );
    }
    fixture.backend.slow_pack_gets();
    let reads =
        operations
            .into_iter()
            .zip(&fixture.blob_paths)
            .map(|(operation, (path, expected))| {
                let snapshot = snapshot.clone();
                let path = path.clone();
                let expected = expected.clone();
                async move {
                    let result = snapshot.read_blob(&path, &operation).await;
                    assert_eq!(result.as_ref().expect("blob").bytes.as_ref(), expected);
                    operation.finish(result.map(|_| ())).await
                }
            });
    for result in futures_util::future::join_all(reads).await {
        result.expect("finish read");
    }
    assert_eq!(fixture.backend.max_active_pack_gets(), 2);
    let occupancy = fixture.runtime.snapshot().await;
    assert_eq!(occupancy.active_object_flights, 0);
    assert_eq!(occupancy.active_pack_index_flights, 0);
    assert!(occupancy.object_entries <= runtime_options.max_object_cache_entries);
    assert!(occupancy.object_bytes <= runtime_options.max_object_cache_bytes);
    assert!(occupancy.parsed_entries <= runtime_options.max_parsed_cache_entries);
    assert!(occupancy.parsed_bytes <= runtime_options.max_parsed_cache_bytes);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_one_cold_waiter_does_not_cancel_shared_origin_work() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let setup_cancellation = CancellationToken::new();
    let setup_operation = fixture
        .repository
        .operation(OperationKind::Repository, &setup_cancellation)
        .await
        .expect("setup operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &setup_operation)
        .await
        .expect("snapshot");
    setup_operation.finish(Ok(())).await.expect("finish setup");

    fixture.backend.block_next_pack_get();
    let cancelled = CancellationToken::new();
    let cancelled_operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancelled)
        .await
        .expect("cancelled operation");
    let cancelled_snapshot = snapshot.clone();
    let cancelled_path = fixture.target_path.clone();
    let cancelled_task = tokio::spawn(async move {
        let result = cancelled_snapshot
            .read_blob(&cancelled_path, &cancelled_operation)
            .await;
        cancelled_operation.finish(result).await
    });
    fixture.backend.wait_for_blocked_pack_get().await;

    let survivor = CancellationToken::new();
    let survivor_operation = fixture
        .repository
        .operation(OperationKind::Repository, &survivor)
        .await
        .expect("survivor operation");
    let survivor_snapshot = snapshot.clone();
    let survivor_path = fixture.target_path.clone();
    let survivor_task = tokio::spawn(async move {
        let result = survivor_snapshot
            .read_blob(&survivor_path, &survivor_operation)
            .await;
        survivor_operation.finish(result).await
    });
    tokio::task::yield_now().await;
    cancelled.cancel();
    let cancelled_result = tokio::time::timeout(Duration::from_secs(1), cancelled_task)
        .await
        .expect("cancelled waiter returns promptly")
        .expect("cancelled task joins");
    assert!(matches!(cancelled_result, Err(Error::Cancelled)));

    fixture.backend.release_blocked_pack_get();
    let survivor_blob = tokio::time::timeout(Duration::from_secs(5), survivor_task)
        .await
        .expect("surviving waiter completes")
        .expect("surviving task joins")
        .expect("surviving read succeeds");
    assert_eq!(survivor_blob.bytes.as_ref(), fixture.expected);
    assert_runtime_is_within_configured_bounds(&fixture.runtime, RuntimeOptions::default()).await;
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_ofs_delta_using_verified_pack_index() {
    let fixture = publish(DeltaKind::Ofs, false, RepositoryOptions::default()).await;
    let bytes = read_target(&fixture).await.expect("read OFS delta");
    assert_eq!(bytes.as_ref(), fixture.expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_packed_entry_with_wrong_crc() {
    let fixture = publish(DeltaKind::Ref, true, RepositoryOptions::default()).await;
    let error = read_target(&fixture)
        .await
        .expect_err("CRC mismatch must fail");
    assert!(matches!(error, Error::PackedEntryCrcMismatch { .. }));
    assert_runtime_is_within_configured_bounds(&fixture.runtime, RuntimeOptions::default()).await;
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_prevents_operation_start() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = match fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
    {
        Ok(_) => panic!("cancelled operation must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Cancelled));
}

#[tokio::test(flavor = "multi_thread")]
async fn operation_deadline_cancels_work_and_reports_timeout() {
    let operation_limits = OperationLimits {
        max_duration: Duration::from_secs(1),
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation_limits)
        .expect("repository options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation opens before deadline");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let result = async {
        fixture
            .repository
            .snapshot(
                &Revision::Reference("refs/heads/main".to_owned()),
                &operation,
            )
            .await?;
        Ok(())
    }
    .await;
    let error = operation
        .finish(result)
        .await
        .expect_err("expired operation must fail");
    assert!(matches!(
        error,
        Error::Timeout {
            operation: "repository operation"
        }
    ));
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_operation_cancels_deadline_and_shutdown_drains_cleanup() {
    let operation_limits = OperationLimits {
        max_duration: Duration::from_secs(60),
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation_limits)
        .expect("repository options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");

    drop(operation);

    tokio::time::timeout(Duration::from_secs(2), fixture.runtime.shutdown())
        .await
        .expect("shutdown drains dropped operation cleanup before its deadline");
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_shutdown_cancels_and_waits_for_active_operation_cleanup() {
    let operation_limits = OperationLimits {
        max_duration: Duration::from_secs(60),
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation_limits)
        .expect("repository options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let operation_cancellation = operation.cancellation().clone();
    let runtime = Arc::clone(&fixture.runtime);
    let mut shutdown = tokio::spawn(async move {
        runtime.shutdown().await;
    });

    tokio::time::timeout(Duration::from_secs(1), operation_cancellation.cancelled())
        .await
        .expect("runtime shutdown cancels active operations");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err(),
        "runtime reported drained while an operation still owned its locator"
    );

    drop(operation);

    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("shutdown drains operation cleanup")
        .expect("shutdown task joins");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_entry_before_fetch_when_packed_budget_is_too_small() {
    let object = ObjectLimits {
        max_packed_entry_bytes: 1,
        ..ObjectLimits::default()
    };
    let options = RepositoryOptions::new(object, Default::default()).expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let error = read_target(&fixture)
        .await
        .expect_err("packed entry budget must fail");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "packed entry bytes",
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_path_stops_at_the_traversal_depth_budget() {
    let operation = OperationLimits {
        max_depth: 2,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation).expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Entry, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(
                &Revision::Reference("refs/heads/main".to_owned()),
                &operation,
            )
            .await?;
        let path = GitPath::new(Bytes::from_static(b"dir/nested/deep.txt"))?;
        snapshot.entry(&path, &operation).await
    }
    .await;
    let error = operation
        .finish(result)
        .await
        .expect_err("the third component must exceed the depth budget");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "traversal depth",
            actual: 3,
            maximum: 2,
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_fetch_budget_rejects_before_origin_range_read() {
    let operation = OperationLimits {
        max_fetched_bytes: 1,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation).expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    fixture.backend.reset_pack_gets();
    let error = read_target(&fixture)
        .await
        .expect_err("aggregate fetched bytes must fail");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "fetched bytes",
            ..
        }
    ));
    assert_eq!(fixture.backend.pack_gets(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_inflated_budget_rejects_before_decode_and_cache_insert() {
    let operation = OperationLimits {
        max_inflated_bytes: 1,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation).expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let first = read_target(&fixture)
        .await
        .expect_err("aggregate inflated bytes must fail");
    assert!(
        contains_limit_exceeded(&first, "inflated bytes"),
        "unexpected first error: {first:?}"
    );

    fixture.backend.reset_pack_gets();
    let second = read_target(&fixture)
        .await
        .expect_err("failed decode must not become a cache hit");
    assert!(
        contains_limit_exceeded(&second, "inflated bytes"),
        "unexpected second error: {second:?}"
    );
    assert!(fixture.backend.pack_gets() > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn warm_object_cache_cannot_bypass_the_logical_work_budget() {
    let operation_limits = OperationLimits {
        max_logical_objects: 10,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation_limits)
        .expect("repository options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    read_target(&fixture).await.expect("prime verified caches");
    fixture.backend.reset_pack_gets();

    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result: crab_remote_git::Result<()> = async {
        let snapshot = fixture
            .repository
            .snapshot(
                &Revision::Reference("refs/heads/main".to_owned()),
                &operation,
            )
            .await?;
        loop {
            snapshot.read_blob(&fixture.target_path, &operation).await?;
        }
    }
    .await;
    let error = operation
        .finish(result)
        .await
        .expect_err("logical work must remain bounded on cache hits");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "logical objects",
            ..
        }
    ));
    assert_eq!(fixture.backend.pack_gets(), 0);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_response_budget_is_enforced_after_verified_read() {
    let operation = OperationLimits {
        max_response_bytes: 1,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation).expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let error = read_target(&fixture)
        .await
        .expect_err("response bytes must fail");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "response bytes",
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_oversized_blob_before_allocating_its_result() {
    let object = ObjectLimits {
        max_object_bytes: 50 * 1_024,
        ..ObjectLimits::default()
    };
    let options = RepositoryOptions::new(object, Default::default()).expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let error = read_target(&fixture)
        .await
        .expect_err("oversized decoded blob must fail");
    assert!(
        contains_limit_exceeded(&error, "decoded object bytes"),
        "unexpected oversized decoded blob error: {error:?}"
    );

    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("metadata operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let metadata = snapshot
            .blob_metadata(&fixture.target_path, &operation)
            .await?;
        assert_eq!(metadata.git_size, fixture.expected.len() as u64);
        assert_eq!(metadata.classification, ContentClassification::OrdinaryGit);
        assert_eq!(metadata.logical_size, Some(fixture.expected.len() as u64));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish metadata");
}

#[tokio::test(flavor = "multi_thread")]
async fn history_pages_are_deterministic_and_bound_to_start_and_mode() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let first = snapshot
            .history(
                HistoryTraversal::AllParents,
                &PageRequest::new(1, None)?,
                &operation,
            )
            .await?;
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].oid, snapshot.commit_oid());
        let cursor = first.next.expect("history continuation");
        let second = snapshot
            .history(
                HistoryTraversal::AllParents,
                &PageRequest::new(1, Some(cursor.clone()))?,
                &operation,
            )
            .await?;
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].oid, fixture.root_commit);
        assert!(second.next.is_some());
        assert!(matches!(
            snapshot
                .history(
                    HistoryTraversal::FirstParent,
                    &PageRequest::new(1, Some(cursor))?,
                    &operation,
                )
                .await,
            Err(Error::InvalidCursor {
                reason: CursorError::ContextMismatch
            })
        ));
        let first_parent = snapshot
            .history(
                HistoryTraversal::FirstParent,
                &PageRequest::new(10, None)?,
                &operation,
            )
            .await?;
        assert_eq!(first_parent.items.len(), 2);
        assert!(first_parent.next.is_none());
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn incomplete_commit_graph_cannot_hide_raw_history() {
    let fixture = publish_with_summary(DeltaKind::Ref, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let page = snapshot
            .history(
                HistoryTraversal::AllParents,
                &PageRequest::new(10, None)?,
                &operation,
            )
            .await?;
        assert_eq!(
            page.items
                .iter()
                .map(|commit| commit.oid)
                .collect::<Vec<_>>(),
            vec![
                snapshot.commit_oid(),
                fixture.root_commit,
                fixture.side_commit
            ]
        );
        assert!(page.next.is_none());
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn path_history_uses_raw_commits_and_omits_unchanged_merges() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let history = snapshot
            .path_history(
                &fixture.target_path,
                HistoryTraversal::AllParents,
                &PageRequest::new(10, None)?,
                &operation,
            )
            .await?;
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.items[0].commit.oid, fixture.root_commit);
        assert_eq!(history.items[0].kind, crab_remote_git::ChangeKind::Added);
        assert!(history.next.is_none());
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn semantic_changes_cover_modes_types_rename_like_paths_binary_and_pointers() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let base = fixture
            .repository
            .snapshot(&Revision::Reference("semantic-base".to_owned()), &operation)
            .await?;
        let head = fixture
            .repository
            .snapshot(&Revision::Reference("semantic-head".to_owned()), &operation)
            .await?;
        assert_eq!(base.commit_oid(), fixture.semantic_base_commit);
        assert_eq!(head.commit_oid(), fixture.semantic_head_commit);

        let comparison = head.compare(&base, &operation).await?;
        for (path, expected) in [
            (b"binary.dat".as_slice(), ChangeKind::Modified),
            (b"mode.txt".as_slice(), ChangeKind::ModeChanged),
            (b"new-name.txt".as_slice(), ChangeKind::Added),
            (b"old-name.txt".as_slice(), ChangeKind::Deleted),
            (b"pointer.crab".as_slice(), ChangeKind::Modified),
            (b"text.txt".as_slice(), ChangeKind::Modified),
            (b"type".as_slice(), ChangeKind::TypeChanged),
        ] {
            assert_eq!(
                comparison
                    .changes
                    .iter()
                    .find(|change| change.path.as_bytes() == path)
                    .map(|change| change.kind),
                Some(expected),
                "unexpected comparison result for {}",
                String::from_utf8_lossy(path)
            );
        }
        assert!(
            comparison
                .changes
                .iter()
                .all(|change| !change.path.as_bytes().starts_with(b"same/"))
        );

        for (path, expected) in [
            (b"mode.txt".as_slice(), ChangeKind::ModeChanged),
            (b"type".as_slice(), ChangeKind::TypeChanged),
            (b"old-name.txt".as_slice(), ChangeKind::Deleted),
            (b"new-name.txt".as_slice(), ChangeKind::Added),
        ] {
            let path = GitPath::new(Bytes::copy_from_slice(path))?;
            let history = head
                .path_history(
                    &path,
                    HistoryTraversal::FirstParent,
                    &PageRequest::new(10, None)?,
                    &operation,
                )
                .await?;
            assert_eq!(
                history.items.first().map(|entry| entry.kind),
                Some(expected)
            );
        }

        let binary = head
            .diff(
                &base,
                &GitPath::new(Bytes::from_static(b"binary.dat"))?,
                &operation,
            )
            .await?;
        assert_eq!(binary.classification, DiffClassification::Binary);
        assert!(binary.hunks.is_empty());
        let pointer = head
            .diff(
                &base,
                &GitPath::new(Bytes::from_static(b"pointer.crab"))?,
                &operation,
            )
            .await?;
        assert_eq!(pointer.classification, DiffClassification::CrabPointer);
        assert!(pointer.hunks.is_empty());
        let text = head
            .diff(
                &base,
                &GitPath::new(Bytes::from_static(b"text.txt"))?,
                &operation,
            )
            .await?;
        assert_eq!(text.classification, DiffClassification::Text);
        assert_eq!(text.hunks.len(), 1);
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn textual_diff_reports_too_large_without_partial_hunks() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let strict_options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_diff_input_bytes: 1,
            ..OperationLimits::default()
        },
    )
    .expect("strict options");
    let cancellation = CancellationToken::new();
    let repository = RemoteGitRepository::open(
        fixture.store.clone(),
        fixture.layout.clone(),
        crab_remote_git::RepositoryIdentity::new("memory", "org/repo", 1).expect("identity"),
        Arc::clone(&fixture.runtime),
        strict_options,
        &cancellation,
    )
    .await
    .expect("strict repository");
    let operation = repository
        .operation(OperationKind::Diff, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let base = repository
            .snapshot(&Revision::Reference("semantic-base".to_owned()), &operation)
            .await?;
        let head = repository
            .snapshot(&Revision::Reference("semantic-head".to_owned()), &operation)
            .await?;
        let diff = head
            .diff(
                &base,
                &GitPath::new(Bytes::from_static(b"text.txt"))?,
                &operation,
            )
            .await?;
        assert_eq!(diff.classification, DiffClassification::TooLarge);
        assert!(diff.hunks.is_empty());
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn repository_semantics_match_native_git_ground_truth() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let base_oid = fixture.semantic_base_commit.to_string();
    let head_oid = fixture.semantic_head_commit.to_string();
    let native_history = native_oid_lines(
        &fixture.source_git_dir,
        &["rev-list", "--first-parent", &head_oid],
    );
    let native_path_history = native_oid_lines(
        &fixture.source_git_dir,
        &["log", "--format=%H", &head_oid, "--", "mode.txt"],
    );
    let native_changes = native_changes(&fixture.source_git_dir, &base_oid, &head_oid);
    let native_diff = git(
        &[
            "--git-dir",
            path(&fixture.source_git_dir),
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--unified=0",
            &base_oid,
            &head_oid,
            "--",
            "text.txt",
        ],
        None,
    )
    .split(|byte| *byte == b'\n')
    .filter(|line| {
        (line.starts_with(b"+") && !line.starts_with(b"+++"))
            || (line.starts_with(b"-") && !line.starts_with(b"---"))
    })
    .flat_map(|line| line.iter().copied().chain(*b"\n"))
    .collect::<Vec<_>>();
    let native_blame_output = git(
        &[
            "--git-dir",
            path(&fixture.source_git_dir),
            "blame",
            "--line-porcelain",
            &head_oid,
            "--",
            "text.txt",
        ],
        None,
    );
    let native_blame = parse_oid(
        native_blame_output
            .split(|byte| byte.is_ascii_whitespace())
            .next()
            .expect("native blame object ID"),
    );
    let native_archive = native_tree_entries(&fixture.source_git_dir, &head_oid);

    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let base = fixture
            .repository
            .snapshot(&Revision::Reference("semantic-base".to_owned()), &operation)
            .await?;
        let head = fixture
            .repository
            .snapshot(&Revision::Reference("semantic-head".to_owned()), &operation)
            .await?;
        let history = head
            .history(
                HistoryTraversal::FirstParent,
                &PageRequest::new(10, None)?,
                &operation,
            )
            .await?;
        assert_eq!(
            history
                .items
                .iter()
                .map(|commit| commit.oid)
                .collect::<Vec<_>>(),
            native_history
        );

        let mode_path = GitPath::new(Bytes::from_static(b"mode.txt"))?;
        let path_history = head
            .path_history(
                &mode_path,
                HistoryTraversal::FirstParent,
                &PageRequest::new(10, None)?,
                &operation,
            )
            .await?;
        assert_eq!(
            path_history
                .items
                .iter()
                .map(|entry| entry.commit.oid)
                .collect::<Vec<_>>(),
            native_path_history
        );

        let comparison = head.compare(&base, &operation).await?;
        assert_eq!(
            comparison
                .changes
                .iter()
                .map(|change| (change.path.as_bytes().to_vec(), change.kind))
                .collect::<BTreeMap<_, _>>(),
            native_changes
        );

        let diff = head
            .diff(
                &base,
                &GitPath::new(Bytes::from_static(b"text.txt"))?,
                &operation,
            )
            .await?;
        assert_eq!(diff.hunks.len(), 1);
        assert_eq!(diff.hunks[0].bytes.as_ref(), native_diff);

        let blame = head
            .blame(&GitPath::new(Bytes::from_static(b"text.txt"))?, &operation)
            .await?;
        assert_eq!(blame.ranges.len(), 1);
        assert_eq!(blame.ranges[0].commit.oid, native_blame);

        let archive = head.archive(&operation).await?;
        assert_eq!(
            archive
                .iter()
                .map(|entry| (entry.path.as_bytes().to_vec(), entry.mode.raw(), entry.oid))
                .collect::<Vec<_>>(),
            native_archive
        );
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn comparison_prunes_an_identical_root_tree() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let head = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let base = fixture
            .repository
            .snapshot(&Revision::Reference("root".to_owned()), &operation)
            .await?;
        fixture.backend.reset_pack_gets();
        let comparison = head.compare(&base, &operation).await?;
        assert_eq!(comparison.base, fixture.root_commit);
        assert_eq!(comparison.head, head.commit_oid());
        assert!(comparison.changes.is_empty());
        assert_eq!(fixture.backend.pack_gets(), 0);
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_classifies_equal_text_and_pointer_representations() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let head = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let base = fixture
            .repository
            .snapshot(&Revision::Reference("root".to_owned()), &operation)
            .await?;
        let text = head.diff(&base, &fixture.target_path, &operation).await?;
        assert_eq!(
            text.classification,
            crab_remote_git::DiffClassification::Text
        );
        assert!(text.hunks.is_empty());
        let pointer = head
            .diff(
                &base,
                &GitPath::new(Bytes::from_static(b"crab.pointer"))?,
                &operation,
            )
            .await?;
        assert_eq!(
            pointer.classification,
            crab_remote_git::DiffClassification::CrabPointer
        );
        assert!(pointer.hunks.is_empty());
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn blame_attributes_unchanged_lines_to_the_first_parent_root() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let blame = snapshot.blame(&fixture.target_path, &operation).await?;
        assert_eq!(blame.commit, snapshot.commit_oid());
        assert_eq!(blame.path, fixture.target_path);
        assert_eq!(blame.ranges.len(), 1);
        assert_eq!(blame.ranges[0].start, 1);
        assert_eq!(blame.ranges[0].lines, 2);
        assert_eq!(blame.ranges[0].commit.oid, fixture.root_commit);
        assert_eq!(blame.ranges[0].source_path, fixture.target_path);
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn blame_rejects_pointer_content_without_materializing_artifacts() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        assert!(matches!(
            snapshot
                .blame(
                    &GitPath::new(Bytes::from_static(b"crab.pointer"))?,
                    &operation,
                )
                .await,
            Err(Error::BlameUnsupported {
                reason: BlameUnsupportedReason::CrabPointer,
            })
        ));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn blame_unchanged_blob_does_not_spend_comparison_budget() {
    let options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_blame_comparison_cells: 1,
            ..OperationLimits::default()
        },
    )
    .expect("options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let blame = snapshot.blame(&fixture.target_path, &operation).await?;
        assert_eq!(blame.ranges.len(), 1);
        assert_eq!(blame.ranges[0].commit.oid, fixture.root_commit);
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn immutable_blame_cache_is_bounded_and_replays_semantic_budgets() {
    let runtime_options = RuntimeOptions {
        max_blame_cache_entries: 1,
        max_blame_cache_bytes: 1024 * 1024,
        ..RuntimeOptions::default()
    };
    let fixture = publish_with_runtime(
        DeltaKind::Ref,
        false,
        RepositoryOptions::default(),
        runtime_options,
    )
    .await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &operation)
        .await
        .expect("snapshot");
    let first = snapshot
        .blame(&fixture.target_path, &operation)
        .await
        .expect("cold blame");
    operation.finish(Ok(())).await.expect("finish cold blame");
    assert_eq!(fixture.runtime.snapshot().await.blame_entries, 1);

    fixture.backend.reset_pack_gets();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let warm = snapshot
        .blame(&fixture.target_path, &operation)
        .await
        .expect("warm blame");
    operation.finish(Ok(())).await.expect("finish warm blame");
    assert_eq!(warm, first);
    assert_eq!(fixture.backend.pack_gets(), 0);

    let strict_options = RepositoryOptions::new(
        ObjectLimits::default(),
        OperationLimits {
            max_blame_lines: 1,
            ..OperationLimits::default()
        },
    )
    .expect("strict options");
    let strict_repository = RemoteGitRepository::open(
        fixture.store.clone(),
        fixture.layout.clone(),
        crab_remote_git::RepositoryIdentity::new("memory", "org/repo", 1).expect("identity"),
        Arc::clone(&fixture.runtime),
        strict_options,
        &cancellation,
    )
    .await
    .expect("strict repository");
    let operation = strict_repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("strict operation");
    let strict_snapshot = strict_repository
        .snapshot(&Revision::Reference("main".to_owned()), &operation)
        .await
        .expect("strict snapshot");
    let error = strict_snapshot
        .blame(&fixture.target_path, &operation)
        .await
        .expect_err("cached blame must honor the stricter line budget");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "blame lines",
            actual: 2,
            maximum: 1,
        }
    ));
    operation.finish(Ok(())).await.expect("finish strict blame");

    let other_path = fixture
        .blob_paths
        .iter()
        .map(|(path, _)| path)
        .find(|path| *path != &fixture.target_path)
        .expect("second blame path");
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("eviction operation");
    snapshot
        .blame(other_path, &operation)
        .await
        .expect("second blame");
    operation.finish(Ok(())).await.expect("finish second blame");
    let runtime = fixture.runtime.snapshot().await;
    assert_eq!(runtime.blame_entries, 1);
    assert!(runtime.blame_bytes <= runtime_options.max_blame_cache_bytes);
    fixture.runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_traversal_preserves_modes_links_submodules_and_raw_order() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let snapshot = fixture
            .repository
            .snapshot(&Revision::Reference("main".to_owned()), &operation)
            .await?;
        let archive = snapshot.archive(&operation).await?;
        assert!(archive.windows(2).all(|pair| pair[0].path < pair[1].path));
        let link = archive
            .iter()
            .find(|entry| entry.path.as_bytes() == b"link")
            .expect("symlink");
        assert_eq!(link.mode, EntryMode::Symlink);
        assert_eq!(
            link.bytes.as_deref(),
            Some(b"dir/nested/deep.txt".as_slice())
        );
        let module = archive
            .iter()
            .find(|entry| entry.path.as_bytes() == b"module")
            .expect("submodule");
        assert_eq!(module.mode, EntryMode::Submodule);
        assert!(module.bytes.is_none());
        assert!(archive.iter().any(|entry| {
            entry.path.as_bytes() == b"dir/nested/deep.txt"
                && entry.bytes.as_deref() == Some(b"deep content\n".as_slice())
        }));
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_stream_is_incremental_and_cancellation_terminates_with_cleanup() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let snapshot = fixture
        .repository
        .snapshot(&Revision::Reference("main".to_owned()), &operation)
        .await
        .expect("snapshot");
    let mut stream = snapshot.archive_stream(operation).expect("archive stream");
    assert!(
        stream
            .next()
            .await
            .transpose()
            .expect("first entry")
            .is_some()
    );

    cancellation.cancel();

    assert!(matches!(stream.next().await, Some(Err(Error::Cancelled))));
}

#[tokio::test(flavor = "multi_thread")]
async fn parses_merge_signature_nested_tags_and_root_commit() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let result = async {
        let main = Revision::Reference("main".to_owned());
        let snapshot = fixture.repository.snapshot(&main, &operation).await?;
        let commit = snapshot.commit(&operation).await?;
        assert_eq!(commit.parents.len(), 2);
        assert_eq!(commit.encoding.as_deref(), Some(b"UTF-8".as_slice()));
        assert_eq!(commit.signature_headers.len(), 1);

        let root = fixture
            .repository
            .resolve(&Revision::Commit(fixture.root_commit), &operation)
            .await?;
        assert_eq!(root.commit, fixture.root_commit);

        let tag = fixture
            .repository
            .resolve(&Revision::Reference("v2".to_owned()), &operation)
            .await?;
        assert_eq!(tag.commit, snapshot.commit_oid());
        assert_eq!(tag.tags.len(), 2);
        assert!(tag.tags[0].signature.is_some());
        Ok(())
    }
    .await;
    operation.finish(result).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_wrong_kind_and_unreachable_retained_commit() {
    let fixture = publish(DeltaKind::Ref, false, RepositoryOptions::default()).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let wrong_kind = fixture
        .repository
        .resolve(&Revision::Reference("blob".to_owned()), &operation)
        .await;
    assert!(matches!(
        wrong_kind,
        Err(Error::Revision {
            reason: crab_remote_git::RevisionError::NotCommit
        })
    ));
    let unreachable = fixture
        .repository
        .resolve(&Revision::Commit(fixture.unreachable), &operation)
        .await;
    assert!(matches!(
        unreachable,
        Err(Error::Revision {
            reason: crab_remote_git::RevisionError::NotReachable
        })
    ));
    operation.finish(Ok(())).await.expect("finish operation");
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_commit_resolution_checks_both_merge_parents_before_deeper_history() {
    for (first_parent, maximum) in [(true, 2), (false, 3)] {
        let options = RepositoryOptions::new(
            ObjectLimits::default(),
            OperationLimits {
                max_history_commits: maximum,
                ..OperationLimits::default()
            },
        )
        .expect("repository options");
        let fixture = publish(DeltaKind::Ref, false, options).await;
        let (mut manifest, etag) = read_manifest(&fixture.store, &fixture.layout)
            .await
            .expect("manifest");
        manifest.refs.retain(|name, _| name == "refs/heads/main");
        manifest.peeled_refs.clear();
        manifest.seal_git_validation();
        write_manifest_cas(&fixture.store, &fixture.layout, &manifest, &etag)
            .await
            .expect("publish merge as the only ref");
        let cancellation = CancellationToken::new();
        let repository = RemoteGitRepository::open(
            fixture.store.clone(),
            fixture.layout.clone(),
            fixture.repository.identity().clone(),
            Arc::clone(&fixture.runtime),
            options,
            &cancellation,
        )
        .await
        .expect("reopen fixture");
        let target = if first_parent {
            fixture.root_commit
        } else {
            fixture.side_commit
        };
        let operation = repository
            .operation(OperationKind::Resolve, &cancellation)
            .await
            .expect("operation");
        let result = repository
            .resolve(&Revision::Commit(target), &operation)
            .await;
        let resolved = operation.finish(result).await;
        fixture.runtime.shutdown().await;
        assert_eq!(
            resolved.expect("nearby parent fits the budget").commit,
            target
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_commit_reachability_stops_at_the_operation_budget() {
    let operation_limits = OperationLimits {
        max_history_commits: 1,
        ..OperationLimits::default()
    };
    let options = RepositoryOptions::new(ObjectLimits::default(), operation_limits)
        .expect("repository options");
    let fixture = publish(DeltaKind::Ref, false, options).await;
    let cancellation = CancellationToken::new();
    let operation = fixture
        .repository
        .operation(OperationKind::Repository, &cancellation)
        .await
        .expect("operation");
    let error = fixture
        .repository
        .resolve(&Revision::Commit(fixture.unreachable), &operation)
        .await
        .expect_err("reachability must stop at its budget");
    assert!(matches!(
        error,
        Error::LimitExceeded {
            limit: "history commits",
            ..
        }
    ));
    operation.finish(Ok(())).await.expect("finish operation");
}

fn path(path: &Path) -> &str {
    path.to_str().expect("test path UTF-8")
}

fn git(args: &[&str], stdin: Option<&[u8]>) -> Vec<u8> {
    let mut child = Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "Crab Test")
        .env("GIT_AUTHOR_EMAIL", "test@crab.invalid")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "Crab Test")
        .env("GIT_COMMITTER_EMAIL", "test@crab.invalid")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn git");
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(input)
            .expect("write git stdin");
    }
    let output = child.wait_with_output().expect("wait for git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn hash_object(git_dir: &Path, bytes: &[u8]) -> gix_hash::ObjectId {
    parse_oid(&git(
        &["--git-dir", path(git_dir), "hash-object", "-w", "--stdin"],
        Some(bytes),
    ))
}

fn hash_typed_object(git_dir: &Path, kind: &str, bytes: &[u8]) -> gix_hash::ObjectId {
    parse_oid(&git(
        &[
            "--git-dir",
            path(git_dir),
            "hash-object",
            "-t",
            kind,
            "-w",
            "--stdin",
        ],
        Some(bytes),
    ))
}

fn make_tree(
    git_dir: &Path,
    entries: &[(u32, &str, gix_hash::ObjectId, &[u8])],
) -> gix_hash::ObjectId {
    let entries = entries
        .iter()
        .map(|(mode, kind, oid, name)| (*mode, *kind, *oid, name.to_vec()))
        .collect::<Vec<_>>();
    make_tree_owned(git_dir, &entries)
}

fn make_tree_owned(
    git_dir: &Path,
    entries: &[(u32, &str, gix_hash::ObjectId, Vec<u8>)],
) -> gix_hash::ObjectId {
    let mut input = Vec::new();
    for (mode, kind, oid, name) in entries {
        write!(&mut input, "{mode:o} {kind} {oid}\t").expect("write tree header");
        input.extend_from_slice(name);
        input.push(0);
    }
    parse_oid(&git(
        &["--git-dir", path(git_dir), "mktree", "-z"],
        Some(&input),
    ))
}

fn native_oid_lines(git_dir: &Path, args: &[&str]) -> Vec<gix_hash::ObjectId> {
    let mut command = vec!["--git-dir", path(git_dir)];
    command.extend_from_slice(args);
    git(&command, None)
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|value| !value.is_empty())
        .map(parse_oid)
        .collect()
}

fn native_changes(git_dir: &Path, base: &str, head: &str) -> BTreeMap<Vec<u8>, ChangeKind> {
    let output = git(
        &[
            "--git-dir",
            path(git_dir),
            "diff-tree",
            "--raw",
            "--no-abbrev",
            "--no-commit-id",
            "--no-renames",
            base,
            head,
        ],
        None,
    );
    let mut changes = BTreeMap::new();
    for line in output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let tab = line
            .iter()
            .position(|byte| *byte == b'\t')
            .expect("raw diff path");
        let header = std::str::from_utf8(&line[1..tab]).expect("raw diff header");
        let mut fields = header.split_ascii_whitespace();
        let old_mode =
            u32::from_str_radix(fields.next().expect("old mode"), 8).expect("octal old mode");
        let new_mode =
            u32::from_str_radix(fields.next().expect("new mode"), 8).expect("octal new mode");
        let old_oid = fields.next().expect("old object ID");
        let new_oid = fields.next().expect("new object ID");
        let _status = fields.next().expect("diff status");
        let kind = if old_mode == 0 {
            ChangeKind::Added
        } else if new_mode == 0 {
            ChangeKind::Deleted
        } else if old_mode & 0o170000 != new_mode & 0o170000 {
            ChangeKind::TypeChanged
        } else if old_oid != new_oid {
            ChangeKind::Modified
        } else {
            assert_ne!(old_mode, new_mode, "native diff emitted no change");
            ChangeKind::ModeChanged
        };
        let path = line[tab + 1..].to_vec();
        if let Some(previous) = changes.insert(path.clone(), kind) {
            assert!(
                matches!(
                    (previous, kind),
                    (ChangeKind::Added, ChangeKind::Deleted)
                        | (ChangeKind::Deleted, ChangeKind::Added)
                ),
                "only a native blob/tree replacement may repeat one path"
            );
            changes.insert(path, ChangeKind::TypeChanged);
        }
    }
    changes
}

fn native_tree_entries(git_dir: &Path, revision: &str) -> Vec<(Vec<u8>, u32, gix_hash::ObjectId)> {
    git(
        &[
            "--git-dir",
            path(git_dir),
            "ls-tree",
            "-r",
            "-t",
            "-z",
            "--full-tree",
            revision,
        ],
        None,
    )
    .split(|byte| *byte == 0)
    .filter(|record| !record.is_empty())
    .map(|record| {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .expect("tree entry path");
        let header = std::str::from_utf8(&record[..tab]).expect("tree entry header");
        let mut fields = header.split_ascii_whitespace();
        let mode =
            u32::from_str_radix(fields.next().expect("tree mode"), 8).expect("octal tree mode");
        let _kind = fields.next().expect("tree object kind");
        let oid = gix_hash::ObjectId::from_hex(fields.next().expect("tree object ID").as_bytes())
            .expect("tree object ID");
        (record[tab + 1..].to_vec(), mode, oid)
    })
    .collect()
}

fn parse_oid(bytes: &[u8]) -> gix_hash::ObjectId {
    gix_hash::ObjectId::from_hex(
        String::from_utf8(bytes.to_vec())
            .expect("object ID UTF-8")
            .trim()
            .as_bytes(),
    )
    .expect("parse object ID")
}
