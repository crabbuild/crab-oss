# crab-remote-git

`crab-remote-git` is the canonical filesystem-free read API for Git data stored
by Crab. It reads committed manifests, immutable pack inventories, exact object
locations, pack ranges, and typed Git objects directly from `crab-storage`. It
does not clone a repository or create a local object database.

## Consistency model

A repository handle is pinned to one validated tuple:

- manifest generation;
- immutable pack-inventory hash;
- exact object-locator coverage.

Opening retries that complete handshake once when publication races a reader.
An older or absent locator returns `RepositoryIndexing`; inconsistent newer
metadata fails closed. A snapshot then pins a reachable commit and root tree.

The caller supplies `RepositoryIdentity`, including the current physical
placement generation. This identity scopes every shared cache and single-flight
key. A managed service must authorize and resolve the active placement before
constructing it.

## Public API

The supported entry points are:

- `RemoteGitRuntime`: process-wide bounded caches, origin/decode admission, and
  metrics;
- `RemoteGitRepository::open`: generation-consistent repository open;
- `RemoteGitRepository::is_current`: metadata-only manifest identity check for
  safely reusing a pinned immutable handle;
- `RemoteGitRepository::operation`: one typed operation kind,
  cancellation-aware locator session, protected correlation ID, and aggregate
  work budget;
- `RemoteGitRepository::{refs,resolve,snapshot}`: ref and reachable-revision
  selection;
- `RemoteGitRepository::{generate_pack,generate_pack_cached,generate_pack_request_cached}`:
  verified response packs, with immutable reuse after selection or before an
  exact request is planned;
- `RemoteGitSnapshot::{entry,list_directory,blob_metadata,read_blob}`: browser
  navigation and Git-representation content;
- `RemoteGitSnapshot::{history,path_history,compare,diff,blame}`: bounded Git
  semantics without a checkout;
- `RemoteGitSnapshot::{archive,archive_stream}`: bounded traversal, with the
  stream owning operation cleanup.

Paths and cursor payloads are opaque bytes. Callers must preserve `GitPath`
bytes at transport boundaries and must sign `PageCursor` values before exposing
them to untrusted clients.

Every operation must be finalized with `OperationContext::finish`. Streaming
archive traversal owns and finalizes the context itself. Dropping either uses a
tracked best-effort cleanup fallback, while explicit completion preserves close
errors. `OperationLimits::max_duration` bounds locator open and semantic work;
expiration cancels the operation and returns a typed timeout.

## Performance model

Object storage is the correctness authority. Runtime memory is disposable and
bounded. Exact locators avoid pack scans, range reads avoid complete pack
downloads, immutable reads are single-flight, and object, parsed-object,
manifest, inventory, negative, blame-result, and pack-index caches are byte
bounded. Cached blame results remain subject to the current operation's
logical, traversal, history, blame, and response limits; a warm result cannot
bypass a stricter caller budget.
Batch scheduling is lazy and its concurrency is the minimum of origin,
blocking-decode, object-flight, logical-object, storage-request, fetched-byte,
and inflated-byte limits. Archive traversal produces one entry at a time; its
pending tree work is bounded by the verified tree-object limit.
Services may keep a bounded cache of cloned immutable repository handles and
use `is_current` after a short freshness interval. A changed manifest always
requires a new complete open handshake; cached state is never refreshed in
place.

Response packs can be persisted beneath the repository's immutable
`generated-packs/v1` namespace. Selection-bound keys cover physical repository
identity, manifest Git state, the visible authorization union, canonical
request semantics, output policy, and canonicalized object selection.
Request-bound keys let identical non-deepening shallow fetches acquire the
renewable cross-process producer lease before reachability planning; the
producer must return a verified self-contained pack. Both key forms include
the generated-pack descriptor format version, so stale derived descriptors
naturally miss after a format change. Complete pack bodies and descriptors are
verified on every read. Runtime single-flight and the renewable internal-lock
contract coalesce concurrent producers; cancelling one waiter does not cancel
work still needed by another process.
Catalog-exact dense filters (`blob:none` and `object:type`) can assemble a
large selected response directly from verified packed entries, preserving
delta payloads and materializing only bases omitted from the selection. The
assembler uses OID-based REF_DELTA links across read batches; shallow,
path-context, and other filters retain the conservative selected-repack path
until their reachability proofs can bound the same optimization. Repository
GC treats these objects as a soft acceleration cache: recent descriptors
retain their referenced artifacts through the configured grace period, after
which stale descriptor/artifact pairs become collectible.
GC resolves recent descriptors with bounded list-concurrency and streams
validated pairs, keeping response-cache cleanup from turning into an
unbounded read or memory wave as request history grows.

Large response producers download committed source `.pack`, `.idx`, and `.rev`
artifacts. The pack body and both sidecars are validated against the pinned
inventory, then staged with hard links when the workspace permits it, avoiding
the CPU and I/O cost of rebuilding a source index. Shallow selection also keeps
the source installation bounded by skipping an OID enumeration that the
selection planner does not consume; exact response-set validation remains in
place.

Directory listing reads only the selected tree. Child sizes are absent unless
the caller requests bounded page-only metadata. Directory cursors resume after
an exact entry in the pinned tree, preserving Git order when files and
directories share a name prefix. Comparison prunes equal tree IDs. History, diff, blame, archive, storage, inflation, and response work have
independent aggregate limits.

History remains authoritative over verified raw commit objects. When the
manifest names an immutable split commit graph, open bounds the complete graph
to 128 MiB, verifies every descriptor and layer Blake3 identity, validates
stable ordinals, parent closure, corrected generations, and the exact manifest
generation/pack/digest tuple. A snapshot uses it only while each positional
parent list exactly matches the corresponding raw commit; missing or corrupt
acceleration falls back to raw parent order and can never hide a reachable
commit.

Each operation emits one structured span with only its bounded operation kind,
process-local correlation ID, outcome, and safe error category. Raw OIDs,
paths, content, provider endpoints, storage prefixes, and credentials are not
trace or metric fields. Integrity incidents use the same safe correlation ID
without formatting the source error into normal logs.

Deploy latency-sensitive services in the same region as the object store. A
local RustFS run proves protocol behavior and correctness, not production cloud
latency; cold/warm request counts, bytes, CPU, memory, and tail latency still
need measurement against a representative large repository.

Point reads and bulk traversal have different cost shapes. A deep path has one
dependent tree lookup per component on a cold runtime. History and blame can
perform many small random reads, while an uncached archive may read every tree
and blob. Services should reserve separate admission for these expensive
operations and should not infer archive or blame latency from root-listing
latency.

Resolving a full commit ID proves reachability by walking verified raw commits
breadth-first from the pinned refs. Nearby merge parents are checked before
older ancestry on either branch. Deep or unreachable revisions can still exceed
the operation's history/object budgets; resolving a named ref avoids that walk.

## Live qualification example

### Local HTTP browser and latency measurements

`browse_http` is a small Rust/Axum example with a bundled browser UI. It uses
the crate directly for refs, commit metadata, first-parent history, directories,
and exact Git blob bytes. It needs an already-published repository and current
object catalog, as described below. No Git executable, checkout, or local object
database is used by the server. HTTP dependencies are dev-dependencies only.

Configure S3 credentials in the process environment; for local RustFS, also set
`AWS_ENDPOINT_URL=http://127.0.0.1:9000`, `AWS_ALLOW_HTTP=true`,
`AWS_REGION=us-east-1`, and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`.
See the [local RustFS guide](../../crab/docs/guides/local-dev-rustfs.md).
Build using a separate target directory for this checkout on the workspace volume:

```sh
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/crab-http-example" \
  cargo build --locked --release -p crab-remote-git --example browse_http

# Run from an empty directory; a source repository is not an input.
"$HOME/Workspace/crabbuild-target/crab-http-example/release/examples/browse_http" \
  <bucket> <repository-prefix> 8787
```

Open `http://127.0.0.1:8787`. Select a ref or full commit SHA, navigate the tree,
read files, or browse commits. **Benchmark this request** performs one cold
read, one shared-runtime priming read, and five measured warm reads. It reports
the warm median and retains the individual request measurements. Ctrl-C drains
requests and shuts down the reader runtime.

The JSON API and binary blob endpoint also work with `curl -i`:

| GET endpoint | Response |
| --- | --- |
| `/api/refs` | Pinned generation, pack count, HEAD, and refs |
| `/api/commit?rev=main` | Commit OID, tree, parents, author, and message |
| `/api/commits?rev=main&limit=20` | First-parent commit page, including the selected commit |
| `/api/tree?rev=main&path=pkg&limit=50` | Immediate entries with OID, mode, kind, and byte-preserving `path_hex` |
| `/api/blob?rev=main&path=README.md` | Exact Git bytes; blob OID in `X-Crab-Blob-Oid` |

`rev` defaults to the pinned HEAD. `path_hex` can replace UTF-8 `path` to preserve
arbitrary Git path bytes. Display strings are lossy UTF-8; commit `message_hex`
preserves message bytes. Pages return a signed opaque `next` value; pass it as
`cursor` with the same revision, path, and limit. Page limits are 1–200, default
50. Cursors expire when the server restarts. Submodules are metadata-only;
symlinks return their stored target bytes. Crab/LFS pointers remain pointers.

Every handled read returns `Server-Timing` durations in milliseconds:

- `open`: repository handshake for `mode=cold`; zero for `mode=warm`.
- `read`: snapshot resolution, semantic read, response encoding, and explicit
  locator close. `/api/refs` reads the already-open handle's in-memory refs.
- `shutdown`: draining a cold request's runtime.
- `total`: handler time through response construction, excluding HTTP body
  transmission. The UI separately measures round trip through full body receipt.

`mode=warm` (default) shares the startup repository handle and bounded runtime
caches; it does not guarantee cache hits. `mode=cold` creates a fresh runtime and
reopens the repository without disturbing the shared caches. It shares the S3
transport and does not flush OS or RustFS caches. This compares Crab cache
behavior on local RustFS, not production cloud latency. Responses disable HTTP
caching so repeated browser requests reach the server.

The server pins one generation at startup. Restart after publishing changes;
a cold read of a different generation returns 409 instead of comparing different
data. The example binds only to loopback and allows four concurrent reads
(additional requests get 429), with 30-second semantic operation budgets and
an 8 MiB response budget. This is a local inspection tool, not an authenticated
multi-user service.

### Command-line qualification

`qualify_remote` exercises repository open, snapshot/commit reads, cold and
warm directory/blob reads, history, path history, compare, diff, blame, and a
complete archive through one shared runtime:

```console
cargo run -p crab-remote-git --release --example qualify_remote -- \
  <bucket> <repository-prefix> <path-changed-by-head>
```

The example reports elapsed time plus canonical `crab-storage` read attempts
and bytes for each operation. Those counters include manifest, inventory,
pack-index, and pack-body reads but exclude SlateDB locator-internal reads, so
they are useful for regression comparison rather than complete provider
billing. The example uses an explicit larger archive qualification budget; it
does not change library or service defaults.

## Browsing correctness against a real repository

`qualify_browse` emits JSONL evidence using only the remote storage API. It
paginates 1,000 first-parent commits and the complete HEAD tree, checks 128
spread-out blob paths twice, and streams every HEAD blob through an independent
Git SHA-1 calculation. Paths and commit messages are emitted as byte arrays.
The run succeeds only when a final `complete` record is emitted.

A fresh push can precede object-catalog publication.
Run `crab metadb owner --once` from the uploader repository to advance the
catalog; repeat owner passes until `action=none` to finish all derived maintenance. The direct reader
returns `RepositoryIndexing` while locator coverage is absent or stale and does
not perform this write-side work. See [metadata ownership](../../crab/docs/guides/metadb.md).

Run the built example from an empty directory with the local RustFS environment
configured as described in the [local development guide](../../crab/docs/guides/local-dev-rustfs.md):

```console
/path/to/qualify_browse <bucket> <repository-prefix> > browse.jsonl
python3 crab/scripts/e2e/verify_remote_browse.py \
  /path/to/read-only/source-repository browse.jsonl --output report.json
```

The verifier expects the fixture to publish the source revision as `refs/heads/main`
with no other refs (`--revision` selects the source revision; default `HEAD`).
It compares every tree path, mode, and object ID, commit metadata and parent
order, sampled blob sizes, and all streamed content hashes
against native Git. Only the separate verifier accesses the source checkout;
the reader neither runs Git nor creates an object database. The explicit larger
archive budgets belong to this qualification workload, not service defaults.
This proves the uploaded HEAD snapshot and sampled history, not every historical
blob, every API, or production performance.

## Content representations

`read_blob` returns the exact Git blob representation. It classifies ordinary
Git blobs, Crab pointers, and Git LFS pointers but never materializes pointer
targets. Logical Crab content belongs to `crab-read`; verified LFS content
belongs to `crab-lfs`. Service composition decides whether those representations
are enabled.
