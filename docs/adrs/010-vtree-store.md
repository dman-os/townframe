# ADR 010: Pauperfuse bridge & virtual tree store (vtree)

- **Status:** Draft
- **Supersedes:** none
- **Depends on:** FDR 001 (dpaths), FDR 002 (vocabulary), FDR 003 (VC primitives), FDR 004 (workspace CLI)
- **Depended on by:** ADR 011 (checkouts & reconciliation), ADR 012 (lenses), ADR 013 (blob strategy)

## 1. Context

Pauperfuse is **daybook/automerge-agnostic**: from its point of view the
world is N **backends** (the real filesystem, the daybook-backed content
source in a deployment, a wasi scratch VFS for lens codecs, later maybe git
working trees) and one **bridge** — the library itself — that keeps a
virtual tree (*vtree*) recording the latest-known state of each backend,
reports and brokers changes between backends, and is the object the CLI and
higher-level code talk to.

Core commitments (each scoped to *this crate*, per review — see §5 for
what deliberately does **not** live here):

- **Diff as the core primitive**: backend-vs-rep (change reporting),
  rep-vs-rep (transfer planning).
- **Blobs first-class**: photos/videos are the main payload; hashing or
  diffing gigabytes on a stat bump must never happen, and transfers between
  backends must be able to move delta bytes (chunk-level) when that's the
  smart shape.
- **Availability-aware, atomic-per-file materialization**: a rep entry can
  be a **stub** — "there should be a file at this path, but the bytes are
  not here yet" (still in transit from a relay or another backend). Bytes
  land atomically per file (iroh-blobs hard-link/export); what is lazy is
  *availability*, never the write itself (per review — this is not git's
  lazy checkout, and we should not lean on SCM concepts to explain it).
- **Watch fast-path**: the fs backend's authoritative scanner is accelerated
  by a stat cache; hashing happens only when its change-tracking demands it.
- **Crash-safe, single-writer state updates**: one writer per checkout; rep
  updates are atomic (sqlite txn). A crashed transfer re-runs; nothing
  needs a journal to recover (§3.4).
- **Local-only**: tree nodes never sync. File *contents* are handled by
  ADR 013 (external-tracked blobs, hardlink-first, never double-copied);
  the vtree stores content *references*, not bytes.

This ADR fixes the architecture, the model, and the costs, derived
bottom-up from pseudocode for the core cycles (§4) — and deliberately
**not** from FDR-level flows (CLI verbs, imports, status surfaces), which
are ADR 011+ and the CLI's business (per review: design the core, don't
absorb every FDR concern).

## 2. Architecture: the bridge

### 2.1 The library is a bridge, nothing more

- The bridge **asks each backend "what changed since last time?"**; the
  backend answers with whatever tracking facilities *it* has (fs: scanner +
  stat cache; daybook: heads + its own change-tracking facilities, leaning
  on lens idempotence; git: ref walks), and the bridge updates **its rep** —
  the recorded latest-known state of that backend's files.
- Change-tracking cleverness (stat caches, hash diffs, chunking) is a
  **backend implementation detail, not bridge core**. The bridge just
  consumes `ChangeSet`s.
- The bridge **owns the transfer shape** between backends: when one backend
  needs bytes another holds, the bridge brokers it — full bytes, chunked
  deltas, or backend-specific encodings — so transfer policy stays flexible
  and centralized. Transfers are a brokered surface between backends; the
  vtree records states, not traffic (per review).
- **The bridge is itself a backend from the backends' perspective**: the
  wasi sandbox reads files from the bridge; the bridge serves what the
  owning backends hold — including "metadata now, bytes later" stubs (§2.4).
- The bridge is also **the API surface** for the CLI and higher-level code.

### 2.2 Reps (the states)

One mutable ref per backend, git-refs style — the bridge's record of the
latest-known state:

```
rep:fs        -> what the fs backend last reported/held
rep:daybook   -> (deployment name) what the daybook backend last reported
wasi:<lens>   -> scratch tree for a lens codec run
```

There is **no privileged "daybook tree"** — in the daybook deployment the
daybook backend's rep happens to be the lens projection of the branch of
record, but the bridge doesn't know or care. `rep:<backend>` is the base
for that backend's next change report and for three-way staging — with
**no merge-base search** anywhere.

A rep is a **state, not a version** (per review — §5): the bridge keeps the
current rep per backend plus whatever older tree nodes remain referenced;
it records no history and promises no retrievability of past states.

### 2.3 Availability: stubs, not deferred writes

The lazy-materialization requirement (FDR 003/004, review) is precisely:

> a backend knows *there should be a file at dpath `/my/video.mp4`* but it
> does not yet have the bytes.

So an entry's content reference carries an availability state:

- **present** — bytes are locally addressable (this backend holds them);
  materialization is **atomic per file** (iroh-blobs hard-link or export;
  never a half-written video);
- **stub** — path/stat/metadata are known, bytes are not local yet; the
  entry exists in the rep, reads against it report not-available, and the
  transfer planner treats it as pending work.

Stubs propagate the way change reports do — "file exists but not avail
yet" is just another report from a backend (and the same mechanism future
blob-version reporting will use, §5.1). No byte-level streaming laziness is
claimed beyond that: when bytes *do* move, they move atomically.

### 2.4 Content references

```
struct ContentRef {
    chunks: Vec<ChunkHash>,          // FastCDC-style; 1 chunk for small files
    origin: Option<Origin>,          // Rendered { doc_ref, lens_id, lens_ver }
    avail: Avail,                    // Bytes | Stub { hint }
}
```

- **Blob entries** (`Origin` absent): hash-addressed chunks; identical
  content dedups; delta transfer = send only the chunks the target lacks.
- **Rendered entries** (`Origin::Rendered`): carry the regenerating pair
  (doc ref, lens version) — content can be re-rendered rather than retained
  (which is also why old blob versions are droppable, §5.1).
- Chunk hashes are computed **lazily**: large files are not rehashed on an
  mtime bump "just in case" (§3.2).

## 3. Model details

### 3.1 Tree nodes are content-addressed

```rust
struct TreeHash(Hash);              // blake3 over canonical serialization

struct DirNode { entries: Vec<DirEntry> }   // sorted by name; binary search
struct DirEntry {
    name: OsString,                 // FS encoding, checkout-relative
    kind: EntryKind,                // File | Dir | Symlink
    content: Option<ContentRef>,    // File only (§2.4)
    stat: StatFingerprint,          // fs backend's stat cache (§3.2)
    lens: Option<LensAnno>,         // {doc_ref, lens_id, lens_ver} mapping
}
struct StatFingerprint {
    len: u64, mode: u32, mtime: (i64, u32), ctime_ino: Option<(u64, u64)>,
    // NB: deliberately NO atime — reads churn it; never an ingest signal.
}
```

Consequences we rely on:

- **Node sharing between reps**: identical subtrees (same `TreeHash`) are
  stored once — across a checkout's reps, and trivially across rep updates
  that leave subtrees untouched.
- **Diff by hash-pruning**: `diff(a, b)` prunes identical subtrees —
  O(changed entries), not O(tree).
- **Identity is path-shaped** (like git trees): paths are the address;
  renames are del+add; content dedups via `ContentRef`. Doc identity lives
  in `lens`, not in tree shape.
- **Flat-huge-dir caveat**: path-copying copies a dir's entry vec when that
  dir changes; 100k-entry flat dirs are acceptable at v1 (chunked nodes is
  a localized later swap — §7).

### 3.2 Stat cache & the large-blob problem

The fs backend's scanner is authoritative (FDR 003): walk sorted, stat
everything. The stat cache makes hashing rare:

- stat equal to rep ⇒ unchanged, **no hashing**;
- stat differs on a **small file** ⇒ hash it; equal hash ⇒ stat-only touch;
- stat differs on a **large blob** ⇒ **do not hash now** (hashing 4 GB
  because someone bumped an mtime is exactly the failure mode review
  rejected). Mark the entry **dirty**: the change is real to the scanner,
  but content verification (chunk rehash) is deferred until something
  actually needs the bytes — semantic ingest, or a brokered transfer, where
  the chunking pass runs anyway.
- Availability and history need no hashing: a stub materializes by atomic
  export when its bytes arrive; rendered entries re-render from `Origin`.

### 3.3 Storage

Single sqlite per checkout at `.dtree/store.sqlite3` (WAL):

- `nodes(hash PK, data)` — serialized `DirNode`s (small; LRU-cached).
- `refs(name PK, tree)` — current rep pointers.
- contents: **never here** — `ContentRef`s address backend-held blobs
  (ADR 013 owns hardlink-first external tracking, dedup, GC).
- **No ops table** (per review — §5.1): the store holds states, not
  history. The CLI's "op log"-like experience comes from the eager-commit
  design and is served by the VCS-capable backend (daybook), not by the
  vtree.

Writer model: one in-process writer per checkout; each rep update is one
`BEGIN IMMEDIATE` txn. There is nothing to journal: backends are the truth
for their own state, reps are caches, and recovery is a fresh change report
(§4.1). An interrupted transfer leaves a stub (or a dirty entry) — the next
cycle finishes it; per-file atomicity is the backend's export contract.

## 4. Core cycles (pseudocode)

```rust
// Hash-pruning two-way diff over rep trees. O(changed entries).
fn diff(a: &TreeHash, b: &TreeHash) -> Vec<Delta>;
// Path-copying update: new root hash. Touches only changed dirs.
fn apply(tree: &TreeHash, deltas: &[Delta]) -> TreeHash;
```

### 4.1 Change report (fs backend)

```rust
async fn report_changes(fs: &FsBackend, rep: &TreeHash) -> ChangeSet {
    let mut deltas = vec![];
    for entry in walk_sorted(fs.root) {                 // O(files) stats
        match lookup(rep, entry.rel_path) {
            Some(old) if old.stat == entry.stat() => {} // cache hit
            Some(old) if is_large(entry) => deltas.push(Dirty{..}),   // lazy §3.2
            Some(old) => {
                let h = hash_content(entry);
                if Some(h) != old.content { deltas.push(Changed{.., h}); }
                else { deltas.push(TouchStat{..}); }
            }
            None => deltas.push(Added{..}),
        }
    }
    let removed = diff_paths_only(rep, walk_set);
    ChangeSet { deltas }
}
// Bridge folds a report into the rep WITHOUT re-hashing:
fn rep_from_report(rep: &TreeHash, cs: &ChangeSet) -> TreeHash {
    apply(rep, cs.deltas)                               // stat payloads ride along
}
```

A no-change report reproduces the identical `TreeHash` — identity preserves
no-op.

### 4.2 Bridge reconcile cycle

```rust
async fn reconcile(cx: &mut Bridge) {
    // 1. reports: every backend tells the bridge what changed since its rep
    let reports = for b in cx.backends {
        yield (b.id(), b.report_changes(cx.rep(b.id())).await)
    };
    // 2. fold into reps (daybook's report is head-delta -> lens-rendered
    //    deltas — backend's business, §2.1); one sqlite txn per rep
    for (id, cs) in &reports { cx.update_rep(id, cs); }

    // 3. transfer planning: diff each rep against the others' current state;
    //    the bridge OWNS the shape (full / chunk-delta / backend codec §2.1)
    let transfers = for target in cx.backends {
        yield plan_transfers(target, cx.reps)
    };
    for t in transfers { execute(t).await; }        // §4.3; stubs fill in here
    // dirty blobs from step 1 resolve inside their first real transfer (§3.2)
}
```

`db commit` (FDR 004) is reconcile + summary/naming at the CLI layer;
watch ticks are reconcile with a cheap report; reads like `db status` are
step 1 against a fresh report — pure reads, no state updates.

### 4.3 Transfer execution (atomic per file; brokered shape)

```rust
async fn execute(t: Transfer) {
    for item in t.items {
        match item.shape {
            FullStream  => target.write_atomic(item.entry,
                            source.read_stream(item)).await,
            ChunkDelta  => {  // photos/videos: only missing chunks cross
                let have = target.chunk_inventory(item.entry);
                for chunk in item.chunks.difference(&have) {
                    target.stage_chunk(item.entry, source.read_chunk(chunk)).await
                }
                target.commit_atomic(item.entry).await;   // iroh-blobs export
            },
            ReRender    => { let bytes = source.render(item.origin);
                             target.write_atomic(item.entry, bytes.stream()).await; }
        }
        cx.mark_avail(target, item.entry);          // stub -> bytes present
    }
}
```

- **Atomic per file is the contract** (per review): a target never
  observes a half-written video — chunk staging + atomic commit
  (hard-link/export) is the fs backend's job; the bridge sequences it.
- **Stubs transfer first, bytes later**: a target can receive "this path
  should exist" ahead of its bytes (e.g. blob still coming from a relay);
  the entry lands as a stub and promotes to bytes when the transfer
  completes.
- Item-level retry = redo that item; no journal needed.

### 4.4 Three-way staging (deployment policy hook)

Per backend, the three-way needs **no merge-base search**:

- **base** = the backend's rep before this cycle,
- **ours** = the backend's reported current state,
- **theirs** = the bridge's merged view of the other backends.

```rust
fn stage(base: &TreeHash, ours: &TreeHash, theirs: &TreeHash) -> Staging {
    three_way_merge(base, ours, theirs)     // tree-level, hash-pruned
    // conflicts -> deployment policy hook. The bridge stays agnostic;
    // e.g. the daybook deployment routes conflicts to its own branch
    // machinery (FDR 003 §3) rendered by the lens (ADR 012).
}
```

### 4.5 The `Backend` trait (the whole surface)

```rust
#[async_trait]
trait Backend: Send + Sync {
    fn id(&self) -> BackendId;
    async fn report_changes(&self, rep: &TreeHash) -> ChangeSet;  // observe
    async fn write_atomic(&self, entry: &Entry, bytes: impl AsyncRead) -> Res<()>;
    async fn read(&self, entry: &Entry, range: Range) -> impl AsyncRead;
    fn capabilities(&self) -> Caps;     // chunking? ranges? codecs? stubs?
}

impl Backend for FsBackend     { /* §4.1 scanner + stat cache */ }
impl Backend for WasiBackend   { /* stubs served lazily; bridge is its source */ }
impl Backend for DaybookBackend{ /* heads + lens rendering (deployment-side) */ }
// GitBackend (later): report = git tree diff; read = object streams.
```

The bridge consumes `ChangeSet`s and brokers transfers; it never knows
whether a backend is git, wasi, or daybook underneath.

## 5. What deliberately does NOT live in the vtree

### 5.1 History / VCS (locked per review)

The vtree is **not a VCS** and keeps no durable op log. The strongman case
for durable ops — crash journaling, `--at`, undo — fails on inspection:

- **Crash recovery doesn't need them**: backends are the truth for their own
  state; reps are caches; recovery is a fresh change report (§3.3).
- **VCS history is a backend capability, not a bridge state**: the daybook
  backend *is* a VCS (automerge: append-only, branch-only, per-doc); a git
  backend would be another. The CLI's history features (`db log`, `--at`,
  fork-at-version) are fulfilled **by the VCS backend, over the vtree** —
  not by vtree-internal history (per review; and note the data itself never
  passes through ops anyway — transfers are a separate brokered surface).
- The bridge's "op-log-like" feel is a **byproduct of the eager-commit
  design** (FDR 003/004), not a store feature.

**VCS positioning (review thread, resolved here):** we *almost* have VCS for
free — everything except blobs lives in automerge. Blobs are mostly
never modified in place (photos/videos; a metadata change is a new version,
not an edit). So: no workspace-wide "checkout at point N" exists or should
be pretended (there is no total linear history to stand on — a backing doc
at a path can shift under forks/dpath moves, and prior history can become
inaccessible); append-only + branch-only is the whole model, no rebases.
**CLI consequence (feeds FDR 003/004 revision):** history surfaces are
per-doc/per-file/per-group, served by the VCS backend — never a vtree
feature.

**Blob versioning** is a future story, deliberately out of scope here (per
review): a blob-VC layer that maintains last-N versions per file, lets
lenses/backends report version lists *and* "not avail", and gives blobs a
relay-backup story. Flagged for a future FDR/ADR (013 adjacent); the
vtree's only job today is to not *pretend* VCS capability it doesn't have.

### 5.2 FDR-level flows

CLI verbs, import pipelines, status surfaces, conflict UX: these live in
ADR 011 and the FDRs. §4 contains only the core cycles they compose from.

## 6. Costs summary

| Operation | Dominant cost | Whole-tree? |
|---|---|---|
| fs report (scan) | O(files) stats + O(small-changed bytes) | walk only |
| daybook report | O(changed docs) heads/render (its own tracking) | no |
| diff(a,b) | O(changed entries) via hash-pruning | no |
| apply(deltas) | O(Σ depth·fanout over changed dirs) | no |
| transfer (chunk-delta) | O(missing chunk bytes) | no |
| transfer (full stream) | O(bytes) — the point | per item |
| rep update | O(changed dirs) + one sqlite txn | no |

The invariant: **nothing pays O(tree) except the authoritative walk**, and
**nothing pays O(blob bytes) except real transfers** — mtime bumps are
cheap, gigabytes move only when bytes must move.

## 7. Consequences

- **The crate is backend-agnostic by construction**: daybook is just one
  deployment's backend; fs, wasi, and future git are peers.
- **State, not history**: the vtree holds current reps with shared nodes;
  all historical/VCS storytelling belongs to VCS-capable backends (§5.1).
- **Availability is a first-class state**: stubs make "file should exist,
  bytes not here yet" representable without fake semantics; per-file
  materialization stays atomic via backend export contracts.
- **Transfer shape is centralized**: smarter delta formats or backend
  codecs touch the bridge's planner, not every backend.
- **Lazy ≠ sloppy**: hashing waits for real need (dirty blobs), bytes move
  atomically per file, stubs fill in when content arrives.
- **Corruption is loss** (per review, recorded honestly): externally
  tracked blobs and hardlinks share fate — corrupt the file and every
  link sees it. The mitigation is *backup*, not vtree magic; see §5.1's
  blob-versioning/relay-backup pointer (future FDR, ADR 013 alignment).
- The sketch code in `src/pauperfuse/` is superseded wholesale; the
  `Backend` trait survives in the §4.5 form.

## 8. Open questions

1. **Flat-huge-dir node shape** — sorted-vec path-copying is v1; chunked
   nodes later if 100k-entry flat dirs hurt watch ticks. Localized swap.
   _Blocks: nothing (perf follow-up)._
2. **Dirty-blob resolution** — strictly on-demand (per review), plus a
   cheap size pre-check at report time. Recorded risk: external/hardlinked
   blob corruption is silent until read — mitigation is the future
   blob-backup story (§5.1), not speculative verification.
   _Blocks: fs backend impl detail._
3. **Transfer shape negotiation surface** — `Caps` needs to say exactly
   what (chunking scheme? CDC parameters? range granularity?). Lean: fix
   on FastCDC with a version tag in `Caps`; refuse mixed-CDC pairings
   rather than re-chunk. _Blocks: 013 (blob strategy) alignment._
4. **Symlink payload details** — target in `content`? exec bit in `mode`
   only? Leans yes/yes; needs one pass with ADR 011 conflict semantics.
   _Blocks: 011._
5. **Stub promotion semantics** — when bytes arrive for a stub, who flips
   `avail` (the backend post-commit, or the bridge observing the
   transfer's completion)? Lean: backend reports it like any change;
   transfer completion is just the usual report. _Blocks: 011._