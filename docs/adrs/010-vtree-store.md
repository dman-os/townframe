# ADR 010: Pauperfuse bridge & virtual tree store (vtree)

- **Status:** Draft (rev. 2 — supersedes the content-addressed node model)
- **Supersedes:** ADR 010 rev. 1 (content-addressed `DirNode` DAG)
- **Depends on:** FDR 001 (dpaths), FDR 002 (vocabulary), FDR 003 (VC primitives), FDR 004 (workspace CLI)
- **Depended on by:** ADR 011 (checkouts & reconciliation), ADR 012 (lenses), ADR 013 (blob strategy)

> Pseudocode and SQL in this ADR are advisory: they fix shape, cost, and
> invariants, not APIs. Nothing here prescribes Rust constructs.

## 1. Context

Pauperfuse is **daybook/automerge-agnostic**: from its point of view the
world is N **backends** (the real filesystem, the daybook-backed content
source in a deployment, a wasi scratch VFS for lens codecs, later maybe git)
and one **bridge** — the library itself — holding a **vtree**: the
latest-known state of every backend, as **relational rows keyed by path**.

**Identity is provenance, not content addressing** (rev. 2, per review).
Rev. 1 addressed trees by hashing their entries, which breaks on exactly the
case lenses exist for: a rendered entry's hash requires its bytes, so the
vtree could not record that a doc moved without *fully rendering every
changed doc first*. A blob does not have this problem — the blob store is
already content-addressed, so the token is free — but produced content has no
pre-existing bytes to hash. Rendering is therefore the edge case that
breaks hash-based identity, and we design around it:

- an entry carries an **origin**: an opaque, scheme-tagged token that the
  owning backend promises to be a deterministic function of the content it
  stands for. A blob hash, an fs content hash, and a producer's "how to make
  this again" recipe are the same thing to the core, which never reads the
  scheme (daybook's recipe is `doc_ref` + doc state + lens id/version);
- a **content hash is optional evidence**, filled when bytes are in hand,
  never required to describe a change;
- **rendered bytes are device-local**: docs sync, rendering is a cache, and
  materialized rendered output is **not** synced over iroh-blobs (review).

Commitments (each scoped to *this crate*; §5 lists what deliberately is not
here):

- **Diff is the core primitive**: backend-vs-rep (change reporting),
  rep-vs-rep (transfer planning), both as path-ordered merge joins.
- **Blobs first-class**: photos/videos are the payload; hashing or diffing
  gigabytes on a stat bump never happens, and materialization is usually
  just symlinking/hardlinking an existing blob — never a copy.
- **Availability-aware, atomic-per-file materialization**: a rep entry can
  be a **stub** ("there should be a file here, the bytes are not yet"); when
  bytes do land they land atomically per file (hardlink/export, or one write).
- **Watch fast-path**: the fs backend's authoritative scan is stat-cached;
  hashing happens only when its change tracking demands it.
- **Crash-safe, single-writer state updates**: one sqlite transaction per
  rep update; no journal; recovery is a fresh change report.
- **Local-only**: rows and tokens never sync; bytes belong to backends.

## 2. Architecture: the bridge

### 2.1 The library is a bridge, nothing more

- The bridge **asks each backend "what changed since last time?"**; the
  backend answers with whatever tracking it has (fs: scan + stat cache;
  daybook: heads and its own change-tracking facilities, leaning on lens
  idempotence; git: ref walks), and the bridge updates **its rep** — the
  recorded latest-known state of that backend's files.
- Change-tracking cleverness (stat caches, hashing, chunking) is a backend
  implementation detail; the bridge consumes change reports.
- The bridge **owns the transfer shape** between backends (hardlink/export,
  byte stream, or re-render — §2.5). Transfers are a brokered surface; the
  vtree records states, not traffic.
- **The bridge is itself a backend from the backends' perspective**: the
  wasi sandbox reads files from the bridge, which serves what the owning
  backends hold — including "metadata now, bytes later" stubs.
- The bridge is the API surface for the CLI and higher-level code.

### 2.2 Reps are row sets

One **rep** per backend (or scratch surface), and one **row per path**:

```
rep(name, generation, updated_at)            -- 'fs', 'daybook', 'wasi:obsidian'
entry(rep, path, kind, origin, content, avail, stat, claim)
```

- A rep is a **state, not a version**: rows are updated in place, so there
  is no orphan tree to garbage collect and no ref-to-old-state machinery.
- `generation` bumps on every rep update: an O(1) "did this rep move?" test
  for callers, and the only change detector the store itself offers.
- Directories are rows too (so "there is a directory here" is addressable
  and empty directories survive), but they carry no payload.
- No merge-base search anywhere: for a backend, its rep *is* the base, both
  for its next change report and for three-way staging (§4.4).

### 2.3 Entry identity and equality

```text
payload  File { origin, content } | Dir | Symlink { target }
         -- a file's identity lives in `origin` (plus optional `content` evidence);
         -- a directory's identity is its existence; a symlink's is its target
origin   External { token }                      -- the backend's name for these bytes:
                                                 -- a git blob hash, an fs digest or stat
                                                 -- token, a producer's "how to make this
                                                 -- again" recipe. A scheme tag says which,
                                                 -- and the core never reads it.
content  Option<Token>   -- evidence: the digest of bytes this device has produced or ingested
avail    Present | Stub  -- a stub is "there should be a file here, bytes not here yet"
stat     Option<StatFingerprint> -- the fs backend's cache: len, mode, mtime (never atime),
                                 -- and absent for directories, whose size and mtime are
                                 -- derived from children each recorded on their own
claim    Option<Claim>   -- who put this path here: one opaque value per backend scheme
```

`origin` is **opaque to this crate**. A git backend names bytes by blob hash,
a checkout by digest (or a stat-derived dirty marker until hashed), daybook by
a recipe: "render this doc, at this state, with this lens, version N". All of
them are the same thing to the core — bytes with a scheme tag — because the
core never has to interpret one (see "who decides" below).

Content digests are the one identity two backends must be able to compare directly,
so they use Daybook's shared encoding (ADR 001): a self-describing blake3
multihash, base58btc. A checkout's digest of a file and the digest a blob facet
carries for the same bytes are therefore the *same token*, minted by whichever
side computed it — which is what lets an adopted blob be agreed about without
either side re-hashing it.

A recipe must be a *per-path* identity, and the deployment owns that: if a doc
reports one state token for the whole document, then every doc edit changes
every path's recipe and the whole checkout re-renders on each keystroke. A doc
that wants incremental rendering (ADR 012 §2) reports the state of the facet
that path renders from.

Shapes, which are all the core itself compares:

| case | equal iff |
|---|---|
| shapes differ (file / dir / symlink, or different symlink targets) | never |
| both directories | always: a directory's identity is its existence |
| both symlinks | their targets are equal |

For files there is no core rule, and that is deliberate. "Do these two entries
stand for the same bytes?" is a question only the backends can answer, because
the answer depends on the schemes involved: for one pair of backends a different
token with an equal digest means *nothing to do* (a checkout re-observing a file
a producer wrote), and for another it means *produce it again* (a producer whose
own recipe changed, and whose new lens may not render what the old one did).
Those two are indistinguishable from the core's side — same disagreement, same
digests, opposite right answers — so the core must not be the one deciding.

**Who decides**: the backend that holds the path is asked — the interface is
§4.5's `accept`, the policy inside it is the deployment's (ADR 012 §3). It has both values — what it recorded, and what it is
being offered — so it can settle it with a policy it owns: compare tokens if the
scheme is its own, fall back to the digest, read and hash the file if that is
worth it, or say "send it anyway" and let the copy happen. Every such policy is
correct; only the cost differs, and the cost is paid by the backend that chooses
it. Directories are deliberately stat-free: their
mtime and size move whenever a child does, and each child is recorded on its
own, so carrying them would turn one child edit into two deltas and could
never detect anything by itself.

**Drift** is the reverse direction: an fs file whose bytes no longer match
its origin is re-reported by the backend with a new `External` token, and a
`Rendered` recipe is never silently reused for it.

**Identity continuity** is the direction that is easy to miss. When a scan
finds a recorded entry whose bytes *are* unchanged, the recorded provenance
stands: a small file's digest confirms it, and a matching stat settles it
without opening the file at all. So bytes the bridge materialized from a doc
keep their render recipe on disk, and comparing the two sides is comparing two
equal recipes rather than a recipe against "some bytes I found". The bridge
holds up its end by recording what it wrote (§4.3): the entry it materialized
from, the stat it observed, and the digest of the bytes it wrote.

### 2.4 Availability: stubs, not deferred writes

- **present**: bytes are locally addressable; materialization is atomic per
  file — hardlink/export of an existing blob, or one write.
- **stub**: path/kind/origin known, bytes not local yet. Reads report
  not-available; transfer planning treats it as pending work.

Stubs propagate like any other report ("file exists, not avail yet"), which
is also the mechanism future blob-version reporting will use (§5.1).

### 2.5 Rendered content is device-local (review, locked)

- The canonical artifact is the **doc**. Rendering is a cache; a
  materialized rendered file is a cache entry, not an artifact to sync.
- **Rendered bytes are never uploaded to iroh-blobs** and never synced as
  content. A device that can run the lens regenerates them.
- Therefore a **content hash of rendered bytes is never a cross-device
  identity** — the **recipe** is. The hash is local evidence (did the cache
  drift?), and must not be "optimized" into a shared identity later.
- Transfer shapes, in preference order, and **chosen by the side receiving
  them** — it is the one that knows what it can do with them and what it will
  have to pay:
  1. **ByReference** — the bytes already exist on this machine in an owning
     backend (a blob store's file, another checkout, a worktree), so the
     receiver takes a path: hardlink it, export it, or reference it in place,
     at O(1) data movement. **Only when the source's bytes are immutable.** A
     hardlink aliases two paths onto one inode, so an in-place edit through
     either one lands in both: a blob store's files are safe to alias, a
     **checkout's are not** — its files are its user's, and an edit in the
     checkout would reach back into whatever it was linked from. Both sides
     therefore have to agree, and both say so as part of the handshake rather
     than as a core policy: "I can take a path" / "my bytes do not change".
     This is what makes an iroh-blobs video materialize without a 4 GB copy.
  2. **ProduceAgain** — the source can make the bytes again (it holds the
     doc and can run the lens), so the receiver asks it to.
  3. **CopyBytes** — the universal fallback: ask for the bytes and write
     them. This is the only shape that needs a buffer, so it is the one that
     eventually wants a streaming form (open gap: `materialize` takes
     `&[u8]`).

## 3. Model details

### 3.1 Schema

The schema itself lives in `sql/migrations/001_init.sql`, where the comments explain why
each column is typed the way it is. Every statement a store runs lives in `sql/queries/`,
**one file per statement**, with its bind order in a header comment and its `EXPLAIN QUERY
PLAN` twin next to it, and is pulled in with `include_str!` — so nothing is assembled at
runtime and a plan a test checks cannot drift from the statement the store runs. The SQLite
store is behind the crate's `sqlite` feature; the core, the memory store and the filesystem
backend need no database.

```sql
CREATE TABLE pauperfuse_rep (
    name        TEXT PRIMARY KEY,
    generation  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE pauperfuse_entry (
    rep      TEXT    NOT NULL REFERENCES pauperfuse_rep(name) ON DELETE CASCADE,
    path     BLOB    NOT NULL,   -- canonical encoded components, byte-ordered as the walk orders
    kind     INTEGER NOT NULL,   -- 0 file, 1 dir, 2 symlink
    origin   BLOB,               -- encoded identity: scheme tag + opaque bytes (files only)
    content  BLOB,               -- optional digest evidence (files only)
    target   BLOB,               -- symlink target, verbatim (symlinks only)
    avail    INTEGER NOT NULL,   -- 0 bytes, 1 stub
    stat     BLOB,               -- optional fingerprint
    claim    BLOB,               -- optional: who put this path here (scheme + opaque bytes)
    PRIMARY KEY (rep, path)
) STRICT, WITHOUT ROWID;
```

- `PRIMARY KEY (rep, path)` **is** the ordering structure: every walk and
  every diff is an index scan in path order, and no separate path index is
  needed. Paths are encoded so their byte order equals the walk order
  (component-wise, prefix first) on every platform.
- Bytes are never stored (backends own them); no history is stored (§5.1).
- The rep row is written first in a rep update: entries reference it, and a
  transaction that creates a rep creates nothing orphaned.
- Each blob column is versioned and length-prefixed, so a future format can
  migrate or refuse rather than misread.

### 3.2 Stat cache & the large-blob policy

The fs backend's scan is authoritative, stat-cached:

- stat equal to the rep's row ⇒ unchanged, no hashing;
- stat differs on a **small file** ⇒ hash it; equal hash ⇒ `TouchStat`;
- stat differs on a **large blob** ⇒ **do not hash now** (hashing 4 GB
  because someone bumped an mtime is the failure mode we rejected). Record
  the new stat and a dirty origin token; resolution happens when the bytes
  are actually needed — a transfer, or a semantic ingest the deployment asks
  for.
- **directories record nothing to compare**: their mtime moves whenever a child
  does, so a directory's recorded identity is that it exists.
- Blob-backed entries need no hashing at all: the blob store's token is the
  identity.

### 3.3 Ordered walks are index scans

A walk over a rep is `SELECT ... WHERE rep = ? AND path > ? ORDER BY path`,
streamed. That gives, for free:

- the same order as diff (`RelPath` order), so a partial diff and a walk
  resume from the same cursor;
- O(1) cursor persistence (the last path emitted);
- bulk passes (initial materialization, imports, large transfer plans) that
  crash-resume at their cursor with no journal (ADR 011 §6).

## 4. Core cycles (pseudocode)

```text
scan(rep)                        -- path-ordered row stream
merge_join(scan(a), scan(b))     -- streaming, path-keyed, O(1) memory per key
```

### 4.1 Change report (fs backend)

```text
report(rep, root):
  out = []
  walk root sorted:
    row = lookup(rep, path)
    if shape_changed(recorded, entry):        out += Changed(path, observed)   # §2.3
    elif recorded.stat == stat(entry):        continue                          # cache hit
    elif is_large(entry):                     out += Changed(path, External{stat_token})  # §3.2
    elif row:
      tok = content_token(entry)               # hash small files
      if tok != row.origin:  out += Changed(path, External{tok}, stat)
      else:                  out += Touch(path, stat)
    else:                    out += Added(path, External{content_token(entry)})
  for row in rep not seen and not under a missing dir:
    out += Removed(path)
  return out
```

A no-change report is empty — "nothing happened" needs no hashes at all.

**A backend that produces content reports a different comparison** — daybook's
lens layer is the first implementation (ADR 012 §3):

```text
report(rep):
  for path in my paths, in order:
    row = lookup(rep, path)
    if row.origin == origin_of(path): continue         # I am the authority for this
    elif row:                        out += Changed(path, produced(path))
    else:                            out += Added(path, produced(path))
  for row not reported:              out += Removed(path)
  return out
```

The comparison is between **identities**, not between digests, because a
producer is the authority for its own identity and the digest in hand is the
*old* production's: a new lens version may produce byte-identical output, and
only the producer knows that the identity (not the bytes) moved. A producer
that stayed quiet would leave every checkout holding output attributed to a
lens version that no longer exists (ADR 012 §2).

It also means a producer re-asserts its own identity after a write has recorded
the *source's* identity in that row (§4.3), which is how the row gets its claim
back and with it the licence for a later removal.

Reporting still produces nothing: `rendered(path)` in this sketch is the entry a
render *would* produce — an identity, a digest it already knows, and no bytes.

### 4.2 Reconcile cycle

```text
reconcile(bridge):
  reports = for b in bridge.backends: (b, b.report(bridge.rep(b)))
  for (b, r) in reports: bridge.apply(b, r)        # one txn per rep update
  for target in bridge.backends:
    for path in merge(reps, target):               # in path order, §3.3
      case disagreement of
        target_only -> if target.may_remove(path, recorded): remove(path)
        offered     -> case target.accept(path, recorded, offered) of
                         Current     -> record(offered)      # settles it for good
                         ByReference -> source.locate + target.link_from
                         Bytes       -> execute(§4.3)
```

`db commit` is reconcile plus a summary/checkpoint at the CLI layer; watch
ticks are reconcile with a cheap report; `db status` is reports only, no
writes.

### 4.3 Transfer execution

```text
execute(item):
  case item.shape of
    ByReference      -> target.link_from(item.path, item.source)         # O(1) bytes
    ProduceAgain     -> bytes = source.produce(item.origin)              # lazy, local
                        target.materialize(item.path, item.entry, bytes)
    CopyBytes        -> target.materialize(item.path, item.entry, source.read(item))
  # Record what was written, under the identity it was written from and with
  # the digest of those bytes: this is what keeps the next scan quiet (§2.3).
  target.record(item.entry, observed_stat, digest_of(bytes))
  mark_avail(target, item.entry)          # stub -> present

remove(path):
  # A path the source dropped. Only removed when the target's recorded entry
  # carries a claim saying the source put it here; without one, the target's own
  # file and the source's deleted file are indistinguishable, and no pass
  # deletes what it cannot account for.
  target.remove(path)

execute_removals(paths):
  # Deepest first. Canonical order puts a directory before its children, so
  # reversing it is what makes the directory empty by the time it is removed —
  # which is why `remove` never has to recurse, and so can never delete a path
  # the pass did not know was there.
```

Atomic per file is the contract: a target never observes a half-written
file, and an interrupted item is retried as an item.

**A write must not echo.** Writing to a target changes it, and that change comes
back through the target's own report. Two rules keep that from being a loop:

- **the core records what the target reports after the write**, not what it
  intended, so a byte-faithful write leaves the target agreeing with its own
  record and nothing is reported at all;
- **a backend must not report its own write as a change.** Trivially true when
  it stores exactly the bytes it was given; when it does not — git normalizing
  line endings, a lens canonicalizing on ingest, a VFS appending a newline —
  *that backend* is the only party that knows, so making its report agree is its
  job. An interface that lets a backend say "that write is mine and it is
  current" is what makes such a backend expressible rather than a permanent
  re-transfer per pass (ADR 012 §3).

A write may also create paths the source never had — `.git/`, a lock file, a
temp file, a directory implied by a child. Those come back as paths the source
does not know, carry no claim, and are left alone: that is what makes a backend
that keeps its own bookkeeping inside the tree possible at all.

### 4.4 Three-way staging

Per backend: **base** = its rep before the cycle, **ours** = its reported
state, **theirs** = the bridge's merged view of the others. Merge is
row-wise in path order; conflicts go to the deployment's policy hook (the
daybook deployment routes validation failures to its own conflict
machinery, FDR 003 §3; the bridge stays agnostic). Because identity is
provenance, "ours changed and theirs changed" is decided **without
rendering or hashing anything**.

### 4.5 Backend interface (shape)

```text
Backend:
  id() -> BackendId
  capabilities() -> { immutable_content }     # whether its own bytes may be linked from
  report(session) -> ok                       # pushes deltas, in path order; the session
                                              #   carries the recorded rep as a cursor
  accept(path, recorded, offered) -> Current | ByReference | Bytes
                                              # does it already hold these bytes, and how
                                              #   does it want them (the one question the
                                              #   core cannot answer: §2.3)
  may_remove(path, recorded) -> ok            # does its own record license a removal (§8.7)
  locate(path) -> local_path | none           # where its bytes are on this machine
  read(path, range) -> bytes                  # ranges are the streaming primitive
  materialize(path, entry, bytes) -> Option<stat>   # atomic per file; dirs record no stat
  link_from(path, source) -> stat             # hardlink/export when it can
  remove(path) -> ok                          # one path, never recursive
  verify(path) -> Entry                       # upgrade an identity we declined to compute
```

`accept` and `may_remove` are the whole of the core's dependence on a backend's
own vocabulary, and neither returns an entry: the core learns *what to do*, never
what the bytes mean. Everything else is bytes, paths, and a record.

The bridge consumes reports and brokers transfers; it never learns whether
a backend is fs, wasi, git, or daybook.

### 4.6 Errors, because these traits are implemented by embedders

One concrete error type, not `Result<T, Self::Error>`: a generic error type
would make both traits object-unsafe and force every caller to name whose error
it is holding. Implementations box their own failures into a variant, keeping
the source chain, so the layer that knows what its error means still gets to say
so.

The failure classes are the boundary's, and only the boundary's:

- **the environment** — a filesystem operation (`op`, `path`, `source` kept
  separately, because "failed writing" and "failed statting" on the same path
  are different bugs), the store's database, its migrations;
- **the input** — a path a checkout cannot hold, named component by component;
- **recorded data** — a row that cannot be read back: corrupt, or written by a
  version this build does not know, with the row's path and the field that
  refused it;
- **an implementor's own error**, boxed.

Everything else is a programming error and panics. In particular a scan that
cannot read a path *reports a failure instead of omitting the path*: an omission
looks exactly like a deletion downstream, and "the user deleted this" versus "we
could not look at this" is the difference between a removal and silent data
loss.

## 5. What deliberately does NOT live in the vtree

### 5.1 History / VCS

The vtree is **not a VCS**: rows are the current state, updated in place, no
op log, no past-state retrievability. The strongman for durable history
fails on inspection: crash recovery doesn't need it (backends are truth,
reps are caches, recovery is a fresh report); VCS history is a **backend
capability** (automerge for docs, git for a future git backend), fulfilled
by that backend over the vtree; and transfer traffic never passes through
storage anyway.

**VCS positioning:** we almost have VCS for free — everything except blobs
lives in automerge. Blobs are mostly never modified in place (photos/videos;
a metadata change is a new version). There is **no workspace-wide "checkout
at point N"** and we should not pretend otherwise: a backing doc at a path
can shift under forks or dpath changes and prior history can become
inaccessible. Append-only + branch-only; history surfaces are per-doc /
per-file / per-group, served by the VCS backend (feeds the FDR 003/004
revision).

**Blob versioning** (last-N per file, version lists, "not avail", relay
backup) is a future story, out of scope; the vtree's only job is to not
pretend VCS capability it doesn't have.

### 5.2 Lens policy, blob GC, FDR-level flows

Lens definitions and conflict UX are ADR 012; blob bytes, GC and backup are
ADR 013; CLI verbs and imports are ADR 011 and the FDRs. This ADR fixes the
bridge, the rows, and the costs.

## 6. Costs summary

| Operation | Dominant cost | Renders? | Whole-tree? |
|---|---|---|---|
| fs report | O(files) stats + O(changed bytes) hash | no | walk |
| daybook report | O(changed docs) per its own tracking | no | no |
| diff | path-ordered merge join: O(entries) IO, O(changed) output | **no** | no |
| walk | index scan in path order, cursor-resumable | no | walk |
| apply | O(changed) upserts, one txn | no | no |
| transfer | O(1) hardlink, or O(bytes) when bytes must move | only for ReRender | no |

The invariants: **nothing pays O(bytes) except real byte movement**, and
**no operation produces content (or hashes its output) merely to learn that
something changed**.

## 7. Consequences

- **A doc move is cheap to record**: provenance is a row update; rendering
  waits until bytes are actually wanted.
- **No content-addressed DAG**: no tree/node hashing, no path copying, no
  subtree pruning — the path index gives the same asymptotics with less
  machinery (this is the "relational, not in-memory" shape the review
  asked for).
- **Backends keep their own currencies**: blob tokens come from the blob
  store; fs tokens from content hashes (or a dirty marker); lens recipes
  from the lens layer. The vtree compares tokens, nothing more.
- **Rendered hashes are local**: recorded in §2.5 so nobody promotes them to
  a cross-device identity later.
- **Degenerate cases get honest answers**: stubs for "known path, missing
  bytes"; dirty rows for "big file, stat bumped"; conservative `changed` for
  mixed-currency comparisons.
- Rev. 1's implementation (node hashing, path-copying `apply`, hash-pruned
  `diff`, ordered node walk) is superseded; its behavior tests remain the
  spec for the relational rewrite.

## 8. Open questions

1. **`doc_state` token comparability** — the one real unknown. It must be a
   deterministic function of doc state so equal recipes mean equal content
   across devices (a heads digest is the safe choice; a per-device revision
   counter needs a caveat, or must be scoped to local comparisons only).
   Owner: the daybook backend; the vtree treats it as opaque bytes.
   _Blocks: pruning of rendered entries in reconcile._
2. **(resolved, conservative on cost)** Drift: a small file's stat change is
   confirmed by digest — equal keeps the recorded provenance, different
   re-tokens it; a file above the hash limit drops the provenance rather than
   asserting it, and `verify` is how a decision upgrades that identity to a
   content hash. The *cost* of the above-limit branch is deliberately
   pessimistic today: a stat-only touch leaves the identity unprovable, a
   checkout asked about it demands bytes, and the pass pays a full read and
   rewrite for an mtime. The fix is a backend policy, not a change here — keep
   the belief and drop the proof (or refuse the offer), and let the asking
   operation decide when to hash. Deferred: nothing has measured the dynamics.
   _Blocks: nothing; a knob (`hash_limit`) and a branch in the fs backend._
3. **Storage volume** — a 1M-entry checkout is 1M rows (`WITHOUT ROWID`,
  `(rep, path)` PK). Acceptable? Measure in the phase-5 harness.
   _Blocks: nothing; measure._
4. **Chunk inventories** — byte-level delta transfers for photos/videos are
   the blob backend's concern (ADR 013); the vtree stores a token plus an
   optional hash. Confirm 013 exposes chunk inventories per token.
   _Blocks: 013._
5. **(resolved)** Symlinks carry their target inline, in their own column, and
   are identified by it. Creating one needs a target *kind* on platforms that
   distinguish links to files from links to directories — a limitation for a
   backend to report, not something for the row to encode.

6. **(resolved, and load-bearing)** Both are kept, and they answer different
   questions: the origin is "what bytes are these" (and changes when the bytes
   do), the claim is "who put this path here" (and does not — a user editing a
   rendered file changes the origin and keeps the claim). A backend that
   produces content fills both from the same recipe so they cannot disagree;
   whoever writes a record owns keeping them consistent.

   The claim does real work: a path the source no longer has is removable
   *only* if a claim says the source (or some other hand) put it here. Without
   one, the target's own file and the source's deleted file look identical, and
   no pass may guess.
7. **Deletions without a claim** — the two records cannot distinguish "the
   source dropped this path" from "the target made it itself", so a pass leaves
   those paths alone (`Outcome::target_only`). This is not a checkout-vs-doc
   distinction: it is the same for git into a working tree, or one checkout
   into another. A *mirror* policy that wants deletions to propagate fills the
   claim column with the source backend's name when it copies — the column
   exists, and it stays empty until a policy asks for it. _Blocks: mirror/prune
   policy._