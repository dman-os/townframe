# ADR 010: Pauperfuse bridge and virtual tree store

- **Status:** Proposed replacement for review; architectural decisions, not a claim about the current implementation.
- **Supersedes:** ADR 010 rev. 2's N-way reconciliation and generic stubs. The older content-addressed `DirNode` DAG remains superseded.
- **Depends on:** FDRs 001–004.
- **Related:** ADR 011 defines durable Daybook checkouts; ADR 012 defines projection and ingestion through lenses; ADR 013 owns blob retention and transfer.

## 1. Scope

Pauperfuse connects backends exposing filesystem-shaped content. Its current use cases are a Daybook projection, a real filesystem checkout, and a WASI virtual filesystem. Git integration is not part of this design.

Each backend owns its canonical state and its interpretation of changes. The bridge records backend observations in a path-ordered **virtual tree store (vtree)**, compares selected pairs, and brokers offers and access to bytes. It does not parse documents, choose lenses, invent document identities, or implement semantic merging.

A **rep** is simply one backend's latest recorded tree in the vtree. It is not a new layer above the vtree, not the backend's canonical state, and not a record of successful delivery to another backend. Reps are updated in place, rather than stored as immutable historical trees.

The design deliberately separates three facts:

1. What a backend currently reports.
2. What it has actually accepted from another backend.
3. What correspondence the pair last established successfully.

A report must not erase an outstanding offer merely because the new observation has been persisted. Similarly, copying an offered row into the receiver's rep does not change the receiver's canonical state.

## 2. Why a relational path tree

A content-addressed tree requires bytes, or hashes of those bytes, to describe an entry. A lens may know that a document now projects at a different path before rendering that output. Requiring a render to discover that movement defeats incremental projection.

Instead, one row describes each observed path. A file can have an opaque producer recipe before its bytes have been generated. A blob backend can use its already-known blob identity. No tree-node hashing, path-copying DAG, historical-root search, or subtree GC is required.

Paths have canonical component-wise order, with a directory before descendants. Backends report changes in this order; stored-tree walks and diffs are streaming merge joins. Backends may report incrementally, but the bridge does not require them to use the same change-tracking mechanism: filesystem stat caches and document-head tracking remain backend concerns.

## 3. Entry identity and evidence

Entries distinguish files, directories, and symlinks. Directory identity is its existence; children have their own rows, and directory size/mtime changes are not separate content changes. A symlink carries its target verbatim. Platform-specific requirements such as a file/directory hint for symlink creation are backend capabilities, not universal document semantics.

A file description carries:

- **Origin/production identity:** an opaque scheme-tagged token naming content or the declared recipe for producing it. The core does not interpret the scheme.
- **Optional content evidence:** a digest of bytes actually inspected or produced. This is not required for reporting a change.
- **Optional stat evidence:** a filesystem fingerprint such as length, mode, and mtime, never atime.
- **Optional provenance:** evidence of who supplied an existing path. Existing implementations may call this `claim`; it is not a Daybook dpath facet, and it is not proof that the file remains unchanged.

Stable output identity, content identity, and production version are different facts. A backend may expose an opaque stable output key for correspondence across path changes. A path-only backend need not manufacture one. A checkout policy can require stable output keys from its Daybook-producing side without requiring them from the filesystem or every future backend.

A changed filesystem entry may carry an uncertain/dirty identity until verified. Such a marker is not a content hash. A producer recipe must include all inputs that affect its output: relevant document/facet versions, lens and execution versions, configuration, and any contextual dependency that can affect the result. ADR 012 owns those dependencies. One whole-document token for every output is correct but unnecessarily invalidates unrelated outputs; per-output dependencies permit incremental work.

The shared byte-digest representation remains ADR 001's self-describing BLAKE3 multihash/base58btc encoding. Equal verified digests can avoid redundant byte movement; they cannot establish document ownership or prove a rename. Producer version changes remain meaningful even when the output bytes happen to be identical.

## 4. Pairwise comparison and policy

Reconciliation is between a chosen pair, not a universal N-way merged tree. Each side reports its current tree. Differences against the pair's acknowledged correspondence generate offers. A policy decides which direction to process first and how observations are correlated with existing outputs.

For a Daybook checkout, ADR 011 specifies local ingestion before upstream projection, interpretation against the recorded render base, and durable staging branches. A WASI mount can simply accept producer references and resolve them on access. The generic bridge does not assume both consumers run a bidirectional ingestion cycle.

Durable checkout correspondence belongs to the checkout's state: output bindings, last acknowledged render versions, pending ingestion/publication outcomes, and application progress. Backend-private state carries the semantic meaning of opaque identities. The generic store may provide storage for these records, but a read-through WASI mount is not required to instantiate the full checkout recovery ledger.

This separation avoids both extremes: the vtree is more useful than a one-off copying loop, but it is not an opaque universal VCS or a Daybook-aware merge engine. Reps supply a common ordered observation surface; policies and receivers supply interpretation.

## 5. Offers, application, and acknowledgement

An offer describes a proposed path operation and enough expected receiver state or opaque base context for the receiver to evaluate it. The receiver may accept, defer, or refuse. Infrastructure failures are errors, not successful acceptance or semantic conflicts.

For every operation:

1. Compare recorded observations and correspondence to construct an offer or plan.
2. Ask the receiver to prepare/evaluate it against expected state.
3. Apply through the receiver's real backend operations.
4. Record its actual outcome, not the original proposal.
5. Advance the applicable correspondence only when the endpoint outcomes that it represents are known.

A receiver can transform content. Daybook may canonicalize or merge a proposed edit; the filesystem may report updated native evidence. The bridge must not silently stamp transformed bytes with the identity of the original proposal. Receiver outcomes and refreshed observations prevent echo loops without suppressing genuine new work.

An acknowledgement is relative to the receiver's contract. A real filesystem acknowledges installed files; a WASI filesystem can acknowledge installed virtual references. The latter does not assert that bytes have been fetched, retained locally, or backed up. Receipt of metadata and materialization of bytes are distinct observable facts.

Concrete Rust signatures are not fixed here. The interface must preserve these responsibilities from the existing design:

| Responsibility | Contract |
|---|---|
| `report` | Ordered observations against the backend's previous rep; no implicit cross-backend acknowledgement. |
| `accept` / prepare | Evaluate offers, expected state, and receiver-specific context; choose transfer shape or refuse/defer. |
| `may_remove` | Receiver policy plus provenance and unchanged-target verification. |
| `locate` | Safe local access where available; a local path is not automatically an immutable source. |
| `read` / open | Obtain bytes at the offered production version, with streaming or ranged access where supported. |
| `materialize` / `link_from` | Apply through the backend, reporting the actual result. |
| `remove` | Remove one known entry, never recursively erase unknown descendants. |
| `verify` | Upgrade uncertain observation evidence when a decision needs stronger proof. |

An implementation may combine methods, but must not erase the distinctions between observation, preparation, application, and acknowledgement.

## 6. Production references and lazy byte access

Tree reporting describes outputs without eagerly rendering or embedding their bytes. An opaque production reference identifies the backend, output, and version needed to reproduce or serve content. The bridge routes access; the producer owns rendering and caching.

A WASI backend accepts an offered entry by installing its path-to-production-reference mapping. Later `stat`, `open`, or `read` requests resolve that reference through the bridge. An immutable blob may support efficient ranges; a codec may have to render the full output on first access, cache it locally, then serve ranges. Efficient ranged rendering is not a universal requirement. If size is unknown before rendering, an operation requiring an accurate size may trigger production; enumeration must not fabricate a size.

A versioned reference must not silently serve a later version. If the required input or codec version is no longer available, report that inability. Retention and pinning sufficient to satisfy a reference are producer/backend concerns coordinated with ADRs 011–013, not an unlimited promise that every reported version remains available forever.

There are **no generic stub files**. A valid lazy production reference is an executable promise of a particular output, not a placeholder for missing blob bytes. Unavailable inputs must be reported distinctly from absence; failure to obtain bytes is never an empty ordinary file or a deletion event.

Rendered output is device-local cache data. Documents sync; rendered bytes are not uploaded to iroh-blobs as replicated artifacts. Their digest is local verification evidence, not a shared document identity. Real blob payloads retain their ordinary blob semantics.

## 7. Transfer, preparation, and filesystem safety

The receiver chooses among safe transfer shapes:

- **ByReference:** use locally available content through an enforceably safe reference/export mechanism.
- **ProduceAgain:** ask a producer to generate the specified output version locally.
- **CopyBytes:** stream existing bytes to the receiver.

Neither arbitrary buffer-sized APIs nor a full-file memory allocation are requirements. A mutable checkout is not an immutable hardlink source. Even an immutable source is insufficient if a writable target can modify their shared inode: use copy/reflink or enforce a write discipline that prevents modifying the source through the alias.

A filesystem projection accepts a complete plan, validates destination conflicts, and prepares bytes outside the checkout before publication. Known obstructions reject the requested projection batch before modifying visible files. The backend can stage files then move them into place, minimizing the mutation window. Local metadata/plan updates can transact; multiple filesystem moves are not automatically one atomic transaction. Unexpected failure can leave partial results, which must remain recoverable. Stronger atomic visibility is a backend capability, not the generic guarantee.

A removed source output licenses target removal only if provenance identifies this pair's installed entry **and** the target is unchanged from acknowledged application. Preserve dirty or unaccounted-for paths. Remove descendants before parents; nonrecursive removal protects unknown children. Byte equality alone does not give ownership of an occupied destination.

## 8. Store shape and observation costs

The existing relational store remains the starting point: rep metadata includes name/generation/update time; entries are keyed by `(rep, path)`, with kind, origin, optional content/stat/provenance, and symlink target. The old `avail`/stub column is superseded. Stub-bearing stored data needs an explicit migration or rejection, not reinterpretation as a present file.

Use canonical path encoding whose byte ordering matches ordered walks. SQLite STRICT/`WITHOUT ROWID` entry storage lets the primary key serve as the walk index. Blob encodings are versioned and length-prefixed; unsupported encodings fail with field/path context. Backends own bytes; rows hold descriptions and evidence. Reps update in place and therefore have no orphan historical trees to sweep.

The retained rep-table shape is explicit below. This is advisory SQL, not a complete migration or the future checkout-ledger schema:

```sql
CREATE TABLE pauperfuse_rep (
    name        TEXT PRIMARY KEY
  , generation  INTEGER NOT NULL
  , updated_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE pauperfuse_entry (
    rep      TEXT    NOT NULL REFERENCES pauperfuse_rep(name) ON DELETE CASCADE
  , path     BLOB    NOT NULL
  , kind     INTEGER NOT NULL
  , origin   BLOB
  , content  BLOB
  , target   BLOB
  , stat     BLOB
  , claim    BLOB
  , PRIMARY KEY (rep, path)
) STRICT, WITHOUT ROWID;
```

`kind` distinguishes file/directory/symlink; origin/content apply to files, target to symlinks, stat to backend observation, and claim to provenance. Create the rep row before its entries within the update transaction. Generation advances with each committed report update, providing an O(1) change indicator, not a historical version or an acknowledgement. Additional opaque output/production-reference fields must be designed against the lazy WASI and checkout cases rather than inferred from this legacy column list.

Keep migrations in `sql/migrations/`, statements in `sql/queries/` with bind-order comments and matching query-plan tests, and static inclusion rather than runtime SQL assembly. SQLite remains feature-gated; the core, memory store, and FS backend need no database. Final correspondence schema is deferred to implementation experiments, not specified by an invented third universal tree table.

A rep walk is a streamed `WHERE rep = ? AND path > ? ORDER BY path` scan. Continuation needs the generation of the observed tree: a path cursor alone cannot safely resume a changed plan. A no-change FS report uses stat evidence where valid, without opening every file. A changed small file may be hashed to distinguish a metadata touch; a large file can be reported as uncertain until verification is actually needed. That avoids unconditional scan-time hashing, not all later O(bytes) work. Measure current full-read behavior on mtime changes rather than claiming it is eliminated.

| Operation | Expected work |
|---|---|
| FS report | O(files) stats; byte inspection where its verification policy requires it. |
| Daybook report | Changed-input tracking and affected recipes, without eager production. |
| Pair diff / walk | Ordered index scans and merge joins, bounded working memory per key. |
| Rep update | O(changed rows), local transaction and generation advance. |
| Transfer | Safe reference where possible, otherwise O(bytes) production/copy/verification. |

Storage at million-entry scale, plan query performance, and chunk-level blob transfer remain measurements or ADR 013 concerns. Verification can read bytes without moving them; the old claim that only transfer ever costs O(bytes) is withdrawn.

## 9. Errors, concurrency, and recovery

One checkout writer serializes observation and application metadata, including recovery; read-only inspection does not ingest or publish. External filesystem writers still exist, so expected-state checks are necessary even with the checkout lock.

Failures preserve operation/path/source context for FS operations, database/migration details, offending external path components, unsupported/corrupt row fields, and backend-defined source chains. Use a concrete object-safe error boundary if appropriate; invariants are programming errors. An unreadable subtree fails observation—it must not silently generate removals.

A rep can be rebuilt from backend truth. Bindings, acknowledged bases, and local-only work cannot always be reconstructed from today's tree. Losing them is not 'just rehashing.' Real FS operations and SQLite acknowledgement are not one atomic transaction; recovery verifies or safely retries actual outcomes. ADR 011 owns the durable checkout lifecycle and its partial-publication recovery.

## 10. Validation and remaining technical work

The TypeScript checkout experiment and Unison review are bounded design aids, not evidence that current Rust traits satisfy the contract. Exercise a new checkout, same-result changes, transformed ingest, simultaneous moves, dirty removal, inaccessible inputs, crashes on both sides of acknowledgement, multi-output plans, lazy WASI reads, and version unavailability before freezing interfaces.

Outstanding implementation choices include streaming handle shape, storage of policy-owned correspondence, receiver result encoding, generation-aware bulk progress, byte/cache retention, and plan recovery after interrupted moves. None should change the separation of observed state, applied outcomes, and semantic backend ownership fixed above.
