# big_hive: storage model and write paths

Status: Operator-approved direction (2026-11-26). The API shapes are provisional; they settle once
the stores and the keyhive boundary are built.
Owner: the automata lane (prod-readiness).
Scope: `src/big_hive`, the replacement for `big_repo`'s runtime, and the changes it needs in our
keyhive and subduction forks.
Updated: 2026-11-26.

Sources:
- `prod-readiness-ledger.md`: the 2026-11-26 big_hive sections.
- `big-repo-runtime-design.md`: the goals and earlier decisions; this document supersedes its
  storage parts.
- Research reports from the 2026-11-26 lanes: sub-trees, kh-archive, multi-id, sql-review,
  sqlite-web. They are summarized below where they matter.

## The decision: we own keyhive's storage semantics

Upstream keyhive moved only its authority graph (Keyline) behind a storage trait. Everything else
(CGKA trees, prekeys, content keys, pending events) is an in-memory object model. It persists
by event sourcing: keyhive mutates memory, announces the change through listeners, and the app
saves the events plus a whole-state archive. Upstream's subduction_keyhive applies changes in
memory before saving them, deliberately, and only ships `MemoryKeyline`. They never promise
"durable before visible" or one transaction across documents, so they do not hit our problems.

We do not wait for upstream. Our keyhive fork (`keyline/hive-store`) adds a storage seam inside
`keyhive_core`. It is kept minimal and upstream-quality, and will be proposed upstream as
keyhive's storage contract next to Keyline's.

**Why big_hive survives whatever upstream does:** big_hive programs against the storage-correct
version of the API: a store trait, lazy per-document loading, prepare/apply for encryption, and
our own transaction around everything else. All keyhive calls go through one boundary module in
big_hive. If upstream lands a different shape, only that module changes.

## One database per hive

A hive is one node's slice of the global graph, so there is exactly one per database (no
`scope_id`). Nothing is stored in files. Every durable thing is a table:

| Tables | Holds | Read by |
|---|---|---|
| Sedimentree: `trees(rev)`, commits, fragments | Ciphertext. Covered history is pruned in the write transaction, so storage *is* the minimized tree (subduction discussion #284). | subduction `Storage` and `MinimizedTreeStore`, both straight from SQL |
| Keyline: certificates plus evaluated state | Who holds what power over what; out-of-order certificates are stored and count once their target arrives | on-demand queries for serving and authorization |
| Keyhive (`KeyhiveStore`) | Key pairs (encrypted), prekey ops per individual, CGKA ops per doc with a predecessor index, content heads, content keys, pending events | keyhive's lazy loads; serving ("which ops may this peer see") |
| Secrets | Per-kind data keys, wrapped by a master key held by `secrets_rs` | the encrypted rows above |
| big_automata | Part store, outbox, cursors | sync |

Each write that touches a tree bumps its `rev`, and those bumps are the **change feed** ("doc D
changed"). In-process wakeups come from the commit; other processes poll `PRAGMA data_version`.

**Memory holds caches only**, each rebuildable from the database:
- the keyhive instance: the CGKA trees of the documents loaded so far, loaded individuals, the
  content-key cache, our identity;
- the live automerge documents;
- at most one small tree cache, validated against `rev`.

Throwing the keyhive instance away and calling `Keyhive::open` again is always safe. `open` loads
only our identity and the pending events.

## Lazy loading, per document

A document's CGKA state loads whole on first use. BeeKEM rebuilds the tree from all of that
document's ops, so the unit is the document, not the database. Loading is explicit:
`keyhive.hydrate(doc)` reads the store and returns its errors there. After that, operations on
the document never touch the store, so they cannot fail on a database read halfway through.
Operations on a document that is not loaded return `NotLoaded`. Methods that return `bool` or
`Option` answer from memory only.

What needs a loaded document:

| Operation | Loaded CGKA? | Our key pairs? |
|---|---|---|
| Serving peers: tree, fingerprints, visibility, ops a peer may see | No: SQL rows | No |
| Local read of content whose key is cached | No: content-key row plus ciphertext | No |
| First read of remote content with no cached key | Yes, to derive the key | Yes |
| Writing (encrypt, maybe rotate) | Yes | Yes |
| Merging incoming CGKA ops | Yes | Yes |

Content keys are mostly a cache, because any epoch we can reach is re-derivable from the ciphertext
header, our leaf secrets and the ops. Two cases are not derivable: the write path needs the
predecessors' keys, and content from before we had access arrives only inside a later commit's
envelope. So the content-key rows must be durable in practice.

## Write paths

### Local content write (the common path)
1. Build the automerge change against a scratch copy, take its bytes, roll the scratch back. The
   live document is untouched.
2. `keyhive.prepare_encrypt`: ciphertext, key, and a rotation if one is due. Memory is untouched.
3. One SQL transaction: commit row, tree `rev` bump and pruning, content-key row, CGKA op and
   secret if rotated, part-store and outbox rows. Several documents can share the transaction.
4. After commit: `keyhive.apply`, automerge `apply_changes`, wake the change feed.

A failure before commit changes nothing, in memory or on disk.

### Membership changes and incoming keyhive events
Keyhive cannot split these into prepare/apply. Membership needs a "what if these certificates
were inserted" over Keyline, which the Keyline contract lacks, and ingest is a retry loop. So:
1. Take the keyhive lock exclusively. (Lock first, then begin the transaction; the reverse can
   deadlock against a task holding the lock and waiting to begin.)
2. Begin the transaction. Our SQL Keyline handle routes inserts into the open transaction while
   the lock is held. (Proposal; to verify.)
3. Run keyhive's operation. Certificates land in our transaction. The CGKA ops it emits are
   written into the same transaction. Membership changes load the documents whose readers
   changed, so their CGKA ops are issued at once rather than at each document's next write.
4. Commit, then release the lock.
5. If the commit fails, roll back (which also erases the Keyline rows), drop the keyhive instance
   and reopen.

**Why the lock exists:** the database needs none, because a write transaction already serializes
writers. Keyhive changes its in-memory state during the call, before our commit. The lock stops
any other operation from seeing that state, for example an encrypt using a key that might roll
back, and lets us discard it on failure. Encryption takes the lock only around `apply`.

Open: whether a per-document lock (for encrypt/apply) next to the exclusive one (for membership
and ingest) is worth it. Membership changes can span many documents, so they stay exclusive.

Later, as an upstream proposal: make the transaction itself the "what if". If keyhive's prepare
ran against a Keyline view bound to the caller's transaction, membership changes could become
real prepare/apply. That is a Keyline contract change.

### Remote content commits
Subduction calls `Storage`: one transaction for the commit rows, the `rev` bump, pruning and the
part store. The change feed fires. A document is hydrated only if someone has it open or
interested.

## Errors
- **Contention (`SQLITE_BUSY`)** never reaches keyhive. Writers use `BEGIN IMMEDIATE`, because a
  deferred transaction that upgrades to a write gets `SQLITE_BUSY` at once without honouring the
  busy timeout. With one writer process, a write that still times out means a second writer
  exists, which breaks an invariant, so we crash.
- **Everything else** (I/O, corruption, a host error) is unrecoverable for that instance. Our
  store adapter traps (the `big_sync/trap.rs` pattern): report to the supervisor, tear the
  instance down. Persist-then-apply means nothing in memory is lost.

## Goals check

| Goal | Status |
|---|---|
| One transaction across N documents, Keyline and CGKA | Yes, for both write paths |
| Durable before visible | Yes: prepare/apply for content; lock plus transaction for membership and ingest |
| No files; wasmCloud and browser | Yes; keys come from `secrets_rs` |
| Separate serving from local use | Yes: serving reads SQL rows and needs no keyhive memory |
| Several processes (one writer, read-only others) | SQL reads work. A reader's hydrated document is checked against a per-document version row and evicted (`keyhive.evict(doc)`) when it moved |
| A second store backend later (e.g. Postgres) | Everything sits behind `Storage`, `Keyline` and `KeyhiveStore` |
| Several principals per node | Not yet: keyhive has one active identity. Separate fork work |
| Single-threaded, sans-IO, DST-testable | The stores fit; the keyhive boundary machine is still to build |

## Work in flight (2026-11-26)
- Keyhive fork `keyline/hive-store`: `KeyhiveStore`, lazy loading, prepare/apply for encryption
  (`4053d96`). Next: explicit hydration, loading affected documents on membership changes,
  decrypting with a cached key without hydrating, `evict(doc)`.
- `stack/automata-24-hive-sedimentree`: sedimentree tables, `Storage`, the SQL tree store, the
  change feed, tests.
- `stack/automata-25-keyline-eval`: on-demand Keyline evaluation from SQL, plus the schema behind
  `KeyhiveStore` (`docs/scratch/hive-store-design.md`).
- Later: the keyhive boundary machine (the two write paths above), the `sqlite_rs` executor
  split and browser backend (the JS host implements the `townframe:sqlite` operations in the
  node's dedicated worker), and multi-principal identities.
