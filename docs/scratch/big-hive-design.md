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

### Pruning decisions (operator, 2026-11-26)
- **Pruning a commit we could decrypt is accepted** before checkpoints exist. It follows
  upstream's "only the latest heads" stance. The checkpoint machine must account for it: after
  pruning, the content we can decrypt may sit only inside a covering fragment.
- **Library minimization semantics stay.** A pruned commit that arrives again is stored again.
  A stronger local rule would make our fingerprints differ from a peer's, and the peer would
  resend the commit on every sync (the loop in subduction discussion #284). Upstream's eager
  pruning is the real fix.
- **Two payloads for one head** keep only the lowest digest, so the result can depend on arrival
  order. Pinned by a test. It goes upstream as a note on #284, not as a local fix.
- Untested so far: whether fragment domination is order-independent when fragments arrive
  before their commits.

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

## Decisions, 2026-11-26 evening (operator)

- **No per-write clone.** Our lane introduced it: persist-then-apply plus automerge exposing a
  change's bytes only after commit. The fix: commit the user's transaction on the live document,
  take the bytes, persist. Reads are held while the local change is not yet durable; on a failed
  persist the document is re-hydrated. Remote changes may still be applied during an in-flight
  local write (automerge merges in any order).
- **Multi-document writes:** a surelock-style handle that locks N documents at the
  `BigDocHandle` level, over one back-end ingest path taking `(document, change bytes)` for N
  documents. Local handles and external authors (clients holding their own automerge document)
  feed the same path.
- **No `ensure_causal_coverage`.** Checkpoints exist only for the concurrent-epoch gap, checked
  on the write path: a local write checks before encrypting and puts any checkpoint in the same
  plan; arriving remote writes are checked as they come in. No public method, no checkpoint
  machine, no reaction to membership changes.
- **Sync:** the big_automata approach stays (part replay, heads comparison, subduction pull per
  sedimentree, durable outbox and held wants). Ephemeral is not used for change notification.
- **Keyhive notifications** are rebuilt in the Keyhive boundary: after a keyhive event commits,
  notify directly connected peers who may see it, never gossip, with no echo to the source (the
  source is a column on the event row). Not yet implemented in the new stack.
- **Invite keys** (a link carrying a principal's private key): option (a), a second subduction
  session signed by the invite principal over the same iroh endpoint and node identity, opened
  explicitly by the layer above (big_node). Sessions above the connection, upstream, if that
  proves awkward. To check: whether subduction can run a second handshake on an existing iroh
  connection.
- **Read-only open** is deliverable (reads need commit rows and content-key rows, not keyhive's
  memory) but deferred until the main path works.
- **Author filtering** needs nothing from whoever holds the automerge struct. It is decided where
  the Document machine hydrates, from the sedimentree commit signer, the automerge footer author
  and Keyline. Backdating is handled by Keyline's rule to rotate a role after removing an admin
  (`design/keyline/README.md`, "The Ex-Admin Sharp Edge").
- **Attestation (automerge #1742):** build around it. Being researched: where signatures live,
  whether a node can sign a change a client created with the node as author, and whether unsigned
  live changes over ephemeral remain possible.
- **Process:** drafts without CI; only the stack tip is greened, when the operator names it.

## Decisions, 2026-11-26 late (operator)

- **automerge stays in unsigned mode.** Trust lives on the sedimentree commit, signed by the node
  that commits it. The automerge author footer is a claim checked at read time against the commit
  signer and Keyline. The ingest path accepts change bytes plus an optional signature chunk, so
  automerge attestation (upstream #1742, unreleased, heading for 0.13) can be adopted later
  without an API change. Required mode is not adopted: it fixes a document's mode at creation,
  forbids mixing, and disables `fork_at` (used by the cabinet's branch, list and merge) and
  `bundle` (used by the doc worker), and a required-mode watcher cannot apply the unsigned live
  changes streamed over ephemeral.
- **Upstream proposal:** require signatures only on published, served history, not on local
  copies.
- **Drawers need a new architecture** with roles that support Keyline's admin-rotation rule
  ("The Ex-Admin Sharp Edge"). Cabinet-level, above big_hive.
- **Identity and interest (being researched):** one hive holds N local principals (our own plus
  invite/app principals whose keys we hold); a big_automata peer is the remote node's identity;
  a session is (peer, the local principal the subduction handshake authenticates as). Fetching
  document D uses sessions (P, L) where both P and L stand at Relay or above on D. Interest in a
  document that does not exist yet is a durable want with no provider, held until a peer
  advertises it. Keyhive needs extra secret-only local principals for this.
