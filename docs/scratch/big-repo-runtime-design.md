# big_repo::runtime: high-level design

Status: Direction and decisions so far (operator-approved); designs per machine are proposals until the file-by-file pass settles them.
Owner: the automata lane (prod-readiness).
Scope: the replacement for `big_repo::runtime2`, captured as discussed with the operator. This
is not a full design. It records the direction and the decisions so far. Each part gets designed
during the file-by-file pass, recorded in `big-repo-port-ledger.md`.
Updated: 2026-11-26.

Sources:
- `prod-readiness-ledger.md`: Decisions, Phase 0, W2/W3, and the 2026-11-25 and 2026-11-26
  big_repo entries.
- `big-repo-port-ledger.md`: the inventory, the write path, Keyline impact and hydration.

## Goals

- **Runs in the browser and single-threaded** (wasmCloud): sans-IO machines, driven by
  runtime-agnostic drivers. `n0_future`, `tokio::sync` only for channels, no `tokio::spawn`,
  `tokio::time` or `tokio::net`. Shared code is `Send`.
- **Storage is the coordination point.** One SQLite database. A local write across N documents
  commits in one SQL transaction. More than one process can share a checkout.
- **Writes are atomic and durable before anyone sees them.**
- **Deterministic simulation and formal models** for every state machine, the big_automata
  treatment.
- **No wire changes to subduction** beyond what upstream ships. Atomicity is local only; peers
  see per-document changes.

## Decisions so far

### The document face stays samod-shaped
- The live Automerge doc is the object callers program against. We keep that.
- **Persist, then apply.** Today the mutation is applied to the live doc under a short sync lock,
  and persistence happens after the lock is released (`lib.rs:1607-1665`). Readers can see a
  change that never becomes durable, and a plain persistence failure leaves it in memory. The new
  path:
  1. build the change in a transaction;
  2. take its bytes and roll the transaction back;
  3. persist (encrypt, CGKA, store) in the write transaction;
  4. `apply_changes` to the live doc under the lock, only after commit.

  The cabinet's dry-run (`big_cabinet/doc/write.rs:108-136`) already does steps 1–2. A failed
  write leaves the live doc clean, so the cabinet's `Failure::Ambiguous` disappears.
- One writer per document stays (the doc worker is single-threaded). The worker's one ordered
  stream per document is no longer a correctness requirement; storage is.

### Keyhive and CGKA join the write transaction
- **SQL Keyline:** a SQL-backed `Keyline` backend, so delegations and revocations are rows in
  the write transaction.
  - Upstream ships only `MemoryKeyline`, which recomputes on every insert and persists through
    keyhive's event log.
  - Its design doc says relays need a materializing backend, and the trait (async, fallible,
    atomic batch insert, version per answer) is built for one. We build it, and may contribute
    it upstream.
- **CGKA prepare, then apply.**
  - CGKA ops and secrets are already in SQLite. The defect is order: encryption mutates the
    in-memory BeeKEM tree, app keys and `known_decryption_keys` first, then persists one
    transaction per event.
  - Needed in our keyhive fork: an API that prepares an update op without mutating anything.
    The op and secret are persisted in the write transaction and applied in memory after commit.
- **The `CgkaOp` listener event** fires after durability, not before. Today it fires before
  (`hub.rs:2207`), and its handler swallows an error.

### Remote content
- Remote commits flow through subduction per sedimentree, as today. Puts are authorized by the
  connected peer (Relay or above), per upstream Keyline subduction (#342).
- **Read-time authorization is the materializer's job.** Whether an author's write counts is
  decided at read time, from commits and certificates. That is automerge's path:
  author-filtered materialization by actor id, from automerge 0.12's author APIs.
- **Hydrate only when someone is interested** (an open handle or interest). Today a writer
  hydrates on arrival to check causal coverage (`doc_worker.rs:1508`). That moves to the
  checkpoint machine and stops needing hydration.

### Checkpoints
- A checkpoint concerns keys and heads, not document content, so producing one should not
  hydrate the Automerge doc.
- keyhive #262 (a CGKA Add wraps an Invitation sealing root secrets) may make most checkpoints
  unnecessary.
- Still open:
  - whether uncovered concurrent epochs still need one;
  - whether it can be detected from the commit DAG plus each ciphertext's key reference, without
    decrypting;
  - whether a checkpoint's parents must be automerge heads or can be sedimentree heads.

  Study in progress: upstream keyhive, subduction #342, automerge-repo-keyhive.

### Sync
- big_repo's `SyncBackend` moves onto big_automata's driver and durable outbox (cut-over).
- The frontier and delta-walker family (`RevisionedStore`, the delta walkers) is built in
  big_automata at its first user here, not ported up front.
- The part store is big_automata's, on the same database and in the same write transaction.

### Not in big_repo
- No key-value datum projection. If one happens, it lives in big_cabinet's datum-set index.
  big_repo offers "commit these change bytes across N docs in one transaction".
- For big_cabinet later:
  - clients that hold the Automerge doc and send change bytes, with the node adding the signed
    dmeta commit on top;
  - unsigned live changes streamed over ephemeral messages, never entering subduction or
    encrypted as commits, signed once on a debounce;
  - deferred: dmeta signatures as map entries.

## Rough machine inventory (to confirm in the file-by-file pass)

| Machine | Owns |
|---|---|
| Document | live doc, persist-then-apply, hydration cache, handle validity |
| Write plan | a multi-doc plan, prepared, then committed in one transaction, then applied |
| Materializer | remote content, author filtering, notifications |
| Checkpoint | epoch coverage over heads and keys, no hydration |
| Keyhive boundary | SQL Keyline, CGKA prepare/apply, event ingest ordering |
| Sync backend | big_automata driver, durable outbox |

## Testing (the big_automata treatment)

- Laws plus a naive oracle and differential proptests per machine.
- Deterministic simulation of the whole runtime, with crashes at every durability point.
- Quint where interaction is subtle: persist-then-apply under crashes, checkpoints against
  concurrent epochs.
- A trimmed conformance subset of today's ~28k lines of end-to-end tests over real
  iroh/subduction.

## Order

1. **Upstream support first, before touching big_repo:**
   - the SQL Keyline;
   - the keyhive CGKA prepare/apply API;
   - the subduction fork on `keyline-take-2` (fragment-head and membership-on-ingest fixes);
   - the keyhive `keyline/townframe` branch.
2. **In townframe:** automerge 0.12 (`dman-os/townframe#54`), then the Keyline repin.
3. **The file-by-file pass over big_repo**, landing `big_repo::runtime` next to `runtime2`, one
   machine at a time.

## Queued

- Upgrade surelock and survey its new release. Its async mutexes are not to be used until
  futurelock is ruled out.
