# Production-readiness lane — ledger

Status: Living ledger. Items are proposals until marked decided; the operator sets priorities.
Owner: prod-readiness lane; operator reviews.
Scope: Everything at or below `big_cabinet` (keyhive and subduction forks, `big_repo`, `big_sync`, `big_cabinet`), the web node, performance, and repo/agent hygiene.
Updated: 2026-11-17 (ledger opened)

This lane cleans up behind the three feature lanes and makes the stack ready to ship. The
first product on the stack is **intxt**: it captures what happens between git events (agent
sessions, context, review) as part of the PR. Update this file whenever an item lands, a
decision is made or a finding turns up. Findings go here, not only in chat.

## Mandate (operator, 2026-11-17)

- This lane owns production readiness. The other lanes do deep feature work.
- Most of the hard questions are answered. The work is mostly reviewing code that already
  works: does it meet the performance and security bar we expect, and how can its quality
  improve. Regroup and move code more often than change semantics.
- First pass: everything below `big_cabinet`.
- Performance is measured from `big_cabinet` down. The cabinet is the main user, and the goal is
  a fast cabinet, not fast layers in isolation.
- Deliverable: one **stack of PRs** owned by this lane. Only the tip has to be green.
  Intermediate PRs are mechanical groupings of commits or hunks, mostly drafts, and need not
  compile.

## Vocabulary (operator's overview, condensed)

- **Node**: a P2P virtual computer where things run (ADR 033, ADR 038).
- **Relay**: an always-available, server-side node. It enables traditional automerge-style sync.
  The `btress_*` crates are its supporting services.
- **Cartridge**: the plugin package. It contains **nodules** (plugins that run on nodes) and
  **micros** (front-end plugins) (ADRs 027, 028).
- **Claytext**: the front-end malleability environment, the "modern Emacs" half. It is built on
  Dioxus (`clay_*`, `dayframe_*`, `micro_*`).
- **keyhive**: identity, document identity, membership and end-to-end encryption over automerge.
  It is CRDT-based, multi-writer and eventually consistent. We run forks of keyhive and
  subduction.
- **big_repo**: wraps keyhive and subduction. It is the primary persistence and IO layer.
- **big_sync**: our homebrew mass-collection sync (`docs/wiki/big_sync.md`).
- **big_cabinet**: a service over automerge docs, keyhive and big_sync (`docs/wiki/docs.md`, ADRs
  019–021, 025, 026).
- **Legacy, slated for removal**: `daybook_*`, `plug_*` and the Compose app. Don't invest in them.

## Decisions (operator)

- 2026-11-17: **Understand what has to change before refactoring anything.** Phase 0 below
  comes first.
- 2026-11-17: No real installs exist, so on-disk formats need **no backwards compatibility**.
- 2026-11-17: Keyhive's `Public` principal can go. Nobody needs it right now, and the operator
  is in talks with upstream.
- 2026-11-17: Transactions mean **data** (automerge doc) transactions, not keyhive transactions.
  They may turn out to be impossible; finding out is part of the work.
- 2026-11-17: big_automata is a stale work in progress. Process: the operator and this lane go
  through big_sync file by file, find what needs solving, and port the solutions into
  big_automata. After that, big_repo gets the same file-by-file treatment, landing the new
  design in a new module inside big_repo next to `runtime2`.

- 2026-11-17: **A SQL-backed Keyline is a goal.** It should scale to very large relays and
  possibly to high availability, and it fits the multiprocessing angle. The drawer wiki mostly
  stands. The Keyline-facing *architectures* need reshaping: admin removal means recreating
  keyhive constructs (rotation), and our drawer architectures are less flexible than the
  upstream `design/keyline/patterns.md`.
- 2026-11-17: The new big_repo module is **`big_repo::runtime`**, replacing `runtime2`.
- 2026-11-17: The `subduction_keyhive` Keyline migration: the operator is talking to the
  maintainers. We may carry a temporary hack of our own, since the crate is small. References:
  automerge-repo's keyhive integration, which rolls its own subduction↔keyhive layer, and the
  latest `alexjg/samod`.
- 2026-11-17: Rebased `refactor/big_automata` onto main as a local branch. Its worktree lives in
  the session scratchpad. Only `Cargo.toml` (workspace members, a duplicate `ciborium`) and
  `Cargo.lock` (regenerated) conflicted. It has not been built yet; machine house rules are
  pending from the nodules lane.
- 2026-11-17: Multi-region scale-out is not a goal. A node stays single-machine. Relays scale as
  shards that sync with each other as a leaderless cluster, not as a Durable Object per doc.

- 2026-11-17: **A keyhive grant inside a transaction joins the same SQL transaction.** Keyline
  (SQL-backed) is learned before the big_repo rework. Design one coordination abstraction for
  the disparate systems (sedimentree, keyhive/Keyline, CGKA, part store, cabinet state) that
  share the one SQLite database. The cabinet's ledger/saga designs (`drawer/ledger.rs`,
  command rows) may be thrown away once it exists.
- 2026-11-17: **The doc worker's linear per-doc story is up for sacrifice.** It gives
  observation a single ordered stream of local and remote changes, and the samod-style "live
  automerge object to program against". But automerge is a CRDT, so one log per doc isn't
  required. The live hydrated instance already drifts from disk on failure: the handles' broken
  and closed flags, the cabinet's `Ambiguous`. Decide whether the new gains justify dropping
  the live-object property. Remote commits arriving over subduction already skip the doc
  worker; they're a pure storage operation (`hub.rs:2957`: no materialization retry without an
  existing worker).
- 2026-11-17: **Multiple principals per connection: next week.** The operator is talking to
  upstream. This week has no bandwidth to reach Keyline.
- 2026-11-17: The lane owns its stack. Pushing needs no permission. No attribution trailers.

- 2026-11-17: **Verification split approved:**
  - Quint + TLC full-graph conformance for each interacting machine (Apalache only where
    symbolic depth is needed);
  - Kani for small self-contained pieces;
  - proptest for APIs;
  - a bespoke DST simulator for liveness;
  - shared test tooling in a new dev-only crate `test_utils_rs`.

  The first machine is `WatermarkMachine`, done with both Quint+TLC and Kani to compare them.

## Phase 0 — what has to change

### Keyline: what it changes for us

Sources: upstream `design/keyline/{README,edge-cases,evaluation-notes}.md` and
`keyline/src/contract.rs` @ `d41b053a`.

**Correction to the version-skew note in W2.** Upstream subduction's `keyhive_core 0.7.0` is
**pre-Keyline**. It comes from crates.io, its lockfile has no `keyline`, and 0.7.0 was cut on
10-13 (#265). On upstream main, keyhive's own membership moved onto Keyline on 10-23 (#288),
the `Keyline` trait became async and fallible on 10-20, and serde was ripped out on 10-27 (#311),
all while the version still read 0.7.0. So bumping subduction to keyhive 0.12 means doing the
whole Keyline migration in `subduction_keyhive`. It is not a version bump.

**The model in one paragraph.** Keyline is a state-based CRDT of two certificate kinds:
delegations `{issuer, audience, subject, power, citation}` and revocations
`{issuer, revoke, retain}`. Authority is never stored; it is derived at every query by
stratified Datalog:
- First, a positive pass that ignores revocations. It gives each key's *admin reach*: every node
  the key ever held Admin over.
- Then a live pass. Each revocation removes routes that pass through its signer's admin reach.

Certificates carry no proof field. A delegation is live while its issuer has standing (late
binding); it dies implicitly when the issuer loses standing and revives if the issuer regains it.
Power is `Relay < Read < Edit < Admin`. "Write" is now called "Edit".

**Shifts that touch our designs:**

1. **An ex-admin keeps revocation power over its admin reach forever.** The only way to durably
   remove an admin is to rotate the role: mint a fresh key, re-supply it, re-roster its members.
   For drawers (`docs/wiki/drawer.md`, ADR 019):
   - A rotation changes a group's id. Drawer datums name groups (`metaGroup`, `docsGroup`), and
     big_sync part keys are derived from groups, so removing an admin rewrites drawer metadata
     and moves parts.
   - Rotating the *collection* group would mean re-supplying every document in it, O(docs).
     Admins therefore must never sit in the collection group. They belong in role groups that
     reach the collection group through a single supply. The agent-groups architecture roughly
     has this shape already. Single-group and self-group drawers do not: an admin there can
     never be durably removed without re-supplying every document.
   - Rotation escapes an ex-admin only if the rotated role held at most Edit over every subject
     it reaches. A drawer that grants Admin over documents leaves those documents in every
     ex-admin's reach permanently.
2. **A document's root role is append-only.** Once the document key is destroyed, which is
   keyhive's `EphemeralSigner` practice, membership in the root role can never be durably
   removed. Upstream's guidance is to root documents at Edit to a minimal owners role and do all
   churn in second-layer roles.
   - That changes how big_repo and the cabinet should create documents, and what "owner" means
     in our APIs.
   - Today, does big_repo root documents at Admin, with the creator as a direct admin?
     Unverified.
3. **Answers two open questions in `drawer.md`.**
   - Q2: re-sharing a document you reached through drawer A into drawer B works by late binding.
     No proof field is needed; your delegation lives as long as your standing does.
   - Q1, public writes: moot now that `Public` is gone.
   - The drawer FIXME ("create new collection groups and redo delegations using the old
     delegation") is legal under Keyline and lives exactly as long as the re-issuer's standing.
4. **Hiding certificates bounds nothing.** Edge-cases Finding 3: set reconciliation enumerates
   hashes to any peer, and relays carry the graph. Replicas need the **closure** of a subject:
   every node that ever had standing over it, plus all certificates about those nodes and all
   revocations of them. The trait has `closure(subject)`; the bigger peer derives the closure
   and ships it.
   - Our keyhive pull pipeline, ADR 013's access delta stream, and big_sync's per-part filtering
     of keyhive events were all built on "send each peer only what it can see". The membership
     layer probably becomes closure-shaped. Content-key and CGKA visibility is a separate
     question.
5. **Evaluation is graph-global and superlinear, and the reference backend doesn't cache.**
   `MemoryKeyline` recomputes both strata on every query. big_repo's `access_policy.rs`
   answers big_sync per-part authorization on hot paths. Either:
   - write our own backend: a SQL-backed `Keyline`, with a recursive-CTE sketch in
     evaluation-notes §5, which can be differentially tested against `MemoryKeyline` with the
     conformance generator; or
   - cache by `digest()` / `Version`.

   This is the storage-model change. The trait is async, generic over `FutureForm`, and
   batch-inserts certificates, so it fits SQLite natively and a browser store.
6. **Removal needs explicit revocations to survive a re-add.** Implicit cascade deaths revive
   when the issuer regains standing (Finding 4). Drawer and cabinet removal flows must decide
   between implicit and explicit removal per use case.
7. **Revocations carry a `retain` watermark** (per-subject content heads to keep), which the
   evaluator ignores. This "whiteout" question — what happens to content a revoked writer
   already wrote — is open upstream. It lands in our data layer: subduction, big_repo and
   cabinet decide which of a revoked writer's commits stay materialized.
8. **Relay revocation needs a key rotation**, through BeeKEM, to actually stop decryption.
   Upstream hasn't specified where the two couple. This affects relay sponsorship (ADR 005).

### Transactions and the doc-worker shape (first read, 2026-11-17)

**Today's write path** (read from code, not run):

1. The cabinet takes a live handle and builds its change in memory, against explicit base heads
   (`doc.transaction_at(.., &base)`, `big_cabinet/doc/write.rs:96`). The change is applied to
   the hydrated automerge bundle first.
2. `Runtime2Handle::commit_delta` sends a command to the hub, which routes it to the doc's
   worker (`hub.rs:809`).
3. The worker (`doc_worker.rs:1041`) runs, in order:
   - the bundle validity gate;
   - the write-access gate;
   - a causal-checkpoint rewrite of parents across epoch boundaries;
   - `DocIo::persist_local_commits` (`native.rs:483`). For each commit it encrypts, which
     **may emit a CGKA update op**, i.e. a PCS key rotation, so writes can rotate keys. It then
     persists that update (keyhive tables) and calls `subduction.store_commit` (sedimentree
     tables).
4. Separately, the automerge-frontier worker publishes the new heads into the big_sync part
   store later.

**One database, many transactions.** Subduction trees and commits, the keyhive event log,
admissions and secret blobs, and the big_sync part store all live in one SQLite database
(`SqliteBigRepoStore`, `big_repo/sqlite/migrations/*`). Yet a single commit spans at least two
SQL transactions, plus an asynchronous part-store update. Even a multi-commit batch on **one**
doc is not atomic: a crash mid-loop persists a prefix.

**The cabinet already pays for the missing transactions.**
- Single-doc writes carry command rows, a two-phase pipeline and boot recovery. There is an
  explicit `Failure::Ambiguous` class: "the change may already be committed in memory while its
  persistence failed" (`big_cabinet/doc/pipeline.rs`). This comes from the handle shape: memory
  is updated before the change is durable.
- Drawer ops (`drawer/ledger.rs`) are sagas with an op ledger and idempotent resume, because
  they span roster datums in several drawer docs plus keyhive grants. `enter()` even narrows
  access by revoke-then-grant, which opens a window with no access.

**What the doc worker buys:**
- One hydrated automerge instance per doc per process.
- Per-doc serialization of encrypt and persist, which keeps CGKA epochs and checkpoint
  rewiring ordered.
- A natural "instance per doc" shape.

It is evicted after a 3s idle TTL (`types.rs:11`), which already suits the cabinet's
hydrate → write → drop workload.

**What it costs:** the actor is the correctness boundary. Atomicity, ordering and the single
writer are enforced in process memory, not in storage. That blocks:
(a) cross-doc atomic commits,
(b) more than one process per checkout (log.md §Multiprocessing),
(c) honest persistence semantics. Memory runs ahead of durability, which is the cabinet's
    `Ambiguous` class.

**Candidate reshape, for discussion, not decided:**
- **Durable commit as the linearization point.** A write is a plan against base heads. The
  runtime encrypts and prepares every doc in the plan, then persists *all* sedimentree
  commits, CGKA updates and part-store payloads in **one SQLite transaction**, and only then
  publishes to the in-memory bundles. The single writer per doc becomes a storage-level
  conditional ("heads unchanged since base", or append-only, which needs no check because
  automerge merges). The actor stops being the guarantee.
- Doc workers become hydration caches plus per-doc encrypt sequencers. A coordinator acquires
  the workers in canonical order (no deadlock), and the plan is two-phase within the process.
- **Snag:** encryption mutates in-memory keyhive and CGKA state *before* persistence. A failed
  SQL transaction leaves memory ahead of disk, the same bug one layer down. Fixing it needs
  keyhive mutations staged and rolled back, or keyhive state living in the same SQL
  transaction. A SQL-backed Keyline (plus CGKA state in SQL) makes that possible, so the two
  goals reinforce each other.
- **What else it buys:** with storage as the coordination point, multiple processes can share a
  checkout, a browser node needs only one "atomic batch" storage API, and per-doc or per-shard
  instances become a deployment choice rather than a correctness requirement.

**Atomicity at peers: out of scope (operator, 2026-11-17).** We will not make transactions
atomic as other peers see them. Changes to the subduction *wire protocol* should be minimal or
none, so that off-the-shelf subduction releases keep working. The read-side gate idea is
dropped.

### Transactions: refined after the operator's input (2026-11-17)

- **Motivating use case.** Cabinet services perform operations that touch N documents:
  - create a doc;
  - modify docs;
  - grant someone access, so keyhive *is* in the transaction;
  - the `big_blobs` encryption worker.

  Doing these as cleanup sagas is painful; `drawer/ledger.rs` and the cabinet command rows are
  the evidence. Automerge assumes the single document is the boundary, but the datum layer uses
  automerge as a key-value database. Transactions resolve that tension. Everything already
  lands in one SQLite database, so it looks doable.
- **The in-memory-keyhive snag, re-assessed.** Keyhive storage is already in SQLite: event log,
  admissions, secret blobs. A SQL-backed Keyline puts the authority graph there too, and the
  trait's `insert(batch)` can join the same transaction if our backend shares it. **What
  remains:** BeeKEM/CGKA state and the encryption path still mutate `keyhive_core`'s in-memory
  structures before persisting. So the snag shrinks to "stage CGKA updates and apply them in
  memory after commit". Needs checking against upstream's new CGKA code.
- **Proposed shape (operator): two write paths around subduction.**
  - **Local (internal) writes** are transactional. They go straight to our storage in one SQL
    transaction (sedimentree rows for N docs, CGKA updates, part-store payloads), then surface
    to subduction.
  - **Remote writes** (sync) keep flowing through subduction per sedimentree, as today. The
    network can't be transactional anyway.
  - Subduction's `Storage` is already our `SqliteBigRepoStore`, so it's the same tables;
    subduction keeps a resident per-tree cache with sharded locks. **Upstream hook:** subduction
    main added "observe storage changes for document loading" (`92b1b15`) and
    `Subduction::load_and_watch` (`edca7fd`). If subduction can be told "storage changed under
    you", our transactional writes can bypass `store_commit` without wire changes.
    *Checked:* the observer (`subduction_core/src/storage/state.rs`, `stored.rs`) watches
    mutations that pass through subduction's own storage-state layer (pending write → cache
    update → watcher publication, under the shard lock). It has an internal "rescan request"
    path. It does **not** see rows written to the backend behind its back. So the two-path
    design needs either a public "invalidate / rescan tree X" entry point, a non-wire API
    addition we could upstream, or our transactional writes routed through a batched
    subduction storage-mutation API that accepts an outer transaction.
  - **Constraint:** passing transaction context across the subduction boundary must not block
    or deadlock subduction's sharded locks. Automerge is multi-writer, so we don't need to hold
    per-doc locks across the transaction. Subduction only needs a single linear story per tree
    to serve, which means monotonic, gap-free sedimentree state. The doc-worker engineering
    exists to guarantee the same thing for materialization.
- **Blocker to look at: automerge copies.** Hydration and `save()` copies (see H2, H4, H9) cost
  memory and time on every transactional write that has to materialize.

### Identities, multiple processes, storage boundaries (operator, 2026-11-17)

- **Today:** one node = one identity = one big_repo = one subduction repository = one keyhive
  "universe", tied to the connection identity. Projects on the same machine serialize on that
  single repo, which is locked while it is modified.
- **Multiple processes per checkout is a big win.** Storage becomes the coordination point; see
  the transaction reshape.
- **Multiple identities, or several keyhive principals on one connection.** This is prompted by
  the November Automerge post (https://automerge.org/blog/2026-november/):
  - The `Public` principal is removed upstream. Public access is now ordinary capabilities:
    the "app-public" pattern has an *App Reader* principal, whose X25519 secret ships with the
    app, and an *App Fetcher* principal, whose signing key ships with the app. The reader gets
    Read and grants Relay to the fetcher.
  - Upstream plans an API for this. A node may therefore act as several principals at once:
    its own plus app principals.
  - Open question: do several principals share one connection, or does each get its own?
- **Separate storage boundaries per project** would today need separate identities. Explore
  whether one node can hold several repositories (sedimentrees plus keyhive authorization)
  without requiring a separate service and identity each. Keyhive is one universe per
  principal, so identity probably stays per hive.
- **Separating compute from storage further:** an inert, inscrutable on-disk format for
  sedimentrees, e.g. served directly from S3, since it's ciphertext anyway. This breaks
  subduction's "authenticate before serving" model, so it's only for certain deployments. It
  is not a read-only replica goal; it's a file format a service sits on top of. Related:
  Litestream or LiteFS for relays and node-side scaling. See H13 (the `wal_checkpoint`
  conflict) and H16 (the state inventory).

### SQLite multi-process blockers (opened 2026-11-18)

Goal: several processes share one checkout's SQLite database (log.md §Multiprocessing; the
transaction reshape makes storage the coordination point). Anything that keeps correctness or
liveness in one process's memory blocks it. Found so far:

1. **Process-local wakeups.** Keyed frontier and part-store readers wait on a `tokio::sync::Notify`
   that only same-process commits fire (`big_sync/keyed_frontier/sqlite_*`,
   `big_sync/part_store/sqlite*`). Another process's commit never wakes them, so live tails
   stall until an unrelated local commit. The same pattern appears in
   `big_repo/store/sqlite.rs` (`local_revision_wakeups`, and admission tailers parking on
   `admitted_head`), and in big_blobs (`admission`, `projection`, `work_queue`, `service`) and
   big_cabinet (`datum_set`, `drawer/state`, `authority`, `datum_store`). This needs a per-site
   audit.
2. **Manual `notify_changed()` after `apply_in_context`.** Same root, and it is correctness by
   convention.
3. **In-memory state that must match disk:**
   - in-memory keyhive and CGKA state mutated before persistence;
   - subduction's resident tree cache;
   - `SqliteBigRepoStore`'s `tree_cache`;
   - `admitted_head` kept in memory.

   Another process's writes make these stale.
4. **Single-writer-by-actor assumptions:** doc workers, the hub, and the per-checkout lock file
   (log.md).

Candidate fix for wakeups: fire from the transaction's commit hook in-process, and poll
`PRAGMA data_version` (O(1)) on the waiting side to see other processes' commits.

### Read-path scale: hydrating many docs (operator, 2026-11-17; deferred)

- UIs scroll past thousands of entries. Examples: a drawer of photos sorted by update time; the
  chat "media I've sent" view in a private one-to-one chat. Today each entry means load
  ciphertext → decrypt → hydrate automerge, which is compute-bound. Relays don't hydrate, so
  they're fine.
- **The direction is indexes on top**: the cabinet `index` module, ADR 024 "indexes are fed by
  the change stream". The first index build is the slow part. Options to measure: a
  multi-threaded cabinet; parallel decrypt and hydrate; a custom decode path that extracts the
  indexed datum without full automerge hydration. Ties to H4 and H5.
- **Deferred** by the operator. Keep it as a performance-harness scenario in W5: a cold index
  build over N encrypted docs.

### Runtime shape: single-threaded, wasmCloud, DST (operator, 2026-11-17)

- **The new `big_repo::runtime` must work single-threaded:** in the browser, and on wasmCloud
  until its planned cooperative threads land. The operator believes the current design does
  not.
  - Evidence so far: runtime2 is generic over `FutureForm` and a `TaskRuntime`, and has a
    `Local` lever (`doc_worker.rs:107`). But the only IO implementation is `native.rs`, which
    is `Sendable`-only. Not verified end to end.
- **Reference: samod** (`alexjg/samod` v0.16.0, 2026-10-23, read at scratch clone):
  - `samod-core` is **sans-IO**. The hub and document actors take events and return effects;
    they never spawn tasks or start threads.
  - `samod` wraps the core for any runtime.
  - `samod-test-harness` drives the core deterministically.
  - Recent work: "preserve document actor task ordering on the threadpool"; access-controlled
    sync via share policies; binding a transport to an authenticated peer id.
  - samod has **no subduction or keyhive** code. It's an architectural reference, not a
    protocol one.
  - By contrast, runtime2 actors `await` injected IO traits inline, which is not sans-IO, and
    that makes deterministic simulation testing (DST) harder.
- **Other references:**
  - automerge-repo 3.0 alpha, rebuilt on subduction: a "Sedimentree backend", a single storage
    read per doc, loose commits compacted later, and the repo stops on write failure.
  - `automerge-repo-keyhive` v0.8.0-alpha.1 (on keyhive 0.6.0 with causal encryption), which
    rolls its own keyhive↔subduction layer.
  - Automerge core's author masking (`maskAuthor`, #1581, "built for Keyhive revocation") is
    relevant to whiteout (Keyline shift 7).
- **Goals for every state machine we touch:** DST, and quint formal models (the wflow lane
  already uses quint and quint-connect; reuse its setup).

**Note on version skew:** per the blog, `subduction_keyhive` tracks `keyhive_core` 0.7.0
upstream, while the blog's own keyhive release is `keyhive_core` 0.10.0 (main is now at 0.12.1).
Upstream subduction's keyhive layer is behind upstream keyhive by their own account.

## Workstreams

### W1 — Agent and contributor onboarding

| # | Item | State |
|---|---|---|
| W1.1 | Delete the stray agent report `final.md` (committed in #77) | done, uncommitted |
| W1.2 | Delete `src/AGENTS.md`. It was a pasted Dioxus 0.7 tutorial under a Dayframe header, and it applied to every crate under `src/`: it was injected into a wflow session, see the 11-16 shakeout. Removed the reference to it from `dayframe-native-dev-loop`. The root AGENTS.md already points at the Dayframe skills. | done, uncommitted |
| W1.3 | Split root `AGENTS.md`: keep only global rules there and move area rules into scoped `AGENTS.md` files. Candidates: the big_sync↔keyhive racing note, the SQL style and the mbx details. First verify which harnesses load nested `AGENTS.md` lazily (Claude Code, pi, codex) so that scoped rules are actually seen. | open |
| W1.4 | De-duplicate CONTRIBUTING.md and AGENTS.md (test timeouts, integration tests, `#[expect]`). Each rule should have one home. | open |
| W1.5 | Rewrite README.md for the current product: nodes, cartridges, Claytext, intxt. Daybook and Compose are legacy. | open |
| W1.6 | Turn `docs/README.md` into a real index: start here, architecture map, the load-bearing ADRs | open |
| W1.7 | Refresh the crate map. `docs/scratch/dayframe-crate-map.md` is stale. CONTRIBUTING's repo guide covers about 30 of the 99 entries in `src/`; none of the `big_*`, `clay_*`, `dayframe_*`, `micro_*`, `method_*`, `service_*` or `btress_*` crates is listed. | open |
| W1.8 | A README per crate, starting with the crates below the cabinet | open |
| W1.9 | More skills and guides, e.g. how big_repo's runtime2 is wired and how to debug it | open |
| W1.10 | `src/README.dayframe.md` names packages `ui`, `showcase`, `desktop`, …; the crates are `dayframe_*`, and `dx serve -p showcase` won't resolve. This belongs to the Claytext lane: flag it to them, don't fix it here. | flag |
| W1.11 | Wiki inconsistencies. The wiki is human-maintained, so flag these, don't edit them: `docs/wiki/big_sync.md` links `./subduction.md`, which doesn't exist; `docs/wiki/README.md` indexes only 2 of its 8 pages. | flag |

### W2 — keyhive and subduction: port to upstream, upstream our fixes

Pins live in root `Cargo.toml:284-295`: subduction `5f418129`, keyhive `a84137d1`, both on
`dman-os` forks (branch `townframe-changes`). Local checkouts are `../keyhive` and `../subduction`;
neither has an `upstream` remote configured yet. Upstream is still alpha and recently went
through a large rewrite.

**Drift survey (2026-11-17).** Fetched with `git fetch <upstream url> main`; no remotes were added.

| | fork pin | merge-base | fork-only | upstream-only | upstream diff |
|---|---|---|---|---|---|
| keyhive | `a84137d1` | `b7968be7` (10-01, #259) | 17 | 105 | 300 files, +45.6k/−21.8k |
| subduction | `5f418129` | `2566cbf` (09-30, v0.23.0) | 38 | 41 | 273 files, +25.7k/−14.2k |

- **Version skew.** Upstream subduction main pins `keyhive_core = "0.7.0"` (#323, 10-13), while
  upstream keyhive main is at `0.12.1`. "Latest of both" doesn't exist upstream, so someone has to
  bump `subduction_keyhive` across keyhive 0.7→0.12: us, or upstream first.
- **What landed on upstream keyhive** after our base, by theme:
  - **Keyline.** Membership moved into the new `keyline`/`keyline_memory` crates, and the
    `Keyline` trait is now async and fallible (#230, #288, `09609b2`). This is the biggest API
    break for `big_repo/keyhive.rs`.
  - **Serde and bincode ripped out** (#311) in favour of the new `keyhive_codec`, which has
    prefix-free decoding and codec laws (#310, #299). Our `keyhive_storage.rs` archives and events
    and the runtime2 workers serialize keyhive types (10 files hit `bincode|serde`). The on-disk
    format will need a migration or a reset.
  - **Content encryption moved** into `keyhive_content` (#313), with envelope bodies (#322),
    causal encryption exposed (#317) and a chunked format (#297). This touches
    `encrypted_blob.rs` and subduction_keyhive.
  - **The `Public` "world" user was removed** (`6a9f35f`). big_repo still uses it at
    `keyhive.rs:670`, `runtime2/keyhive_dispatcher.rs:297`, `runtime2/native.rs:790` and
    `test2/harness/fixtures.rs:69`.
  - **CGKA overhaul:** concurrent updates are merged instead of replayed (#256); ops cite
    certificates (#295, #302, #303) and stay pending until those are in closure (#329); node
    versions and the causal past (#341–#346); invalid paths are blanked (#331, #336); signing
    happens before apply (#342).
  - **Prekeys:** picked uniformly and deterministically (`03ccd31`), carried in the KH frame
    (#287), sent per membership (#260, #266).
  - **Other:** a deadlock fix (#254); the causal-decryption loop fix (#327); zeroize (#312);
    Padmé padding (#339); `keyhive_wasm` can be linked by other crates (#362), which matters for
    the web node.
- **Fork-only keyhive commits likely superseded** (to verify one by one): our CGKA conflicted-leaf
  resolution (`7948922`, `a84137d`), prekey selection (`4661560`, `7f3e438`, `1063446`) and the
  deadlocks (`47daa41`, `e9344ff`). **Likely still ours:** `state_generation` and the shared
  generation counter (`a77133c`, `7948922`), incremental prekey persistence (`6e59f97`), CGKA
  secret storage (`b6ae79c`), pending document allocation (`4b54eec`), the
  static_membership_subject fix (`c500983`), holder-seats-readers (`0a3689c`), and
  "big_repo changes" (`d7e0b39`, unread).
- **What landed on upstream subduction:**
  - **Sedimentree fragment fixes** (#319, #325, #326) overlap our `f413f30`, `61f2c98` and
    `20176e9`.
  - `Subduction::load_and_watch`, storage-change observation (`92b1b15`, `edca7fd`), and a
    managed node and connection lifecycle in `subduction_tokio` (`f4e25b6`). These are directly
    relevant to replacing our doc-worker layer.
  - Connection cleanup (#353–#356), not forwarding to denied peers (`c5bde74`), and a split-out
    effect config (#381).
- **Fork-only subduction:** 38 commits, mostly `subduction_keyhive`: the sync-session sketch,
  SyncStatus, rejection reasons and request ids, removing the protocol mutex, the
  DurableIncorporationHook, the keyhive change reporter with source ids, per-sedimentree
  membership enforcement on ingest (`0ce4ff8`), a single lock hold spanning persist and apply
  (`7e15a2d`), and `944800b`, labelled a "hack". Its classification waits on the keyhive bump,
  because most of it is keyhive glue.

1. Add upstream remotes. For each fork commit not in upstream main, classify it: still needed /
   fixed upstream / obsolete after the rewrite.
2. Start a fresh branch on upstream main and carry over only the learnings that are still
   relevant.
3. Open an upstream PR for every fix that is still relevant. *Outward-facing: confirm each PR
   with the operator before opening it.*
4. Follow `.agents/skills/fork-pinning-and-upstream-sync`. Instrumentation commits stay separate
   from fixes (AGENTS.md).

### W3 — big_repo on the new keyhive and subduction, and the web node

- Port big_repo onto the latest keyhive and subduction. Use the port to rework the storage model.
- Headline deliverable: **big_repo running in the browser**. It is the primary persistence and IO
  layer, so this unblocks running nodes in the browser (the web node).
- `runtime2/` will need heavy rework (`hub.rs` 4.3k lines, `native.rs` 2.8k,
  `doc_worker.rs` 2.3k). The crate has about 62k lines of Rust in total, including `test2/` and
  `test.rs`.
- Background: `docs/DEVDOC/log.md` §"2026-07-04 Multiprocessing" lists today's storage
  formats: SQLite, redb (subduction, blob index), keyhive archives and events, keyring. It also
  covers the browser goal, the actor-and-daemon direction, and which components need cross-instance
  coordination.
- Related prior art: the `feat/native-wflow` worktree (`../townframe-p3N-5`) built a
  transactional kvstore layer with a SQLite backend. Nobody has tested whether it would work on
  IndexedDB. The Clay ledger decided on an ordered KV store (prefix scan + atomic batch) for
  SQLite and IndexedDB, and noted that `wflow_core::KvStore` lacks scan and batch. Reconcile
  these before inventing a third storage abstraction.
- **Design thread: transactional subduction** (operator, 2026-11-17; the original discussion
  is lost, this is the restated framing).
  - Goal: explore **cross-document transactional writes** as a big_repo API surface. Today
    subduction assumes every doc is independent, with its own sedimentree, shards and sync
    session. A transaction spanning documents has no home in that model.
  - No MVCC needed. Every write already references the heads it was made against, and the
    system is friendly to concurrent writers from the top down. So a "transaction" is
    probably *atomic visibility and durability of a set of per-doc changes*. Isolation is not
    the question.
  - Suspicion to test: the doc-handle/doc-worker design, copied from `alexjg/samod`, together
    with today's subduction storage design, is what blocks this. It is decent but has gaps. One
    worker per doc gives one serialization point per doc, and there is no cross-doc commit
    point.
  - Read it together with log.md §Multiprocessing. Per-doc singleton workers are also what
    makes multiple processes per checkout and a browser node awkward. One rework could address
    transactions, multiprocessing and the web node together.
  - First step: map the write path, from `BigRepo` handle → hub → doc_worker → subduction
    storage → big_sync part store. Mark where atomicity is lost today. Bring back questions
    before proposing a design.

### W4 — big_sync cleanup and big_automata

- `origin/refactor/big_automata` is one commit, `4afffca6`, not merged. It adds a new crate,
  `src/big_automata` (about 4k lines: cursor, fingerprint, watermark, outbox, scheduler, rpc,
  tasks), with small edits to `big_sync_core`. It is a rewrite of big_sync's state machines
  around shared shapes (delta walkers, keyed frontiers).
- Decide: salvage it onto main, or treat it as a design reference while big_sync is cleaned up
  in place.
- Method: operator and lane go file by file, identify flows and issues, and mostly regroup code.
  Semantics mostly hold.

**Process (operator, 2026-11-17):** big_automata *is* the big_sync rewrite. It is mostly good
after dozens of redesigns, but unfinished. Nothing gets "fixed" in #165 as such. The operator
drives the file-by-file pass and brings the items he struggled with during the rewrite. This
lane builds the verification tooling in parallel.

### W7 — Verification tooling for big_automata (proposal, 2026-11-17)

**big_automata's machines are already sans-IO**, which is the shape that DST and quint-connect
need:

| Machine | Shape | State |
|---|---|---|
| `BigSyncMachine` (`lib.rs`) | `handle_evt(&mut self, evt)`; drains spawn and stop queues; injected `Clock` | 6 of 8 event handlers are `todo!()` |
| `CursorSyncMachine` (`cursor.rs`) | `on_subscription_evt` / `on_obj_sync_job_evt` push into `out: &mut Vec<CursorMachineCommand>` | delete story is a FIXME |
| `WatermarkMachine<Key, StreamId, Cursor, Lane>` (`watermark.rs`) | generic and pure: `add_job`, `settle_on_cursor`, `settle_all_on_lane`, `watermark`, `retire_stream` | one `todo!()` |
| `StatMachine` (`stat.rs`) | full-sync waiters, `drain_evts` | |
| `Scheduler<Seed>` (`scheduler.rs`) | queue, stop with `Retry`, drain | |
| `SerialOutbox` (`outbox.rs`) | push, front, complete | |
| `DecidePeerStrategyTask` (`tasks/decide_peer_strat.rs`) | cursor-vs-bucket choice | |
| `fingerprint.rs` | seeded SipHash-2-4 `u64` fingerprints | |

**Tooling that already exists:**
- **On main:**
  - proptest 1.12 **model tests**: randomized operation scripts checked against a reference
    oracle. Examples: `big_cabinet/{drawer,doc}/e2e/model.rs`,
    `big_blobs/admission/tests/model.rs`, `clay_collection/tests/model.rs`,
    `clay_surface/tree/tests/property.rs`. Regressions persist under `src/proptest-regressions/`.
  - `insta`, `loom 0.7.2` (in the workspace), the `utils_rs::table_tests!` macro.
  - The cross-engine **conformance scenario suite** pattern in `clay_guest_session/lifecycle_conformance`.
  - **No quint anywhere on main.**
- **On #117 (`feat/native-wflow`, unmerged):**
  - `src/nix/formal-tools.nix`: quint 0.33.0, Rust evaluator 0.7.0, and Apalache 0.62.4
    pre-provisioned. 0.62.4 matters: 0.62.1's 20 MB JSON limit rejected the model before
    checking anything.
  - Workspace dep `quint-connect = "=0.1.2"`.
  - The conformance pattern in `wflow_core/partition/authority/conformance.rs`: a `Driver::step`
    with `switch!` over model actions; a mirrored `model.rs`; a **coverage map that refuses a
    trace set that never reaches some action**; constants mirrored between the model and the
    Rust code.
  - CI in the Lint job: `quint typecheck`; `quint run --invariant` simulation (100 samples,
    16 steps, fixed seed); `quint verify --max-steps=3`, about 7.5 min.
  - Lessons: depth 4 didn't fit (Apalache costs about 100 s per extra step); quint 0.33 rejected
    the model's inductive invariant; keep each model small.

**Proposed layering**, with each tool on the property it is good at:
1. **quint spec + quint-connect conformance, per machine.** These cover safety across
   interleavings. Suggested order:
   - `WatermarkMachine`. It's pure and generic, and it carries the core cursor rule: a cursor
     advances only on ack or obsolescence and never skips an unacked event.
   - `CursorSyncMachine`.
   - `Scheduler` and retry.
   - `BigSyncMachine` as the composition.
   - The bucket strategy last.

   Each gets bounded `quint verify` in CI plus simulation, and a coverage assertion.
2. **proptest model tests** for local data invariants against an oracle:
   - an incrementally maintained bucket fingerprint equals a recompute;
   - `ids` encodings round-trip;
   - part-membership bookkeeping matches a naive model.
3. **A DST harness**, for convergence and liveness under faults, which quint's bounded checks
   won't reach. It runs single-threaded with a seeded RNG, a virtual clock, and a simulated RPC
   that drops, reorders and partitions. It drives the sans-IO machines for two or more peers
   and asserts convergence (equal part-store views) and cursor monotonicity. Reference:
   `samod-test-harness`.

   Keep it inside big_automata's test modules unless big_repo's new runtime needs it too. Then
   it gets one shared home, not a new crate per machine.
4. **insta snapshots** for the RPC wire shapes (`rpc/{bucket,replay,summary}.rs`).
5. **CI placement:** quint goes in the Lint job, as #117 does. proptest and DST seeds run under
   nextest on CI or mothership, never on the laptop.

**Dependency, resolved 2026-11-17:** #117 has no owner and is blocked on the operator's
review. The operator said to split out what we need. **#167** carries `formal-tools.nix` (as of
`b3b614d3`), its devshell and package wiring, and the `quint-connect` pin, on top of #165.
`nix eval .#packages.x86_64-linux.quint` gives `quint-0.33.0`, and the version assert passes.
#117 will conflict trivially on those hunks when it rebases. The stack is tracked on GitHub
(`gh stack link 164 165 167`); local `gh stack` tracking only knows layers 01 and 02.

**Testing strategy (operator, 2026-11-17).** Streamline testing across the repo, with **one
shared crate** that supports four approaches:

| Approach | For | Existing art |
|---|---|---|
| End-to-end | whole features | crate-local `e2e` modules; `big_repo/test2/harness` (converge, topo, heads, dump, fixtures); `big_sync/test/stress.rs`; `big_ndk_testing` |
| Formal verification | state machines and small self-contained pieces | quint + quint-connect (#117 pattern, now #167) |
| Property ("prop") testing | APIs | proptest model tests against an oracle (`big_cabinet`, `big_blobs`, `clay_*`); `src/proptest-regressions/` |
| Deterministic simulation (DST) | liveness, convergence under faults | none yet |

Today the support code is scattered across per-crate harnesses plus `utils_rs::table_tests!`.
`src/test_fixtures/` holds a single image and is not a crate. A shared crate should hold:
- the quint-connect driver scaffolding, including the coverage assertion;
- proptest oracle and strategy helpers, and the regression-path convention;
- the DST simulator: seeded RNG, virtual clock, simulated transport with faults;
- convergence assertions.

Name and scope are to be decided with the operator. Mind symbol proliferation.

**Liveness: can we model it?** Partly.
- quint has temporal operators (`always`, `eventually`, fairness), and Apalache can check
  temporal properties over bounded executions. But #117 needed about 7.5 min for *safety* at
  depth 3, and depth 4 didn't fit.
- So liveness in quint is realistic only for tiny, self-contained machines, such as
  `WatermarkMachine` with two streams and three jobs.
- Proposal:
  - **quint for safety** invariants per machine;
  - **liveness in DST** as "eventually converges / every waiter resolves within N simulated
    steps under fair scheduling and healed partitions";
  - quint temporal checks only where a model stays tiny.

**DST options (survey, 2026-11-17):**
- **Our sans-IO machines (big_automata, a sans-IO `big_repo::runtime`) need no framework.** A
  hand-written simulator in the FoundationDB / TigerBeetle VOPR style drives the machines
  directly, owns time, RNG and message delivery, and injects faults. samod's
  `samod-test-harness` is the reference. This is the cleanest fit, and it works single-threaded
  (browser, wasmCloud).
- **For tokio-shaped code we keep** (runtime2 until it's replaced, node-level e2e):
  - **madsim**, a drop-in tokio replacement that also intercepts libc time and randomness
    (used by RisingWave);
  - **turmoil**, which runs on tokio and simulates many hosts in one thread with a simulated
    network and filesystem (no disk fault injection; links are pairwise);
  - **shuttle / loom** are interleaving explorers for in-process concurrency, not whole-system
    DST; `loom` is already in the workspace.

  The common guidance is madsim or turmoil for real tokio code, and a bespoke simulator for
  sans-IO designs. Sources: <https://s2.dev/blog/dst>,
  <https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html>,
  <https://refpath.github.io/nuif/research/items/deterministic-simulation-testing.html>.
- **Rewind VM** (fzakaria, rewindvm.dev, MIT; posts from 10-03, 10-24 and 11-05) is **not
  Nix-only**. It runs "a Nix build, a test suite or any Linux command" in a KVM guest with a
  patched kernel. The monitor owns time, interrupts and RNG, so every run is a function of its
  inputs (seed included).
  - It can replay failures, scrub through them, fork them under different interleavings
    (`rewind check` perturbs the schedule), and export replayable `.rwd` runs.
  - Limits: **one vCPU** (true-parallel races are out of reach); no preemption between
    syscalls; **loopback-only network**; **x86_64 Linux with KVM only**, so not mothership
    (aarch64); replay only on the same CPU vendor.
  - Fit: a black-box tool for catching and replaying flaky *existing* tests (big_repo `test2`,
    big_node stress seeds like S1) on an x86 CI runner with KVM. It complements, not replaces,
    in-process DST of our machines. Worth a trial on S1.

**Tool choice, Quint vs the alternatives (survey, 2026-11-17).** The operator asked why Quint
has mindshare and whether Aristotle and the others are better.

| Tool | What it verifies | Tied to our Rust how | Fit for us |
|---|---|---|---|
| **Quint** (+ TLC / Apalache) | Protocol specs with TLA+ semantics in typed, programmable syntax. TLC checks safety exhaustively, and liveness under stated fairness; Apalache does bounded symbolic checks. | quint-connect trace replay, or **full-graph conformance**: export TLC's reachable state graph, and a Rust test checks every (state, event) pair against `step` | **Interacting state machines**: big_automata, the doc runtime. The mindshare comes from TLA+ power with a programmer-friendly syntax plus a real Rust link (Informal Systems / Malachite). |
| **Kani** (AWS) | Bounded model checking of the *actual Rust code*: no panics or overflows, user assertions, contracts | None needed, it checks the code itself | **Small self-contained pieces**: `WatermarkMachine` and `Scheduler` over bounded event sequences, fingerprint and id encodings. No spec drift is possible. Bounded; async and big collections get expensive. Used in production CI on Rust std. |
| **Verus** | Full functional-correctness proofs in a Rust dialect | Proofs live beside the code (own toolchain) | Highest annotation cost. Only if a component needs proofs, not bounded checks. |
| **Aristotle** (Harmonic) | An *AI prover* for Lean 4. Also checks C and Verilog per Harmonic. | A Rust→Lean extraction pipeline (Charon/Aeneas or hax) whose proofs the Lean kernel re-checks | Functional correctness of pure algorithms, e.g. crypto (arXiv 2605.30106). Doesn't fit concurrent or interacting machines. The spec is still ours to write. Commercial API. **Not for this lane now.** |
| Stateright | Explicit-state model checking of Rust actor code | Runs the Rust directly | No commits since 2025-07; documented false negatives in "eventually" over cycles. **Avoid.** |
| P (AWS) | Communicating state machines, plus PObserve on production logs | — | Needs .NET and Java, no Rust integration. |

**Strongest evidence: flotilla's research issue** (flotilla-org/flotilla#3109, 2026-10-12). It
asks our exact question for Rust lifecycle machines. Their decision:
- **Quint specs, with TLC as the exhaustive CI gate**, Apalache for deeper bounded checks, and a
  Rust simulator for fast sampling.
- **Full-graph conformance** as the main link: "A Rust test checks every (state, event) pair, so
  `step` accepts exactly the transitions the spec allows". quint-connect trace replay is used
  only for code too rich for the full graph.
- Spike numbers:
  - TLC: 9,377 states in 7.2 s, mostly JVM start-up.
  - Full-graph conformance: milliseconds, and it **caught both planted bugs**.
  - Trace replay: caught only one of the two.
- Gotcha: `quint run --seed` defaults to a single sample and can report "ok" falsely, so always
  pass `--max-samples`.

**Implication for #117's pattern:** #117 uses Apalache to depth 3 (about 7.5 min) plus trace
replay. For small machines, **TLC with full-graph conformance is cheaper and stronger**. Proposal:
- Quint + TLC full-graph conformance for each big_automata machine;
- Apalache only where a model needs symbolic depth;
- Kani harnesses on the pure pieces;
- proptest for APIs;
- the bespoke DST simulator for liveness.

**TLC is not a quint backend:** `quint verify` drives Apalache only. TLC runs on the TLA+ from
`quint compile --target=tlaplus` (<https://quint-lang.org/docs/model-checkers>), which is the
route flotilla's spike took, plus a converter from TLC's dumped state graph. So `formal-tools`
must also provision `tla2tools.jar` (TLC). The JSON-limit fix to Apalache 0.62.4 also unblocked
`compile --target=tlaplus` (per #117's comments).

**`WatermarkMachine` read for modelling (2026-11-17; `src/big_automata/watermark.rs`).**
Found before writing any spec:
- **W-1 (bug):** `add_job` opens every slot with `ready: true`. `drain()` takes the leading run
  of ready slots as the watermark, so unsettled jobs count as done, and the watermark passes
  unacked work. That violates the cursor rule (advance only on ack or obsolescence), and
  `settle_*` setting `ready = true` becomes a no-op. Expected `ready: false`.
- **W-2 (bug):** `add_job` skips when `cursor <= last_emitted.unwrap_or_default()`. If
  `Cursor::default()` is a real cursor (0), a fresh stream's first job at cursor 0 is dropped.
  Compare against `Option` instead.
- **W-3 (semantics):** `add_job` lets different streams carry *different* lanes at the same
  cursor and key, but `settle_on_cursor(key, cursor, lane)` removes the whole waiter and panics
  if any stream's lane differs. Either mixed lanes per cursor are illegal (assert in `add_job`)
  or `settle_on_cursor` should settle only that lane's streams, like `settle_all_on_lane`.
- **W-4 (gap):** obsolescence, where a newer cursor for the same key supersedes an older
  pending one, is not expressed here. Is it the caller's job (`CursorSyncMachine`), or should
  `add_job` on a newer cursor settle the older waiter for that key?
- **W-5:** `retire_stream` is `todo!()`. What happens to its waiters (drop that stream from
  every waiter, remove empty waiters and jobs)?
- **W-6 (tests):** `tests` holds two empty fns without `#[test]`.

These go to the operator's file-by-file pass (FIXMEs in `watermark.rs`), not edited by this
lane. The spec waits on W-3, W-4 and W-5.

**DST at n0 (survey, 2026-11-17).** n0's `patchbay` (MIT/Apache-2.0) builds **Linux
network-namespace emulations** (veth, nftables, tc netem) and runs real code in them. iroh's
0.98 release used it as a NAT-traversal test matrix. It is **not deterministic**: "It does not
virtualize per-node clocks or provide deterministic virtual time." It needs unprivileged user
namespaces, `tc` and `nft`. **Fit:** transport and holepunching e2e for `big_node/peers*` and
relays in CI. Not a DST substitute.

**Shared test crate placement (operator offered `utils_rs` or a new `test_utils_rs`).**
Recommendation: **`test_utils_rs`**, used only as a dev-dependency.
- `utils_rs` is a production dependency of wasm guests and declares target contracts.
- Test tooling (proptest, quint-connect, the DST simulator, convergence checks) must stay out of
  the production and guest closure.
- `utils_rs::table_tests!` can stay where it is, or move later.

**Open:**
- Name and scope of the shared test crate.
- Which machine to model first. The suggestion is `WatermarkMachine`.
- Whether to trial Rewind on S1, which needs an x86 KVM host: the laptop, or a CI runner with
  `/dev/kvm`.

### W5 — Performance

- There are no benchmarks anywhere in the workspace: no `[[bench]]`, criterion or divan.
- Build a cabinet-level harness first, then attribute costs downward to big_sync, big_repo,
  keyhive and subduction. Use `.agents/skills/perf-investigation`.

### W6 — Security review below the cabinet

- Review the authorization paths: big_sync per-part authorization, `access_policy.rs`, keyhive
  admission and dispatch. Open to scoping.

### Handoff from the nodules lane (2026-11-17)

Source: `docs/scratch/nodule-runtime-ledger.md` on `origin/stack/node-37-adr049-permissions`
(line numbers at that commit). Their D30 (operator, 11-05): big_repo is a rewrite candidate once
upstream finishes the keyhive and subduction rewrite. Until then, fix obvious costs in place and
flag wrong turns.

**big_repo costs, flagged and not fixed** (their ledger ~373–416). Each item gets a disposition
of fix-in-place or fold into the `big_repo::runtime` design:

| # | Item | Disposition |
|---|---|---|
| H1 | The admission reader polls with no wakeup (its comment claims `notify_one`) | |
| H2 | Every commit stores a full `doc.save()` snapshot as the head payload | |
| H3 | `persist_local_commits`: one transaction per change, each a full `mutate_tree_in_tx`; an `InsertBatch` mutation already exists. Overlaps the Phase 0 transaction finding. | design |
| H4 | `load_doc_snapshot` re-walks and re-decrypts all history on every call, including every `ReattemptMaterialization`. The cold walk is N(N+1)/2 ancestor decrypts: 2.6 s of a 3.1 s reboot at 200 notes. It rebuilds the doc from scratch, about 30 MB per drawer load. | |
| H5 | `try_causal_decrypt` builds a fresh `NativeCiphertextStore` per call: two SQL loads per ancestor per walk | |
| H6 | Quadratic: each admitted `Delegated`/`PrekeysExpanded` row triggers `ReconcileReaders` → `docs_for_agent` (`keyhive.rs:448`) → `transitive_members_short_locked`, which walks every doc | |
| H7 | The frontier worker's per-doc task is replaced on every admitted CGKA op row, cancelling the in-flight publish (8.2k–8.6k "caller dropped before response" in one failing seed) | |
| H8 | `register_bundle_lease` (`doc_worker.rs` ~708) swallows a send error and `.ok()`s the reply, against the AGENTS.md channel rule | fix in place |
| H9 | Hub `put_doc` saves the whole doc twice to detect a content mismatch; comparing heads would do | |
| H10 | `process_pending_fragment_requests` keeps pending fragment work only in memory; it's lost on a crash | design |
| H11 | Nothing repacks old snapshot blobs | |
| H12 | Open: CGKA resolver liveness. If the lowest issuer is offline or lacks access, the others stall (fix `0c831218`; their ledger ~409) | |

**Tagged for this lane** (their ledger ~752–757):
- H13: `wal_checkpoint(TRUNCATE)` every 5 min in big_repo maintenance (`5a0f4aad`, #41) fights
  Litestream. There's also a catch-and-log at `native.rs:2484`.
- H14: `INSERT OR IGNORE` is too broad in `insert_commit`/`insert_fragment`, and the
  conflict-identity invariant test is missing.
- H15: the keyhive archive loads everything at startup (`keyhive_storage.rs:11` FIXME). This
  folds into the SQL-backed Keyline.
- H16 relay ops, a node state inventory:
  - `node.json` seed;
  - wrapping key only in the keyring;
  - keyhive DEKs stored as files;
  - `node.sqlite` and `big_repo.sqlite` in WAL mode;
  - blobs in redb;
  - no health endpoint, a random QUIC port, no stop timeout.

  "Serve from S3" is still to be explored. Their D48: proper relay ops (backup via Litestream,
  Helm) wait on this refactor, and wasmCloud relays come before a Helm chart.
- H17: the alpha relay's status showed no connected peers while it was syncing. Not
  investigated.
- H18: from the ADR 033 placement notes (~888): sync and transport plumbing in `big_node/peers*`
  and blob part bookkeeping stay below the cabinet, and the blob part bookkeeping should go
  behind a big_sync interface rather than big_repo.

**Shared, no owner yet:**
- S1: `big_node e2e::stress::long_test_seeded_partitions_restarts_and_grants_converge`, seed
  `0xb160de5eed00001`. `docs.recover()` fails with "capability does not permit" on a restarted
  node; it failed once, then passed 3 times out of 3. It may be cabinet-side, which would make it
  nodules'.
- S2: a member admitted by an owner who doesn't hold a doc can never decrypt it (their ~310).
  Likely moot or changed under Keyline and the new CGKA.

**Boundary:** nodules owns everything above the cabinet. Ping them before changing how
`big_node` wires big_repo or big_sync.

## Open questions for the operator

1. ~~W3: which design discussion?~~ Answered: the discussion is lost; its substance is restated
   under Phase 0.
2. ~~W4: rebase or reference?~~ Answered: rebase. Done locally.
3. Order of work: W2 (upstream classification) is the critical path for W3. Start it now, with
   W1 in the gaps?
4. Stack base: this lane's branch is `prod-readiness` off `main`.
5. Transactions: does "grant access" inside a transaction mean the keyhive certificate insert
   joins the SQL transaction (needs our SQL Keyline), or is it sequenced right after it?
6. Multiple principals per connection: wait for upstream's app-public API, or design for it now?

## Log

- 2026-11-17: Ledger opened. W1.1 and W1.2 done in the working tree.

## Hash scheme review (operator, 2026-11-18)

Review every hash scheme across the repo at some point, for consistency and for what each one
is relied on for. Known so far:
- **`utils_rs::hash`:** `hash_obj` is SHA-256 over RFC 8785 JSON (`json_canon`), multihash,
  base32. The blake3 helpers are multihash, base58.
- **big_automata:** `PayloadDigest` is blake3 over RFC 8785 JSON with a domain tag. Bucket and
  leaf fingerprints are seeded SipHash over the digest, with domain tags (`-v2`).
- **`cheapstr`:** `DefaultHasher`, fine in process only.
- **To find:** keyhive/subduction content addressing, sedimentree, `big_blobs`, cabinet datums,
  and any `std::hash::Hash`-derived value that crosses a process or a peer (it isn't stable
  across Rust versions).

Questions: one canonicalization (JCS) everywhere? multihash prefix for stored digests? domain
separation tags everywhere?
