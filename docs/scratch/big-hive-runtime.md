# big_hive runtime: execution model

Status: Aspirational (operator, 2026-11-26): agreed direction, known to be incomplete. It is refined file by file as runtime2 is ported, and the code's module docs become the authority as each part lands. Order: execution model,
then the simulation harness, then machines, then the hub, then the services.
Owner: the automata lane (prod-readiness).
Scope: how big_hive's logic is split into sans-IO machines and drivers, how they talk, how
storage and keyhive are reached, and how multi-document writes commit.
Updated: 2026-11-26.

Sources:
- `big-hive-design.md`: the storage model and the two write paths.
- `big-hive-runtime-inventory.md`: what runtime2 has today and what each part becomes.
- `src/big_automata/driver.rs` and `src/big_automata/sync/peer.rs`: the machine/driver
  conventions adopted here, and `sync/peer/sim.rs` for the simulation.

## 1. What changes from runtime2

The topology stays: a hub that routes and starts things, one actor per open document, a few
singletons, and short-lived work. What changes is what each actor is made of.

| runtime2 | big_hive |
|---|---|
| An actor is an async task whose logic awaits IO (`DocIo`, `RuntimeIo`) inline | An actor is a **machine** (pure state) plus a **driver** (the loop that does its IO) |
| Requests carry an embedded oneshot; the hub spawns a future that answers it | A request is a command with a `CallId`; its outcome comes back to the machine as an input |
| Spawned futures make decisions (retry, reply, reorder) | Spawned futures do IO only; every decision is in a machine |
| Read-only queries go through the hub as tracked tasks | Handles read a storage snapshot directly |
| Clock read inside the hub | `now` is passed into `flush`; machines expose `next_due` |
| Unbounded channels | Bounded mailboxes sized from the machines' own windows (section 5) |
| `FutureForm` throughout | `Send` and native `async fn` in our code; `FutureForm` only where we implement upstream traits (`Storage`, `Keyline`, `KeyhiveStore`) |

## 1a. What we reuse from big_automata

big_hive is built to be isomorphic with big_automata: the same machine and driver conventions,
the same building blocks, and the same read contract, so the code reads the same everywhere.

| big_automata piece | What it is | big_hive use |
|---|---|---|
| Machine convention (`sync::peer::PeerSync`, `durable_outbox::Outbox`) | typed input methods, `flush(now, &mut Vec<Command>)`, `next_due`, a `CallId` per call, `Cancel` commands whose calls still report | every big_hive machine |
| Driver laws (`driver.rs`, `durable_outbox/driver.rs`) | one loop owns the machine, calls polled in sets, the embedder spawns the loop | every big_hive driver (section 3) |
| `kit::scheduler::KeyedScheduler` | at most one task per key; replace (merging seeds), cancel, park/wake, fail with backoff, stopped-before-replaced | the hub's document wakes: frontier catch-up and materialization retry keyed by document, parked until keys arrive and woken when the document's keyhive epoch moves. A machine embeds it and its `flush` turns the spawn queue into commands (`call` = task id) and the stop queue into `Cancel`, as `Outbox::flush` does |
| `kit::watermark` | a stream cursor that only advances past settled jobs | the durable cursor of the hub's catch-up over the change feed |
| `rev::RevSource` / `RevStream` | the one read contract for revisioned sources (`latest_rev`, a bounded `read`, `watch`) and its single consumer | the sedimentree change feed implements `RevSource` (the bespoke `ChangeFeed` on branch 24 is converted); keyhive epoch changes as a second source |
| `okv` | one row per key with a store-wide rev, written in the caller's transaction, tombstones pruned by consumer cursors | small local tables that need no part membership. **Not** the automerge frontier, which needs group parts (below) |
| `durable_outbox` (and `HeldTable`) | durable fetch wants written with the admission; held wants until membership | content fetches in the sync backend; held wants are the parked-notification design from the 2026-11-26 membership work |
| `part_store` | big_automata's part store | big_hive's, in the same database and transaction |
| `long_poll` | the long-poll serving machine | browser serving, unchanged |
| `sync::peer::sim` | whole-machine simulation: fake backend, network ops, virtual clock, crashes, determinism checks | the template for big_hive's simulation harness |

New in big_hive: the Hub, Document, Write coordinator and Keyhive boundary machines. If three or
more drivers end up repeating the same loop skeleton (mailbox, call set, timer, flush), extract it
into `kit` once, rather than copying it a fourth time.

## 2. A machine

The conventions are big_automata's (`PeerSync`):

- One method per input kind, taking `&mut self`: for example `prepared(call, outcome)`,
  `committed(call, outcome)`, `handle_dropped(handle)`.
- `flush(now, &mut Vec<Command>)` emits the commands the inputs since the last flush caused.
  `next_due() -> Option<Instant>` says when the machine next needs time to pass.
- Every command that expects an answer carries a fresh `CallId`. Its outcome is reported
  exactly once: `Done(value)`, `Failed(error)`, `Lost` (transport), or `Cancelled`.
- No awaits, no clock reads, no IO, no channels, no locks, no spawning. Anything that cannot be
  decided from the machine's state is a command.
- Laws are stated in the module docs and tested against a naive oracle (the
  `state-machine-verification` skill).

## 3. A driver

Also big_automata's laws, restated for big_hive:

1. **One loop owns the machine.** Inputs reach it one at a time from that loop. The machine is
   never borrowed across an await.
2. **Every command executes once; every outcome is reported once.** A cancelled call still
   reports `Cancelled`, so the machine frees its slot.
3. **The driver adds no concurrency and no queues of its own.** Everything in flight was issued
   by the machine, so the machine's bounds are the driver's bounds.
4. **Timers.** The loop waits for the next input or `next_due`, whichever is first, and flushes
   after every batch of inputs.
5. **The driver spawns nothing.** Its loop is a future the embedder spawns (`tokio::spawn`
   natively, `spawn_local` in the browser). Calls are polled inside the loop in sets with
   inferred types, never `dyn`-boxed.
6. **Errors.** Transport errors are `Lost`, storage and keyhive errors are `Failed`, and both
   are decided by the machine. A storage error that is not contention traps (section 6).
   Anything else is a bug and panics.

A driver may run a call's IO on another thread (the native SQLite pool, or a CPU-heavy decrypt)
without the machine knowing. That is how per-document work still spreads across tokio threads.

## 4. Actors

| Actor | Instances | Owns | Its driver |
|---|---|---|---|
| Hub | 1 | routing; which documents have a live machine (refcounts, TTL, generation); interest and demand; the in-flight table of hub calls; draining | starts and stops Document loops via an embedder-provided spawn function |
| Document | 1 per open document | live automerge doc; persist-then-apply; materializing remote content; author filtering; checkpoint coverage; handle validity | its own task, so documents run in parallel on a multi-threaded runtime |
| Write coordinator | 1 | multi-document write plans (section 7) | runs plan commits |
| Keyhive boundary | 1 | the keyhive instance; hydration; the keyhive lock; prepare/apply for encryption; membership and ingest under lock plus transaction; peer notification targets; prekey refill policy | runs keyhive calls and their transactions |
| Sync backend | 1 per peer connection | big_automata's `PeerSync` and driver, unchanged | big_automata's |

The Materializer and the Checkpoint logic are sub-machines inside the Document machine rather
than separate actors: their state is per document, and keeping them in the document's loop keeps
per-document ordering for free. They stay separately testable modules.

Dropped (see the inventory): the admission store and its process-local wakeups, the
prekey-janitor loop, archive maintenance, per-message lease tasks, the plain-read tasks, and the
test-only quiescence and freeze seams. A simulation reads "no outstanding commands" instead.

Moved into the write transaction rather than dropped:
- **Group-part membership** (today's group-part worker, a walker over the admission log). When
  membership changes, the Keyhive boundary's locked path computes the affected documents and
  writes the part-store membership rows in the same transaction. No log walk.
- **Prekey refill** (today's janitor). It becomes a reaction inside the same write plan.

### Automerge frontier: published by the Document machine

Not every runtime2 worker is a state machine. Most are incremental maintainers of derived state:
they follow a change stream and do idempotent per-key work. The automerge frontier (published
`(doc, heads)`, the primary signal downstream work runs on) needs no worker of its own
(operator, 2026-11-26):

- **It is a part-store scope, not a plain table** (operator, 2026-11-26). Today the frontier
  worker writes each document's frontier object into a separate frontier part store, with the
  heads as its payload, **and makes it a member of the same group parts as the document**
  (`automerge_frontier_worker.rs`, `publish_heads`: `desired_parts` from the document's parts,
  filtered by the frontier scope). Downstream consumers follow the frontier per group part,
  through `AutomergeFrontierRevisionStore`. big_hive keeps that: a frontier scope in
  big_automata's part store, in the same database. Its membership is written in the same
  transaction as the document's group-part membership (the Keyhive boundary's locked path), so the
  two never diverge. Consumers follow it through the part store's rev stream.
- **Local writes:** the heads after the change are known before commit, so the frontier payload
  goes into the same write plan and commits with the change.
- **Remote content:** the Document machine already hydrates to materialize it. After applying,
  it publishes the new heads in a transaction.
- **Catch-up:** each document keeps `frontier_rev`, the tree `rev` its published heads reflect.
  A document whose tree `rev` is ahead of its `frontier_rev`, **and which we can read**, is
  stale. The hub finds stale documents from the change feed (and at startup by query) and wakes
  their Document machines. Unreadable documents are never hydrated.
- **Bounded catch-up:** a sync wave can make thousands of documents stale at once. The hub wakes
  them through a keyed scheduler with a concurrency limit. A document whose keys have not arrived
  parks, keyed by document, and wakes when its keyhive epoch moves. This replaces runtime2's 30 s
  wait on `await_cgka_ops_count` and its 1 s retries.
- The scheduler is big_automata's `kit::scheduler` (keyed, backoff, park/wake; its retry half has
  no consumer yet) with `kit::watermark` for the feed cursor. This is the part of the deferred
  concurrent delta walker that big_hive needs. It lives in the hub's policy, not in a separate
  actor. A shared publisher would only buy batching several documents' heads into one
  transaction, which the hub can do as well.
- Checkpoint coverage can follow the same wake pattern (keyhive epoch moves for a document).

**big_sync's payload** is not affected. It already carries **sedimentree** heads, minted by
storage in the commit transaction (`big_repo/store/sqlite/sedimentree.rs:787-803`, `tree.heads`),
so it works on relays without keys. big_repo only encodes them through automerge's `ChangeHash`
type, a pun to remove in big_hive.

## 4a. Every state machine, named

Implicit state machines (phases hidden in flags and `Option`s) are what made runtime2 hard to
reason about. Every machine in big_hive is listed here with its states, and each one is
explicit in code: one enum, its transitions in one place, its laws in its module docs.

| Machine | Owner | States | Notes |
|---|---|---|---|
| Document phase | Document | `Cold → Hydrating → Live`, `Parked` | section 11 |
| Document writes | Document | `Idle`, `Committing(call)`, `HeldForPlan(plan)`, with a queue of held writes | law 2 and 4 |
| Remote materialization | Document | `UpToDate`, `Behind(known_rev)`, `Applying(call)`, `Blocked(refs)` | replaces runtime2's materialization retry protocol |
| Frontier publication | Document | `Current`, `Stale(rev)`, `Publishing(call)` | `frontier_rev ≤ applied_rev` |
| Plan lifecycle | Write coordinator | `Queued → Preparing → Encrypting → Committing → Applying`, or `Aborting` | section 12 |
| Document lifetime | Hub | `Absent → Running(generation) → Draining → Absent` | refcounts and idle deadline |
| Catch-up per document | Hub (`KeyedScheduler`) | `Queued`, `Running`, `Backoff`, `Parked(epoch)` | big_automata's scheduler states |
| Keyhive round per peer | Hub | `Idle → Active → Deferred` | from runtime2 `hub.rs:2808` |
| Connection per peer | Hub | `Dialing`, `Connected(conn)`, `Closing` | starts and stops the sync backend's drivers |
| Hub lifecycle | Hub | `Running → Draining → Stopped` | replaces `cmd_closed` |
| Hydration per document | Keyhive boundary | `NotLoaded → Loading → Loaded`, `Evicted` | the fork's explicit `hydrate`/`evict` |
| Locked operation | Keyhive boundary | `Waiting(lock) → InTransaction → Committed` or `RolledBack → Reopening` | membership and ingest path |
| Prepared encryption | Keyhive boundary | `Prepared → Applied`, or `Dropped` (re-prepare on stale) | |
| Interest and demand | Hub | `Wanted`, `Grace(deadline)`, `Released` | the 30 s sync grace from `interest.rs` |
| Peer sync | big_automata `PeerSync` | its own (part lifecycle, walk, cursor) | unchanged |
| Durable outbox | big_automata `Outbox` | its own (want phases, held wants) | unchanged |
| Tree store generation | storage | per-tree `rev`, populate compare-and-set | upstream's rules 1-8, implemented in SQL |

Derived-state maintenance is **not** a state machine and does not get one: following a change
stream and doing idempotent per-key work is the keyed scheduler plus a watermark.

## 5. Messages between actors

**Mailboxes** live in `kit`, as an improved version of `big_sync_core::mpsc`. That module named
both ends of a channel (its errors say which actor's mailbox closed), offered `recv_many` for
batching, and left a TODO to tag messages with send and receive counters for deterministic
replay. The kit version keeps the naming and batching, is bounded with `try_send` only, and in a
simulation records `(sender, sequence)` per message for replay and `SIM_TRACE`.

**Windows.** A window is the most requests one actor may have outstanding toward another. For
example, a Document may have at most N encryptions in flight at the Keyhive boundary, as
`ObjectJobs` caps its record and fetch calls. The machine enforces it by not issuing more, so
each mailbox's capacity is known: the sum of the windows of the actors that send to it. Sizes
come from measurement (likely tens).

- A machine talks to another actor through a command (`Send { to, input }`). The receiving
  driver delivers it as an input. Requests carry a `CallId`, and the answer is an input to the
  requester. **Nobody awaits another actor.** Cycles between actors cannot deadlock, because no
  loop is ever parked on another loop.
- **Backpressure without blocking sends.** Each machine bounds what it has outstanding toward
  each other actor: a window, like `ObjectJobs`. Each mailbox is sized to the sum of the windows
  that feed it, so a driver's send never waits. A full mailbox is then a bug, and it panics.
  This replaces runtime2's unbounded channels and `DocLease::drop`'s `unreachable!` on `Full`.
- **Ordering:** inputs from one actor to another arrive in order (one mailbox per receiver,
  FIFO). There is no ordering between different senders, and no machine may rely on one. That
  rules out runtime2's trick of ordering `DocSyncRoundDone` by riding the event channel behind
  session events.
- **Handles** (`BigDocHandle` and the like) are driver-side. Their drop sends an input
  (`HandleDropped`), and reads go to a storage snapshot or to the document's committed state,
  never to uncommitted state.

## 6. Storage and keyhive as commands

- **Reads:** `Read { call, query }` runs against a snapshot. Most reads skip the machines
  entirely: a handle reads a snapshot itself.
- **Writes:** `Commit { call, plan }`. A plan is a value: rows for each store (sedimentree,
  Keyline, keyhive, secrets, part store and outbox). The driver runs it in one write transaction
  through each store's `*_in(tx, …)` entry point, then calls each store's `committed`. The
  machine applies in-memory state only on `Committed`.
- **Keyhive calls** go to the Keyhive boundary's driver, which owns the instance:
  `Hydrate(doc)`, `PrepareEncrypt(doc, change)`, `ApplyEncrypt(prepared)`, and for membership
  and ingest `UnderLock(op)`, which takes the keyhive lock, opens the transaction, runs the
  operation with Keyline inserts routed into that transaction, writes what the listener
  reports, and commits. On failure it rolls back and reopens the instance.
- **Errors:** contention (`SQLITE_BUSY`) is handled in the SQLite driver (`BEGIN IMMEDIATE`
  waits out the busy timeout). With one writer process, a write that still times out means a
  second writer exists, which breaks an invariant, so it panics. Any other storage failure traps
  (`big_sync/trap.rs`): the error goes to the supervisor and the hive is torn down. Nothing in
  memory is lost, because memory only ever holds committed state.

## 7. Multi-document write plans

The caller hands the coordinator changes for documents D1..Dn.

1. **Admission.** The coordinator admits a plan only if its document set is disjoint from every
   plan in flight; otherwise it queues the plan. Because it is the only place plans start,
   overlapping plans cannot wait on each other in a cycle.
2. **Prepare.** For each document it sends `Prepare { plan, change }`. The Document machine
   builds the change against a scratch copy, rolls the scratch back, and answers with the
   change bytes. It holds later local writes for that document until the plan resolves, which
   keeps per-document order. A document with its own single-document commit in flight answers
   after that commit; that wait has no cycle, because a single-document commit waits on
   nothing.
3. **Encrypt.** For each document, `PrepareEncrypt` goes to the Keyhive boundary. No memory
   changes.
4. **Commit.** One `Commit` with the whole plan: commit rows, tree `rev` bumps and pruning,
   content-key rows, CGKA ops and secrets for any rotation, part-store and outbox rows.
5. **Apply or abort.** On `Committed`: `ApplyEncrypt` for each prepared encryption, then
   `Apply { plan }` to each document, which applies the change to the live doc and releases its
   held writes. On `Failed`: `Abort { plan }` to each document, which releases them with nothing
   applied.

A single-document write is the same protocol with n = 1, run by the Document machine itself
without the coordinator. Commits serialize in SQLite regardless, so the coordinator only has to
avoid cycles, not to order commits.

## 8. Time

Machines take `now` in `flush` and answer `next_due`. Drivers read the clock (`n0_future::time`
natively, `web-time` in the browser). Nothing in big_hive uses `tokio::time`. A simulation
drives a virtual clock.

## 9. What the simulation needs from this

The design must make the simulation (section 2 of the order) possible:

- Every driver runs on one seeded single-threaded executor. Scheduling, message delivery order,
  call completion order and clock jumps come from the seed.
- **Crash points** are `Commit` outcomes: a crash before the commit (nothing landed) or after it
  (the plan landed, no machine heard). A crash drops every machine and every cache and keeps the
  database. On restart, the laws must hold over what the database says.
- Storage is real SQLite, wrapped so the simulation decides when each call completes and can
  inject `Failed` and trap outcomes.
- The oracle for the whole runtime: the set of committed plans, and what each document's readers
  may see given Keyline at each point.

## 10. The simulation harness

Modelled on `big_automata::sync::peer::sim` (ops, scenarios, checks, determinism, `SIM_TRACE`).
Law zero is the same: **a seed replays**, or a failure the simulation finds cannot be reproduced.

### Three tiers

| Tier | What runs | Determinism | Cost |
|---|---|---|---|
| 1. Machine simulations | one machine (Hub, Document, Write coordinator, Keyhive boundary) with fake neighbours and a fake store, the way the peer sim fakes the backend and the peer | full | cheap: many seeds per run |
| 2. Whole-hive simulation | several hives, each with every machine and driver, real SQLite and real keyhive, over a simulated network, on one seeded executor | full, if subduction can be made deterministic (below) | moderate |
| 3. Acceptance | the ported big_repo end-to-end suite over real iroh and subduction | none | expensive: a subset per PR |

### What tier 2 models
- **Executor:** every driver loop on one single-threaded executor whose task order comes from
  the seed. Subduction's spawns go through its injected spawner, so they land on the same
  executor (`connection/manager.rs:351`, `handler/sync.rs:309` take a `spawner`).
- **Storage:** real SQLite per hive, wrapped so the seed decides when each call completes and
  can inject `Failed` (and a trap).
- **Network:** every message between hives is a simulation message. Ops deliver any message
  (reorder, delay), drop one (`Lost`), duplicate one, or disconnect a pair.
- **Time:** a virtual clock for machines (`now`, `next_due`) and for subduction's timeout
  effect.
- **Crashes:** at a `Commit` outcome, before or after the transaction lands. A crash drops every
  machine, cache and in-memory keyhive and keeps the database; the hive restarts and its
  embedder re-adds interest.
- **Ops:** local single- and multi-document writes; membership changes (add, revoke,
  re-add); remote writes on other hives; key arrivals delayed relative to content; handle
  drops; peer restarts; crashes; clock jumps.

### Checks
- **After every step:**
  - no reader sees uncommitted state: a document's live state equals the committed rows for it;
  - per-document write order is preserved;
  - every multi-document plan is all-or-nothing in the database;
  - `frontier_rev` never exceeds the tree `rev`;
  - a parked document has a reason that still holds (its keys are missing);
  - in-flight calls are within each machine's windows;
  - no mailbox overflows.
- **After a quiet drain** (reliable network, no writes): every readable document's frontier is
  current, every content want has landed, and every pair of hives agrees on each document they
  may both read. Then every case crashes, restarts and drains again.
- **Oracle:** the set of committed plans and Keyline's answers at each point, giving what each
  hive's readers may see.

### Subduction and keyhive in the simulation
We use subduction as the serving layer, and keyhive's sync protocol is a serving layer too:
both receive what other nodes send. Our machines see their results only at two boundaries:
remote commits arriving through `Storage`, and remote keyhive events arriving at the Keyhive
boundary's ingest path. So tier 2 can start by **modelling remote hives at those two
boundaries**, like the peer simulation's remote: a simulated remote produces commits and keyhive
events, and the network ops reorder, drop and duplicate them. Running real subduction and the
keyhive protocol between simulated hives is a later step, once the clock reads below are settled.

### To verify before building tier 2
`subduction_core` still reads `std::time::Instant::now` in `connection/manager.rs:459,471` and
`metrics.rs`, and `multiplexer.rs` uses `tokio::time::timeout`. If those only feed metrics or
tests, the seeded executor is enough. If they decide behaviour, we either inject a clock in our
fork (an upstreamable change) or tier 2 models remote sync at the `Storage` boundary (remote
commits arrive as storage writes) instead of running real subduction.

## 11. The Document machine (draft)

One per document with a handle, an interest, or catch-up work. It replaces runtime2's doc worker
(`DocWorkerMsg`: `PutDoc`, `AcquireInterest`, `AcquireHandle`, `CommitDelta`,
`ApplySyncSession`, …) and owns the live automerge document.

### State
- **Phase:** `Cold` (no live doc) → `Hydrating { call }` → `Live { doc, applied_rev }`, or
  `Parked { missing }` when content cannot be decrypted yet.
- **Writes:** at most one commit in flight; later writes held in order. While the document is
  prepared for a multi-document plan, writes are held too.
- **Remote content:** `known_rev` (the newest tree `rev` the hub reported) against `applied_rev`;
  refs that are blocked on keys.
- **Frontier:** `frontier_rev`, the tree `rev` the published heads reflect.
- **Lifetime:** handle and interest counts, a generation that handles carry, an idle deadline.

### Inputs
- From handles: `opened`, `handle_dropped`, `interest(+/-)`, `write { call, op }` where `op`
  builds a change against a transaction (user code runs inside the machine's step: synchronous
  CPU work, no IO), `read { call, op }` against the live doc.
- From the hub: `tree_changed { rev }` (the change feed), `epoch_moved` (the keyhive epoch for
  this document moved).
- From the coordinator: `prepare { plan, op }`, `apply { plan }`, `abort { plan }`.
- Outcomes: `hydrated`, `decrypted`, `encrypted` (prepared encryption), `committed`.

### Commands
- `Hydrate`: the driver reads the committed commits and keys, decrypts through the Keyhive
  boundary, and returns automerge changes. Only for documents we can read.
- `PrepareEncrypt { change }` to the Keyhive boundary.
- `Commit { plan }` for a single-document write: commit rows, content key, any CGKA rows, and
  **the frontier payload** in the frontier part-store scope (the heads after the change are
  known before commit).
- `Prepared { plan, change }` to the coordinator.
- `Reply { call, result }` to a handle; `Notify` to change listeners after an apply.
- `PublishFrontier { heads, rev }` after materializing remote content.

### Laws (to test against an oracle that holds the committed changes per document)
1. **Live equals committed.** The live document contains exactly the committed changes it has
   applied. Nothing is applied before `Committed`. A failed or aborted write leaves it unchanged.
2. **Per-document order.** Writes apply in submission order, and each is built on the committed
   state that includes every earlier write.
3. **Read your writes.** A handle whose write returned sees that write.
4. **Plans hold the document.** While prepared for a plan, no other write is prepared or
   committed on this document; on `apply` or `abort` the held writes resume in order.
5. **Frontier.** `frontier_rev ≤ applied_rev ≤ known_rev`. A local write publishes its frontier in
   its own commit. Remote content publishes once applied. A readable document is never left with
   `frontier_rev < known_rev` and nothing in flight or parked.
6. **Parked means blocked.** A parked document lacks keys for some content, and leaves `Parked`
   on `epoch_moved` by trying again.
7. **Eviction.** Only with no handles, no interest, nothing in flight and nothing held. Evicting
   loses nothing, because everything live is committed.
8. **Unreadable documents are never hydrated.**

### Open, for the operator
- **Author filtering at read time** (automerge 0.12 author APIs): remote changes from an author
  without Edit at that point are filtered during materializing. Where the filter sits (in
  `Hydrate`, or in the machine over decrypted changes) depends on the 0.12 API.
- **Write pipelining:** one commit in flight per document is simple and gives law 2 for free.
  Pipelining (prepare the next write on top of an uncommitted one) would raise throughput, but
  an earlier failure must then abort every later write. Start without it.
- **Checkpoint coverage:** inside this machine on `epoch_moved`, or its own wake through the hub.

## 12. The Write coordinator (draft)

The protocol is section 7. As a machine:

- **State:** plans in flight (plan id → document set, the prepared changes and encryptions
  received so far, the commit call); queued plans whose document set overlaps one in flight.
- **Inputs:** `submit { call, plan }` from a caller; `prepared`, `encrypted` and `committed`
  outcomes.
- **Commands:** `Prepare` to each Document, `PrepareEncrypt` to the Keyhive boundary, one
  `Commit`, then `Apply` or `Abort` to each Document and `ApplyEncrypt`, and `Reply` to the
  caller.
- **Laws:** a plan's documents are disjoint from every other plan in flight; a plan commits
  all its documents' rows or none; every document that was prepared gets exactly one `Apply` or
  `Abort`; queued plans start in submission order once their documents are free.

## 13. The Hub machine (draft)

Only the policy from runtime2's hub survives (see the inventory's policy column). Plumbing goes
to its driver, and plain reads leave it entirely.

- **Document lifetime:** which documents have a running machine; handle and interest refcounts
  forwarded from Document machines; idle deadlines; a generation per start so a stale handle is
  recognised. Starting and stopping a Document loop is a command to the driver.
- **Routing:** change-feed rows (`tree_changed`) to running Documents; keyhive epoch moves to
  the Documents they unblock. **No Document is started just because keyhive changed.**
- **Catch-up:** a `KeyedScheduler` over documents (concurrency limit, park until keys arrive,
  wake on epoch) and a `kit::watermark` cursor over the change feed: a readable document whose
  tree `rev` is ahead of its `frontier_rev` gets a Document started to materialize and publish.
  The cursor is durable, so a restart resumes from it.
- **Keyhive rounds per peer:** `Idle → Active → Deferred` until our admission state catches up,
  with waiters, coalesced notifications and cancel on disconnect, from runtime2's hub
  (`hub.rs:2808`, `:244`). The rounds themselves run in the Keyhive boundary; the hub owns only
  the per-peer policy.
- **Connections:** which connection is current per peer, and starting or stopping the sync
  backend's per-peer drivers.
- **Draining:** a state in which no new work starts and the machine reports when nothing is
  outstanding. This replaces runtime2's `cmd_closed` and the test-only quiescence probes.
- **Laws:** at most one running Document per document id; a Document stops only when it reports
  itself evictable; every stale readable document is either running, queued or parked; parked
  documents wake on the epoch move that unblocks them; draining ends exactly when nothing is in
  flight.

## Open questions

1. **Subduction in the simulation.** It spawns through an injected spawner, which suits a seeded
   executor. Its remaining clock reads are listed in section 10 and still need checking.
2. **Keyhive calls are IO-like commands.** As in runtime2, where `DocIo` methods implemented in
   `native.rs` wrap keyhive and subduction, encryption and decryption run in the driver, not
   in a machine. The Keyhive boundary's driver may run independent calls concurrently and, on a
   multi-threaded runtime, on other threads, because keyhive locks per document internally.
   Only membership and ingest take the exclusive lock. What changes from runtime2 is that
   decisions inside those IO methods (persist order, checkpoint writes, retries) move into
   machines, and the IO surface narrows to plain calls.
3. **Window sizes** per actor pair, and whether a document should refuse new writes (back off
   the caller) when its held-write queue is full.
4. **Automerge heads for consumers:** settled (operator, 2026-11-26). The Document machine
   publishes them, hydrating only documents we can read (section 4). big_sync already uses
   sedimentree heads.

## 14. The machines through database theory

The write paths map onto textbook concurrency control. Use this vocabulary when reasoning about
correctness, throughput and latency, and measure against it.

| Our mechanism | Database-theory name | What the theory says |
|---|---|---|
| A Document machine holds its document from prepare until commit or abort, and holds reads meanwhile | **Strict two-phase locking (S2PL)**, one exclusive lock per document, held to commit | No dirty reads, recoverable, conflict-serializable per document. Lock hold time = prepare + encrypt + commit, so latency of later writers to that document grows with it |
| The coordinator admits a plan only when all its documents are free, acquiring them all at once | **Conservative (static) 2PL**: every lock taken before the transaction starts | Deadlock-free by construction: no transaction waits while holding a lock. The cost is lower concurrency than dynamic 2PL, because locks are held for the whole plan |
| Per-document admission order (operator, 2026-11-27) | **FIFO lock queues per object** | Starvation-free and fair per document; unrelated transactions are never blocked by each other (no convoy across documents) |
| Single-document writes | One-lock transactions | Never wait on anything but their own commit, so they cannot be part of a deadlock cycle |
| Remote commits applied while a local write is in flight | **Commutativity-based concurrency control** (CRDT merges commute) | Commuting operations need no lock against each other; only non-commuting reads of uncommitted state need protection |
| SQLite `BEGIN IMMEDIATE`, one writer process | A **global commit lock** | All commits serialize; commit latency, not CPU, bounds write throughput. Batching several documents' rows into one plan amortizes it (group commit) |
| Keyhive boundary: encryption under a shared lock, membership and ingest under an exclusive one | **Reader/writer (S/X) locks** | Membership changes are rare; under load they wait for in-flight encryptions to drain and block new ones meanwhile, so their latency is the encryption tail |
| Persist-then-apply; commit to the live doc with reads held, re-hydrate on failure | **Deferred update** with a **no-steal** buffer policy | Memory never holds uncommitted state that a reader can see, so a crash needs no undo; a failed commit is rolled back by discarding memory and reloading |
| The change feed, `rev`, `frontier_rev` | A **commit log with log sequence numbers**; derived state tracked by its LSN | A reader is current iff its LSN equals the store's; staleness is a number |

### What to measure (later, with `x/bench.ts`)
- Per-document lock hold time, split into prepare, encrypt and commit.
- Queue wait per document, and plan admission wait.
- Commit latency and commits per second under one writer; the gain from grouping rows into one plan.
- Keyhive exclusive-lock wait (membership latency) under concurrent encryption.

### Levers the theory suggests, if measurements call for them
- Shorten lock hold time by encrypting before taking the document's write slot, where the
  prepared change does not depend on the live document's state.
- Group commit across unrelated single-document writes.
- Optimistic concurrency for plans (validate at commit instead of locking at admission), if plans
  turn out to conflict rarely.

## Decisions, 2026-11-27 (operator)
- Coordinator admission uses per-document order.
- The operator delegated the remaining detail calls to the lead: refusal clearing on standing
  change only, backoff reset tuning, the O(objects) scan on session loss, cheaper aborts deferred,
  single-document writes keep the Document machine's own path.
