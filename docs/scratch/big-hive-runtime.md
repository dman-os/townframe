# big_hive runtime: execution model

Status: Draft for operator review (2026-11-26). Section 1 of the agreed order: execution model,
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

- **Local writes:** the heads after the change are known before commit, so the frontier row goes
  into the same write plan and commits with the change.
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

## 5. Messages between actors

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

## Open questions

1. **Subduction in the simulation.** Subduction is not sans-IO. Either the simulation runs real
   subduction over an in-memory transport on the seeded executor, which is deterministic only if
   subduction spawns nothing of its own, or remote sync is modelled at the `Storage` boundary
   (remote commits arrive as storage writes). To check: what subduction spawns.
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
