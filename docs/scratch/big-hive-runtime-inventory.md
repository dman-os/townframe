# big_hive: runtime2 inventory

Status: Research input for the big_hive machine design. Read-only mapping of
`src/big_repo/runtime2/` on `stack/automata-22-sql-shared`; nothing was built or run. Items
marked (inferred) were not read directly from code.
Owner: the automata lane (prod-readiness).
Scope: every actor, worker and task in big_repo's runtime2, and what each becomes in big_hive.
Updated: 2026-11-26.

Sources: `big-hive-design.md` (target), `big-repo-port-ledger.md` (inventory and write path),
`src/big_automata/driver.rs` (the machine/driver laws big_hive adopts). Paths below are under
`src/big_repo/runtime2/` unless stated.

## Topology today
- Built in `spawn_native_runtime2` (`native.rs:2114`). Three task sets, all real `tokio::spawn`
  (`tasks.rs`):
  - `machine_tasks`: the hub loop (`hub.rs:3671`).
  - `worker_tasks`: group-part, causal-checkpoint, prekey-janitor and frontier (AFW) workers and
    the keyhive dispatcher (`native.rs:2375-2505`), stopped before the command channel closes.
  - `child_tasks`: doc workers, every `spawn_tracked`/`spawn_background` task, connection
    watchers, the subduction listener and manager, the keyhive maintenance loop.
- Two unbounded channels (`Runtime2Cmd`, `Runtime2Evt`), plus one unbounded mailbox per doc
  worker. The hub polls commands before events, then a 500 ms janitor timer.
- Outside producers into the event channel: subduction's observer bridge (`native.rs:2081`), the
  keyhive sync-done observer (`native.rs:2258`), the keyhive listener (fires `CgkaOp` before the
  op is durable), the incorporation sink, and the per-connection RPC subscription task
  (`native.rs:1638`).
- The caller mutates the live document on its own thread before persisting (`lib.rs:1583-1612`).

## Per element ([D] = state that must match disk)

| Element | Kind | State and hidden machines | big_hive fate |
|---|---|---|---|
| Hub (`hub.rs:26-155`) | persistent singleton | connected peers; keyhive round per peer `Idle → Active → Deferred` until `admitted_head` [D] catches up (`:2808`); worker lease/eviction with two refcounts, TTL and a generation (`:3047-3380`); materialization retry `NoRetry → InFlight(seq) → Completed/Stale` with a latch (`:2947-3005`); test-only quiescence/freeze | split: Hub/router, Document, Keyhive boundary, Materializer, Sync backend; test seams dropped |
| Doc creation tasks (`:343-520`) | one-shot | five-step reservation protocol with hub round-trips | Write plan (one create transaction) plus Keyhive boundary |
| `contains_sedimentree`, `has_local_doc_state`, `inspect_stored_doc_blobs` | one-shot | plain storage reads | dropped as tasks; handles read a snapshot |
| `start_sync` (`:1157`) | one-shot | starts a keyhive round | Keyhive boundary command, outcome as input |
| `sync_doc_with_peer` (`:1734`) | one-shot | emits `DocSyncRoundDone` on the event channel to order it after session events | `SyncBackend::fetch`; the ordering hack goes |
| Connection open/accept/close/watch (`:1495-1733`) | tasks | transport lifecycle | driver and transport adapter |
| `release_lease` (`:3166`, `:3207`) | one task per routed message | | dropped (performance footgun) |
| `forward_materialization_retry`, `await_worker_fence`, `persist_prekey_state_snapshot` | one-shot | | dropped |
| Doc worker (`doc_worker.rs:43`) | per-doc actor, one task each | `DocState {Unloaded, Transient, Live, PendingMaterialization}` (`:247`); `causal_epoch`, `cgka_ops_count` [D]; `blocked_refs`; fragment queue (lost on crash); handle validity incl. `Broken(CommitRejected*)` (`types.rs:253`); awaits `DocIo` inline and a `RegisterDocLease` round-trip (`:716`) | Document machine, plus Write plan (commit), Materializer (apply/retry), Checkpoint (coverage). `CommitRejected*` becomes unreachable |
| Group-part worker (`group_part_worker.rs:47`) | singleton, keyed sub-tasks (64) | walker over the admission log [D]; decode → parked/unresolved → fan-out → ack | dropped: membership and access rows are written in the transaction; the pure "affected docs of an event" decode (`:790`) moves to the Keyhive boundary |
| Causal-checkpoint worker (`causal_checkpoint_worker.rs:51`) | singleton | own walker [D]; per-doc `covering` flag; calls through hub → doc worker, whose checkpoint emits a new admission row that wakes it again; terminates only by idempotence (inferred) | Checkpoint machine, without hydration |
| Frontier worker, AFW (`automerge_frontier_worker.rs:129`) | singleton | two walkers; acquires the doc through the hub, waits up to 30 s on `await_cgka_ops_count`, writes 3+ transactions into a frontier scope; retries every 1 s | dropped: the sedimentree `rev` bump is the change feed. **Open:** does sync still need published automerge heads (`backend.rs:~120` "heads equal → Noop")? `automerge_doc_obj_id` needs a new owner |
| Keyhive dispatcher (`keyhive_dispatcher.rs:151`) | singleton | subscription map; 100 ms / 1 s debounce; reads admissions from head, not a durable cursor; a slow subscriber stalls the turn (inferred) | target selection to the Keyhive boundary (`NotifyPeers`), debounce as `next_due`, sends in the driver |
| Keyhive admission store (`keyhive_admission.rs`) | library | process-local `watch` wakeup (`:37`) | dropped; the change feed replaces it |
| Keyhive access stream (`keyhive_access_stream.rs`) | library | `Watched` is still `todo!()` | concept kept (read-time Keyline truth), re-homed on the change feed |
| Prekey janitor (`prekey_janitor_worker.rs:52`) | singleton | on `Add{local, pk}` rotate and refill to 8; panics if the pool does not grow | policy becomes a Keyhive boundary reaction inside one write plan; loop dropped |
| Leases and interest (`lease.rs`, `interest.rs`) | RAII types | `DocLease::drop` `try_send`s to the hub, `unreachable!` on `Full` (`lease.rs:42`); 30 s sync grace | Document inputs `HandleDropped`, `Interest*`; interest maps to big_automata `RouteUpdate`s; a small Interest/Demand machine (suggested) |
| `tasks.rs`, `handle.rs`, `messages.rs`, `types.rs`, `io.rs` | plumbing | | driver owns spawning (`ManualTime` is the model for the test clock); handle becomes driver-side; messages become input/command enums with call ids instead of embedded oneshots; `io.rs` seams become the storage and effect surface |
| `native.rs` adapters | | iroh transport, keyhive RPC subscription, subduction listener/manager, ephemeral switchboard | stay as native adapters; the keyhive maintenance loop (archive compaction) is dropped |

## Waits and cycles
- Callers, spawned tasks and workers await oneshots through the hub; the hub never awaits, so it
  cannot deadlock. No sync lock is held across an await in runtime2.
- The causal-checkpoint feedback loop (task → hub → doc worker → keyhive persist → admission
  row → same worker) terminates by idempotence (inferred). Model-check it in its new form.
- AFW stall chain: a lost wakeup stalls 30 s, then retries after 1 s.
- Receipt ordering depends on `DocSyncRoundDone` riding behind the round's session events while
  the hub polls commands first (`hub.rs:2125`). Fragile.
- Keyhive's internal async locks across `DocIo` awaits are a risk on a single-threaded executor
  (inferred; see the queued surelock/futurelock note).
- runtime2 already uses `tokio::time::timeout` (AFW `:310`), `Notify` and `watch` directly,
  against the "no `tokio::time`" goal.

## Hub responsibilities: policy versus plumbing

| Group | Policy (machine) | Plumbing (driver) |
|---|---|---|
| Routing | which doc machine gets an input; never create a machine just because keyhive changed | delivery, reply channel per call id |
| Worker lifecycle | TTL, refcount rule, generation as handle epoch | one driver task per Document machine, abort on eviction |
| Leases and interest | refcounts, deadlines | `Drop` turned into an input |
| Keyhive rounds | waiters, coalescing, watermark deferral, cancel on disconnect | the `initiate_sync_with_request` call |
| Keyhive event fan-out | which docs re-materialize after which events | the membership read, notification delivery |
| Materialization retry | reduces to "retry if the observed rev moved" | none |
| Sync session routing | receipt semantics `Ready / Pending / Stored`, failure translation | subduction callbacks, the round future |
| Connections | which connection is current | dial, accept, close, watcher |
| Doc creation | the create plan | executing it |
| Quiescence, freeze, inject seams | dropped: the DST harness reads "no outstanding commands" | none |
| Shutdown order | none | driver, reverse of construction |

The hub is about 80% policy (fields and `handle_*`/`finish_*` methods) and 20% plumbing (the
`F::...` future constructors in `hub.rs:289-1830`). Policy reads `clock.instant()` (`hub.rs:600`,
`:3519`); it becomes an injected `now`, as in big_automata's `PeerSync`.

## Scaling across threads
Today it comes from one `tokio::spawn` per doc worker and a hub that never awaits workers. Under
the machine/driver laws the same holds if each Document machine has its own driver task and the
hub driver only forwards inputs. Two properties must be kept deliberately: per-doc input order and
single writer come from each document's driver loop (storage transactions do not give them), and
time is an injected `now`.

## Flags
- `hub.rs:2205` FIXME ("a rotation can correspond to multiple CGKA ops") sits in the path the
  Materializer replaces.
- `native.rs:2470-2490` logs and continues on maintenance failure; `native.rs:2450` discards the
  subduction manager error with `.ok()`. Both against AGENTS.md.
- Four walkers (group-part, causal-checkpoint, AFW, prekey janitor) read the same admission log
  with four progress rows; three `bincode`-decode every row with `.expect`.
- `reattempt_pending_materialization` (`hub.rs:2930`) retries every pending doc on every keyhive
  completion, admission advance and delegation; each cold retry is a full hydrate and decrypt.
  The Materializer keys retries by (doc, observed keyhive rev).
- Unbounded channels and mailboxes give no backpressure.
