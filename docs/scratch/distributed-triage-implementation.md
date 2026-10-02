# Distributed triage implementation planning

Status: implementation phases 1 and 2 approved for parallel execution. This scratch document supplements ADR 010 and ADR 011; later phases remain outside this implementation checkpoint.

## Accepted direction

- Keep generic task coordination in daybook_core now; make its ownership boundaries suitable for later extraction and cloud/service placement.
- A node composition owns triage, task coordination, and local execution. Triage does not own the node. Keeping triage under rt is acceptable; exact layout remains to be reviewed.
- Triage and generic task coordination have separate machine loops. Explicit commands and other domains can submit tasks without triage.
- Reuse/evolve DispatchRepo and wflow for local durable execution; do not build a second workflow runtime.
- The task executor owns distributed attempt supervision. Origin fast-path starts use the same durable attempt path as router offers, without requiring the router round trip.
- Routers continue from locally retained replicated state after partition/takeover. No complete previous-router snapshot is guaranteed or required. Executors register current attempts with the replacement router.
- Executor readiness is authoritative for its local inputs, capabilities, authorization, and capacity. Missing local inputs are not proof of global task obsolescence.
- Cancellation is best effort across nodes. Distributed scheduling does not provide exactly-once external effects or global rollback.
- Tasks consume successful local execution finalization; they do not independently inspect every workflow effect.
- Unwitnessed candidates are durable local triage state. No additional distributed witness acknowledgement or adoption-election protocol has been requested.
- Use standard iroh/IRPC mirroring existing BigRepo RPC. XRPC and service-interface migration are deferred to another PR; ADR 016 is an unaccepted deferred draft.
- Do not add automatic historical processing on processor enablement, upgrade, or effective configuration change.

## Protocol facts already specified by ADR 010/011

Storage is not one Automerge document per task:

| State | Owner | Representation |
| --- | --- | --- |
| Pool descriptor | TaskRepo | Low-churn authorized Automerge document |
| Task declaration and terminal facts | TaskRepo | Custom encrypted/signed BigSync task-ticket object |
| Router election | Task coordination | Custom signed BigSync router-slot object |
| Router discovery | Router | BigEphemeral heartbeat |
| Allocations and executor sessions | Router/executor | Live RPC; local attempts remain durable |
| Desired processor state and settlement | Triage | Custom encrypted/signed BigSync processor-slot object |
| Receipts, candidates, cursors, activation acknowledgement | Triage | Local durable projection; exact facet/SQL split unresolved |

Terminology and identities:

- Processor slot: stable (document, branch, processor identity) coordination record.
- Processor work generation: exact source version + processor generation + effective configuration generation.
- Processor task: one execution obligation for that slot/generation.
- Attempt: one actual local execution of the task; duplicate attempts can exist across partitions.
- SlotId = domain-separated hash of the stable slot key.
- TaskId = domain-separated hash of SlotId and work generation. Random IDs are available for unrelated one-shot submissions, not independently derived triage work.

Consequently two isolated triage nodes derive the same task ID for the same obligation. Router-only declaration is not needed to avoid different task IDs and would not eliminate duplicate execution across isolated routers. Triage, not the router, projects processor work. Pool descriptor document discovery/creation is a separate issue from deterministic ticket identity.

## Classification and invalidation contract to clarify in ADRs

Recommendation: distinguish router-side proven obsolescence/invalidity from executor-side readiness. A router consults its local domain projection before offering; lack of router-local source/artifacts must not veto a remote ready executor. Executor checks again before accepting. Triage does not contact remote triage services to allocate work.

Local domain changes reactively invalidate tasks, request best-effort cancellation, and prune local active tickets. A remote NotReady reply is local unreadiness. An Obsolete reply is not an unconditional authority to globally cancel: the durable domain evidence must synchronize and be validated. No new witness acknowledgement is required.

Inside triage, distributed decisions remaining after the split are: exact source/generation evaluation; desired-state supersession and concurrency; permanent settlement acceptance; missing-coordination candidate adoption; and settlement-driven invalidation/pruning. Router election, executor choice, and live attempt allocation are not triage decisions. Task IDs are derivable from slot/generation; storing an extra mutable task-ID mapping is not necessary for identity, though local projections may index them.

## Local cancellation/finalization ordering

`DispatchRepo` owns an in-process `DispatchAttempt` snapshot alongside the unchanged durable `ActiveDispatch` payload. Metadata snapshots share one private atomic cell; exact workflow-job replacement creates a new cell and retires the old attempt. There is no per-dispatch lock registry or durable Finalizing schema.

Active → CancelRequested and Active → Finalizing compete by CAS. Cancellation persists its existing SQLite mark before acknowledgment; the short repository transition mutex fences that write, duplicate cancellation, and attempt replacement. A finalizer that lost to cancellation waits only for the mark transaction, then cleans staging and reports Cancelled without merging or running success hooks. A cancellation that lost to a publication claim returns immediately without waiting for target merge/hooks. CancellationToken is an execution wakeup, not the arbiter.

Dropping an uncommitted cancellation write releases its unacknowledged claim. Dropping cancelled cleanup preserves the cancellation for cleanup retry. Dropping a publication claim seals the attempt as publication-failed in-process: partial effects cannot be safely rolled back or reclassified as cancelled. Errors propagate through the runtime worker; recovery retains the existing durable restart semantics. Restart restores accepted cancellation marks into the atomic cells. Exact-attempt checks prevent stale completion or cancellation from settling a newer job.

In-crate regressions exercise real handler target content, staging cleanup, runlog hooks, and terminal status for both winning orderings, cancellation while publication is deliberately paused, and stale success after job replacement. Additional repository coverage checks concurrent shared-snapshot CAS ownership, metadata refresh, cancellation persistence failure, and durable cancellation restoration.

## Plug activation acknowledgement fence: required implementation item

User-reported blocker: processor tests schedule the triggering edit after PlugsRepo enablement but before triage has installed that plug revision. Existing asynchronous refresh cannot establish readiness from the enable call alone. User reports disabled tests; exact test inventory remains to be located, not inferred from ignore attributes.

Required observable state:

- Exact plug/manifest revision and effective configuration revision accepted by triage, not only plug ID or semantic version.
- Active processor identities/generations derived from that revision and their observation boundary.
- A durable activation acknowledgement emitted only after triage installs the corresponding evaluation snapshot and source-consumption policy.
- Query/wait surface that cannot miss acknowledgement between initial query and subscription, and can distinguish rejected, disabled, superseded, and pending activation.
- Restart behavior must reinstall the acknowledged snapshot before exposing it as active; persisted acknowledgement alone does not prove a live worker loaded it.
- A fence means installed evaluation configuration, not historical document processing or remote readiness.

Proposed use: enable plug and capture its exact revision; wait until triage acknowledges that revision; perform the source edit; wait for the resulting evaluation/dispatch settlement. Do not replace this with sleeps, retries, cursor resets, or automatic historical scans.

Open choices: the version token returned/exposed by PlugsRepo; exact triage-owned local configuration/progress facet shapes; whether a superseding revision satisfies a wait for an earlier revision or returns Superseded; ordering of already-inspected source work against activation. Define these before re-enabling the affected tests.

## Remaining data-model decisions

### Declaration equivalence

ADR 011 currently rejects unequal immutable declarations for one task ID. ADR 010 permits independent equivalent declarations. Resolve logical equality separately from signature/encryption envelope equality. Producer identity, origin preference, changed-facet invocation data, and processor placement/effect policy must not silently disagree under an identical generation. Recommendation: canonical immutable execution meaning, with separately authenticated publisher evidence and mergeable non-semantic hints. Do not weaken collision validation solely to accommodate differing ciphertext.

### Causal slot merge and compaction

Define signed writer versions and observed causal context explicitly. The current sketch claims causal observations without encoding them. Source versions may help the triage adapter establish ancestry, but TaskRepo does not order opaque source heads, and all source history may not be materialized when a slot arrives.

Specify how a newer desired observation retires observed predecessors; how concurrent desired siblings survive; how equal-generation settlement suppresses work; how late settlements cannot regress desired state; and when old settlement evidence can be removed while rejecting stale tickets. Distinguish history boundedness from growth in writers/concurrent siblings. No fixed last-N truncation without a proof.

### Candidate adoption

Candidates resolve from ordinary slot/ticket/settlement synchronization. Same connected component does not guarantee identical candidates: subscriptions, visibility, activation, materialization, and stream progress can differ. Design local policy knobs for promotion, timing, replay evidence, and mobile participation. Do not introduce a separate candidate winner-election protocol by default. Deterministic identity deduplicates declarations; pool routing coordinates execution. Defaults and router-only versus any-authorized-node adoption remain unapproved.

### Pool discovery and authority

Define how a stable processor identifies/discovers its pool descriptor and authority scope. The descriptor is an authorized document, not the deterministic task object itself. Resolve cross-drawer/group processor pools without assuming a document belongs to one drawer or that knowing a drawer proves bytes are present. Task/slot authority, subscription interest, retention, and execution policy remain separate.

## Proposed macro layout: awaiting review

Keep flat module seams initially, not a new crate or a parallel execution engine:

- daybook_core/tasks.rs: repository/handle boot and public task programming surface.
- tasks/model.rs: IDs, declaration/terminal/router-slot formats and pure validation/merge rules.
- tasks/store.rs: BigSync backend integration and local durable projections.
- tasks/router.rs: per-pool router machine, election observation, offers, live allocations.
- tasks/executor.rs: sessions, offer acceptance, durable attempt integration and recovery.
- tasks/rpc.rs: explicitly versioned cross-node request/response contract and transport serving.
- rt/triage.rs plus a small triage/ directory: repository boot/machine, evaluation, processor-slot merge/projection, task-domain adapter, local durable state. Exact file split should follow ownership rather than one helper per file.
- rt/dispatch.rs and rt.rs: evolve current local execution/finalization; move distributed supervision into tasks rather than copying it.

Node composition wires explicit dependencies and shutdown order. Router and executor have separate workers and lifecycles (accepted): execution, routing candidacy, and passive retention remain independent. TaskRepo owns tickets, pool metadata, and observation, not every scheduling loop. Triage owns receipts and processor slots; DispatchRepo/wflow owns local execution.

Naming direction: task-pool coordination is the subsystem; PoolRouter and PoolWorker are pool-bound roles. PoolWorker accepts offers and supervises DispatchRepo attempts rather than implementing execution itself. Use RouterMachine/PoolWorkerMachine for sans-I/O transitions and role-specific drivers for Tokio I/O. TriageWorker is the reactive processor subsystem; its handle exposes activation/progress/repair without making repository queries its primary identity. Use PoolTaskId for distributed obligations, distinct from scheduler-local IDs. A node can host multiple pool instances with different domain adapters. Names are design direction, not yet code symbols.

Sans-I/O decision: pure payload validation/merge functions plus explicit router/executor machines taking input events and returning bounded actions. Drivers own clocks, transport, storage, and dispatch I/O and report outcomes. Inject time and identities; persist-before-start is an explicit action/completion ordering. BigSync replicates state but does not implement live offers, sessions, or allocations. No new core crate or universal actor framework. Triage reuses concurrent walkers/keyed scheduling.

Accepted pool mapping: one stable task pool per distributed processor, preserved across plug upgrades. Per-drawer subdivision is future scope. Pool metadata may share a document with domain/drawer metadata; drawer integration is not a prerequisite. The pool descriptor supplies the authority locus and pool-specific task subscription.

Declaration decision: resolve equivalent authorized publishers in the BigSync backend, not by router selection. Equality means canonical execution meaning; publisher signatures/envelopes are separate evidence. Publishers are nodes independently asserting the same derived obligation. Different execution parameters under one ID are a collision, not ordinary duplicate publication. Reject malformed or conflicting remote declarations without crashing the node; a local producer violating the deterministic-ID invariant should fail loudly. Successful terminal facts must remain monotonic and must never be erased when publisher evidence merges. Do not add arbitrary domain winner callbacks unless a domain actually requires competing meanings under one identity.

Adoption delay is local triage policy, such as first-seen age plus replay evidence, not an execution claim interval or router election delay. No extra adoption election. Delegate causal merge details to implementation under the invariants above rather than blocking macro-layout discussion.

## Cross-node RPC review checklist

Discuss before freezing types: registration/incarnation and active-attempt introduction; offer/accept/decline; origin-attempt registration; attempt outcome notification; cancellation; session liveness/capacity/readiness changes; router change/reconnection. Decide protocol identity/version negotiation, supported compatibility range, stable wire discriminants, unsupported-message behavior, message limits, and whether offers carry full declarations or references that can be fetched. An offer must not assume the recipient has already synchronized the ticket. Missing/pending verification is not malformed data. Persisted signed payload versions and live RPC versions are separate contracts.

Final rollout direction: one shared election per processor pool, no independent compatibility cohorts. Claims carry scheduling/RPC compatibility information independently of descriptor/ticket schemas. Prefer newer live eligible routing versions; incompatible workers wait rather than create a competing router. Supported older nodes must understand the common election envelope sufficiently to stand down, and dead historical newer claims must not permanently block takeover.

Relay implementation requirements: keep encrypted task/slot semantics and authenticated minimal envelopes as currently specified. Passive relays do not acquire execution or election duties. Unlike BigRepoSyncBackend::remove_obj_from_parts, which intentionally does nothing because document membership has other owners, these new backends must implement validated authoritative removal and idempotent membership pruning. Relay-local quota eviction must not publish cancellation/removal. Bound encrypted object sizes, writer/envelope growth, retained facts, and metadata; enforce sponsorship admission. A relay must validate removal authority without decrypting task semantics, which requires an authenticated removal/envelope design. Document and exercise stale reintroduction suppression without assuming a ciphertext-only relay can evaluate triage settlement. Exact envelope/removal implementation is left to the implementer under these requirements.

Physical storage gap: removing active ticket membership and dropping ciphertext does not delete BigSync dead membership rows. ADR 012 decision 9 defers tombstone vacuuming. Its earlier router-authority explanation conflicts with domain-owned pruning and has been corrected. Implement an explicit authority/reconstruction and stale-reintroduction rule before promising bounded total relay/pool storage; test historical churn after a drained pool, not only bounded live payload sizes. No arbitrary tombstone TTL or cursor-epoch shortcut.

Pool rotation proposal (not yet a complete GC contract): retain compact per-ticket removal metadata within a physical active-part generation, then retire that generation while retaining stable logical pool identity and processor-slot settlement. This is backend/domain membership policy, not generic cursor-epoch semantics. Rotation must preserve/reassert unsettled tasks with unchanged PoolTaskId, define authorized concurrent-rotation convergence, prevent old-generation tickets from becoming current without domain validation, and allow an offline producer to reconcile its still-needed work on reconnection. Relay deletion must follow authenticated generation retirement and remove dead rows across all membership targets, including authority/group parts; merely ceasing an active-part subscription does not delete local history. Rotation does not prevent a disconnected old-generation partition executing duplicates before it learns settlement/retirement. Distinguish bounded historical metadata from arbitrarily many pending tasks, offline retained generations, and task-authoritative result archives.

Accepted rotation direction: rotate physical BigSync task parts while preserving logical pool and task identities. Rotation belongs to generic task-pool coordination, not only triage. Every participating replica reconciles its locally known still-needed work into the selected generation; the router is not the sole migrator. Returning offline replicas perform the same reconciliation. Encrypted passive relays adopt authenticated retention/retirement decisions without domain execution. Exact rotation mechanics are implementation design, subject to the safety requirements above. For task-authoritative domains, retained terminal facts or immutable execution deadlines must suppress stale pending resurrection independently of triage settlement.

Processor coordination parts may use the same generation/retirement pattern. This does not authorize dropping slots merely because a document is absent locally: selective mirroring or missing bytes is not deletion. Reducing historical slot count requires authenticated deletion/obsolescence evidence or an explicitly retired eligibility scope that rejects stale source/task reintroduction. Copying every slot into each generation preserves all historical slots rather than collecting them. Implementation must classify what remains needed; do not promise a total-state bound solely from rotation.

Macro design discussion is complete enough to start implementation. Resolve exact payloads, rotation transition/authority proof, canonical declaration inputs, activation tokens, and RPC request types in code-facing design with deterministic scenarios. Product policy defaults (especially candidate adoption and external-effect acknowledgements) must remain explicit rather than guessed; first implement their named policy surfaces and report any genuinely missing product choice. Do not introduce XRPC, additional service frameworks, or per-drawer pools.

## Implementation sequence after design approval

1. Local execution correctness: cancellation/finalization arbitration, staging cleanup, success hooks, and both winning orderings.
2. Generic task protocol model and sans-I/O `RouterMachine`/`PoolWorkerMachine`: IDs, canonical declarations, publisher/terminal merge, common election, sessions, offers, durable start, cancellation, disconnect, and takeover. Phases 1 and 2 may run concurrently.
3. Encrypted task/router-slot BigSync backend, authorized passive relay retention/removal, pool discovery, and generic physical task-part rotation.
4. Versioned IRPC role drivers and DispatchRepo attempt integration/recovery; origin fast path uses the same durable start as offers.
5. Concurrent triage evaluation and exact plug/config activation acknowledgement fences; migrate affected callers/tests without automatic historical processing.
6. Distributed processor-slot projection, generation/settlement/invalidation, candidate adoption/reconciliation, and separate processor-coordination rotation.
7. End-to-end acceptance and clean cutover: partition duplicates, loss/takeover, stale bridges, cancellation, mismatch/deletion/disable, activation-before-edit. Narrow in-crate tests plus actual runtime smoke; no full-suite agent runs.

## Implementation operating procedure

The parent owns design arbitration, review, independent verification, integration, and user handoff. Workers own bounded implementation lanes, not product decisions. Never use git or automatically managed git worktrees.

- Base: `04a0b548` (`feat: more design`). Phase 1 workspace: `../townframe-phase1`; phase 2 workspace: `../townframe-phase2`. One writer per workspace.
- Both workspaces receive the existing `.env` privately and append `CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target`. Verification exposed stale same-package binaries across workspaces even after Cargo locking. Use outer `flock /tmp/townframe-cargo-validation.lock` over build **and test execution**, touching the active workspace `src/daybook_core/lib.rs` first to force dependency-graph refresh. Zero matching tests is a failure, never green verification.
- Updated user model policy: prefer `openai-codex/gpt-6.1-sol` at **low** thinking for implementation and correction assignments. New context-heavy, verification, greening, and clippy agents use Ollama Cloud **GLM 5.3 Flash, high thinking**, replacing DeepSeek v4.1 Flash. Today's already-running agents retain their models; do not cancel or restart them for this change. Parent owns review and independent acceptance; a worker report alone is not acceptance.
- Phase 1 owns `rt.rs`, `rt/dispatch.rs`, and relevant crate-local execution regression tests. User-requested simplification replaces the new mutex/Weak registry/GC threshold with per-attempt atomic arbitration: cancellation and finalization compete for Active; finalization winner makes cancellation immediately too late, cancellation winner forbids subsequent publication/hooks. Preserve durable cancellation and restart behavior.
- Phase 2 owns `tasks.rs`, `tasks/`, and the `lib.rs` module declaration. Implement generic model/merge rules and `RouterMachine`/`PoolWorkerMachine`, explicit injected time/IDs and persistence acknowledgements; no transport/backend/triage implementation yet. The user endorses the two-machine sans-I/O shape mirroring BigSync and suitable for later `big_tasks` extraction; extraction is not current scope.
- Worker prompts require deterministic consumer-visible regressions: transition ordering, stale sessions/attempts, protocol compatibility and takeover, declaration collisions/equivalent publishers, monotonic terminal success, and persist-before-start. No wiring/source-text/mock-echo tests or tests pinned to private implementation shape. Current model/router/worker/e2e file separation is sufficient; tier/category expansion waits until suite scale warrants it.
- Workers read AGENTS.md, README.md, CONTRIBUTING.md, this document and relevant ADRs; escalate unresolved product choices. No full-suite runs, pushes, child fanout, or premature commits. Report changed files, design decisions, exact commands/results, and remaining risks.
- Parent reads each diff, sends concrete fixes to the same retained worker where possible, independently runs narrow nextest and `cargo clippy --all-targets --all-features -p daybook_core`, and exercises changed behavior through a runtime smoke. A worker report alone is not acceptance. Sol improves nearby comments only when already editing a file: concise mechanism/invariant, not verbose narrative or a repository-wide cleanup.
- After acceptance, create one JJ commit per phase: `fix(rt): serialize cancellation and dispatch finalization`; `feat(tasks): define pool protocol and coordination machines`. Integrate with JJ, retain workspace evidence until accepted, and present the user only the reviewed checkpoint.
- After compaction refresh AGENTS.md and its linked project documents; recover lane IDs/status from retained children and this procedure before launching replacements.

Lane board: parent final verification passed 25 runtime tests for phase 1 and 74 task-machine tests for phase 2; both all-targets/all-features clippy runs completed successfully. Earlier actual cancellation/publication and public task-API executable smokes passed; temporary smoke scaffolds were removed. Waiting-activation refusal now has a gated runtime regression. Fresh critic review remains the acceptance gate; no accepted phase commits yet.

Next-phase overlap: user authorized fresh Sol-low task `Phase 3 storage discovery design` to recover bounded session decisions, inspect ADRs/current backends, and propose concrete phase 3 details before editing. Parent challenges and approves derivative design decisions, escalating only genuinely missing policy or unresolved safety conflicts. Reuse that session for implementation in an isolated JJ lane after approval. Phase 3 storage and phase 4 IRPC/runtime integration may run concurrently once their minimum persistence/discovery/attempt contracts are agreed; serialize actual dependencies and final integration, not entire phases. Existing critic and acceptance checks continue in parallel.

Parent review checks: atomic ownership is shared across dispatch snapshots but fenced across replaced attempts; only one of cancellation or finalization can win; cancellation acknowledgement follows durable persistence; error/drop paths do not reopen partially published effects unsafely; no registry/GC or async lock held across publication. Machine review checks: persistence acknowledgements gate execution and leadership; delayed responses cannot revive canceled/stale attempts; registered origin attempts suppress duplicate offers across workers; remote obsolete declines are not global proof; terminal success and publisher evidence converge and rejected merges make no partial mutations.

Infrastructure notes: pi-subagents direct fork launch failed because `sourceManager.createBranchedSession` is absent; explicit fresh background launch failed because standalone Pi lacks the npm package directory required by its runner. User approved fallback to working `task` launcher. Task profiles resolve under `~/.omp/agent/agents/`: `townframe-sol` uses `openai-codex/gpt-6.1-sol`, low; `green-deepseek` uses `ollama-cloud/deepseek-v4.1-flash`, high.

Agent reuse policy: retain and message existing sessions across different bounded assignments to preserve context and prompt-cache savings. Do not relaunch for every slice. Check latest per-request input plus cache-read/write usage (not cumulative billed tokens); checkpoint near 350k and compact/handoff before 400k. A prompt instruction is not a hard pre-request enforcement mechanism. Parent transcript path: `~/.omp/agent/sessions/-repos-rust-townframe/2026-10-02T14-41-27-869Z_01a0fd10-213d-702d-9067-c0b26ec19e8d.jsonl`. Workers may extract bounded clean user/assistant recaps for accepted context; never load the entire raw transcript.

Pre-expansion design gate: fresh DeepSeek v4.1 task `Fresh pool design critic` completed read-only review and parent cross-examination. Vetted current issues: worker readiness/watch changes lack a session-fenced route back to router eligibility; transient Unavailable must not become a permanent readiness block or immediate reoffer loop; origin start bypasses explicit Only placement/capability eligibility. Retained Sol proposes the bounded repair. Invocation input/resolved dispatch arguments are a real phase 3/4 interface gap: canonical meaning must bind execution input, and durable attempts must retain resolved invocation. Phase 3 designer owns this proposal.

Critic dispositions: alleged finishing-record zombie retry is a nonconforming-driver scenario, not a phase 2 bug; phase 4 must define durable recovery/incorporation ownership. Premature matching terminal acknowledgement is an invalid driver event, not a reachable normal transition. Domain-specific slot encoding belongs domain adapters, not a new generic typed triage tuple. Local liveness can disagree and duplicate routing is accepted; no quorum/lease requirement. Zero capacity and unavailable Only destinations are valid, not producer errors. Time-window/effect enforcement must be explicit in domain/driver execution contracts. Cache/lane retirement belongs authenticated phase 3/6 retention with stale-reintroduction proof, not arbitrary GC. No raw or speculative critic claims are accepted as defects.
