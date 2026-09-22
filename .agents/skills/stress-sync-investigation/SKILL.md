---
name: stress-sync-investigation
description: Diagnose Townframe/Daybook synchronization, worker-liveness, blob projection, plug projection, and stress-test timeouts from large nextest debug logs. Use to identify the exact stalled fence, peer, part, document, cursor, or cancellation boundary without flooding context.
---

# Stress Sync Investigation

Use this workflow for load-only hangs and `nextest` timeouts. Read the repository `AGENTS.md` first, especially the warning about BigSync/Keyhive races. Suggest and update this skill with better scripts, methodology, step-by-step runbook whenever you use it by evaluating what uncessary steps you took. Mantain a hint cache section at the end for stuff that was recently failing.

## Non-negotiable output discipline

Redirect every cargo invocation to a file. Never run raw `grep`, `rg`, `cat`, or `tail` over a large log. Use `scripts/bounded-log.py`, which defaults to at most 20 lines, 160 characters per line, and 3000 characters total.

```bash
python3 .agents/skills/stress-sync-investigation/scripts/bounded-log.py \
  /tmp/flake.log 'TIMEOUT|FAIL|Summary'
```

Use `--start`, `--end`, and `--last` to isolate one nextest stdout section. First count or identify boundaries; only then inspect exact events. Extract fields with a small Python parser instead of printing long structured tracing lines.

## Reproduction

Full load run:

```bash
RUST_LOG_TEST=debug cargo nextest run -p daybook_core --no-fail-fast \
  >/tmp/flake.log 2>&1
```

Focused stress diagnostics:

```bash
TEST_SEED=<seed> DAYB_STRESS_DIAGNOSTIC_TIMEOUT_SECS=20 RUST_LOG_TEST=debug \
  cargo nextest run -p daybook_core \
  -E 'test(long_test_iroh_sync_randomized_four_node_stress_converges)' \
  >/tmp/stress-diag.log 2>&1
```

The diagnostic timeout is an evidence path, not a production timeout increase. Record the emitted seed before rerunning.

## Hunt loop

A hunt is a fail-fast load run whose only job is to surface the next defect.

- Never run a single test in isolation to reproduce a load race. These failures do not occur
  without concurrent load, and a green isolated run proves nothing.
- Never combine a long `--stress-duration` with `--no-fail-fast`: that burns the whole duration
  after the first failure. Fail fast, read the failure, fix or instrument, relaunch.
- Launch hunts detached, redirecting to a per-hunt log (`/tmp/hunt<n>.log`), and wait on a loop
  that exits as soon as the run reaches a terminal state or the log already holds the answer.
  Do not poll with fixed `sleep`s; it wastes wall clock once the need is met.
- Keep the log of the hunt that failed, and use a fresh name per hunt so a later run cannot
  overwrite the evidence.
- Discard runs that died for environmental reasons instead of reading them: bulk
  `FAIL [0.015s]` spawn errors mean the runner ran out of disk (the target dir was pruned under
  you), and `SIGTERM`, `[double-spawn] failed to exec`, or a mid-run reboot mean harness death.

## Attribution discipline

Every failure a hunt surfaces is pinned or instrumented in the same turn. "Pre-existing",
"unrelated", "probably the same flake", and "my change did not cause it" are not findings, they
are refusals to investigate. Two clean iterations after a change attribute nothing either: keep
hunting until the failure is gone or its mechanism is named.

## Instrumenting the forks

`../keyhive` and `../subduction` are ours to instrument. Ask before concluding a bug is upstream,
not before adding a log line.

- Uncomment the matching `[patch."https://github.com/dman-os/<fork>"]` block in the root
  `Cargo.toml` and restore the `rev` pins once the patch is no longer needed.
- Gate new logging behind the existing env switches (`DAYB_KEYHIVE_DIAG`, `INSTR`-style vars) so
  the hot path stays silent by default. Mark suspicious sites even when they are only suspected.
- Keep instrumentation in its own commit, separate from fixes. Upstream forks move, and a pin
  bump must be able to drop the instrumentation and re-apply only the real fixes on top; a hunt
  whose instrumentation is interleaved with fixes cannot be rebased or pushed cleanly.
- `.agents/skills/fork-pinning-and-upstream-sync/SKILL.md` covers the pin/repin/push procedure.

## Diagnostics already in the tree

Reach for these before adding new logging; they were added for exactly these hunts.

- Keyhive event ledger, per node: logged and admitted counts, admission head, and unapplied events
  grouped by source peer. `unapplied=0` means every durable event received was admitted, so a
  missing event was never delivered rather than still in flight.
- Fixed-point stuck events: hash, variant, exact error, document, causal predecessors, and `Add`
  predecessors.
- Receive-order logs: delegation issuer/delegate/access/proof/document linkage, CGKA op and
  predecessors, batch index/order, source peer.
- Policy rejection detail: document existence, resolved subject and public access, member count,
  hive generation, nested-store counters, durable ledger state.
- Quiescence stalls: active Keyhive rounds, waiters, tracked work by kind, pending document
  syncs/materializations, admission and group-part cursors.
- Cache/direct comparison under `DAYB_KEYHIVE_DIAG=1`: cache generation, changed hashes, per-peer
  selection reason, source suppression, delivery outcome.

## Establish the blocked fence

Do not start with subsystem hypotheses. Determine which awaited condition failed:

- test helper waiting for a plug event or inventory count;
- FacetSet projection or downstream consumer cursor;
- `wait_for_full_sync`;
- `wait_for_quiescence` / freeze;
- shutdown join;
- document materialization.

Instrument begin/completion around the exact await when existing tracing cannot distinguish it. Remove temporary tracing after diagnosis.

## Correlate one document

For a stuck document, build a compact timeline across:

1. hub command/event (`PutDoc`, sync request, connection lifecycle);
2. Keyhive admission and dispatch;
3. BigSync part/object cursor scheduling and completion;
4. AFW publication revision and heads;
5. DocDelta raw source revision;
6. FacetSet delta, defer/wake, projection, and settlement;
7. plug/blob consumer settlement;
8. test-visible event or inventory transition.

Always include node/local peer, remote peer, part, object/document, task ID, cursor/revision, and heads where available. Parse those fields rather than dumping spans.

## Cancellation-safety audit rule

A future used directly as a `tokio::select!` branch is hazardous when it:

1. advances a reader, subscription, cursor, or queue;
2. then awaits another operation;
3. only later returns the transformed item.

Losing the select race can drop already-consumed work. Reader adapters must retain the raw source read in struct state before any later await and clear it only immediately before returning completed output. Add a test that cancels after source advancement and verifies the next call returns the same revision.

Known examples:

- `daybook_core/index/doc_delta_store.rs`: retain `pending_source_read` across sparse-state lookup.
- `big_repo/runtime2/doc_revision_store.rs`: retain the physical read across asynchronous route lookup for removals.

Also inspect task completion handlers for an older completion deleting a wake or pending command owned by a newer replacement.

## FacetSet/AFW interpretation

AFW is the materialization wake source. Its all-parts stream is not `GLOBAL_PART_ID`. A physical route removal is not a logical document removal while another route remains.

A materialization wake without a later DocDelta can mean:

- a cancellation-unsafe adapter consumed the raw AFW revision;
- sparse state already describes the revision as a no-op;
- the walker is not being polled due to capacity/state;
- notification and reader instances do not share wake state.

Do not add synthetic frontiers, sleeps, retries, or timeout increases.

## BigSync/Keyhive interpretation

Repeated local policy failures such as `Policy(DocumentNotFound)` are normally the intentional race between a document task and Keyhive membership ingestion. Do not fix them by parking, cancelling, or suppressing retries. Trace the Keyhive pull pipeline first.

Distinguish:

- local policy rejection: local admission may be behind;
- remote unauthorized rejection: serving-side authorization disagrees with the advertised object view;
- transport/network failure;
- equal advertised heads with partial materialization.

Equal heads do not prove underlying sedimentree blobs are complete. A cursor must not be settled as a no-op if doing so could strand an object below its frontier.

For stress diagnostics, compare per-node:

- `DocHeadState.state`;
- sedimentree and materialized heads;
- BigSync task counts;
- `full_sync_waiters`;
- peer/part flags `(pending, multi_strat, replay_done, cursor_active)`;
- the latest completion/failure for the blocking peer, part, and object.

If documents agree but quiescence does not, find the peer/part with `cursor_active=true`, then locate the task or retry retaining that cursor.

## Keyhive remnant triage

For a persistent `Policy(DocumentNotFound)` tuple, do not infer that Keyhive sync never ran. Correlate the repository peer IDs with the Keyhive IDs in `KeyhiveSyncDone`, then inspect only exchanges containing both Keyhive IDs. Summarize `sending`, `requesting`, `our_pending`, `received`, `pending_after`, and `advanced`. A later explicit sync reporting all-zero differences while local policy still lacks the document is evidence that the serving syncpoint/visible-event projection diverged from actual admissions.

Also extract the object's BigSync notifications as a compact table of timestamp, subscriber, part, and cursor. The vocabulary is two kinds: `Changed` (a touch) and `Removed`. There is no `Added`: a keyed frontier row is the latest transition for a key, so it cannot say whether an object is new to a recipient. Multiple part additions can create concurrent tasks for one object; a later part removal does not settle a cursor owned by another part. Keep this distinct from authorization revocation.

Do not search only for the word `unauthorized`: count exact classifications separately (`remote doc sync was unauthorized`, `Policy(DocumentNotFound)`, and protocol-level rejection variants).

When temporary broad notification fan-out makes the deterministic failure pass while normal visibility-selected fan-out fails, prioritize notification target classification and the visible-event/syncpoint projection. Disabling the local policy check only proves that direct document synchronization can bypass the missing Keyhive admission; it is not evidence that admission converged.

## Keyhive dispatcher cache/direct experiment

Set `DAYB_KEYHIVE_DIAG=1` and run the deterministic stress test with nextest `--no-capture`, redirecting all output to a file. Normal captured nextest output hides successful-test tracing. Analyze the file with a parser that strips ANSI escapes and emits only aggregate counts plus a few bounded examples.

The dispatcher diagnostics report cache generation, changed-hash prefixes, connected peers, per-peer selection reason, source suppression, and delivery outcomes. Compare `cache/direct comparison` records by `peers_equal` and `unclassified_equal`; `stable=false` only means the Keyhive generation changed during the expensive direct walk, not necessarily a set mismatch.

For the local-policy experiment, distinguish: `has_doc_fetch_access` preflight rejection (the disabled historical gate) from `stats.local_policy_rejections` after the wire sync begins. The latter means the receiving local policy rejected incoming commits/fragments. A true remote authorization rejection appears in `stats.remote_rejection` and maps to `SyncDocAttempt::Unauthorized`. Count exact `Policy(DocumentNotFound)` and remote Unauthorized separately.

## Validation

After a demonstrated fix:

1. run the narrow affected test;
2. rerun the exact deterministic stress seed when applicable;
3. run the complete `daybook_core` suite under `RUST_LOG_TEST=debug`;
4. report body failures separately from shutdown-only warnings;
5. remove temporary diagnostics and rerun without them.

Never treat a single passing focused test as proof of a load-race fix.

## Hint cache

### CreateDoc stalls during boot under Keyhive fanout

If logs show `creating doc` without `created doc`, correlate `Document::finish_generate`. A confirmed lock inversion was:

- document generation held `csprng` while `group.pick_individual_prekeys()` awaited the active principal (`csprng -> active`);
- the prekey janitor held the active principal while `rotate_prekey()` awaited `csprng` (`active -> csprng`).

Fix by releasing `csprng` before group generation/prekey selection and reacquiring it only around operations that consume randomness. The regression test `document_generation_does_not_invert_active_and_csprng_locks` deterministically holds `active`, starts document generation, and proves `csprng` remains acquirable. Boundary tracing showed delegation insertion/listeners/rebuild all completed before the stall; do not misdiagnose this signature as a delegation-store deadlock.

### Empty prekey set aborts the whole CGKA operation

`index to be in range` at `keyhive_core/src/principal/individual.rs` (`pick_prekey` ->
`pseudorandom_in_range(seed, prekeys_len)` -> `nth(idx).expect(..)`) means an individual reached in
`Agent::pick_individual_prekeys` has **zero** ingested `Add`/`Rotate` prekey ops: `max == 0` yields
`idx == 0` and `nth(0)` panics on the empty set. It is not an off-by-one (`raw_max < max` whenever
`max >= 1`). The panic unwinds out of the caller that was minting CGKA ops, so there is no retry
path, and skipping the member is not an option because that mints an epoch it cannot read. It is
rare under load (once in six runs, then sixteen clean passes), so one occurrence is still a real
defect. The fix belongs upstream: a `MissingPrekeys` typed error plus a caller-side retry. The
current pin already carries the rotation variant of this guard.

### Stale event projection from unshared keyhive generation counters

When Subduction's cached projection looks stale after a membership change, check the counter wiring
before the cache. Normal Keyhive construction used to give the delegation/revocation, group and
document head, and principal stores *private* generation counters while archive restoration shared
the hive's `Arc<AtomicU64>`, so a mutation could change membership without invalidating the cache.
`note_direct_mutation()` is the manual escape hatch with exactly one call site; it is not the
general mechanism. Regression tests: `shared_stores_carry_the_hive_generation` and
`principal_stores_carry_the_hive_generation`.

### Every probe needs a positive control

A diagnostic that has never been shown to fire is not evidence. Pair each probe with a control that
must produce a positive result, and let it falsify your assumption. A hash-to-object mapping probe
in this repo decoded fine but matched nothing; the control is what exposed the mapping as wrong
before it was used to explain a failure.

### Four-editor stress: settlement finishes but heads disagree

A recent run finished final settlement at 69s, then reported the same one-document
sedimentree-head mismatch on editor-4 until nextest killed it at 240s. Every node had
16/16 documents and both compared head sets had cardinality one. This is an alignment
fence, not a quiescence stall; head counts cannot identify which revision is missing.
Log actual head hashes for differing documents before rerunning under load. The last
local edit on the divergent node resolved `doc_groups={}`; investigate its replication
routes, but do not assume an empty group set is itself wrong (use a working document
as a positive control). Strip tracing span prefixes before truncating log events;
otherwise the useful payload can be entirely hidden behind a long span.


RESOLVED (later session): it was not an alignment fence. Editor-4 was *ahead*: its local commit's
part event was published before that commit was visible in the frontier the sync server serves, so
the peers' sync tasks were answered `Noop`, settled their cursors, and were never re-spawned. A
single-hold write path (subduction `entry_guard_hydrated`, one hold across persist and the in-RAM
apply) fixed it; the test went from a 240s timeout to a ~30s pass, and the republish workaround
was deleted. See "Read-only state that is published before it is servable" below.
## Read the log levels first

Start from `WARN`/`ERROR`, not from debug probes. They exist because someone judged the condition
worth surfacing, they are bounded in volume, and in this system they usually name the missing
message outright. Survey them before adding instrumentation:

    python3 scripts/bounded-log.py <log> 'WARN|ERROR'     # then group by normalised message

A full run carries ~180 distinct WARN/ERROR signatures. The informative ones are the low-frequency,
high-information lines (`unknown peer`, `no registered application peer identity`, `missing entry
encryption key while materializing`, `peer disconnected while sending ...`), not the per-event
chatter (`INSTR ...`, `ERROR_CALLER=caller dropped`). Do not add `debug!` noise for a condition a
`WARN`/`ERROR` already states, and never raise a level just to make a probe greppable.

## Fence -> job -> missing message

Name the await, then the tracked work, then the message that never arrived:

- `wait_for_full_sync` is the big_sync stat machine's waiter (daybook bootstrap and big_sync tests
  use it). big_repo's stress harness does not: its settle fence is the hub quiescence probe
  (`wait_for_quiescence`), whose stall report lists outstanding work by `TrackedWorkKind`.
- A stall report of the shape `tracked_in_flight=0 tracked_work={} probe_pending_docs=0
  pending_doc_syncs=0 keyhive_waiters=0` with only `active_keyhive_syncs>0` set means a keyhive
  sync round has not returned: go to the keyhive lane, not big_sync.
- Distinguish "the machine reports fully synced while heads differ" (a convergence-loop failure with
  no fence outstanding) from "the machine never settles" (quiescence failure, tracked work
  outstanding). They are different bugs with different fences.

## Identity keys are conventional, not typed

`PeerKey`, `KeyhivePeerId`, and subduction's `PeerId` are byte newtypes over 32 bytes, so a transport
(endpoint) id and an application identity differ only by convention. `UnknownPeer` raised in
`sign_and_send` (`subduction_keyhive/src/protocol.rs`) means the target was never `add_peer`ed under
that id: the keyhive map is keyed by `BigRepoKeyhiveConnAdapter::peer_id()` =
`KeyhivePeerId::from_bytes(*auth.peer_id().as_bytes())` (`big_repo/keyhive_conn.rs`) while the sync
path looks up `KeyhivePeerId::from_bytes(peer_id.to_bytes32())` (`runtime2/native.rs`). Log both
values when they disagree, and watch for `PeerKey::new(endpoint_id.as_bytes())`-style fallbacks
(`big_repo/rpc.rs`) that mint an application identity from a transport one.

## Read-only state that is published before it is servable

Two caches, one commit. A write path can emit its "this part changed" event from the *storage*
mutation while the in-RAM tree the sync server answers from is updated only afterwards (subduction:
"persist before the in-RAM mutation"). A peer that reacts immediately is served the *previous*
frontier, completes `Noop`, and — because a completion settles the cursor — never asks again.
Nothing re-notifies, so the divergence is permanent and presents as a convergence-loop failure
with no fence outstanding.

Signature, from the hunt that pinned it: the writer publishes event `cursor=N` for the object; peers
spawn object-sync tasks for it within milliseconds; every one returns `deets=Noop cursors={N}`; after
that no task is ever spawned for that object again; the group-part replay advances and stays
`drained=true`; the resident heads update ~90ms *after* the publish.

Fix shape: make the served state never lag the advertised state — one lock hold spanning persist and
the in-RAM apply, for *every* write path (per-commit, batch ingest, and the bulk local writes).
Republishing the event afterwards also works, but it doubles event traffic and only patches the
origin you remembered; prefer the lock and delete the republish. Never "fix" it by retrying on
`Noop`: an ack that does not advance what the other side compares is an infinite re-arm.

## A completion is not proof of its own effect

The hub polls commands before events and handlers enqueue with `try_send`, so a message pair is
ordered only when both halves are enqueued by the *same task* (or one channel from one sender).
When a hunt surfaces the symptom, audit the whole message surface instead of the single site — the
same inversion usually exists in two or three places. See
`.agents/skills/message-ordering-audit/SKILL.md`.

Symptoms worth recognising immediately:

- a caller gets a success receipt for content that was never applied (a dropped route resolved the
  waiter anyway);
- a "reconciled" Keyhive sync whose admissions are not yet projected;
- a spurious sync failure against a connection that was just closed/re-established;
- a quiescence probe resolving while work it should have fenced is still queued behind the fence.

## Test windows are not verdicts

An expired hold/poll window is not evidence about the log, and an `expect` on a value that an
asynchronous callback fills races that callback. Under load both produce failures that vanish in
isolation. Re-express the test as the contract the client actually follows:

- poll from the same position until delivered/drained (the `big_sync` replay-page tests);
- register-then-check for a notification (`Notified::enable`) instead of checking then awaiting;
- never add a sleep, and never widen an internal timeout to make a load failure disappear.

## Counting iterations in a stress run

With fail-fast on, a `--stress-duration` run ends at the first failure; with everything green it runs
the whole duration. Count progress by the last test of each pass completing (`(N/N)` lines appear
once per iteration) rather than by a `Summary` line, which only appears at the end or on failure.
Do not combine `--no-fail-fast` with a long duration.

