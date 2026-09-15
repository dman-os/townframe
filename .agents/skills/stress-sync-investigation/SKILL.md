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

Also extract the object's BigSync `Added`/`Changed`/`Removed` notifications as a compact table of timestamp, subscriber, part, and cursor. Multiple part additions can create concurrent tasks for one object; a later part removal does not settle a cursor owned by another part. Keep this distinct from authorization revocation.

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
