# Admission cancellation ownership and Waiting cleanup — repaired boundary

This bounded follow-on supersedes the historical silent-admission-hint and Waiting-row cleanup findings. Terminal hook/direct-reply replay remains unfixed. No Rust, Cargo, delegation or full Rust suites were used.

## Independently read source

`persist_task_attempt` sends frozen attempt/dispatch to a bounded64-slot repository channel and awaits a oneshot. The repository worker upgrades its weak repository while handling a request, retains ownership through `add_with_task_attempt`, and publishes cache/notification even when the caller disappears. Closed reply is intentional caller cancellation; orphaned failed admission is unwrapped rather than swallowed. Stop closes intake and drains accepted requests; worker is joined by RepoStopToken.

The writer holds `transition_mutex` through SQL commit, cache update and notification. `expired_task_attempts` acquires the same fence: a clock-triggered due read cannot expose a committed owner before its cache publication. This matters even if another cached deadline triggers a query without the new admission hint.

`set_waiting_failed` now guards current Waiting status and deletes expiry ownership in the same transaction as Failed status. Active and all terminal classes are preserved by stale Waiting verdicts; retained attempt proof is not deleted.

## Model and reproducible evidence

From `specs/task-coordination`:

```sh
node runner.mjs admission
```

`/tmp/quint-admission-owned-publication-final.log`: exit0;2 named cleanup cases;26 admission safety/progress states;10 cleanup states;6 actual mutation failures:

1. Gate cache/notification publication on caller survival.
2. Discard accepted queue on stop.
3. Release transition fence at SQL commit instead of after publication.
4. Omit notification/discoverability.
5. Retain expiry row on Waiting failure.
6. Ignore Waiting guard and overwrite Active/terminal.

The finite admission profile includes abandonment and stop at every stage, the committed-row/absent-cache cut, independent due reads and one postcommit reboot. Reboot loads durable proof/cache and rereads the index; `hint` denotes either emitted notification or boot discovery. Cleanup includes all five dispatch statuses (Waiting, Active, Succeeded, Failed, Cancelled), atomic failure/row removal and reopen. Event alternatives were not removed to obtain passing properties.

A reserved model action name and later ambiguous mutation site were caught and fixed; neither tool error counted as a counterexample. Historical modules in `host_expiry_completion.qnt` remain executable, with labels changed to historical only. Their functional transitions/properties were not altered.

## Explicit limits

- Begins after successful channel acceptance of a valid nonterminal frozen task request. Cancellation before enqueue, queue capacity/multiple-request fairness and invalid admission errors are not proved by this profile.
- Fair progress assumes a live strongly available repository and successful finite IO. Source's weak-upgrade-none branch may drop precommit queued requests when the repository itself is gone. This model does not promise durable queue survival across precommit process death; the postcommit worker already holds a strong repository.
- SQL commit and terminal queue removal are modeled transactionally, not as physical fsync or clock-adjustment theorems. One reboot is not arbitrary repeated crash verification.
- This repairs ownership of admission after caller cancellation and Waiting terminal cleanup, not terminal hook/direct-reply replay. The earlier callback-delivery diagnostic remains an expected failure.
- Descendants/continuations, durable domain expiry/replan notifications, future-origin offset/skew policy and GC remain open. Not-before now equals origin, but that is not a maximum future-origin contract.

## Parent-only evidence and separate timeout

`/tmp/parent-expiry-seams-tests.log`:3/3 passed, including expanded SQL/cancelled-caller/Waiting/reopen test, no-observer expiry and remote native50.964s. `/tmp/parent-expiry-seams-clippy-v2.log`: scoped Clippy exit0. These logs were inspected, not rerun by this worker.

`/tmp/parent-host-expiry-native.log` separately retains remote native TIMEOUT120.033s (6 passes/1 timeout). Parent diagnosed actor withdrawal while the enabled manifest was pending and changed retention to distinguish enabled/unavailable from disabled/rejected. That is separate actor-policy evidence, not an admission-model theorem or a new workstream here; no rerun or complete attribution is claimed by this worker.

Hashes: `/tmp/quint-admission-ownership-repair-hashes.txt`. Parent independent review remains required.
