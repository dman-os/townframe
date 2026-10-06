# Domain acknowledgement contract — approved bounded implementation

Parent approved the bounded implementation below. `DomainIncorporationRequest::{LocalOutcome,ObservedTerminal}` and `DomainIncorporationAck` now separate exact acknowledgement from task selection and original outcomes. The historical failing log and receipt-policy counterexamples remain preserved; the Rust regression now passes for both arrival orders/reopen. Native triage acceptance remains blocked. TTL/retirement/cleanup policy is outside this receipt implementation.

## Three different facts

1. **Incorporation acknowledgement:** this exact requested terminal summary for this immutable task/declaration digest was durably accepted/incorporated by the domain, with its required proof checked. Returning it does not elect a winning result or claim that this node executed a remote attempt.
2. **Selected task result:** `TaskTicket::terminal_summary` over the task's retained authenticated terminal lanes, greatest success `(attempt,result)` or lowest-writer cancellation. This is a projection of distributed task state, not the identity of an acknowledgement.
3. **Original per-attempt effect evidence:** the exact durable attempt/capture/job/dispatch outcome and original result reference. A later winning result neither rewrites that evidence nor authorizes fabricating an effect for another attempt.

The former singleton `DomainReceipt.summary` tried to serve all three. Keeping first success blocks a later success acknowledgement; replacing it with the greatest summary still blocks an older genuine local outcome when the newer one arrives first.

## Full consumer trace

| Source consumer | Current use | Proposed responsibility |
|---|---|---|
| `tasks/driver.rs::TaskExecutionAdapter::incorporate` | returns DomainReceipt | return exact incorporation acknowledgement, separate from selected result |
| driver task-revision ingestion (`adapter.incorporate(...,None)`) | exact-summary receipt check before revision checkpoint | verify exact requested ack; observed terminal proof is checked without claiming local execution |
| driver `WorkerAction::DomainTerminalFact` | incorporate, exact equality, publish original finishing fact, TerminalFactAccepted, free preparation | match exact requested ack; then publish **original finishing fact**, never replace it with selected canonical summary |
| worker `on_attempt_finished` | retain finishing/capacity and request Some(attempt) | retain until exact local outcome incorporation/publish succeeds; older genuine success remains acknowledgeable |
| worker `on_ticket_observed` | handed-terminal dedup, remote incorporation None, cancellation hints | summary changes do not overwrite per-attempt evidence; remote acknowledgement is not a local outcome acknowledgement |
| adapter `outcome` | derive fact from durable dispatch status and opportunistically incorporate singleton receipt | record/ensure exact original outcome acknowledgement; always return actual outcome independently of canonical task selection |
| adapter `incorporate` | retained key/digest checks, own-success bool check, processor exact-slot proof, receipt SQL | retain checks; strengthen own success to exact actual TerminalFact/summary/result-ref equality; acknowledge observed terminal only after exact generation proof |
| adapter `classify` | any matching receipt => Obsolete | query durable domain-obligation-settled marker by task/digest, not a selected summary |
| adapter `is_proven_obsolete` | same presence check | same explicit settled marker; no canonical winner dependence |
| adapter `recover` | inspect actual outcome first => Finishing; otherwise classify/restore | reconstruct actual finishing fact from original durable attempt; query its exact acknowledgement; never substitute newer selected result |
| `rt/dispatch.rs::task_receipt` | singleton row/digest check | break interface into settled-marker query and exact-ack query; selected result comes from task store |
| native sync ladder receipt checks | absence while running, terminal equality after finish | assert no domain settlement before outcome; after finish assert requested ack and original attempt effect, query canonical terminal separately |
| distributed smoke command/reopen/triage checks | singleton summary == signed terminal | assert exact ack for each observed summary, original result/effect survives reopen, canonical result from retained TaskStore remains independently correct |
| new dispatch regression | real two-success merge, receipt reopen, driver equality condition | keep red baseline until new contract is implemented; add both arrival orders and repeated post-reopen ack |

Search scope: every `DomainReceipt`, `task_receipt` and `incorporate_task_receipt` reference under `src/daybook_core`; no additional production consumer was found. This does not assert external downstream API users are absent.

## Minimal interface proposal

- Replace `DomainReceipt` with an exact `DomainIncorporationAck { task_id, declaration_digest, incorporated: TerminalSummary }`. `incorporate(declaration,summary,attempt)` returns this ack only after durable incorporation of that exact request. Digest is part of the returned identity, not merely an implicit SQL parameter.
- Split `task_receipt` into `domain_obligation_settled(task,digest)` and `incorporation_ack(task,digest,requested_summary)`. Do not add a compatibility shim retaining the ambiguous old signature.
- Keep canonical selection in the existing authoritative `TaskStore::ticket().terminal_summary()` projection. **Do not create a second independent winner algorithm in DispatchRepo.** If callers require selection after task archival, that is the task store's existing retention/archive contract, not justification for reusing an acknowledgement as an archive.
- Keep original outcome/effect lookup by durable `AttemptKey` and retained `PreparedAttempt`/dispatch mapping. Existing local success fact is `(attempt_id, result_ref=dispatch_id)` from the actual persisted dispatch status. A remote selected result is never written over that mapping.

`Some(attempt)` must not silently mean that every requested summary is that attempt's own outcome: current adapter explicitly distinguishes own success from superseding terminal observation. Parent may choose to make this distinction an explicit request enum (`LocalOutcome` versus `ObservedTerminal`) instead of retaining an overloaded Option. This is an interface choice requiring signoff; either shape must preserve the proof distinction and exact ack identity.

## Minimal durable storage proposal

Use one task/digest settlement owner row plus exact acknowledgement rows:

- Retain/migrate the singleton task identity/digest as a **settled marker**, with no winning-summary role.
- Store each incorporated terminal-summary identity in a child acknowledgement table, unique by `(task, canonical-summary-bytes)` and bound to the owner digest. Store full typed summary payload; if a hash indexes it, compare full canonical meaning before reuse.
- One SQL transaction under the existing transition serialization: validate/fix task/digest identity, idempotently ensure exact acknowledgement, ensure settled marker, commit, then return the exact request ack. The marker cannot commit ahead of its acknowledgement/proof.
- Repeated request after commit/before response or after process restart returns the same ack. Ascending and descending successes can both be acknowledged without regressing canonical selection or erasing either original effect reference.
- No new per-attempt effect journal is needed **if** existing immutable attempt mappings and durable dispatch outcomes remain retained while acknowledgements need them. Current code stores `dispatch_task_attempts` and persisted dispatch state; this proposal does not invent a purge lifetime for those records.
- Existing legacy singleton receipt can migrate to one known acknowledgement plus settled marker. Do not invent missing historical acknowledgements; recovery can re-incorporate original retained outcomes. Do not replace a persisted digest on conflict.

Acknowledgement rows may grow with real distinct terminal outcomes. Safe compaction/deletion requires the parent-owned lifetime/recovery contract; no forever-retention, TTL deletion threshold or premature ack-row GC is selected here.

## Proof/admission conditions

- Local outcome acknowledgement requires exact task, digest, attempt, actual durable outcome and result-ref agreement; bool `Succeeded` alone is insufficient.
- Remote observation is authenticated task-state information, not proof of local execution. Processor acknowledgement retains exact slot/capture settlement materialization checks, even if domain already has another ack for the task.
- Ack existence can establish domain settlement/obsolescence, but must not bypass pending per-attempt finishing publication/recovery. Success and cancellation observations may both have exact acknowledgements; canonical success dominance remains in TaskTicket.
- Driver may acknowledge/free a local attempt only after exact requested ack **and** original signed fact publication. A canonical selection change cannot change which original fact is published.
- Retry and crash/reopen must not lose exact acknowledgement or original effect evidence. Separate transactions/awaits between domain proof, ack commit, signed publication and worker release stay distinct in composition models.

## Executable arrival-order evidence

`domain_receipt_arrival_orders.qnt` covers current first-success policy and a **diagnostic, unapproved** max-summary-only alternative, ascending/descending genuine success requests, commit-before-response and crash/reopen/retry.

Executed commands in scoped package:

```sh
node_modules/.bin/quint test --backend typescript --main domain_receipt_arrival_orders --seed 1 domain_receipt_arrival_orders.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main domain_receipt_arrival_orders --invariant safety domain_receipt_arrival_orders.qnt
node_modules/.bin/quint verify --backend tlc --tlc-config tlc.json --main domain_receipt_arrival_orders --temporal progress domain_receipt_arrival_orders.qnt
```

Diagnostic witnesses pass2 tests; safety passes21 generated/17 distinct/depth5. Fair exact-ack progress **fails** on the same finite graph. Logs `/tmp/quint-receipt-orders-{tests,safety,progress}.log`. Named witnesses show current policy fails both arrival orders and max-summary-only fails descending older genuine request. This is not verification of the proposed new contract: no ack-ledger implementation/model has been approved yet.

## Signoff requested before implementation

Proposed minimum is exact request acknowledgements + separate settled marker, authoritative task-store selection and preserved original attempt evidence. Parent must approve the interface proof distinction (Option versus explicit request kind), exact-ack storage/legacy migration and retention responsibility before production changes. Original full verification and durable composition remain incomplete; router compilation remains pinned and no more compiler work is included in this item.
