# Retention / rotation / retired allocation ingress — source audit only

Research-only item. No Quint, Cargo, Rust tests, Git, delegation or verifier commands. Shared Apalache port reserved for parent's serial gates. Source inspection does not establish safe physical collection. No TTL, skew, default budget or collection threshold chosen.

## Requirement and guard ledger

| ADR011 requirement | Current source seam / implemented evidence | Missing collector/allocation contract |
|---|---|---|
| Authenticated monotonic selected generation | tasks/storage.rs selection transaction compares generation and identity, retains locally admitted predecessor; transport.rs selected current control/native authority checks | generation selection is not retirement certificate or global observation |
| Every retaining node carries required state | storage.rs::carry_forward reads each locally known predecessor, receives exact ciphertext into current, commits per-source/destination checkpoint after publication | local progress only; disconnected/offline/Relay only-copy preservation cannot be inferred from another node's checkpoint |
| Offline node migrates directly to selected current | selection accepts newer current and accounts locally admitted predecessor; carry_forward publishes directly to self.part | verify node missed several generations with interrupted publication/checkpoint; no need to recreate intermediate generations |
| Concurrent old writes fenced | storage.rs::check_current transaction checks scope/part/authority document; merge_and_commit holds write transaction through current publication | every generic allocation bypass must enforce retirement; selected-owner fence only stops old RegisterStore, not all raw HostPartStore callers |
| Exact original ciphertext/key provenance | carry_forward calls receive on Changed payload; source evidence/key provenance admitted separately; native transport checks current Relay and imports missing source control | no automatic proof that every required source remains recoverable before deleting last ciphertext/source certificate |
| Successor consumer checkpoint | driver.rs replacement reader opens current part, then uses durable native-scheduling consumer checkpoint; preserves attempts/effects/unread state | opening reader cursor0 is replay reconstruction, not permission to erase checkpoint; purge must include obsolete consumer state only after dependent progress |
| No stale ingress recreates retired allocation | transport.rs::remove_obj_from_parts refuses unauthenticated remote removal; storage prepare_receive binds hints/object/scope and current native authority | no authenticated retired-generation deny record consulted at every object/member/part/pending/bucket/cursor allocation surface |
| Finite expired task rejects execution | model/declaration, adapter/host/engine fences from earlier artifacts | task plaintext deadline not a ciphertext-only Relay collection proof; clock/skew/rollback and verifiable outer original deadline evidence still required |
| Terminal leaves active / archive policy | TaskStore projects current authenticated lanes, rejects invalid declarations; retention enum represented in TaskTicket | inspected TaskStore exposes no semantic archive/prune operation; archive placement/deletion transaction and independent authority rule remain missing |
| Domain compact settlement permits semantic removal | processor slots retained capture settlement; adapter exact acknowledgements | generic domain-specific prune API/proof and reintroduction classification not equivalent to physical membership purge |
| Absolute dead-row/garbage-byte budgets | ADR requires positive absolute budgets, current-generation attribution, either trigger, checked next generation | no inspected pool/storage garbage counter/collector/budget-trigger path; define measured accounting and explicit config/API before enablement |
| Relay-local eviction not protocol cancellation | remote removal unsupported; Relay can carry ciphertext without Read | guaranteed versus opportunistic retention admission/backpressure and eviction accounting need explicit API; no cancellation minted by eviction |

## Concrete storage resurrection path

`RegisterStore::merge_and_commit` reads retained encrypted_register_current and joins incoming authenticated snapshot. It queries current membership; `outcome != Unchanged || !is_member` writes snapshot and publishes current part membership. Therefore authenticated replay can recreate a removed **current** membership even when lane state is unchanged. This is intentional current evidence repair absent an implemented semantic purge contract; deleting membership alone cannot become permanent retirement.

`big_sync/part_store/sqlite_write.rs` mutation UPSERT transitions removed membership back to present and updates added_at; no generation-retirement predicate there. `sqlite.rs::remove_obj_from_part` calls ensure_part_ref, deletes pending-membership row, bucket Live->Dead transition and tombstone frontier; payload retained. `remove_obj_payload` removes all live memberships and payload in one revision, retaining dead membership metadata. `sqlite_core.rs::ensure_part_ref` and generic ensure_part/add_obj_to_parts/set_obj_payload allocate independently of task transport generation. This generic behavior is not itself a defect; the task backend must provide authority/fences before purge. Without those, stale authentic traffic after physical deletion can allocate objects, parts, live/dead/pending rows and bucket fingerprints again.

Native transport receives through bound_store/prepare_receive, fresh Relay and current RegisterStore transaction. This authenticates selected logical owner and provenance, not task semantic expiry before ciphertext allocation. A Relay unable to decrypt cannot safely derive original task deadline from current plaintext projection. Reauth audience and startup permission reset (task-scope serving maintained by native transport over BigRepo access policy) stop unauthorized serving, not retained-row allocation or storage growth. BigRepo's native document/group authority grants are not a collection/retirement certificate.

## Distinct retention owners (must not merge policies)

1. Result archive: explicit TaskTicketAuthoritative semantic horizon/explicit deletion; finite execution still mandatory. Intentional indefinite archive is not garbage.
2. Unacknowledged outcome: dispatch completion outbox and exact domain incorporation/progress obligation; actor/materialization Pending retains it. Task expiry does not release it. Domain acknowledgement releases delivery retention, not still-required domain checkpoint/effect evidence.
3. Execution/deletion tombstone: stale-task rejection knowledge before supported safe expiry; finite deadline can replace permanent tombstone only under all-path clock and admission contract.
4. BigSync dead membership/fingerprint: synchronization deletion knowledge; generic cursor does not prove all peers saw deletion. Distinct from execution tombstone and dispatch status.
5. Retired allocation fence: compact authenticated deny information for generation/part namespace and all allocating surfaces; must outlive historical traffic independently of task payload deletion.
6. Key provenance/source certificates and local carry-forward checkpoints: recoverability/transfer progress; neither global retirement nor archives/outcome ack.

## Existing model correspondence

register.qnt/register_checks/validation/convergence verify bounded lane joins, replay fences, canonical representation, poison, signature evidence, invalid batch rejection, replica delivery/restart. Their binding classes are semantic acceptance inputs, not real physical generation allocations or deletion authority. task_ticket models declaration/retention validity and terminal joins, not archive movement. Lifecycle/triage models retain durable delivery and exact settlement, not collection. No existing property proves carry-forward durability across transaction/checkpoint cuts, all-node last-copy survival, retired namespace allocation rejection or garbage budgets. Existing pass counts must not be repurposed as collector evidence.

## Exact next verification obligations (no disconnected toy model)

- Source-owned handoff trace: paused offline writer learns generationN directly, old write loses selected-owner transaction, accepted pre-fence write copied, publish crash before checkpoint repeats exact ciphertext, checkpoint crash/reopen does not skip. Include Relay only-copy without Read and two different-depth nodes; local success not global purge.
- Retirement allocation trace bridge: late authentic old/current hints after purge attempt across object payload, live/dead/pending membership, part metadata, bucket/frontier, peer cursor and consumer checkpoint. Assert no retired allocation reappears, not merely no task executes. Requires runtime retirement API first.
- Retention-owner trace: expire task while outcome unacked/Pending; archive policy differs; no collector deletes required outcome/domain checkpoint. Retained deadline witness still fences replay after terminal evidence collection.
- Budget accounting trace: absolute dead rows/bytes current generation, either threshold crossing triggers once; predecessor backlog cannot churn clean successor; below-threshold tiny high-ratio pool does not rotate. Threshold values require parent policy/measurements, not model defaults.

Missing APIs/mechanisms to design before implementation: authenticated retirement authority and retained deny namespace; bounded transactional allocation gate used by all native/generic ingress relevant to task scope; physical purge transaction/page ownership and restart checkpoints; Relay-verifiable original deadline evidence plus supported time contract; domain prune/archive retention interface; independent outbox acknowledgement/backpressure; current-generation garbage accounting and configured absolute budgets; durable retention admission/only-copy carry-forward guarantee. Scope/clock/GC authority decisions remain parent/operator-owned.

Parent reported serial verification/native passes and ENOSPC Clippy retry; not worker-executed evidence. This audit closes no physical GC guarantee and makes no new runtime bug claim from intentional generic re-add semantics.
