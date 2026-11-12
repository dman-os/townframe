# Review: big_cartridges, nodule config and lifecycles

Status: For operator review
Owner: Townframe maintainers
Updated: 2026-11-04
Scope: Operator-requested high-level design review of the big_cartridges catalog, nodule config datums and nodule lifecycle (commit `57a7de13`), compared with the old `daybook_core` PlugsRepo and `rt/init.rs` and reframed by the decisions in `docs/scratch/nodule-runtime-ledger.md` (D1–D6). Findings are tagged **[bug]**, **[gap]**, **[design]** or **[perf]**. Recommendations marked *(proposal)* await the operator; the rest are fixes I intend to make.

## Summary

- **The config-datum and reconciler core is sound and survives the services reframing.** Keep desired state in portable datums and effective state in the host-local actor and grant tables, and keep per-nodule failure isolation.
- **Two structural problems:**
  1. Admission validates the *whole* contribution surface: commands, views, processors, stores, routine execution rules. One unknown vocabulary member therefore refuses the entire nodule. Per D1 the runtime should validate only services, facilities and lifecycle, and carry bindings opaquely.
  2. Lifecycle is not a queryable state machine. `Readiness` has no ready state, and the catalog, dependencies and init are declared but unconnected.
- **Old behaviour we lost and need back, in a new form:**
  - init run modes and done-ness;
  - ordering by dependency;
  - init gating calls;
  - upgrade gating that can't wedge.
- **Rule violations to fix now:** reconcile errors are swallowed and the actor exits silently, which AGENTS.md forbids. Every reconcile, and even reads, re-admits every envelope.

## 1. Catalog (`big_cartridges_core`)

- **[gap]** `Catalog` (in-memory import/resolve) has no consumer, `Cartridge.dependencies` is never read, and `admitNodule` takes the full envelope inline. The ADR's "catalogued, then admitted" is therefore not what the code does.
- **[design] (proposal)** Keep the **inline envelope in `NoduleInstall`.** It makes the config document self-contained and portable: a mirrored node gets everything it needs except the artifact bytes, which travel by digest through big_blobs. Shrink the catalog to a **distribution tool**: import (OCI or doc), version resolution, and fetching or pinning artifacts into big_blobs. It is not a runtime state holder. Drop the in-memory `Catalog` type unless a consumer appears.
- **[design] (proposal)** Split **dependencies** by concern:
  - **Package dependencies** (`Cartridge.dependencies`, id + semver) are a *distribution* concern: "also fetch these packages". The catalog resolves them at import time.
  - **Runtime dependencies** are operation NSIDs (`Nodule.requires`), resolved by dispatch's routing table at bind time. They are not cartridge ids.
  - A missing provider makes the *dependent's* affected services `NotReady(ProviderMissing(nsid))`. It does not refuse installation.
  - This replaces old `collect_cartridge_and_dependency_order` (cartridge-id DFS) with a graph over operations, which also gives init its ordering (§4).

## 2. Admission scope (D1)

- **[design]** `Nodule::admit` validates commands, views (presenter must be `call`), processor targets, initializers, stores, private state and routine execution kinds (O23–O26). An unknown member in any of these refuses the whole nodule, config and grants included. This contradicts the open-union/carry-unknowns principle and D1.
- **Fix (intended):** admission validates only:
  - identity and authority (NSID prefix, no overlap);
  - Lexicons;
  - **services:** implemented operations and the handler artifact;
  - **facilities:** private state, stores, config schema;
  - **lifecycle:** initializers.

  Bindings (commands, views, processors) become opaque tagged sections that the runtime carries. Unknown vocabulary inside an *executable* part makes that service unavailable with a reason; it does not refuse the nodule.
- **[design] (proposal)** Reshape the manifest for D2:
  - **Collapse `routines`** (`key, implements, execution: Call|Workflow, state`) into a single **XRPC handler component** (artifact digest) plus `implements: [NSID]`. Dispatch's routing table registers from that list.
  - **Workflows** become a section owned by the wflow service later.
  - **The executor rules** ("workflow must implement a procedure", "one routine per operation") disappear from admission.

## 3. Lifecycle as a state machine

- **[gap]** `Readiness {Refused, Disabled, ArtifactsUnavailable, ExecutorUnavailable}` has no ready state. `ArtifactState` has one variant, and `is_ready()` is constantly false. The ADR's Known/Admitted/Selected/Upgraded/Uninstalled are operation names, not states you can query.
- **Fix (intended):** two levels.
  - **Per nodule:** `Refused(reason)` or `Admitted(revision)`, `enabled`, `observed_heads` vs `applied_heads`, and grant status. This is mostly what exists.
  - **Per service** (D4): `Ready`, `Disabled`, or `Unavailable(reason)`, where the reason is one of `ArtifactsPending`, `ArtifactsCorrupt`, `HostUnavailable`, `Initializing`, `InitFailed(..)` or `ProviderMissing(nsid)`. Dispatch only reads ready/not-ready plus the reason.
  - Artifacts fetched through big_blobs (ledger to-do 7) provide the first real transitions.
- **[bug]** `Refused` records echo `applied_heads` from an earlier success with no marker (O20). Make the record say what the applied heads correspond to, or clear them.

## 4. Initialization (prior art: `rt/init.rs`)

The old design was strong on ids and done-ness, but its gating and retry model was weak.

- **Old behaviour:**
  - Ids were `cartridge@version/key`, so an upgrade re-ran inits.
  - There were three modes: `PerInstall` (done-ness synced through the app doc, so once across devices), `PerNode` (SQLite) and `PerBoot` (memory).
  - Inits were chained serially in dependency order.
  - Every dispatch of the cartridge queued *behind* pending init dispatches.
  - On failure, waiters were failed and retry was implicit at the next boot or dispatch, with no failure record.
- **New behaviour:** `Initializer{target, mode: Once|EveryStart}` has no key, no argument and no done-ness (O1, O10–O12).
- **(proposal)** Redesign:
  - **Identity:** `Initializer{key, target, mode}`. Done-ness is keyed by (nodule id, init key, **version label**), mirroring the old behaviour where an intentional version bump re-runs inits. Not the content revision, because a metadata-only envelope change should not re-run init.
  - **Modes:** `PerNode` (done-ness in node-local SQLite) and `PerStart`.
    - Drop the synced `PerInstall`. An init that mutates shared documents is an idempotent operation invoked with the deterministic idempotency key `nodule@version/key`, following the wflow-first convention for durable document mutations (D3). Running it again on a mirrored node is then harmless, and we need no synced done-ness store.
    - Per the "durable across devices → datum" rule, init done-ness is host-local effect state, like actor ids.
  - **Gating without queues:** dispatch is stateless (D2), so it does not hold calls behind inits. A nodule's services are `Unavailable(Initializing)` until its current-version `PerNode` inits succeed. Dispatch returns a retryable `NotReady` error, and callers who want to wait long-poll readiness. That replaces the old waiting-dispatch DAG.
  - **Ordering:** a nodule's inits run once providers of its `requires` are `Ready`, the operation graph from §1. A dependency cycle shows as `InitBlocked(cycle)` and is reported, not executed.
  - **Failure:** recorded as `InitFailed{error, attempts}` in readiness. Retries happen on the next reconcile trigger (boot, config change, explicit operator retry), with no hidden loop. This makes visible what the old system only retried implicitly.
  - **Argument:** settings reach the nodule through a host **facility** (read own settings), not as an init argument. Then every invocation sees them, not just init.

## 5. Upgrade, downgrade and settings

- **[bug]** **An upgrade can wedge (O13).** `admit` rejects a new revision whose config schema invalidates the existing settings. `configure` validates against the *installed* revision, so the operator can't fix the settings first. **Fix (intended):** accept the upgrade and put the settings slot into `Invalid` under the new schema. The nodule becomes `Refused(InvalidSettings)` until it is reconfigured. That is the repairable-by-rewriting rule we already apply to imported config.
- **[design] (proposal)** **Downgrade and rollback.**
  - The old system enforced version monotonicity with a rollback-to-last-enabled exception, plus `check_breaking_changes`.
  - New proposal: an explicit admit of any version is operator intent, so allow downgrade. Gate on *contract* safety instead of version order: an upgrade that stops implementing an NSID which another enabled nodule `requires` makes the dependent `ProviderMissing`. It does not block the upgrade.
  - Proper Lexicon compatibility checks remain ADR §10 work.
- **[design]** The authority-change rule is enforced three times: `config.rs`, `state.rs` at admission, and reconcile (O16). Keep one owner, the config projection, and make the others invariants.

## 6. Actors, grants and authority

- **[design] (proposal)** **Uninstalled actors reserve their NSID prefix forever (O14).** Keeping the actor identity for reinstall is deliberate. But a *retired* cartridge's prefix should not block other cartridges, so restrict the overlap check to installed cartridges.
- **[design] (proposal)** **Bootstrapping grants (O21).** The reconciler can only grant drawers where the nodules actor already holds Admin, and big_node gives it Admin on the config drawer only. Ocap-correct alternative: `setNoduleDrawerGrants` carries the **operator's delegated capability** for the target drawer, and the nodule service stores the delegation. Authority travels with the request instead of pre-granting Admin everywhere. This also fits D4's operation grants.
- **[bug]** `NoduleScope.admit` is global (O22): a scoped admitter can install any package. Scope it per cartridge id.
- **[gap]** The nodule's own Lexicons are never registered with `SchemaRepo` (O2), so nodules can't write their own datums yet, and `SchemaRepo` has no live-registration API. Dispatch keeps operation schemas per provider for edge validation, so that part doesn't need `SchemaRepo`. Live datum-schema registration is a separate item; added to the ledger.

## 7. Code-level fixes (intended, small)

- **[bug]** `state.rs:216` does `drop(state.reconcile(...).await)`, and `:218` ends the actor silently on watch error, after which `Handle::ask` panics on send. Propagate it: a reconcile error that isn't per-nodule is fatal and must surface. The watch closing is a lifecycle error.
- **[perf]** Every reconcile re-decodes and re-admits every envelope. The conflict pass is O(n²). `get`/`list` trigger a full reconcile whenever any grant exists (O18). Cache the admitted result per `(cartridge, revision)`, reconcile only the changed keys, and make reads serve the last records.
- **[design]** `inventory::Inventory` duplicates `NoduleConfig`'s admission logic and has no non-test consumer (O7). Remove it.
- **[bug]** `resolve_entry` takes the nodule id from the first contribution with a string `description.id`, whatever its kind (O19). Filter to the nodule contribution.
- ADR vocabulary drift: `InvalidConfig` vs `Refused`, and `setNoduleGrants` vs `setNoduleDrawerGrants` (O3). Fix as part of the ADR 027 revision.

## Questions for the operator

**Resolved 2026-11-04.** See [ADR 027](../adrs/027-cartridges-nodules-and-operation-contracts.md) §10 and [ADR 028](../adrs/028-cartridge-identity-naming-and-dependencies.md).

1. Keep the inline envelope and reduce the catalog to a distribution tool (§1)?
2. Package dependencies go to the catalog and runtime dependencies are operation NSIDs (§1)?
3. Manifest reshape: one XRPC handler component plus `implements`, with routines collapsed (§2)?
4. Init redesign: `PerNode` + `PerStart`, version-label identity, idempotency-key-based shared inits, and readiness gating instead of queues (§4)?
5. Allow downgrades as explicit operator intent (§5)?
6. Release prefixes held by retired actors (§6)?
7. Drawer grants authorized by an operator capability carried in the request (§6)?
