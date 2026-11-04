# Nodule configuration and datum-store API

Status: Implemented
Owner: Townframe maintainers
Updated: 2026-10-31
Scope: ADR 023 host-side nodule management, node config drawer, portable desired state, and a facet-store-inspired datum store; no big_blobs or big_tasks dependency

## Agreed direction

- Add a fourth required local drawer, a **self-doc node config drawer**, beside
  present, originated, and registry. Its own document contains service configuration
  datums; this PR uses one document, not a document per nodule.
- Each node has its own config drawer. Copying/restoring a laptop's desired config
  can seed a phone's separate config drawer; mirroring does not require the two
  nodes to operate the same document. Relay sponsorship/backup transport is later.
- The nodules subsystem actor writes nodule configuration through the docs domain
  with an appropriately narrowed capability. Nodule actors do not receive access
  to the shared config doc merely because their settings are stored there.
- Configuration that should survive backup and move across devices belongs in
  datums: installed package envelopes and selected revisions, desired enablement,
  settings, and desired drawer grants. SQLite is not a second source of truth.
- Host-specific actor identities, boot capabilities, bootstrap pointers, and
  rebuildable reconciliation bookkeeping remain host-local. A phone reconstructs
  its own actors and effective authority from copied desired configuration.
- Dynamic nodule drawer access is local authority state, distinct from Keyhive
  participant membership used to share drawers with other nodes.
- A foreign configuration that this host cannot operate is carried and reported
  **per nodule**, not treated as justification to refuse the whole node's boot.
  Storage failure or a broken host identity invariant is still a boot failure.

## Existing reference points

The north star is specifically `FacetStore` / `FacetStoreHandle` in
[`stores.rs`](../../src/daybook_core/stores.rs), starting at line 340, rather than
copying the older `AmStore` abstraction. Keep the typed projection, queries,
mutations that flush through a domain, and reads at explicit document heads.
Do not copy the old whole-facet replacement, shared asynchronous lock, or the
mutation-before-flush failure behavior.

[`ReadDatums`, `WriteDatums`, and `ObserveDatums`](../../src/big_cabinet_core/doc.rs)
already provide gated values, explicit heads, multi-datum writes, and membership
observation. These are docs-domain operations; they do not depend on big_tasks.
`CommandId` here is the existing docs-domain idempotency key, not an executor
command or a task.

[`GrantDrawer` / `RevokeDrawer`](../../src/big_cabinet_core/drawer/protocol.rs)
operate on a local actor. **Correction to the earlier scope discussion:**
`RevokePrincipal` revokes a Keyhive peer/group membership; it does not revoke
nodule actors. Uninstall must revoke this service's local drawer grants, not call
`RevokePrincipal`.

## Datum layout: one document, multiple independently addressed datums

A Rust aggregate is a typed view, not one giant serialized map datum. For each
nodule identity, the view assembles:

- Install: the envelope, selected revision, and desired enablement.
- Settings: explicit operator settings; absence means use the admitted envelope's
  defaults, not persist a duplicate of those defaults.
- Drawer grants: desired drawer-to-access bindings using portable drawer references
  and the nodule identity, never a host actor id or boot capability.

The Lexicon names and key spellings are:

- `org.example.daybook.nodule.install/<plug_id>` (`NoduleInstall`)
- `org.example.daybook.nodule.settings/<plug_id>` (`NoduleSettings`)
- `org.example.daybook.nodule.grants/<plug_id>` (`NoduleGrants`)

All three categories coexist with the drawer descriptor, registry/bookkeeping
values, and other services' configuration. The store owns only its declared key
space; it never clears or rewrites a document to flush one service's state.

Known settings retain unknown fields. Unknown grant vocabulary or a future
configuration member is carried, not interpreted as a permissive default.
Pending, rejected, or undecodable reads are distinguishable from absence. A typed
view must retain enough raw state and diagnostics to avoid deleting an entry it
cannot operate. Orphan settings/grants do not implicitly install a nodule.

## Proposed API shape

The following signatures are a design sketch, not a new public API contract or
compiling implementation. `DatumPatch` contains only `set` and `remove` keys;
`StoreError` covers docs/transport failure and attempted invalid local edits.
The projection adapter is synchronous and has no authority or reconciliation
side effects.

```rust
pub trait DatumStore: Clone + Send + 'static {
    fn owns(key: &DatumKey) -> bool;
    fn hydrate(read: &DatumsRead) -> Self;
    fn diff(before: &Self, after: &Self) -> Result<DatumPatch, StoreError>;
}

pub struct StoreSnapshot<S> {
    pub heads: Vec<ChangeHash>,
    pub value: S,
}

impl<S: DatumStore> DatumStoreHandle<S> {
    pub async fn load(
        context: &RequestContext,
        docs: Client<DocsProtocol>,
        capability: CapId,
        doc_id: DocId,
    ) -> Result<(Self, StopToken), StoreError>;

    pub async fn snapshot(&self) -> Result<StoreSnapshot<S>, StoreError>;

    pub async fn at(
        &self,
        heads: Vec<ChangeHash>,
    ) -> Result<StoreSnapshot<S>, StoreError>;

    pub async fn mutate<O>(
        &self,
        context: &RequestContext,
        command: Option<CommandId>,
        edit: impl FnOnce(&mut S) -> Result<O, StoreError> + Send + 'static,
    ) -> Result<(O, Option<WriteReceipt>), StoreError>
    where
        O: Send + 'static;

    pub async fn watch(
        &self,
    ) -> Result<EventStream<StoreSnapshot<S>, StoreError>, StoreError>;
}
```

`NoduleConfig` implements this adapter as a typed aggregate of the nodule config
datums, including entries it cannot currently interpret. The host adapter should
live alongside the cabinet docs machinery, with nodule-specific layout and
validation in the nodule crates; no generic multi-document framework or extra
store crate is needed for this PR. Exact placement is reviewed during implementation.

There is one query shape and one mutation shape, rather than the old family of
async/sync/fallible variants. `snapshot` supplies a coherent immutable value for
ordinary queries; `at` supports exact-head inspection. `mutate` is the supported
load-edit-flush unit: no public bare flush of arbitrarily stale memory. Async
resource effects belong in the service reconciler, not in an edit closure.

### Mutation contract

1. A single owner serializes local edits and externally refreshed snapshots; no
   shared async store lock is held across a docs-domain call.
2. Apply the synchronous edit to a staged copy of the loaded view. Failed
   validation does not mutate or publish the live projection.
3. Diff only changed owned keys. Unchanged values, unknown fields/members, invalid
   imported entries, and other services' keys remain untouched. Editing/removing
   a carried entry must be explicit; inability to decode is not deletion.
4. Flush the patch as one `WriteDatums` operation at the **loaded heads** using
   `WriteMode::AtHeads`, retaining the facet-store concurrent-merge behavior.
   Do not silently use `AtCurrent` to turn stale memory into an overwrite, and do
   not auto-retry an edit closure. Conditional GUI edits can be a separately
   reviewed extension using existing docs write conditions if needed.
5. On a definite failure, discard the staged edit. On an ambiguous transport
   failure, do not claim success or publish speculative memory; resolve through
   the command receipt when supplied, or refresh before accepting another edit.
6. After a successful write, rehydrate the accepted view at the receipt's heads,
   including any concurrent history already present there. Do not simply publish
   the closure's candidate as if it were the merged document. Return the write
   receipt with the closure result; the latter is not a reconciliation verdict.
7. No-op edits return no receipt. A write receipt identifies durable config
   content, not completion of grant application, index observation, relay backup,
   or execution.

### Load and watch contract

- Load establishes observation before taking the initial exact-head read, then
  drains relevant changes queued during that read. No read-then-subscribe gap.
- Hydrate related datums at one document frontier; never assemble a nodule from
  independently read current versions of install, settings, and grants.
- Watch delivers the current snapshot followed by coherent refreshed snapshots.
  It may coalesce intermediate desired states: it is not an audit/version log,
  a durable cursor, or the future ADR 023 management-observation API. Changes that
  arrive during reconciliation schedule a subsequent pass; the snapshot already
  being applied is not modified underneath the pass.
- Treat docs observation as an invalidation signal, not a typed accepted value or
  a fence on a local write receipt. Local writes refresh directly; later matching
  observation can be deduplicated without trusting notification delivery order.
- Refresh the changed owned keys at a captured frontier rather than reload all
  configuration for every notification. A frontier also advances for unowned
  changes so subsequent writes have an accurate base. Historical `at` reads and
  initial load may read the full owned view.
- Verify invalidation for admission-status changes during implementation:
  `ObserveDatums` is an ungated membership projection. Proof/membership/schema
  changes can alter accepted reads without changing content. The store must have
  a real reconsideration trigger for those changes, not poll or assume membership
  notifications cover them. Access loss or a failed/ended subscription is surfaced;
  it must never become an apparently empty configuration.
- Stop joins the owned observation/state tasks before their docs service and
  authority dependencies stop. The store does not hide failures in detached tasks.

## Desired state versus host reconciliation

The store does not open actors, execute nodules, or grant access. The nodule
service consumes a snapshot and reconciles the corresponding desired state:

1. Interpret and validate an install/settings/grants entry independently of
   unrelated nodules. Keep unsupported or invalid configuration visible with a
   per-nodule reason and its source heads. Never execute it or grant from it.
2. Provision/reopen the host's durable actor through `OpenActor`.
3. Apply, downgrade, or revoke local drawer grants **within the reconciler's
   delegated authority**. Writing a config datum does not itself authorize access
   to a target drawer. Admin on the config drawer alone cannot administer every
   other drawer. Missing target drawers or authority on a restored host must be
   reported as blocked bindings, not silently granted or discarded.
4. Report observed config heads separately from successfully applied config
   heads and per-nodule failures. Advance applied status only after the effects
   that it claims have completed, not when a message has been queued.

A management mutation validates local user input, durably writes desired datums,
and requests reconciliation. If effects fail after the write, keep the desired
state and report the apply failure with those saved heads; do not report an
unsaved mutation or roll back accepted config as though both stores were one
transaction. An imported invalid config is per-nodule refusal; a failed docs
service, corrupt host state, or lifecycle programming error is not a readiness
fallback.

Effective grant provenance/bookkeeping must allow removal after a crash or an
uninstall: compare desired bindings against previously applied **service-owned**
bindings, including drawers absent from the new desired config. Actor ids alone
are not enough. Any host-local journal is rebuildable effect state, not portable
configuration. Reconciliation must not revoke another grant source merely to
make its own projection match.

Uninstall removes the nodule's owned config datums in one document write and
revokes its service-owned drawer grants. Keep the durable host actor identity so
reinstall can reopen it; no executor/process termination stub is necessary here.
Capability/grant provenance, downgrade behavior, and crash recovery need tests,
not just repeated grant calls.

## PR cut and verification obligations

Include the fourth drawer and bootstrap/reopen path, the nodule config declarations,
the one-document store and observation lifecycle, datum-backed management,
per-nodule reconciliation refusal, dynamic local drawer grants, uninstall, and
ADR updates. The existing unmerged nodule settings/enabled SQLite model is
superseded, not made a second durable configuration source. Migration changes
must be explicit and checked against tracked history; do not disable the
migration-history guardrail.

Acceptance coverage should demonstrate:

- Config drawer identity survives reboot; a restored copy seeds a different node's
  own drawer and actor identities without copying capabilities.
- Declaration, publication, admission, and unknown-field/member codec round-trips
  for each config datum.
- A failed store write leaves no speculative projection; a concurrent foreign
  write is merged at explicit heads; no sibling service datums are changed.
- Subscribe/load interleaving and changes arriving during reconcile lose no
  invalidation; admission changes and access loss are not mistaken for absence.
- Invalid imported config affects only its nodule and remains preserved; one
  mutation commits related config datums at a single frontier.
- Desired and effective state diverge visibly on authority failure; reboot resumes
  both additions and revocations with no duplicate/outdated grants.
- Grant downgrade, revoke, uninstall, and reinstall respect local actor authority,
  without changing Keyhive participant membership or other grant sources.
- Snapshot and receipt semantics survive forced notification/completion ordering;
  shutdown joins service tasks in reverse dependency order.

Deferred: blob materialization/artifact availability, processors and executors,
invocation/discovery/exposure enforcement, task dispatch, GUI, catalog hosting,
relay sponsorship, the durable cursored management feed, and multi-provider pins.
Catalog hosting and GUI are deferred for scope, not technically blocked by
big_blobs/big_tasks. Existing install envelopes are enough for this PR.

See [ADR 023](../adrs/023-plugs-nodules-and-operation-contracts.md) for the larger
runtime contract. This scratch note records the agreed design and implementation
for datum-backed nodule configuration and host reconciliation.
