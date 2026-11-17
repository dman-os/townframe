# One permission model for nodules and micros (proposal)

Status: Proposal (draft, not accepted)
Owner: Townframe maintainers (nodules and claytxt lanes)
Updated: 2026-11-17
Scope: The permission model from a cartridge's manifest to the capabilities its nodule routines and micros hold, and the gaps in today's minting path. Everything here is proposed unless it cites an accepted ADR.

Written by the nodules lane with the claytxt lane (sections 3, 5 and
half of 8 are claytxt's, in `docs/scratch/permission-model-micro.md` on `docs/claytxt-design`
until merged here). Not accepted. This is the week's focus (ledger D47): the permission model as
one story, the same for both runtimes a cartridge contributes.

Related: ADR 022 (actors and grants), ADR 027 (cartridges and nodules), ADR 028 (requests,
necessity, resolution), ADR 037 (holder-bound capabilities), ADR 044 (configuration documents),
ADR 047 (routines, permbinds, per-routine capabilities), "Clay on the node" (#129, signed by the
nodules lane 2026-11-17).

## 1. The story in one line, and what is already settled

**A cartridge declares the holes in its authority; installing proposes what fills them; a person
grants; the runtime mints one capability per routine or micro from what it declared and was
granted; the services refuse anything outside it; a changed grant re-mints before the next call.**

Settled by ADR 047 (accepted 2026-11-13) and not reopened here:
- **Permbinds** are the cartridge's named holes, by kind: `#datum { nsid, read, write, create,
  delete }`, `#method { nsid, necessity, minRevision? }`, `#drawers { read, write, create, delete,
  resolution }`. Each access is `required` or `optional`.
- **`uses`** is a routine's ACL over the permbinds. A key names a permbind and takes no more than
  it declares; admission refuses anything else.
- **Grants** are keyed by permbind and kept per runtime in the cartridge config document:
  `nodule.grant/<permbind>[/<drawerId>]` and `micro.grant/<permbind>[/<drawerId>]` (ADR 044).
- **One capability per routine**, from `uses` ∩ grants, minted at start and re-minted on a grant
  change. Dispatch and the docs service refuse what it does not cover.
- **Confused deputy:** a call to another cartridge passes none of the caller's access.

What this proposal adds: the micro side on the same terms (with "Clay on the node" §2), the
consent and at-runtime picker flows, how a capability-bearing service's domain handle is reached
(§6), and the gaps that show the current minting path is incomplete (§7).

## 2. Declaration: the nodule side [nodules]

A cartridge's manifest is the only declaration. Nothing a running component says about itself
widens what it holds (`big_nodule/egress.rs`: the signer comes from the executor's workload
table, never from the request).

- **Permbinds are cartridge-level.** A cartridge's nodule and micro draw on one list. Granting
  reasons about that list once; each runtime is granted separately.
- **Routines declare `uses`.** Today a routine's `uses` lowers into up to three handles
  (`big_nodule::executor::RoutineCapabilities`): a dispatch handle (the operations it binds), a
  docs handle over the cartridge config document (its `#datum` kinds) and a drawers handle (its
  `#drawers` access and the create flag). `big_nodule/state.rs` builds those scopes from `uses`
  and the granted drawers.
- **The authoring surface lowers into the manifest, never around it.** A routine written with
  `big_ndk` states its permbinds and `uses` as declarations the build turns into the ADR 047
  manifest (the shape `export_nodule!` already has for routing). The manifest stays the artifact
  that is admitted, granted and audited. The concrete traits are open (§10).

Open in this section:
- `#datum` today scopes only the cartridge config document (its settings). Datum kinds in the
  documents of a granted drawer are reached through the drawers handle, not narrowed by kind.
  Whether `#datum` should also narrow drawer documents by kind is §10's first question.

## 3. Declaration: the micro side [claytxt]

To merge from `permission-model-micro.md`: `uses` per micro, admitted by the routine rule, and
every asymmetry with the nodule side stated explicitly.

## 4. Grants [both]

To write together. Starting points:
- Grant datums keyed by permbind, per runtime, in the cartridge config document (ADR 044 §8).
- Who may write them: the cartridges service (privileged `setGrant { subsystem, permbind }`);
  never the cartridge itself (a nodule writes its settings, not its grants: ADR 044 §6).
- Candidates are proposed automatically and a person accepts (ADR 047 §10).
- An upgrade that widens required access leaves the cartridge not startable until granted
  (ADR 028 §3).

## 5. Consent and at-runtime resolution [claytxt]

To merge: the picker as an introduction (`resolution: atRuntime`), what the person sees, and
whether a picked drawer becomes a grant or stays a one-shot introduction.

## 6. Minting and enforcement [nodules]

### 6.1 Who mints, and from what

Each handle a routine holds is minted by the runtime's service actor (for nodules, the `nodules`
subsystem actor, `state.stores.parent`) and bound to the routine's nodule actor as `subject`
(ADR 037 §5). So **the service actor must itself hold, durably, every right it hands down.** A
right it does not hold cannot be minted for any routine, whatever the grants say. §7's first gap
is exactly this.

The rule this proposal states: a runtime's service actor holds the union of what its cartridges
may be granted, as a *delegator*, and uses none of it on its own behalf beyond its bootstrap
work. Least privilege applies to the routines' handles (which are `uses` ∩ grants), not to the
delegator. Precedent: snapd's `snapd` holds every interface it connects; a Fuchsia component
manager routes capabilities it does not use itself.

### 6.2 Every service domain, not only dispatch

A call reaches a service through dispatch (the routine's dispatch handle admits the NSID), and
the service then demands its own domain handle: docs a doc capability, drawers a drawer
capability, blobs a blobs capability. `RoutineCapabilities` mints only three of these (dispatch,
config docs, drawers), so a routine that binds `blobs.put` passes dispatch and is refused by
blobs (§7's second gap).

Proposed: **a `#method` permbind on an operation whose service takes a domain handle lowers into
that domain's handle too**, with the narrowest scope that operation needs, minted by the service
actor as in 6.1. The service's Lexicon or method metadata states which domain and scope an
operation needs, so the lowering is a table the runtime reads, not code per service. The routine
then holds one handle per domain it reaches, all minted from the same `uses` ∩ grants.

Alternative considered: a permbind kind per domain (`#blobs { put, read }`). Rejected for now:
it puts service structure into every manifest, and a cartridge cares that it may call `put`, not
which capability domain enforces it.

### 6.3 Enforcement points, unchanged

- Dispatch refuses an operation outside the routine's dispatch handle.
- The docs service refuses a datum kind outside the doc handle (`DocScope.read`/`write` map each
  doc to `DatumKinds`).
- Each domain service refuses outside its own handle.
- Holder binding (ADR 037): a handle works only for the actor it is bound to, so a component
  presenting another routine's or nodule's handle is refused.

### 6.4 Revocation

Before a changed grant's handles are retracted, dispatch registers the nodule not ready, so a
call between retraction and re-mint is answered retryably and never with the old or a wider
handle (`apply_nodule_capabilities`). Micros follow the same order through their host.

## 7. Gaps this closes [nodules]

(a) **A routine's drawer `create` cannot be minted on a real node.** `ensure_shared_config_drawer`
(`big_nodule/lib.rs`) revokes the `nodules` service actor's create-drawer right once the shared
config drawer exists, as least privilege. Routine drawer handles are minted under that same actor
(`state.rs`, `MintDrawerCapability` with `create`), so any routine whose `uses` asks for `create`
gets a failed mint. The new test for drawer `create` passes only because it grants the right
back. Fix by 6.1: the service actor keeps create-drawer as a delegator and the revoke goes.

(b) **A nodule with a `blobs.put` permbind cannot reach blobs.** Blobs mints handles only for the
operator's identity or a holder of a wider blobs handle (`big_blobs/service.rs`,
`MintBlobsCapability`), and nothing mints one for a routine. The guest-bodies test mints from the
harness meanwhile. Fix by 6.2, with the `nodules` service actor holding a blobs handle as a
delegator per 6.1.

## 8. The cartridge-level parent actor [both]

To write together (claytxt has the micro half): whether a cartridge's micro and nodule share a
parent actor for consent, and what it changes. Neither side's call path changes ("Clay on the
node" §2).

## 9. Prior art [both]

To fill per decision: snapd plugs and slots (6.1, 6.2), Fuchsia `use` and capability routing
(6.1), Android runtime permissions (5), the Sandstorm powerbox (5, the picker as introduction),
polkit (operator approval).

## 10. Open questions [both]

1. Does `#datum` narrow drawer documents by kind as well as the config document (§2)?
2. Where an operation states the domain handle it needs (§6.2): Lexicon extension, method
   metadata in `method_core`, or the service's registration with dispatch.
3. The concrete `big_ndk` declaration traits that lower into the manifest (§2).
