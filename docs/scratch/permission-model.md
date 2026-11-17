# One permission model for nodules and micros (proposal)

Status: Proposal (draft, not accepted); all sections written, claytxt's merged 2026-11-17
Owner: Townframe maintainers (nodules and claytxt lanes)
Updated: 2026-11-17
Scope: The permission model from a cartridge's manifest to the capabilities its nodule routines and micros hold, and the gaps in today's minting path. Everything here is proposed unless it cites an accepted ADR.

Written by the nodules lane with the claytxt lane: sections 3, 5 and
the micro half of 8 are claytxt's (from `docs/scratch/permission-model-micro.md`, `381a0869` on
`docs/claytxt-design`), merged here. Not accepted. This is the week's focus (ledger D47): the permission model as
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

**A micro declares `uses` exactly as a routine does, once per micro.** The keys name the cartridge's permbinds,
and each asks no more than its permbind declares. Admission applies the routine rule unchanged. A micro has no
routines, so its `uses` sits on the micro contribution (`Contribution::Micro`, "Clay on the node" §5), and the
host mints one micro capability from `uses` ∩ `micro.grant/*`.

6.1 fits as written: the `micros` service actor holds, as a delegator, the union of what its micros may be
granted, and mints each micro's handles with the micro's actor as subject. 6.2 fits too: a micro whose `uses`
binds `blobs.put` gets a blobs handle the same way a routine does.

Asymmetries with the nodule side, stated explicitly:
1. **When the capability is minted.** A routine's capability is minted when its cartridge starts. A micro's is
   minted when its host binds an incarnation. Micros start eagerly or on demand, and a disabled micro holds
   nothing. Every mount of the same incarnation shares that one micro capability; there is no capability per
   mount or per surface. Dropping the incarnation retracts it (Clay's shutdown is RAII: the capability is owned by
   the incarnation).
2. **The guest never holds the capability bytes.** The host attaches the micro's capability to each
   `dispatch.local` call and each docs call. The guest only names the operation. This matters more for micros than
   for nodules, because micro code is user-editable (TS, the end-user tier), so the handle must stay out of the
   guest's memory. On the web the page itself is the host, and the micro still runs in wcmp's sandbox, so the
   same holds.
3. **UI sinks are reads too.** A micro reads a blob or a datum not only through `fetch` but also by rendering it:
   an `<img>` whose source is a blob, a CSS `url()`. These never become network requests (micros have no
   egress). The host resolves them on the surface and checks them against the micro's blobs or docs handle. A
   blob that only a sink reads still needs a `uses` entry, so that "this micro can show your photos" is visible
   in the manifest. Nodules have no sinks.
4. **Clay-local access that isn't a node permbind.** A micro also declares access that never reaches the node:
   - collections it shares with other micros (ADR 031's manifest-declared collections);
   - presentations it publishes or embeds (`mount-component`).

   The host enforces these through the capability bank, and they are not granted through `micro.grant/*`. Proposal:
   they stay in the Clay manifest under ADR 028/031's request rules and aren't modelled as permbinds, because no
   service actor mints them. They do show on the consent screen (§5) beside the permbinds, so a person sees one
   list. This is the one place the two surfaces differ in kind and not only in detail; see §10.
5. **Calling its own cartridge's nodule.** A micro calling an operation its own cartridge's nodule serves needs a
   `#method` permbind and a `uses` entry, like any other call. ADR 047's confused-deputy rule applies within a
   cartridge too: the micro passes none of its access, and the nodule acts with its own. That keeps one rule. The
   alternative, implicit access to one's own nodule, is what a cartridge-level parent actor would enable (§8).

## 4. Grants [both]

To write together. Starting points:
- Grant datums keyed by permbind, per runtime, in the cartridge config document (ADR 044 §8).
- Who may write them: the cartridges service (privileged `setGrant { subsystem, permbind }`);
  never the cartridge itself (a nodule writes its settings, not its grants: ADR 044 §6).
- Candidates are proposed automatically and a person accepts (ADR 047 §10).
- An upgrade that widens required access leaves the cartridge not startable until granted
  (ADR 028 §3).

## 5. Consent and at-runtime resolution [claytxt]

**At install** (ADR 047 §10): the install flow proposes a candidate for each permbind, and the person accepts or
changes it.
- The screen is per cartridge, with one list. Each entry shows the permbind's `description`, its access
  (read/write/create/delete, or "can call X"), whether it is required, and which runtime asks for it ("the app"
  for the micro, "in the background" for the nodule). Each runtime is granted separately, as `micro.grant/*` and
  `nodule.grant/*`.
- Clay-local requests (§3, asymmetry 4) appear in the same list.
- Nothing is granted without being shown. Declining an optional permbind leaves the cartridge startable; declining
  a required one leaves it "needs configuration" (ADR 028 §3).

**At runtime** (`resolution: atRuntime`): **the picker is an introduction, in the Sandstorm powerbox sense.**
- The micro asks through the MDK for a permbind it declared with `atRuntime`, for example
  `ctx.pick(permbind)`, an async call answering with a handle or "declined". It never names a candidate.
- The **shell** draws the picker, not the micro. It is a trusted surface that the asking micro can neither style
  nor read, so the micro can't spoof it and never sees the candidates it wasn't given. It shows the cartridge, the
  permbind's description and the access.
- What the person picks comes back as a handle over exactly that drawer, with exactly that access. Nothing else
  becomes visible.

**Does a pick become a grant?** Proposal: **one-shot by default, with an explicit "remember"**, like Android's
"only this time" versus "allow":
- A one-shot pick is an introduction owned by the incarnation. It is minted under the micro's actor and retracted
  when the incarnation ends. No grant datum is written.
- "Remember" writes `micro.grant/<permbind>/<drawerId>` through the cartridges service's `setGrant`, never through
  the micro. The micro capability is re-minted from it and shows in the cartridge's settings, where it can be
  revoked.

**Nodules and `atRuntime`.** A routine has no surface to show a picker on. Proposal: a nodule's `atRuntime`
permbind is resolved through the cartridge's settings screen, which the shell draws, and always becomes a grant.
There is no one-shot introduction for routines, because no incarnation exists to own it. See §10.

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

**Recommendation from both lanes: keep the `micros` and `nodules` subsystems parallel, with no
cartridge-level parent, until something needs one that the config document cannot give.**

### Micro half [claytxt]

Today the two subsystems are parallel: operator → `micros` → one micro actor, and operator → `nodules` → one
nodule actor. Neither side's call path depends on a parent ("Clay on the node" §2; 6.1 mints under the service
actor whatever the tree above the subject looks like).

From the micro side, a parent `cartridge` actor over both would buy three things:
- **One consent and audit view.** Mostly there already: both runtimes' grants live in one config document, so
  the consent screen (§5) groups by cartridge without an actor.
- **Revoking everything a cartridge can do in one step.** Also reachable without a parent: disabling the
  cartridge retracts both runtimes' capabilities.
- **Implicit access between a cartridge's own runtimes** (§3, asymmetry 5). This is the only thing a parent would
  change, and it weakens the one rule that every call is declared.

Recommendation from the micro side: **keep the subsystems parallel**, with no parent, until something needs one
that the config document can't give. Every micro-to-own-nodule call stays a declared `#method`. Revisit the parent
with the cartridge consent ADR if cross-runtime delegation inside a cartridge (a micro handing its nodule a
picked drawer) turns out to need more than passing a handle in the call, which ADR 047 already allows.

### Nodule half [nodules]

Nothing on the nodule side needs a parent either:
- Minting does not depend on the tree above the subject: every routine handle is minted by the
  `nodules` service actor with the nodule actor as subject (§6.1). A parent would sit above the
  nodule actor and change no mint.
- Holder binding (ADR 037) admits a channel to act as its descendants. A cartridge parent would
  let one runtime's channel act as the other runtime's actor, which is exactly the implicit
  cross-runtime access the micro half rejects.
- Grants already group by cartridge, because both runtimes' grants live in its one config
  document (ADR 044), and disabling the cartridge retracts both runtimes' handles.

## 9. Prior art [both]

- **snapd plugs and slots** (§2, 6.1): a snap declares plugs; `snapd` holds every interface it
  connects and mediates the connection. The service actor as delegator is the same shape.
- **Fuchsia `use` and capability routing** (§2, 6.1): a component `use`s only what it declares,
  with `availability: required | optional`; the component manager routes capabilities it does not
  use itself.
- **Android runtime permissions** (§5): "only this time" versus "allow" is the one-shot pick
  versus "remember".
- **The Sandstorm powerbox** (§5): the picker is drawn by the trusted shell, the app never sees the
  candidates it was not given, and a pick is an introduction of exactly what was chosen.
- **polkit** (§4): operator-level approval is an explicit act (`auth_admin_keep`), never ambient
  authority, matching ADR 043's client identity.

## 10. Open questions [both]

1. Does `#datum` narrow drawer documents by kind as well as the config document (§2)?
2. Where an operation states the domain handle it needs (§6.2): Lexicon extension, method
   metadata in `method_core`, or the service's registration with dispatch.
3. The concrete `big_ndk` declaration traits that lower into the manifest (§2).

Micro side [claytxt]:

- **Clay-local access** (§3, asymmetry 4): should collections and presentations become permbind kinds, so that
  one ACL covers everything a cartridge holds? Or do they stay Clay manifest requests shown beside the permbinds?
  This proposal is the latter.
- **One-shot versus remember** (§5): is one-shot the right default for picks, or should a pick become a grant
  unless the person declines?
- **UI sinks** (§3, asymmetry 3): where does a sink read check its handle? On the host surface, as proposed, or
  through a docs/blobs call the host makes for the micro? The answer decides how `<img>` of a blob works on Wry
  and Blitz.
- **Nodule `atRuntime`** (§5): resolved only through settings, or may a cartridge's micro pick on its nodule's
  behalf and hand the result over?
