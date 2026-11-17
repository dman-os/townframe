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

## Operator review, 2026-11-17 (direction; §1, 2, 4, 6 and 10 are revised to it, §3, 5 and 8 next)

1. **Grants happen at runtime too, not only at install.** Like Android asking for the camera after
   the app has started: a cartridge may request access while it runs. The one-line story gains a
   runtime request path for micros and nodules alike.
2. **Runtime requests have one model on every surface.**
   - Micros request through a host service and await the answer (a future).
   - Nodule requests surface in the same UI. Workflows wait like micros. A plain routine files the
     request and fails its call at once with a typed **missing-authority** error.
   - The CLI has a pending-requests queue the person can accept or decline.
   - Missing authority is a conventional error that propagates: when cartridge X's micro calls
     cartridge Y's routine and Y lacks a grant, X can tell the person "Y needs permission for this".
3. **"Only this time" lasts until the current boot ends.**
4. **`atRuntime` as a permbind property goes away.** Any permbind can be requested at runtime under
   items 1–2; claytxt's §5 one-shot-versus-remember question is answered by items 2–3.
5. **Permbinds must also cover network access**: talking to an origin (fetch). Rich external
   services (GitHub, Google) should not be a micro's raw fetch: platform cartridges provide them, so
   a web-modelled micro is analysed in isolation and the general-purpose service cartridges are
   the trusted part.
6. **The user faces the cartridge, never its parts.** No routines, micros, nodules or permbind names
   on the default path. The questions are in familiar nouns: "may it read/write drawer X",
   "may it use data from cartridge Y", "may it talk to services at origin Z". Drawer access uses
   the same read/write/admin vocabulary as the node's other grants. The single consent and audit
   view per cartridge is the value; the subsystems may stay parallel underneath.
7. **One answer per question.** Granting a drawer to a cartridge once satisfies every permbind
   with the same or lesser rules; the person is never asked again for another permbind. Named
   drawer-set permbinds can stay for internal analysis, but they are not a user-facing
   distinction.
8. **Collections are "data from cartridge Y"**, granted at install like other access. They are not
   a separate Clay-only kind: the permbind system is the primary grant experience, and
   claytxt's asymmetry 4 is withdrawn.
9. **Fine-grained ACLs exist for risk analysis.** Per-routine `uses` plus shared local state let
   the registry (the blessed store) analyse statically what can flow where, e.g. a routine with
   fetch access and another with drawer access that share local state can exfiltrate that
   drawer. Consent shows the permissions *and* the risks the analysis derives; the registry may
   flag or refuse publishing very risky combinations.
10. **Built-in host services and cartridges keep isomorphic shapes** (6.2), so host services can
    later move into cartridges.
11. **This document stays the ADR-shaped proposal; the user-facing experience gets its own FDR.**

## 1. The story in one line, and what is already settled

**A cartridge declares the holes in its authority; installing proposes what fills them, and the
running cartridge may ask for more; a person grants, in the cartridge's terms; the runtime mints
one capability per routine or micro from what it declared and was granted; the services refuse
anything outside it, with an error that says what is missing; a changed grant re-mints before the
next call.**

Settled by ADR 047 (accepted 2026-11-13) and not reopened here:
- **Permbinds** are the cartridge's named holes, by kind: `#datum { nsid, read, write, create,
  delete }`, `#method { nsid, necessity, minRevision? }`, `#drawers { read, write, create, delete,
  resolution }`. Each access is `required` or `optional`.
- **`uses`** is a routine's ACL over the permbinds. A key names a permbind and takes no more than
  it declares; admission refuses anything else.
- **Grants** live in the cartridge config document (ADR 044). ADR 047 keys them by permbind and
  runtime; §4 proposes amending that, so a person answers each question once per cartridge.
- **One capability per routine**, from `uses` ∩ grants, minted at start and re-minted on a grant
  change. Dispatch and the docs service refuse what it does not cover.
- **Confused deputy:** a call to another cartridge passes none of the caller's access.

What this proposal adds:
- the micro side on the same terms ("Clay on the node" §2);
- grants per cartridge in user-facing nouns, and runtime requests with a missing-authority error (§4);
- access narrowed by datum kind inside granted drawers, and a permbind kind for network origins (§2);
- how a service's own handle is reached (§6.2), and the gaps that show the minting path is incomplete
  (§7);
- the per-routine ACLs as the input to a static risk analysis (§9a).
The person-facing screens are an FDR of their own (operator review item 11).

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
- **The authoring surface lowers into the manifest, never around it.** Permbinds and `uses` are
  written with the manifest-authoring code that exists, behind a builder that is pleasant to write;
  the manifest stays the artifact that is admitted, granted and audited.

**The default shape of access is a datum kind in a drawer** (operator, 2026-11-17): "read datum
type X from drawer Y", where Y is a named drawer, a drawer type, or the drawers of a cartridge.
Today `#datum` narrows only the cartridge config document, and a granted drawer is reached whole
through the drawers handle. The revision: a routine's `#datum` kinds narrow its access inside every
drawer it was granted too, so its docs handle maps each reached document to those kinds
(`DocScope`'s `DatumKinds`, ADR 047 §6). Broad access ("any datum in this drawer") is an explicit
permbind added when something needs it, never the default. Narrow defaults are also what makes the
risk analysis (§9a) say something specific.

**Network access is a permbind kind** (operator review item 5): `#origin { origin, methods? }`, an
origin a routine or micro may talk to. A rich external service (GitHub, Google) is not a raw origin
grant: a platform cartridge provides it as operations, and a cartridge that wants it declares a
`#method` on those operations. The general-purpose service cartridges are the trusted part; the
cartridges that use them are analysed without network access.

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

### 4.1 A grant is per cartridge, in the person's nouns (amends ADR 047 §5)

The person answers questions about the cartridge, never about its parts (operator review items 6
and 7), so a grant records the answer to that question, once:
- `grant/drawer/<drawerId>` → `{ access: read | write | admin }`, the node's grant vocabulary;
- `grant/cartridge/<cartridgeId>` → `{ access }`, "may use data from cartridge Y" (its collections
  and datums included, item 8);
- `grant/origin/<origin>` → `{}`, "may talk to services at origin Z";
- `grant/method/<nsid or authority>` → `{}`, "may use Y's operation" where the person is asked at
  all (most are proposed and accepted at install, ADR 047 §10).

Permbinds no longer carry grants of their own. Each routine's or micro's handle is derived: its
`uses`, narrowed by its permbinds' rules, intersected with the cartridge's grants. Granting drawer X
read access satisfies every permbind that needs read on X or a drawer like it, whichever runtime
or routine holds that permbind, and the person is not asked again. Named drawer sets stay in the
manifest because the analysis (§9a) and the runtime need them, but they are not a question.
The grant datums are written only by the cartridges service (`setGrant`), never by the cartridge
(ADR 044 §6).

What this drops from ADR 047 §5: the per-runtime split (`nodule.grant/…`, `micro.grant/…`). One
cartridge-level grant feeds both runtimes. What the split bought, a nodule and a micro reaching
different things, is kept by each one's `uses`, which the grant never widens.

### 4.2 Runtime requests

Access can be requested while the cartridge runs, not only at install (operator review items
1–4). Any permbind may be requested at runtime; there is no `atRuntime` property.
- **The request** names a permbind (and, for a drawer set, may let the person choose the drawer).
  It is filed by the runtime on the cartridge's behalf, never written by the cartridge.
- **One queue, every surface.** Pending requests are node state the shell shows, the micro's host
  shows, and the CLI lists (`day grants pending`, `accept`, `decline`). A person answers it once,
  wherever they are.
- **"Only this time"** is a grant that ends with the current boot. "Allow" writes the grant datum.
- **Who waits.** A micro awaits the answer (a future from its host service). A workflow waits as
  workflows do. A plain routine files the request and fails its call at once with the
  missing-authority error below; the caller retries once the person has answered.

### 4.3 The missing-authority error

Every service refuses an ungranted call with one conventional error, carried through dispatch and
across cartridges unchanged: `MissingAuthority { cartridge, permbind, request? }`. It names the
cartridge that lacks the access (not necessarily the caller), the permbind, and the pending request
if one was filed. So when cartridge X's micro calls cartridge Y's routine and Y lacks a grant, X
receives Y's `MissingAuthority` and can tell the person "Y needs permission for this", with the
request to answer. It is distinct from `Denied`, which means the access will not be granted (the
rules forbid it), not that nobody has said yes yet.

### 4.4 Unchanged

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

A domain handle mixes two things. Its operation-level part (`BlobsScope.put`) only repeats what
dispatch already checked: may this routine call `blobs.put`. Its resource-level part
(`BlobsScope.read`, a doc capability's documents) says *which* resources, and dispatch cannot.

Proposed:
- **The operation-level part follows from the `#method` permbind.** A routine that may call an
  operation gets the service handle part that operation needs, minted by the service actor as in
  6.1. Which part each operation needs is stated once, beside the method's definition in
  `method_core`, so the lowering is a table the runtime reads, not code per service.
- **The resource-level part follows from the grants** (drawers, datum kinds, cartridges, §4.1).
- **Later, operation-level flags leave the service handles** and dispatch alone checks
  operations. Built-in services and cartridge-provided services then have the same shape: a
  handle names resources, dispatch names operations (operator review item 10).

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

## 9a. Risk analysis from the ACLs [nodules]

The per-routine and per-micro `uses` exist so the system can say not only "X wants Y" but "X wants
Y, and this is what can follow from it" (operator review item 9). Because every reach is declared,
the analysis is static: it reads a cartridge version's manifest, needs no execution, and gives the
same answer on every node and in the registry.

- **The graph.** Nodes are the cartridge's routines and micro, and the resources their `uses`
  name: datum kinds in drawers (read or write), other cartridges' data, operations, origins, and
  the cartridge's own local state and revisioned stores. Edges are the declared accesses.
- **Flows.** Data read by a routine can reach anything that routine can write or send. Across
  routines, data flows through anything one writes and another reads: shared local state, the
  cartridge's settings, a datum kind both reach. The analysis takes the transitive closure.
- **Risks** are flows from a sensitive source to an outward sink. The first rule is exfiltration:
  a path from a drawer's data to an origin, whether one routine holds both or one writes local
  state another sends out. It is reported with its path ("`sync` can send what `reindex` reads
  from your Photos drawer, through their shared local state `cache`").
- **Where it is shown.** The consent screen (the FDR) lists the permissions in the person's nouns
  and the risks beside them. The registry runs the same analysis at publish and may flag or refuse
  a version whose risks cross its policy; a node installing from elsewhere still shows them.
- **What it cannot see.** Two routines served by one component instance could exchange data in
  memory (ADR 047 §6 states this limit), so the analysis treats a component's routines as one
  unit for flows, and the per-routine split only sharpens reports within that bound. Platform
  cartridges (§2's GitHub or Google services) are trusted to do what their operations say; a
  cartridge using them is analysed as calling those operations, not as holding their network
  access.

## 10. Open questions [both]

Answered by the operator's review: `#datum` narrows inside granted drawers (§2); an operation
states the handle part it needs beside its definition in `method_core` (§6.2); the authoring surface
is the existing manifest code behind a builder (§2).

Still open:
1. The vocabulary that names a drawer in a permbind beyond a drawer id: drawer "types" and "the
   drawers of cartridge Y" (§2) need a definition of what a drawer type is.
2. How long a declined runtime request suppresses the same request (§4.2), so a routine retrying
   does not ask the person in a loop.

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
