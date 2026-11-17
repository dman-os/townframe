# One permission model for nodules and micros (proposal)

Status: Proposal (draft, not accepted); all sections written, claytxt's merged 2026-11-17
Owner: Townframe maintainers (nodules and claytxt lanes)
Updated: 2026-11-17
Scope: The permission model from a cartridge's manifest to the capabilities its nodule routines and micros hold, and the gaps in today's minting path. Everything here is proposed unless it cites an accepted ADR.

Written by the nodules lane with the claytxt lane: sections 3, 5 and
the micro half of 8 are claytxt's (from `docs/scratch/permission-model-micro.md`, revision 2 at
`bbf871d5` on `docs/claytxt-design`), merged here. Not accepted. This is the week's focus (ledger D47): the permission model as
one story, the same for both runtimes a cartridge contributes.

Related: ADR 022 (actors and grants), ADR 027 (cartridges and nodules), ADR 028 (requests,
necessity, resolution), ADR 037 (holder-bound capabilities), ADR 044 (configuration documents),
ADR 047 (routines, permbinds, per-routine capabilities), "Clay on the node" (#129, signed by the
nodules lane 2026-11-17).

## Operator review, 2026-11-17 (direction; every section is revised to it)

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

**A micro declares `uses` exactly as a routine does, once per micro.** Each key names one of the cartridge's
permbinds and asks no more than it declares; admission applies the routine rule unchanged. A micro has no routines,
so its `uses` sits on the micro contribution (`Contribution::Micro`, "Clay on the node" §5). Its handle is derived
like a routine's (§4.1): `uses`, narrowed by the permbinds' rules, intersected with the **cartridge's** grants.
There are no micro-specific grant datums: `grant/drawer/…`, `grant/cartridge/…`, `grant/origin/…` and
`grant/method/…` feed the micro and the nodule alike, and each one's `uses` keeps them apart. The default shape is
§2's: a datum kind in a drawer, with broad access an explicit permbind.

6.1 fits as written: the `micros` service actor holds the union as a delegator and mints each micro's handles with the
micro's actor as subject. So does 6.2: a micro binding `blobs.put` gets a blobs handle like a routine.

Asymmetries with the nodule side, stated explicitly:
1. **When the handle is minted.** A routine's handle is minted when the cartridge starts. A micro's is minted when its
   host binds an incarnation (micros start eagerly or on demand; a disabled micro holds nothing). It is shared by every
   mount of that incarnation, and retracted when the incarnation drops (Clay shutdown is RAII). A grant change
   re-mints it under 6.4's order.
2. **The guest never holds the handle's bytes.** The host attaches the handle to each `dispatch.local` and docs call;
   the guest names only the operation. This matters more for micros, whose code is user-editable (the TS tier). On the
   web, the page is the host and the micro still runs in wcmp's sandbox, so the same holds.
3. **UI sinks are reads and sends.** A micro reads not only through `fetch` but by rendering: an `<img>` whose source
   is a blob, a CSS `url()`. The host resolves a sink on the surface and checks it against the micro's handle, as for
   a call. A sink source outside the node (an `https:` image) is network access and needs an `#origin` permbind like
   `fetch` does; without one the host refuses it rather than loading it (micros have no ambient egress). §9a counts
   sink reads as reads and external sinks as outward sends, so "this micro can show your photos" and "this micro
   loads images from Z" are visible to the analysis. Nodules have no sinks.
4. **Withdrawn (operator review item 8).** Collections are "data from cartridge Y", granted at install through
   `grant/cartridge/<Y>` like other access; they are not a Clay-only kind. What follows for Clay:
   - A collection shared between micros of the **same** cartridge needs no grant; it is the cartridge's own data
     (like a nodule's local state), declared in `uses` so §9a sees the flow.
   - A collection another cartridge publishes is reached through a permbind on that cartridge's data, granted as
     `grant/cartridge/<Y>`. The capability bank keeps enforcing it on the Clay side; the bank's handle is minted from
     that permbind, so it is one more domain handle under 6.2, not a separate grant path.
   - **Embedding another cartridge's presentation** (`mount-component`) is a reach to Y's exported surface, so it is a
     `#method`-shaped permbind on Y's presentation, proposed and accepted at install (§4.4). The embedded micro renders
     with Y's own handle, never the embedder's (the confused-deputy rule). Props the embedder passes are a flow from
     X to Y, which §9a counts.
5. **Calling its own cartridge's nodule.** Still a declared `#method` in `uses`, like any call; one grant per
   cartridge (§4.1) means the person is not asked twice for it, but the micro's `uses` must still name it.

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

## 5. Consent and runtime requests: the micro host's side [claytxt]

§4.2 defines one pending-request queue on every surface. This section is what the micro host and the shell add to it.

**Asking.** A micro asks through an MDK host service: `ctx.request(permbind)` (or `ctx.request_drawer(permbind)` for
a drawer-set permbind, where the person picks the drawer). The host files the request into the node's queue on the
cartridge's behalf, under the micro's actor; the micro never writes it. The call returns a future that resolves to:
- `Granted`: answered "allow" or "only this time". The host re-mints the micro's handle (6.4) **before** resolving,
  so the micro's next call succeeds;
- `Declined`: answered no. §10 decides how long the same request stays suppressed;
- the future stays pending while nobody has answered. The micro may drop it; the request stays in the queue.

A micro does not ask for what its `uses` doesn't declare: admission already refuses it, and `request` takes only a
declared permbind.

**Missing authority from a call.** A call that fails with `MissingAuthority` for the micro's own cartridge (§4.3)
carries the request the runtime filed, if any. The MDK lets the micro await that request (`ctx.await_request(r)`)
instead of filing a second one. Proposal: the MDK never files implicitly on a failed call; a micro asks explicitly,
so a prompt is always tied to something the person just did.

**The prompt is the shell's.** The person sees one prompt, drawn by the shell on a trusted surface the micro can
neither style nor read:
- It names the cartridge and asks in the person's nouns (item 6): "may *Photos* read your *Camera roll* drawer?",
  "may *Photos* talk to services at `example.com`?". Never a micro, routine or permbind name. The §9a risks of
  saying yes are shown beside it.
- Answers: **Allow** (writes the grant datum through `setGrant`), **Only this time** (a grant that ends with the
  boot, item 3; an incarnation that restarts within the boot keeps it), **Don't allow**.
- For a drawer-set permbind, the prompt is the powerbox: the person picks among drawers the micro never sees, and
  only the pick is granted.
- **When it appears.** If the asking micro's cartridge has a visible surface, the shell shows the prompt over it. If
  not (a background incarnation, or a request filed by a routine), it goes to the shell's pending list and the CLI
  queue only, with no interruption. One request is answered once, on whichever surface the person uses.

**Another cartridge's missing authority** (item 2). When X's micro calls Y's operation and gets Y's
`MissingAuthority { cartridge: Y, request }`, X may show "Y needs permission for this" and offer the request. The
prompt is still the shell's and is about Y: X can surface Y's request but never answer it, and answering it changes
only Y's grants. X never gains anything from it.

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

The operator's item 6 settles the person-facing side: the person sees only the cartridge, in one consent and audit
view, and §4.1 puts grants on the cartridge, not on an actor. So a parent actor is not needed for consent or audit.
That leaves only implicit access between a cartridge's own runtimes, which §3 asymmetry 5 rejects. Keep the
`micros` and `nodules` subsystems parallel underneath, as the nodule half agrees.

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

The earlier one-shot-versus-remember question is answered by §4.2 (the boot-scoped "only this time"), the Clay-local
access question by item 8, and the nodule `atRuntime` question by §4.2 (one queue, no `atRuntime`). Still open:
1. **Where a sink checks its handle** (§3 asymmetry 3): on the host surface, as proposed, or through a docs or blobs
   call the host makes for the micro? This decides how `<img>` of a blob works on Wry and Blitz.
2. **Embedding as a `#method` reach** (§3 asymmetry 4): is a presentation an operation of Y's, for the permbind and
   the analysis, or does it need its own kind?
3. **Explicit requests only** (§5): should the MDK ever file a request on a failed call, or always leave it to the
   micro?
4. **Prompts for invisible requesters** (§5): pending list only, or a system notification on Android and desktop?
