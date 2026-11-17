# One permission model: the micro sections (claytxt lane)

Status: draft, 2026-11-17. These are sections 3, 5 and the micro half of 8 of `permission-model.md` (nodules lane,
draft #155, branch `stack/node-34-permission-model`), kept here until they are merged there. The vocabulary is
that doc's: permbind, `uses`, grant, service actor, domain handle (ADR 047). Nothing here is accepted.

## 3. Declaration: the micro side

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

## 5. Consent and at-runtime resolution

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

## 8. The cartridge-level parent actor (micro half)

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

## 10. Open questions (micro side, to add)

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
