# One permission model: the micro sections (claytxt lane, revision 2)

Status: draft, 2026-11-17. These are sections 3, 5, the micro half of 8 and the micro §10 questions of
`permission-model.md` (nodules lane, #155, `stack/node-34-permission-model` at `7185681d`), revised to the operator's
review of 2026-11-17 and to nodules' §2, §4 and §9a. Kept here until merged there. Vocabulary is that doc's.

## 3. Declaration: the micro side

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

## 5. Consent and runtime requests: the micro host's side

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

## 8. The cartridge-level parent actor (micro half)

The operator's item 6 settles the person-facing side: the person sees only the cartridge, in one consent and audit
view, and §4.1 puts grants on the cartridge, not on an actor. So a parent actor is not needed for consent or audit.
That leaves only implicit access between a cartridge's own runtimes, which §3 asymmetry 5 rejects. Keep the
`micros` and `nodules` subsystems parallel underneath, as the nodule half agrees.

## 10. Open questions (micro side; replaces the earlier four)

The earlier one-shot-versus-remember question is answered by §4.2 (the boot-scoped "only this time"), the Clay-local
access question by item 8, and the nodule `atRuntime` question by §4.2 (one queue, no `atRuntime`). Still open:
1. **Where a sink checks its handle** (§3 asymmetry 3): on the host surface, as proposed, or through a docs or blobs
   call the host makes for the micro? This decides how `<img>` of a blob works on Wry and Blitz.
2. **Embedding as a `#method` reach** (§3 asymmetry 4): is a presentation an operation of Y's, for the permbind and
   the analysis, or does it need its own kind?
3. **Explicit requests only** (§5): should the MDK ever file a request on a failed call, or always leave it to the
   micro?
4. **Prompts for invisible requesters** (§5): pending list only, or a system notification on Android and desktop?
