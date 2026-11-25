# Clay on the node (claytxt lane, ADR draft)

Owner: claytxt lane; operator reviews.
Updated: 2026-11-17
Scope: Clay's node API: how micros and the node host talk, grants, the TypeScript path.

Status: **accepted (operator, 2026-11-17)**, with the nodules lane's sign-off. Draft 2026-11-12; the operator answered both
rounds on 2026-11-12 (recorded per section). The operator
reviewed the whole on 2026-11-17 and likes it; §6 is now settled. Two conditions remain before it is accepted: it
must work for TypeScript micros as well as Rust ones (see "TypeScript micros" in §3), and the nodules lane must
sign it off, because the micro and nodule surfaces are meant to be as isomorphic as possible. Nodules lane, 2026-11-17:
signed.
Number: to be assigned when it moves to `docs/adrs/` (ADRs 029–031 are Clay's; the node lane is at
045 on `feat/big-node-next`). The app this serves is **daytxt** (the user-facing name, spelled with TXT; "Dayframe"
is free for other use).

Sources read for this draft: ADR 022 (actors and grants), ADR 027 (cartridges and nodules), ADR
033 (what a node is), ADR 037 (holder-bound capabilities), ADR 044 (cartridge configuration
documents), `docs/scratch/cartridge-authoring.md`, `docs/scratch/intxt-node-requirements.md` (R12,
R13), and the code on `feat/big-node-next` (`day_cli`, `big_ndk`, `big_nodule`, `big_cabinet_core`,
`big_cartridges_core`) and `feat/micro-lab` (`clay_mdk`, `clay_docs`, `clay_collection`,
`clay_guest_session`).

## Context

Every Daytext host today loads micros from sidecar package files and serves documents from
`MockDocuments`. Nothing in `clay_*` reaches a node: the capability bank is micro-to-micro only.
The intxt alpha forces the gap closed (R13: "the intxt micro reads and writes intxt datums through
its node"), and the phone must run a node (R12).

The node side is settled enough to build on:
- A node is services, transport-agnostic clients (`Client<Proto>`) and attachments (ADR 033 §2:
  "Node-side and Clay-side code depend only on client handles").
- Nodules call methods with `cx.call::<M>(input)`, a `method_core::Method` (wasm-buildable) carried
  by `service_client_core_rs::Client` over `wasi:http/client` to `http://dispatch.local`; the host
  (`big_nodule/egress.rs`) turns that into a dispatch call signed by the workload's nodule actor,
  taken from the executor's workload table and never from the request.
- The caller's actor is established by the channel (ADR 037 §1). A channel authenticated as an actor
  may act as that actor's descendants (ADR 037 open question 2, as practised).
- The docs service exposes `writeDatums`, `readDatums`, `listDatums` and `pollDatums` (one long poll
  over the datum-set frontier; a current-state read is `after = 0, hold_ms = 0`), each taking a doc
  capability.

## 1. Where a host's node comes from

**Decided (operator, 2026-11-12): exactly as `day` does.** A daytxt host finds a running node by
`day`'s discovery and attaches as a socket client, or boots it in-process and becomes the holder
(`day_cli/client.rs`). Nothing new is designed here: the find-or-hold code moves out of `day_cli`
into a crate both use. Android has no second process, so the app is always the holder; the web
host embeds a webnode (ADR 038).

## 2. Who a micro acts as

**Decided (operator, 2026-11-12): a micro is a subsystem actor, not its cartridge's actor, and the
`micros` and `nodules` subsystems stay parallel for now.**

- Actor tree: operator → `micros` subsystem actor → one actor per installed micro, mirroring
  operator → `nodules` → per-nodule actor (ADR 027). The path segment follows the nodule rule
  (`<name>-<blake3(cartridge id)[..32 hex]>`).
- The host is the channel. A socket client is the operator (peer credentials); an in-process host
  is the operator as holder. Each micro call names the micro's actor to act as, which ADR 037
  admits because micro actors are the operator's descendants. The host takes the micro from its
  own incarnation table, never from anything the guest says, as `big_nodule` does with the
  workload table.
- **Durable access follows ADR 047 (accepted 2026-11-13; revised here 2026-11-17 on the nodules
  lane's review).** Grants are keyed by permbind, not by resource. They are recorded in the
  cartridge's configuration document (ADR 044 §8), one datum per grant: `micro.grant/<permbind>`,
  plus `micro.grant/<permbind>/<drawerId>` for the members of a `#drawers` set. They sit beside the
  nodule's `nodule.grant/<permbind>`. Permbinds belong to the cartridge, so its micro and its nodule
  draw on one list of permbinds and are granted separately per runtime. Privilege is refused at mint.
- **A micro reaches only its declared `uses` that were granted.** A micro declares `uses` the way
  a nodule routine does (ADR 047 §6). It has no routines, so it declares them once, per micro. They
  are admitted by the same rule: each key must name one of the cartridge's permbinds and ask no more
  than that permbind declares.
  - The host mints one micro capability from `uses` ∩ grants when it binds the micro, as a
    holder-bound capability (ADR 037 §5). It retracts and re-mints that capability whenever a
    grant the micro uses changes, so revocation takes effect at the next call.
  - The host hands that capability to every `dispatch.local` call the micro makes, as nodule egress
    does. The micro presents only its own handles (ADR 037 a1).
  - A micro with no granted `uses` can call nothing.
- **Docs access is datum-kind scoped** (ADR 047 §6: `DocScope.read`/`write` map each doc to its
  `DatumKinds`). A micro's docs access comes from its `#datum` and `#drawers` permbinds, and its
  layer-1 calls (§4) carry that capability.
- The cartridge's settings datums are shared by its micro and nodule; the micro reads and writes them, never its
  grants.
- The micro side of the permission model is written with the nodules lane this week, as one story from manifest
  to grant to capability. It covers the micro side of `uses`, the consent and picker flows (ADR 047 §10; a
  permbind with `resolution: atRuntime` is naturally a micro's picker) and the cartridge-level parent actor below.
- Open (cartridge-authoring.md "next design topic"): whether a cartridge's micro and nodule should
  share a cartridge-level parent actor for consent. This draft keeps the two subsystems parallel;
  a later cartridge-consent ADR can add the parent without changing the call path.

## 3. How a micro calls node methods

**Decided (operator, 2026-11-12): one call API for nodules and micros; a micro's call always goes
through its host.**

- Authors write the same thing in both: `ctx.call::<M>(input).await` with a `method_core::Method`,
  through `service_client_core_rs::Client` with a transport the environment supplies. A typed
  client (`ctx.docs()`, …) is shared code, not two implementations.
- **Decided (operator, 2026-11-12): the micro transport is `wasi:http/client` to
  `dispatch.local`, the nodule transport byte for byte.** Host services should align with the WASI
  ecosystem wherever they can, for Clay and the node alike. So every Clay runtime (wasmtime, wcmp,
  and wcmp in the browser) hosts `wasi:http/client`, with egress admitting only `dispatch.local`
  (the WASI no-network pin becomes "no network except dispatch"). The host answers that origin by
  routing into dispatch as the micro's actor (§2), as `big_nodule/egress.rs` does for nodules.
- Calls are async-only and run only in async contexts: the synchronous mount and dispatch exports
  must not suspend (wcmp's first-poll rule). A call from an event handler is spawned.
- The capability bank stays for micro-to-micro calls (publications, presentations). Node methods do
  not go through it.

**TypeScript micros (operator, 2026-11-17: this must work for TS too).** The transport needs nothing
TS-specific, because it is `wasi:http/client` to `dispatch.local`. The JS engine component exposes it as
`fetch()`, with the same egress rule: any origin other than `dispatch.local` is refused. The typed layer is
where the two languages differ:
- Rust shares the `method_core::Method` types.
- TS gets its request and response types generated from the same Lexicons (`method_lexicon`). So a method's
  Lexicon is the single source for both clients, and a TS client is generated, never written by hand.
  The generator must say what it drops: anything only the JSON Schema sidecar states, such as typed-map
  refinements and storage forms. That is fine for request/response methods, but it must be reported, not lost
  silently (nodules, 2026-11-17; their #139 fixed a tagged-union bug in `method_lexicon` that would have
  reached TS).
- Docs layer 1 (§4) is specified as Lexicon methods plus a thin client, so its TS twin is generated too.

What to check before acceptance: one Lexicon generates both the Rust and the TS client for a test method, and
both make the same call over `dispatch.local`. That's part of the micro-transport lane below.

## 4. Documents: one shared layer, one micro-only layer

**Decided in direction (operator, 2026-11-12): share document access and manipulation between
nodules and micros, but not the partial-sync collection machinery.**

- **Layer 1, shared (nodules and micros):** typed calls over the docs methods (§3): read
  (`readDatums`), list (`listDatums`), write (`writeDatums`, one receipt: accepted or rejected) and
  wait for changes (`pollDatums` long poll). Plus shared typed helpers that encode and decode a
  datum's value (what `Facet<T>` does today). Request/response only; works on a "shallow" host that
  forwards every call. This is what short-lived nodule routines and workflow steps need.
- **Layer 2, micros only:** `clay_docs`'s mirror (`DocumentWatch` push delivery, the optimistic
  `locally-applied → accepted | rolled-back` overlay, tag membership, retention, refresh dedupe) and
  `clay_collection`'s leased partial replication. Its host side gains a node source: a replica fed
  by `pollDatums` long polls per watch selection, and mutations sent as `writeDatums`. Reactivity is
  the host's long poll turned into pushes; guests never poll.
- Answer to the operator's question (can the guest documents API run on a shallow host?): layer 1
  can, by construction. Layer 2's watches, pushes and optimistic overlay need the host-side mirror;
  a nodule that wants them uses layer 1's poll directly.
- **Naming (operator, 2026-11-12): "facet" is retired; Clay says datum.** `clay_docs`'s
  `FacetRoute {document, branch, tag, key_id}` is a doc, a branch and a `DatumKey {tag, slot}`
  under an older name. Clay's types, WIT and manifests are renamed to the datum vocabulary (about
  870 mentions across `clay_*`, `micro_*` and `dayframe_*`), and Clay addresses datums by the
  node's `DatumKey`. The host holds the doc capabilities per binding.

## 5. Micros in cartridges

**Operator (2026-11-12): mostly mechanical.**

- The cartridge manifest's `Contribution::Micro` (`big_cartridges_core`) becomes typed: schema
  `org.example.clay.micro.manifest`, the Clay manifest as its description, the component as the
  `"component"` artifact (as the importer already assumes). A cartridge has at most one micro; its
  micro id is the cartridge id.
- Install state is one datum per cartridge in the node config document (ADR 044 §8):
  `org.example.daybook.cartridges.install/<cartridgeId>` → `{ cartridge: #datumPin, configDoc,
  nodule?: { enabled }, micro?: { enabled } }`, members present iff the envelope contributes that
  component, each enabled independently. One pin means a cartridge's micro and nodule always run
  the same revision.
- Management is the cartridges service's (privileged): `install`, `setEnabled { component }`,
  `uninstall` (keeps config), `setGrant { subsystem, permbind }` (a grant is keyed by the permbind it satisfies, ADR 047 §5), `clearConfig`.
- The host reconciles the `micro` part of every install: `cartridges.listInstalls` and the
  `cartridges.observeInstalls` subscription for live enable/disable, then the pinned envelope's
  typed `MicroContribution` (its `component` artifact, `feat/cart-micros`) to load the component.
  This replaces the sidecar package directory.

## 6. Desktop host shape (decided, operator 2026-11-17)

**Decided (operator, 2026-11-17): most micros are wasm; out-of-process (IPC) micros are the exception.**
- The data above settles it. Micros run as wasm everywhere: in the page on the web and on the Wry desktop (wcmp),
  and under Wasmtime on Blitz/Android, which has neither V8 nor IPC. Wasmtime stays because it is how Blitz runs
  micros.
- Only micros that must reach platform-specific resources run as native (built-in) micros. On the Wry desktop
  those cross IPC.
- **Placement is invisible to guests** (operator, 2026-11-17). A micro doesn't know whether it runs in-process,
  in the page or across IPC: the WIT world, the presentation wire and the `dispatch.local` transport are the same
  everywhere. The distinction is a host choice made today for the Wry desktop; on Blitz it doesn't exist.
- The operator calls this a forcing function: almost every micro has to be written portable, because it is
  written for wasm, and a native micro has to justify its native resource (see "built-in micros" in the
  micro-runtime ADR draft).
- Still to do: decide where each micro is placed. Today's native micros (shell, settings, toolbar, sample, layout,
  in `dayframe_desktop/composition.rs`) touch no native resource, so they all become wasm. Native candidates are
  host services such as file pickers, notifications, clipboard, window management and the camera.

The measurements that led to the decision:

Dioxus desktop runs the Rust UI in the native process and the page in WebKitGTK's web process:
every DOM event and every batch of edits crosses IPC (as in Tauri, whose Wry it uses). Dioxus web
runs the same Rust as wasm inside the page with no IPC; dioxus-native (Blitz, Android) has no IPC
either. Measured 2026-11-12 on the Pierre diff port's 3000-line specimen (blank frame = an
uncovered viewport hole > 12 px; scripts in `/run/media/asdf/p3N/pierre-baseline` and the
showcase web bench):

| Build | Engine | Linear 2000 px/frame | Jump 1/3 → 2/3 |
|---|---|---|---|
| Upstream `@pierre/diffs` | Firefox, WebKit, Chromium | 1 blank frame | 1 blank frame (16–66 ms) |
| Our port, web build (wasm in page) | Firefox | 1 blank frame | 0 blank frames |
| Our port, web build (wasm in page) | WebKit | 0 blank frames | 0 blank frames |
| Our port, desktop build (Dioxus desktop, Wry) | WebKitGTK | gaps of over 1 s (operator, by hand) | gaps of over 1 s |

So the engine and the port are fine; the out-of-process IPC is the cost (a jump fans out into ~200
per-row `onresize` round trips; coalescing them per frame is in progress and helps, but cannot
remove the round trips).

Option: **the desktop daytxt host is a Wry window running the web build of the shell**, talking to
the in-process node through Wry's custom protocol or localhost — exactly as the web host talks to
its node (§1, §3). One UI build for desktop and web; native code only in the node; UI work never
crosses IPC. Cost: window-level conveniences (native menus, window events, file pickers) go
through a small host bridge. Android stays on Blitz (no IPC). This option is what the 2026-11-17 decision above
adopts.

**Update (operator, 2026-11-18): Android moves to the webview (Wry, the Android System WebView) by default.**
Blitz on Android had no touch scrolling and partial CSS, and the intxt review was unusable there. The first port
(`feat/phone-viewer-app`) uses Dioxus mobile: the Rust UI in the app process, the page in the WebView, so it pays
the same per-edit IPC as the Wry desktop. The consistent end state is the desktop option above applied to
Android too: the WebView runs the web build of the shell (wasm in the page, no UI IPC) and talks to the
in-process node through a host bridge. Not decided yet; for the operator. Under either, Wasmtime on Android is no
longer how the shell's micros run once they move into the page (wcmp, as on the web).

## Work, once accepted

Lanes that can run in parallel (files don't collide):
- **Holder crate** (§1): move `day_cli`'s find-or-hold logic into a shared crate; `day` uses it.
- **Micro transport** (§3): `wasi:http/client` in the wasmtime and wcmp Clay runtimes with
  `dispatch.local`-only egress, routed into dispatch as the micro's actor; a micro calls a test
  method end to end.
- **Facet → datum rename** (§4): mechanical, Clay-side only; first, since the other lanes then
  write datum names.
- **Docs layer 1** (§4): shared typed docs helpers over the docs methods (a good Luna lane: an
  interface with tests).
- **Cartridge micros** (§5): typed `MicroContribution`, the config-document install record, a host
  loader from the catalog.
- After those: **host composition** (Daytext desktop and Android boot or attach to the node), then
  **docs layer 2's node source** (the mirror over `pollDatums`), then the intxt micro on real
  datums.

Dependency: the node lane's newer work (method routes, `day carts`, ADRs 037 and 042–045) is on
`feat/big-node-next`, not yet on main. Lanes touching cartridges or dispatch build on that branch.

## Open questions

None from the first two rounds. The cartridge-level parent actor (§2) waits for the cartridge
consent ADR.

Before acceptance (2026-11-17):
- **Nodules sign-off.** Nodules lane, 2026-11-17: signed. This includes §2 as revised to ADR 047 at their request.
- **Placement list.** Which micros stay native under §6.

Order (operator, 2026-11-17): this ADR is delivered before the TypeScript spike in `micro-runtime.md`, so the API
shape is settled before it is ported to TS.
