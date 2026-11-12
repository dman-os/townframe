# Clay on the node (claytxt lane, ADR draft)

Status: draft, 2026-11-12. The operator answered both rounds on 2026-11-12 (recorded per section); remaining work is review of the whole.
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
- Durable access is grants on the micro's actor, recorded in the cartridge's configuration document
  (ADR 044) beside its nodule's, and re-minted into live, holder-bound capabilities when the host
  binds the micro (ADR 037 §5). A micro with no grants can call nothing.
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
- Install state for micros lives in the cartridge configuration document (ADR 044), as nodules'
  does, with the micro's grants (§2).
- The host lists installed cartridges (`listCartridges`, `getCartridgeVersion`, `watchCartridges`
  for changes), takes each one's micro contribution and component, and loads it. This replaces the
  sidecar package directory.

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
