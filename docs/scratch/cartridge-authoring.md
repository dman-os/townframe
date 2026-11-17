# Cartridge authoring: micros, nodules, and one typed-call model

Status: Draft
Owner: Townframe maintainers
Updated: 2026-11-06
Scope: How an author writes a cartridge with micros, nodules, or both. Drafted by the node session (townframe-p3n-2) with the Clay session (townframe-p3n-4), for operator review before an ADR. It builds on the Clay X3 authoring study (`/run/media/asdf/p3N/tmp/x3/authoring.md`) and lane T1 (typed declarations, branch `feat/dayframe-typed-decls`).

## Where things stand

- **Cartridge manifests.** `big_cartridges_core::Cartridge` holds `Contribution::Nodule` and `Contribution::Micro`, so one cartridge can ship either or both. `MicroContribution` is an opaque description today, not clay_mdk's typed `MicroManifest`.
- **Micros** are authored against `clay_mdk`: a context object, `export_micro!`, native and guest backends. They reach documents only through Clay's own WIT (`townframe:dayframe/documents`, facet-based), with no network egress. Lane T1 is replacing the stringly constants and loose-const manifest crates with typed declarations: each declared type carries its NSID, `ctx.call::<Op>()`, and schemas come from the datum macro.
- **Nodules** have no authoring kit. The one nodule is `src/test_nodule_guest`, a raw `wasi:http/handler@0.3.0` component. It parses the capability, idempotency and deadline headers by hand and calls other operations through `http://dispatch.local/xrpc/<nsid>`. Its manifest is built in test code (`big_node/e2e/nodule_wasm.rs::cartridge`).
- **Typed service calls already exist** on the native side:
  - `service_core_rs::Operation` makes the request type the contract (`type Output`, `type Error`, `const XRPC` with the NSID). Each protocol declares its operations once, e.g. `big_cabinet_core/doc.rs` `operations!`.
  - Callers use `service_client_core_rs::Client<T>::call(ctx, op)` over the `direct`, `irpc` or `xrpc` (including UDS) transports of `service_client_native_rs`.
  - Servers implement `service_utils_rs::Handler<Op>`.
- **The old daybook cartridges** (`cartridge_<x>_manifest` + `cartridge_<x>_routines`) used a native manifest crate plus a wflow-bundle wasm crate with bespoke WIT. They are superseded by this design.

## Principles and proposals

Items marked **(proposal)** await the operator's decision; the Clay session has raised them too.

1. **Every name is a Rust type** carrying its NSID, declared once by a macro, never a string constant. This is T1's rule, applied to nodule operations too.
2. **One typed-call model on both sides (decided, operator 2026-11-06).** A call is an `Operation` request type. Serving it means implementing `Handler<Op>`; calling it means `ctx.call(op)` or a typed `Client<Proto>`. Micro calls align on `Operation` (X3's recommendation).
3. **No ambient authority in guests.** Every outgoing call carries the guest's own capability through a host-mediated path:
   - for nodules, wasi:http to `dispatch.local`, where dispatch admits the nodule's capability;
   - for micros, **(proposal)** a Clay host import carrying the micro's capability. Micros never get raw wasi:http.
4. **One cartridge manifest, generated from typed declarations.** There is no hand-written JSON, and the manifest carries its schemas (`Declaration::of` / `Universe::collect`).
5. **Authors write the same code in native and guest form.** The kits hide WIT, streams, headers and envelopes.
6. **Native backends are for tests and native resources, never a privileged path** (operator, 2026-11-06). Built-in micros must not bypass composition boundaries, and anything a built-in can do, a runtime-loaded package must be able to do. The same holds for the NDK's native backend: it exists to test nodule logic without wasm, never as a production shortcut.
7. **Authority is per contract, not per guest.** A micro's grants are bound per requirement (capability bank and lazy capabilities, ADR 029/031). Collection read authority per micro will become query-scoped. The host import resolves the grant for the specific operation's contract, not one guest-wide capability. Nodules should follow the same rule: a nodule's outgoing call presents the grant for that operation, not a single nodule-wide capability.

## Crate layout per cartridge

```text
cartridge_<x>/            (or flat: cartridge_<x>_decls, cartridge_<x>_nodule, cartridge_<x>_micro)
  decls     native + wasm-buildable: typed declarations (datums, operations, micro contracts),
            the cartridge manifest builder. Depends on datum_core, not big_cabinet.
  nodule    wasm32-wasip3 cdylib on the NDK          (omit for a micro-only cartridge)
  micro     clay micro on the MDK                    (omit for a nodule-only cartridge)
```

The declarations crate replaces both `cartridge_<x>_manifest` and `micro_<x>_manifest`. Guests depend on it for types only; the manifest builder sits behind a host-only `schemas` feature, as X3 §3.5 puts it.

```rust
// cartridge_tasks_decls
clay_mdk::package!(Tasks { id: "org.example.tasks", version: 1, authority: AUTHORITY });

// Decls crates must build for wasm, so the datum macro has to target `datum_core`
// (`#[macros::datum(runtime = ::datum_core, ..)]`); its default path, `::big_cabinet_core::datum`,
// is not wasm-buildable. The kits' own declaration macros (`ndk::operation`, `clay_mdk::package!`, ...)
// set that path, so authors never write it.
#[clay_mdk::datum(authority = AUTHORITY)] pub struct Task { pub title: String, pub done: bool, /* extra */ }

// A nodule operation: the request type is the contract.
#[ndk::operation(authority = AUTHORITY, name = "complete", output = CompleteReceipt, error = TasksError)]
pub struct Complete { pub doc_id: DocId }

pub fn manifest() -> big_cartridges_core::Cartridge {
    CartridgeBuilder::<Tasks>::new()
        .nodule(|n| n.implements::<Complete>().requires::<docs::WriteDatums>())   // nodule contribution
        .micro(|m| m                                                                // micro contribution (T1 builder)
            .declares::<TaskRows>()                              // a collection it owns: key, value and selector schemas
            .requires::<settings::Sections>(Access::Write)       // another micro's contract, with access (read, write, call)
            .contributes::<settings::Sections>("tasks", Section { /* row with attachments to its own endpoints */ })
            .serves::<Complete>()                                // calls it serves
            .publishes::<TaskListView>()                         // PLACEHOLDER: becomes the element-contract type (presentation redesign)
            .config::<TasksConfig>())
        .datum::<Task>()
        .build()   // validates; carries Lexicons for every declared type
}
```

## The NDK (nodule development kit)

The NDK copies the MDK's shape without its legacy parts:

```rust
// cartridge_tasks_nodule
use cartridge_tasks_decls::{Complete, CompleteReceipt, TasksError};
use ndk::prelude::*;

struct Tasks;

impl Handler<Complete> for Tasks {
    async fn call(&self, cx: &Cx, req: Complete) -> Result<CompleteReceipt, TasksError> {
        // Typed client over the nodule's own capability. The kit forwards the caller's
        // deadline and idempotency key automatically.
        let receipt = cx.docs().call(WriteDatums { capability: cx.grant()?, doc_id: req.doc_id, .. }).await?;
        Ok(CompleteReceipt { heads: receipt.heads })
    }
}

ndk::export_nodule!(Tasks => [Complete]);
```

- `export_nodule!` generates the `wasi:http/handler` export. It routes `/xrpc/<nsid>` to the right `Handler<Op>`, decodes the input against `Op` and encodes the output and error envelope (the same wire as the host's XRPC router). It also builds `Cx` from the request headers: the caller's capability, the nodule's own capability, the deadline and the idempotency key.
- `Cx` exposes typed clients (`cx.docs()`, `cx.drawers()`, `cx.blobs()`, `cx.call::<Op>()` for another cartridge's operation). They are the existing `service_client_core_rs::Client<T>` with a new **wasi:http transport** (an `HttpTransport` impl wrapped in `Xrpc`) to `dispatch.local`, authenticated with the nodule's capability. Guest code reads the same as native code.
- A native backend serves the same `Handler<Op>` impls in-process for tests: a nodule's logic can be unit-tested without wasm, as micros are tested on the native MDK backend.
- Dispatch validates input and output against the operation's Lexicon (D31), so the NDK's typing is checked at the boundary too.

A micro contribution is more than a view. It declares the collections it owns, requires other micros' contracts with an access level, contributes rows (with attachments, lazy endpoints to its own publications or calls) to other micros' collections, serves calls, publishes presentation, and takes config. `publishes::<View>()` above is a placeholder: the operator is redesigning the presentation boundary into per-mount instances with a web-component-shaped element contract (typed properties, events, slots and methods), and `View` will be that contract type.

## Shared service surface for micros (proposal)

The same `Client<Proto>` calls should work in micro code, with the transport swapped for a Clay host import that resolves the grant for the call's contract (principle 7): `ctx.docs().call(ReadDatums { .. })` is then identical in a micro and a nodule. Whether micros keep the facet-based documents WIT, or move it onto these calls plus a reactive layer, is a Clay decision. Two micro-specific constraints apply:

- **Calls are async-only, and only in async contexts.** A micro's presentation exports (mount, dispatch) are synchronous and must never suspend; a synchronous export cannot await an async import, and the host faults a guest that tries (Clay lane E2). Typed calls therefore run only in activation, spawned tasks, and event handlers that spawn. `ctx.docs().call(..)` is async by type, and nothing offers a blocking form.
- **Reactivity is subscription, not polling.** Micros render from push-based change streams (document events, collection mirrors). Request/response calls serve reads and writes; the reactive layer sits on top as a subscription over typed operations (a streaming operation or the existing event exports), never as a polling loop.

## Operator direction (2026-11-06)

- **One typed-call model.** The generated typed clients (`service_client_core_rs::Client<T>` over `Operation` types) are the model. They already run in-process (`direct`), over irpc channels (`irpc`) and over XRPC (`xrpc`, TCP or UDS). Micros and nodules each gain a host-mediated *backend* for the same clients.
  - **Estimated lift:**
    1. **Done:** `service_client_core_rs` (wasm-buildable: `Client<T>`, the `UnaryTransport`/`SubscriptionTransport` traits, the XRPC envelope codec and the byte-moving `HttpTransport`) split from `service_client_native_rs` (direct, irpc, hyper XRPC, iroh). A crate split, not feature flags.
    2. The nodule backend: XRPC over `wasi:http/client` to `dispatch.local`.
    3. The micro backend: a small host-import WIT, `call(nsid, metadata, input) -> result`, which the Clay host routes into dispatch with the grant it resolves per contract.
- **Permissioning is isomorphic.** A micro requirement (contract plus access) and a nodule `requires` (operation) are the same thing: a declared need for a contract, bound to a grant at install, and resolved per contract on use. The manifest uses one `requires::<Op>()` vocabulary for both.
- **Language-neutral by construction.** Many micros and nodules will be written in TypeScript. The contract is XRPC plus Lexicons; TypeScript types and clients are generated from the same Lexicons (as atproto's lexicon codegen does). The host-mediated backend is a WIT interface (componentize-js / jco), not something Rust-only. The Rust client is one binding of the contract.
- **Why a separate declarations crate (Rust).** It holds pure types and the manifest generator, can be published, and is consumable by other packages for their types. Real code never sits behind feature flags in the crate others depend on: a crate producing several artifacts is the pain the guardrails build matrix shows. A package shares code between its micro and its nodule through the declarations crate (pure helpers) or through its own extra crates; the convention is written down. A TypeScript package can be a single package with several entry points. A static JSON/YAML manifest stays possible for packages without a Rust declarations crate.
- **Testing is part of authoring.** The old `cartridge_test_manifest/e2e/` ran a real package's workflows end to end. Its successor is a real test package (a nodule and a micro exercising every host capability) with e2e tests inside the package, on the native test backend and the wasm build. `test_nodule_guest` shrinks to a low-level dispatch fixture.
- **Workflows** follow ledger D3, already decided: durable work goes through a wflow **service** (start, status long-poll, message, cancel operations), not a bespoke routine kind or a third contribution kind. A durable procedure returns a job reference, the journal is the storage, and components stay stateless. A nodule calls the wflow service like any other service, through the same typed clients. wflow support for nodules waits for the wflow rework PR. (The old `daybook_pdk` was only command URLs plus `invoke_command_effect`; the real kit was `wflow_sdk` / `route_wflows!`.)
- **Naming (decided, D34): cartridge.** "Plug" reads as a plug-in, a slight enhancement of the app, while many micros are big, self-contained apps. A **cartridge** ("cart") is the single user-facing noun for the store item and the distribution unit; it bundles micros, nodules and workflows. Micros and nodules stay user-facing as the classes of thing a cartridge contains. This doc still says "plug" until the code rename lands.

## Agreements with the Clay session (2026-11-06)

- **No typed `Context<M>`.** The operator rejected type lists for micros. Declaration types carry consts (`Type::NSID`), and APIs are generic over those types where needed (`ctx.call::<Op>()`, `ctx.collection::<C>()`).
- **`#[clay_mdk::call]` produces an `Operation`**, not a parallel call trait.
- **`Operation` must carry capability refs.** Micro call and collection values can carry capability handles that the receiver redeems (ADR 029; `GuestValue { body, capabilities }`). The envelope therefore needs an out-of-band capability-ref list beside the body bytes, the way WIT resources travel beside data, and Lexicon needs a "capability ref" leaf. Attachments (lazy endpoints) occur only in collection rows; they are a separate leaf, and the CBOR tag-43000 attachment has no Lexicon form yet.
- **Collections stay a separate capability kind** (stateful streams: bootstrap and paging, cursors, resume after reset, revocation, owner-scoped compare-and-set registration). The grant model is still uniform: per contract, bound at install, one `requires::<T>(access)` where `T` is an `Operation` or a collection contract.
- **Ownership:**
  - (a) the node session creates its own small crate for `Operation` and the envelope codec (working name `operation_core`), built for native, wasm32-unknown-unknown and wasm32-wasip3, depending on `datum_core` for NSIDs and shapes, after the datum_core PR merges. It is not a re-export from `datum_core`: that crate stays data shapes and NSIDs, and the call model grows separately;
  - (b) the micro host-import WIT is deferred with the rest of micro access to node services. When it is picked up: async-only, never callable from mount or dispatch, capability refs out-of-band, per-contract grant resolved by the host, placed in the micro-client world. The WIT is the real authoring contract (TypeScript is first-class); the Rust macros are sugar over it;
  - (c) `requires`: the Clay session owns the micro side and the node session the nodule side; the signature is agreed here first;
  - (d) `CartridgeBuilder` in big_cartridges_core over the typed declarations: the node session.
- **Timing.** T1 verifies the datum_core PR, then the clay_mdk declarations (package, collection, config, component), leaving `call` until the `Operation` crate exists.

## Settled elsewhere, and operator answers (2026-11-06)

- **Dependencies between cartridges: ADR 028 §4.** Manifests request NSIDs, not providers, in the shared `{binding, contract, access, necessity, resolution, minRevision?}` shape. A cartridge carries copies of the Lexicons it depends on, so an incompatible view is caught when it is catalogued. Resolution (NSID -> provider, version, schema revision) is recorded at enable time like a lockfile, and a change of provider is re-consented. Cartridge-to-cartridge dependencies are only a delivery hint.
- **Evolution: Lexicon rules, ecosystem-wide.** Within an NSID a schema only grows compatibly; a breaking change takes a new NSID (ADR 028 §4). This is a global soup of cartridges by independent authors, and every user manages their own dependencies, so the ecosystem is pushed to be conservative about breaking changes. Because every call is dynamically dispatched and host-mediated, the host can still bridge breaking changes, e.g. with Cambria-style schema lenses that let a provider keep serving an old NSID from the new one.
- **Processors** (event-triggered work, "run once per change") belong to big_tasks, a separate large PR with its own decisions (ledger D5). They are not part of nodule authoring here.
- **The developer loop.** Authors include non-programmers, so agentic, "vibe-coded" cartridge development is the target, and Rust is the wrong first language for it. A friendlier authoring language, starting with TypeScript, gets prototyped soon. CLI tooling follows once that story is clear.
- **Open: cartridge-scoped configuration.** Micros and nodules each have bespoke config today. Some of it is probably cartridge-scoped: permissions, enablement, scopes. Which parts is undecided.
- **Next design topic: consent, permissions and actors, aligned per cartridge.** Requests from micros and nodules, the grants a user consents to, and the cartridge's actor identities (ADR 022) should form one model at cartridge level.
- **Naming (decided, operator 2026-11-06):** *commands* is the user-facing term; developers use XRPC naming: *methods*, of kind query, procedure or subscription. The `Operation` trait becomes `Method` (with `Query` and `Procedure`) when the new crate is created after #82, so micro calls build on `Method` from the start.

## Sequencing

1. The Clay session lands the `datum_core` extraction (the pure move, re-exports from big_cabinet_core, and the macro's `runtime =` path) as its own small PR off main. The node session then rebases its datum and macros commits onto it (D32 `f7f1f89c`, `F64` `9e6ec666`, `DmetaRead` `87c25501`, `DatumEvent` `6d7c60f6`), relying on rename detection. The node session holds off further big_cabinet_core datum and macros edits until then.
2. NDK v0: `Operation` declarations for nodules, `export_nodule!`, `Cx`, the wasi:http client transport, a native test backend, and `test_nodule_guest` ported to it as the first consumer.
3. The cartridge manifest builder over typed declarations, plus `day cartridge build | publish | install | enable` porcelain.
4. Micro access to node services is **deferred** (operator, 2026-11-06): once feat/big-node merges, the Clay and node sessions put Clay on top of the node, and the micro backend is designed then, as a client of the local node rather than a parallel path. T1's typed declarations proceed independently.

## Open questions for the operator

1. The declarations crate name: `cartridge_<x>_decls`, `cartridge_<x>` with features, or something else?
2. Should micro calls move onto `Operation` and `Client<Proto>` (one model), or keep the capability-bank CBOR calls with a typed wrapper?
3. NDK crate name: `ndk`, `nodule_ndk`, `clay_ndk`?
4. Does a nodule ever need a reactive or long-poll API beyond `PollDatums` through the client, e.g. a subscription sugar like the MDK's hooks?
