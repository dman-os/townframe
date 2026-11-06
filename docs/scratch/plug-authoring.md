# Plug authoring: micros, nodules, and one typed-call model

Status: Draft
Owner: Townframe maintainers
Updated: 2026-11-06
Scope: How an author writes a plug with micros, nodules, or both. Drafted by the node session (townframe-p3n-2) with the Clay session (townframe-p3n-4), for operator review before an ADR. It builds on the Clay X3 authoring study (`/run/media/asdf/p3N/tmp/x3/authoring.md`) and lane T1 (typed declarations, branch `feat/dayframe-typed-decls`).

## Where things stand

- **Plug manifests.** `big_plugs_core::Plug` holds `Contribution::Nodule` and `Contribution::Micro`, so one plug can ship either or both. `MicroContribution` is an opaque description today, not clay_mdk's typed `MicroManifest`.
- **Micros** are authored against `clay_mdk`: a context object, `export_micro!`, native and guest backends. They reach documents only through Clay's own WIT (`townframe:dayframe/documents`, facet-based), with no network egress. Lane T1 is replacing the stringly constants and loose-const manifest crates with typed declarations: each declared type carries its NSID, `ctx.call::<Op>()`, and schemas come from the datum macro.
- **Nodules** have no authoring kit. The one nodule is `src/test_nodule_guest`, a raw `wasi:http/handler@0.3.0` component. It parses the capability, idempotency and deadline headers by hand and calls other operations through `http://dispatch.local/xrpc/<nsid>`. Its manifest is built in test code (`big_node/e2e/nodule_wasm.rs::plug`).
- **Typed service calls already exist** on the native side:
  - `service_core_rs::Operation` makes the request type the contract (`type Output`, `type Error`, `const XRPC` with the NSID). Each protocol declares its operations once, e.g. `big_cabinet_core/doc.rs` `operations!`.
  - Callers use `service_client_rs::Client<Proto>::call(ctx, op)` over `direct`, `irpc` or `xrpc` (including UDS) transports.
  - Servers implement `service_utils_rs::Handler<Op>`.
- **The old daybook plugs** (`plug_<x>_manifest` + `plug_<x>_routines`) used a native manifest crate plus a wflow-bundle wasm crate with bespoke WIT. They are superseded by this design.

## Principles

1. **Every name is a Rust type** carrying its NSID, declared once by a macro, never a string constant. This is T1's rule, applied to nodule operations too.
2. **One typed-call model on both sides.** A call is an `Operation` request type. Serving it means implementing `Handler<Op>`; calling it means `ctx.call(op)` or a typed `Client<Proto>`. Micro calls align on `Operation` (X3's recommendation).
3. **No ambient authority in guests.** Every outgoing call carries the guest's own capability through a host-mediated path:
   - for nodules, wasi:http to `dispatch.local`, where dispatch admits the nodule's capability;
   - for micros, a Clay host import carrying the micro's capability. Micros never get raw wasi:http.
4. **One plug manifest, generated from typed declarations.** There is no hand-written JSON, and the manifest carries its schemas (`Declaration::of` / `Universe::collect`).
5. **Authors write the same code in native and guest form.** The kits hide WIT, streams, headers and envelopes.

## Crate layout per plug

```text
plug_<x>/            (or flat: plug_<x>_decls, plug_<x>_nodule, plug_<x>_micro)
  decls     native + wasm-buildable: typed declarations (datums, operations, micro contracts),
            the plug manifest builder. Depends on datum_core, not big_cabinet.
  nodule    wasm32-wasip3 cdylib on the NDK          (omit for a micro-only plug)
  micro     clay micro on the MDK                    (omit for a nodule-only plug)
```

The declarations crate replaces both `plug_<x>_manifest` and `micro_<x>_manifest`. Guests depend on it for types only; the manifest builder sits behind a host-only `schemas` feature, as X3 §3.5 puts it.

```rust
// plug_tasks_decls
clay_mdk::package!(Tasks { id: "org.example.tasks", version: 1, authority: AUTHORITY });

#[macros::datum(authority = AUTHORITY)] pub struct Task { pub title: String, pub done: bool, /* extra */ }

// A nodule operation: the request type is the contract.
#[ndk::operation(authority = AUTHORITY, name = "complete", output = CompleteReceipt, error = TasksError)]
pub struct Complete { pub doc_id: DocId }

pub fn manifest() -> big_plugs_core::Plug {
    PlugBuilder::<Tasks>::new()
        .nodule(|n| n.implements::<Complete>().requires::<docs::WriteDatums>())   // nodule contribution
        .micro(|m| m.publishes::<TaskListView>().config::<TasksConfig>())          // micro contribution (T1 builder)
        .datum::<Task>()
        .build()   // validates; carries Lexicons for every declared type
}
```

## The NDK (nodule development kit)

The NDK copies the MDK's shape without its legacy parts:

```rust
// plug_tasks_nodule
use plug_tasks_decls::{Complete, CompleteReceipt, TasksError};
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
- `Cx` exposes typed clients (`cx.docs()`, `cx.drawers()`, `cx.blobs()`, `cx.call::<Op>()` for another plug's operation). They are the existing `service_client_rs::Client<Proto>` with a new **wasi:http transport** to `dispatch.local`, authenticated with the nodule's capability. Guest code reads the same as native code.
- A native backend serves the same `Handler<Op>` impls in-process for tests: a nodule's logic can be unit-tested without wasm, as micros are tested on the native MDK backend.
- Dispatch validates input and output against the operation's Lexicon (D31), so the NDK's typing is checked at the boundary too.

## Shared service surface for micros

The same `Client<Proto>` calls should work in micro code, with the transport swapped for a Clay host import that carries the micro's capability: `ctx.docs().call(ReadDatums { .. })` is then identical in a micro and a nodule. The MDK's reactive layer (`Facet`, mirrors, `use_document_changes`, collections) stays micro-specific and sits on top. Whether micros keep the facet-based documents WIT, or move it onto these calls plus a reactive cache, is a Clay decision.

## Sequencing

1. T1 lands typed declarations and `datum_core`, extracted from big_cabinet_core. The node session holds off big_cabinet_core datum and macros edits until then (D32, `F64`, `DmetaRead` and the `DatumEvent` change are on feat/big-node).
2. NDK v0: `Operation` declarations for nodules, `export_nodule!`, `Cx`, the wasi:http client transport, a native test backend, and `test_nodule_guest` ported to it as the first consumer.
3. The plug manifest builder over typed declarations, plus `day plug build | publish | install | enable` porcelain.
4. Micro typed calls over a capability-carrying host import (Clay).

## Open questions for the operator

1. The declarations crate name: `plug_<x>_decls`, `plug_<x>` with features, or something else?
2. Should micro calls move onto `Operation` and `Client<Proto>` (one model), or keep the capability-bank CBOR calls with a typed wrapper?
3. NDK crate name: `ndk`, `nodule_ndk`, `clay_ndk`?
4. Does a nodule ever need a reactive or long-poll API beyond `PollDatums` through the client, e.g. a subscription sugar like the MDK's hooks?
