# clay_mdk — the Micro Development Kit

The single authoring API every Dayframe micro compiles against (ADR 015 §5; collections per
ADR 017). A micro is a **manifest** (`manifest.rs`) plus an **activation** (`activate(&Context)`)
plus, usually, a published component. The MDK adds ergonomics, never authority: capabilities are still the
host-issued resources, enforced by `clay_host`.

## Backends

The same authoring vocabulary has two backends:

- **`native`** (feature `in_process`): in-process micros, linked by the composition root.
  `activate<R: RendererRegistry<Render = Element>>(context: &Context<R>)` — renderer-agnostic;
  the micro constrains only the element type it writes.
- **`guest`** (feature `dioxus`, Wasm): `activate(&Context)` with the same shape over WIT
  imports, plus `export_micro!(activate)` which generates the `Guest` impl and
  `bindings::export!`. See `micro_counter` for the exemplar. Guest hooks that hold per-document
  state (`use_facet`, `use_document_changes`) must live in the root `render` closure — a child
  component's state is per mounted surface, so a facet watch in a child would split state across
  surfaces of one incarnation.

`view` (Dioxus slot/hook glue for host-linked views), `wasmtime` (host-side provider assembly
for guests), and `wit`/`mirror` (document-mirror authoring over the WIT surface) complete the
feature set.

## Manifests and contracts

A micro's compile-time declaration is a `MicroManifest`: id, version, requirements, provisions,
optional config grant, and **contributions** — data (`Contribution { provision, payload:
serde_json::Value }`) routed at activation to the contract it names. Contracts that accept
contributions implement `Contributable`; a payload that violates the contract is rejected with
`AccessError::Denied`, never trusted (payloads may arrive over the wire).

Contract-crate pattern (`micro_<name>_manifest`, following `plug_<name>_manifest`): shared
contract types, the manifest, and provision identity live in a crate any consumer can depend on
without linking the micro itself. Put a micro's manifest in its own `_manifest` crate when
another crate names its contract types or a binary needs the manifest without the micro's code
(the Wasm case: the desktop links `micro_counter_manifest`, never `micro_counter`); a purely
native, self-contained micro may keep `manifest()` inline (see `micro_sample`).

## Adding a micro to the desktop composition

1. Write the crate(s) following `micro_sample` (native) or `micro_counter` +
   `micro_counter_manifest` (guest).
2. Append one entry to `linked_providers` in `dayframe_desktop/composition.rs` — a manifest plus
   its backend (`activate` fn or `component_package("name")`). Contribution targets are declared
   in *your* manifest; you never name another micro's contract from composition code.
3. Add the instance's entry to the configuration maps in `DesktopRuntime::new` (bindings route
   your requirements to providers by instance name).
4. Name the crate `micro_<name>` (and `micro_<name>_manifest`) and it joins the gate
   automatically: `x/check-dayframe.ts` derives its package list from `cargo metadata` by
   prefix. A Wasm guest also gets one entry in the script's `components` table; load the prepared
   artifact with `component_package("<name>")` / `WasmPackage::from_env("<name>")`
   (`DAYFRAME_<NAME>_COMPONENT`).

## Run

Requires the repository Nix development shell and the Dioxus path dependency. Fetch WIT
dependencies in a fresh checkout with `wash wit fetch` in `src/api_utils_rs`. From the
repository root, prepare the guest fixtures and run the scoped tests plus clippy:

```sh
CARGO_BUILD_JOBS=2 nix develop --command deno run --allow-all x/check-dayframe.ts -j 2
```

For a narrower run, use `--prepare-only`, export the printed absolute
`DAYFRAME_*_COMPONENT` paths, then run e.g. `cargo nextest run -p clay_mdk` in the dev shell.
Guest components are compiled only for Wasm: their WIT-qualified export names are not valid
native shared-library export names, so native tests execute the actual Wasm component, never a
native substitute for guest code.

## The wire

`presentation.wit` carries a typed `list<mutation>`. Its variants mirror Dioxus Core's
`WriteMutations` one-to-one (push/set id, child, pop, create element/text, clone, append,
replace-with, insert after/before, set attribute, set text, add/remove listener, remove). This
is a framework-neutral transport of the retained-tree stack machine, chosen so that a React or
Vue adapter can target the same host writer later. It is deliberately **not** Dioxus's internal
Sledgehammer byte stream: that encoding is macro-generated for the JS interpreter, has no Rust
decoder, and changes with the pinned revision.

Attribute values are `text | float | int | boolean | none`. Dioxus's `Any` attribute carries a
process-local Rust object and is excluded; a guest adapter that reaches it is a bug and panics.

Element ids are surface-local. The host writer owns the element-id → real-node mapping, so each
surface starts from its granted root without any global lookup.

## Boundary findings so far

- An initial mount operation is necessary. Mutation and event interfaces alone leave no way for
  the guest renderer to receive its explicitly granted placement and build its first tree.
- Mounting borrows the placement, while returning an owned surface. Consuming the placement
  merely to initialize a renderer would conflate placement lifetime with an entry-point call.
- Synchronous callbacks can call imported presentation operations and return default-action
  decisions. Data waiting does not need to force presentation callbacks into an async queue.
- The Dioxus fork makes Subsecond's browser patch loader an explicit `web` feature. The guest
  builds Core, hooks, signals, HTML event conversion, and the real virtual DOM for
  `wasm32-unknown-unknown` without importing `wasm-bindgen`; normal Dioxus web builds retain the
  browser loader through the `dioxus/web` feature.
- The event envelope follows Dioxus's element/name/bubbling convention, with its serialized
  HTML payload unchanged. The component installs `SerializedHtmlEventConverter`, dispatches
  through Dioxus's runtime, and renders signal-driven updates.
- The fork exposes native-dom's `DioxusState::writer` and `MutationBuffer`, so the native host
  adapter can replay a guest batch into the subtree under a granted Blitz node with the same
  `WriteMutations` semantics upstream uses for its own virtual DOM.

The first batch receipt describes a committed **logical tree**, not completed layout or paint.
The native bridge must not hold a renderer/document borrow across guest dispatch: a callback can
immediately call the host again to commit edits. Direct Blitz ABI tests and the desktop's
composed DOM harness exercise those distinct rendering paths; neither substitutes for native
window, layout, or accessibility verification.

Source-backed Web IDL/component/browser notes and their scoped implications are in
[WEB_PLATFORM.md](WEB_PLATFORM.md). These are working notes, not full web-compatibility claims.
