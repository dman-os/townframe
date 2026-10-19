# Dayframe guest presentation probe

This is the first executable component-boundary experiment, **not a settled micro SDK**.
`../presentation/wit/{presentation,documents}.wit` supplies both guest and host bindings. Production dependencies
include the Dioxus fork's Core, hooks, signals and HTML event types, but not the application
runtime, Blitz, or Wasmtime. Wasmtime is a test backend only.

The counter example is a Wasm library. Activation receives named, already-approved document grants.
Its exported renderer entry point borrows an explicitly issued mount point, creates a surface,
retains a guest handle, and returns another owned handle to the host. A surface's stable identity
selects a render target in the incarnation's single VDOM; unmount removes that target without
resetting incarnation state. Event delivery borrows the surface back into the guest, where
element identity and event name are dispatched through Dioxus's runtime. The guest callback updates
a signal, Dioxus diffs the virtual tree, and the `wire::WireMutations` writer commits the resulting
edit through the imported surface operation; no Rust callback, component, or reactive state crosses WIT.

The explicit `x/check-dayframe.ts` preparation step builds the example using Cargo's configured
target directory and wraps it as a component. In-crate tests load that artifact and instantiate it;
they never invoke Cargo or `wasm-tools` themselves. The host replays every received batch through a Rust
`WriteMutations` — the same path the Blitz-backed native-dom writer takes — and compares the
result against a natively driven Dioxus virtual DOM rendering the same counter. That proves the
wire preserves Dioxus's edit stream exactly across the address space. The tests also check
independent guest state in two instances of the same component, and resource-scoped revocation.
One test pins a failure with Wasmtime 47.0.3: after a guest traps, dispatch to its neighbour
sharing the same store fails with `CannotEnterComponent`. A separate test proves that a
neighbour in its own store continues operating after the first guest traps. It is **not** a renderer
implementation. The composed host path lives in `dayframe_app::native`, with validation in
`dayframe_presentation` and incarnation cleanup in `dayframe_engine_wasmtime`. A revoked target is
removed by the SDK without invalidating sibling targets. Actual traps discard the engine store
and close that incarnation. Document watches and receipts are delivered by an incarnation-owned
future, independently of whether any surface is mounted.

Source-backed Web IDL/component/browser notes and their scoped implications are in
[WEB_PLATFORM.md](WEB_PLATFORM.md). These are working notes, not full web-compatibility claims.

## Run

Requires the repository Nix development shell and the existing Dioxus path dependency.
Fetch WIT dependencies with `nix develop --command deno run --allow-all x/wit-fetch.ts` in a fresh
checkout. From the repository root, prepare the fixture and run the scoped tests plus clippy:

```sh
CARGO_BUILD_JOBS=2 nix develop --command deno run --allow-all x/check-dayframe.ts --test-threads 2
```

For a narrower run, use `--prepare-only`, export the printed absolute
`DAYFRAME_COUNTER_COMPONENT` path, then run `cargo nextest run -p dayframe_guest` in the dev shell.
Desktop startup uses the same prepared artifact. No custom Cargo target directory is needed.

The example's component exports are compiled only for Wasm: their WIT-qualified names are not
valid native shared-library export names. Native tests execute the actual Wasm component,
not a native substitute for its guest code.

## The wire

`presentation.wit` carries a typed `list<mutation>`. Its variants mirror Dioxus Core's
`WriteMutations` one-to-one (push/set id, child, pop, create element/text, clone, append,
replace-with, insert after/before, set attribute, set text, add/remove listener, remove). This is
a framework-neutral transport of the retained-tree stack machine, chosen so that a React or Vue
adapter can target the same host writer later. It is deliberately **not** Dioxus's internal
Sledgehammer byte stream: that encoding is macro-generated for the JS interpreter, has no Rust
decoder, and changes with the pinned revision.

Attribute values are `text | float | int | boolean | none`. Dioxus's `Any` attribute carries a
process-local Rust object and is excluded; a guest adapter that reaches it is a bug and panics.

Element ids are surface-local. The host writer owns the element-id → real-node mapping, so
each surface starts from its granted root without any global lookup.

## Boundary findings so far

- An initial mount operation is necessary. Mutation and event interfaces alone leave no way
  for the guest renderer to receive its explicitly granted placement and build its first tree.
- Mounting borrows the placement, while returning an owned surface. Consuming the placement
  merely to initialize a renderer would conflate placement lifetime with an entry-point call.
- Synchronous callbacks can call imported presentation operations and return default-action
  decisions. Data waiting does not need to force presentation callbacks into an async queue.
- The Dioxus fork makes Subsecond's browser patch loader an explicit `web` feature. The guest
  builds Core, hooks, signals, HTML event conversion, and the real virtual DOM for
  `wasm32-unknown-unknown` without importing `wasm-bindgen`; normal Dioxus web builds retain the
  browser loader through the `dioxus/web` feature.
- The event envelope follows Dioxus's element/name/bubbling convention, with its serialized HTML
  payload unchanged. The component installs `SerializedHtmlEventConverter`, dispatches through
  Dioxus's runtime, and renders signal-driven updates.
- The fork exposes native-dom's `DioxusState::writer` and `MutationBuffer`, so the native host
  adapter can replay a guest batch into the subtree under a granted Blitz node with the same
  `WriteMutations` semantics upstream uses for its own virtual DOM.

The first batch receipt describes a committed **logical tree**, not completed layout or paint.
The native bridge must not hold a renderer/document borrow across guest dispatch: a callback
can immediately call the host again to commit edits. Direct Blitz ABI tests and the desktop's
composed DOM harness exercise those distinct rendering paths; neither substitutes for native
window, layout, or accessibility verification.
