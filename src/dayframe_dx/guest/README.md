# Dayframe guest presentation probe

This is the first executable component-boundary experiment, **not a settled micro SDK**.
`../app/wit/presentation.wit` supplies both guest and host bindings. Production dependencies
include the locally patched Dioxus fork's Core, hooks, signals, HTML event types, and actual
interpreter mutation writer, but not the application runtime, Blitz, or Wasmtime. Wasmtime is a
test backend only.

The counter example is a Wasm library. Its exported renderer entry point borrows an explicitly
issued mount point, creates a surface, and returns ownership of that surface to the host.
The host retains the placement. Event delivery borrows the surface back into the guest, where
element identity and event name are dispatched through Dioxus's runtime. The guest callback updates
a signal, Dioxus diffs the virtual tree, and its real `MutationState` writer commits the resulting
edit through the imported surface operation; no Rust callback, component, or reactive state crosses WIT.

The in-crate test builds the example using Cargo's configured target directory, wraps it as a
component, and actually instantiates it. The recording backend checks the complete edit trace,
independent guest state in two instances of the same component, and resource-scoped revocation.
One test pins a failure with Wasmtime 47.0.3: after a guest traps, dispatch to its neighbour
sharing the same store fails with `CannotEnterComponent`. A separate test proves that a
neighbour in its own store continues operating after the first guest traps. It is **not** a renderer implementation and
does not claim tree validation, native mounting, automatic incarnation cleanup, or async-loop
integration. Revocation deliberately traps the probe's unrecoverable presentation callback;
closing that failed incarnation belongs to the future runtime adapter.

Source-backed Web IDL/component/browser notes and their scoped implications are in
[WEB_PLATFORM.md](WEB_PLATFORM.md). These are working notes, not full web-compatibility claims.

## Run

Requires the `wasm32-unknown-unknown` Rust standard library and `wasm-tools` on PATH.
No prebuilt guest artifact or custom Cargo target directory is required.

```sh
cargo clippy -p dayframe_guest --all-targets --all-features
cargo nextest run -p dayframe_guest
```

The example's component exports are compiled only for Wasm: their WIT-qualified names are not
valid native shared-library export names. Native tests execute the actual Wasm component,
not a native substitute for its guest code.

## Boundary findings so far

- An initial mount operation is necessary. Mutation and event interfaces alone leave no way
  for the guest renderer to receive its explicitly granted placement and build its first tree.
- Mounting borrows the placement, while returning an owned surface. Consuming the placement
  merely to initialize a renderer would conflate placement lifetime with an entry-point call.
- Synchronous callbacks can call imported presentation operations and return default-action
  decisions. Data waiting does not need to force presentation callbacks into an async queue.
- Surface-local node identities need no global application lookup. Each surface initializes
  Dioxus's interpreter with its granted root as node 0 and the initial stack entry.
- Edits use Dioxus's actual binary encoding, not a second Dayframe mutation language. The Wasm
  probe uses the lower-level interpreter encoder; native tests compare the entire stream against
  the upstream `MutationState` / `WriteMutations` adapter. Opcode padding is explicitly tested.
- The Dioxus fork now makes Subsecond's browser patch loader an explicit `web` feature. The
  guest builds Core, hooks, signals, HTML event conversion, and the real virtual DOM for
  `wasm32-unknown-unknown` without importing `wasm-bindgen`; normal Dioxus web builds retain the
  browser loader through the `dioxus/web` feature.
- The event envelope follows Dioxus's element/name/bubbling convention, with its serialized HTML
  payload unchanged. The component installs `SerializedHtmlEventConverter`, dispatches through
  Dioxus's runtime, and renders signal-driven updates. Native tests compare the complete actual
  component stream with a separately driven Dioxus virtual DOM and still exercise revocation.
- The local Sledgehammer macro patch is documented in `vendor/sledgehammer-bindgen-macro/PATCH.md`.
  It enables non-web Wasm encoding and initializes exported opcode padding without changing
  the operation format. Host and guest encoding versions must match the pinned Dioxus revision.

The first batch receipt describes a committed **logical tree**, not completed layout or paint.
The native bridge must not hold a renderer/document borrow across guest dispatch: a callback
can immediately call the host again to commit edits. That integration remains unproven here.
