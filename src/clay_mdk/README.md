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
for guests), `wcmp` (the browser-engine twin of that provider assembly, ADR 018), and
`wit`/`mirror` (document-mirror authoring over the WIT surface) complete the feature set.

## Manifests and contracts

A micro's static declaration is a `MicroManifest` (ADR 024 §§11–15): schema, id, package
version, entrypoints, `provides` / `requires` (contract refs keyed by stable ids and local
binding names), configuration requests, collection contracts, **contributions** — data
(`ContributionDeclaration { target_binding, extension_contract, payload, .. }`) aimed at one of
the micro's own requirement bindings — and activation defaults (registration templates with a
`target-live` or `explicit-root` trigger). It describes possibilities, never configured identity
or live state, and carries unknown members so newer manifests round-trip through older tooling.
`MicroManifest::validate` checks what serde cannot — the schema this build reads, unique ids and
bindings, contributions aimed at declared requirement bindings, and activation defaults naming a
declared contribution through its own binding and contract — and `Package::from_bytes` rejects a
package whose manifest fails it.
Contracts that accept contributions implement `Contributable`; a payload that violates the
contract is rejected with `AccessError::Denied`, never trusted (payloads may arrive over the
wire).

Contract-crate pattern (`micro_<name>_manifest`, following `plug_<name>_manifest`): shared
contract types, the manifest, and provision identity live in a crate any consumer can depend on
without linking the micro itself. Put a micro's manifest in its own `_manifest` crate when
another crate names its contract types or a binary needs the manifest without the micro's code
(the Wasm case: the desktop links `micro_counter_manifest`, never `micro_counter`); a purely
native, self-contained micro may keep `manifest()` inline (see `micro_sample`).

## Adding a micro to the desktop composition

1. Write the crate(s) following `micro_sample` (native) or `micro_counter` +
   `micro_counter_manifest` (guest).
2. Append one entry to the shell's guest providers — `dayframe_desktop/composition.rs`
   (`guest_providers`) for Wasmtime, `dayframe_web/main.rs` (`guest_providers`) for `wcmp` — a
   manifest plus its backend (`activate` fn, `component_package("name")`, or the `wcmp`
   package factory). Contribution targets are declared in *your* manifest; you never name
   another micro's contract from composition code.
3. Add the instance's entry to the configuration maps in
   `dayframe_composition::CompositionRuntime::new` (bindings route your requirements to
   providers by instance name).
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

## Writing a guest: concurrency rules

A guest is one wasm instance. Every host call into it is a component-model task: `call` is an
async export, one task per call, and many calls can be suspended in the guest at once while the
host also drives the synchronous `mount`/`dispatch`/`unmount` exports between them. The MDK's
`task_hook` (`lib.rs`) keeps that sound: thread-locals are shared by all tasks, and wit-bindgen's
per-task state is swapped on every suspend and resume. Authors owe these rules in return:

- **No `RefCell` borrow, signal read/write guard or Dioxus guard across an `.await`.** Another
  task of the same instance runs while you are suspended, on the same shared thread-locals, and
  meets your live borrow with a panic. Clone out what you need (an `Rc`, a value) first.
- **After an `.await` resumes, Dioxus global APIs only inside `dom.in_runtime`.** The Dioxus
  runtime stack is a shared thread-local that other tasks push and pop; your task cannot assume
  its runtime is current after it was suspended.
- **A panic kills the instance.** The guest is built `panic=abort`, so a panic is a wasm trap and
  traps are not per task: the host fails every call outstanding on the incarnation (callers see
  `Revoked`) and closes it. Return `Err` for anything a caller can cause.
- **Spawned work lives as long as the component-model task that spawned it.**
  `wit_bindgen::spawn_local` futures are owned and polled by the export task that called it
  (`initialize`, `call`, ...), not by the incarnation. That task stays alive until its spawned
  work finishes, so work spawned in `initialize` runs on after it returned and is how a micro
  keeps background work; but it dies with the store, and a spawn in a `call` handler is
  attributed to that call's task, not to the instance. Put long-lived work in `initialize`.
- **Cancelling a call does not cancel the handler.** The host cannot cancel a guest task yet:
  if the caller gives up, the handler still runs to completion and its answer is discarded, so
  handlers must be safe to finish unobserved.

wit-bindgen must stay `default-features = false` (no `std`) in every wasip3 guest crate, or
this task state falls back to a libc thread-local and a second concurrent call aborts the
instance. `x/check-dayframe.ts` fails when any guest closure resolves `std`, and the first async
export that suspends without using the MDK's task-context symbols panics with that explanation.

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
