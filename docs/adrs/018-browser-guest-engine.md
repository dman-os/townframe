# ADR 018: The browser guest engine — splitting the engine seam for `wcmp`

**Status:** Proposed. Gated on the feasibility spike in §Migration step 1 and on the
`wasm-bindgen` pin conflict (§6). Nothing here is implemented; `dayframe_web` is still a
static shell that composes no micros.
**Amends:** ADR 015 §1 (Shape W), §8 (the future browser engine), and §Deferred
(`clay_engine_jco`). Executes the crate-map trigger D-c ("leave `clay_guest_session` alone until a
second engine needs the shared contract").
**Evidence:** the 2026-10-21 investigation of `wcmp` 0.1.1
(<https://github.com/dialog-db/wasm-component-model-polyfill>) against our host stack. Findings
marked *unverified* were read from documentation, not code. The Dioxus-side prerequisite is
published in `dman-os/dioxus` as branch `feat/dayframe-native-components` at
`a3e1e68334b7dfa68cead38e9e8795ec7a6b534a`; Townframe pins that commit in `Cargo.toml` and
`Cargo.lock`. It contains only the three Dayframe prerequisite commits on top of Dioxus
`origin/main`: non-browser Wasm component support, the direct native mutation-writer/state API,
and pointer-event constructors/re-exports.

## Context

ADR 015 Shape W puts the whole host (VDOM, composition, native micros) into one wasm32 module in
the JS realm and instantiates guests as components in the same realm, called synchronously. It
named "jco or equivalent" and deferred the engine. The investigation established:

- **The host stack already builds for the browser.** Every Clay and micro crate except
  `clay_rt_wasmtime` passes Clippy for `wasm32-unknown-unknown`, including `clay_mdk`'s `native`
  and `view` features and `dayframe_web`.
- **Guest surfaces should render on the web unchanged.** `clay_dioxus` projects a guest surface
  through ordinary Dioxus `rsx`, not the Blitz writer, so `dioxus-web` can draw it; the event and
  surface-factory closures are not required to be thread-safe.
- **`wcmp` fits our WIT.** It runs components on the browser's own engine with a Wasmtime-shaped
  API; `wcmp::bindgen!` accepts the options we use (`with:`, `trappable`, `HasSelf`,
  `add_to_linker`). Our WIT has no async functions, streams, or futures, so its suspension
  machinery is unused: per its source, a call that never suspends has finished when
  `WebAssembly.promising` returns. If that holds end to end, synchronous dispatch and
  `prevent_default` survive.
- **`wcmp` differs from Wasmtime in four ways that touch our interfaces:**
  1. everything is async: compile, instantiate, every guest call, and a fallible `Store::new`;
  2. there is no instruction fuel, so a looping guest freezes the tab and the
     `exhaust_fuel` trap tests have no web equivalent;
  3. no memory-growth limiter equivalent to `ResourceLimiter` was found (*unverified*); handle,
     record, and copy-size caps exist, and our own `DocumentLimits` port as-is;
  4. its store is not thread-safe, while published provisions must be `Send + Sync` and
     `WasmMicro` currently holds the runtime.
- **A version conflict blocks adoption today.** `wcmp` and `wcmp-wasm-core-web` pin
  `wasm-bindgen = 0.2.126` exactly; our Dioxus fork requires 0.2.127, and one lockfile serves
  every target. Separately, our lock is on 0.2.127 while the flake's `wasm-bindgen-cli` is
  0.2.126; library and CLI must match exactly, so web builds are probably broken already
  (*unverified*).

## Decision

### 1. The second engine is `wcmp`, as `clay_rt_wcmp`

The browser engine is a `clay_rt_wcmp` crate built on `wcmp`, sibling to `clay_rt_wasmtime`. It
replaces ADR 015's `clay_engine_jco` placeholder. `wcmp` also runs natively, so `clay_rt_wcmp`
is tested under nextest without a browser; browser coverage is an additional lane (§7).

### 2. Split the engine seam before adding the engine

`clay_rt_wasmtime` is roughly 1,600 lines of glue, most of it engine-neutral. A second engine is
the agreed trigger to split it rather than copy it:

- **Engine-neutral, shared:** wire/mutation conversions, surface and placement resource state,
  grant checks, document delivery, and limits. This joins `clay_guest_session`, which already
  owns the WIT sources and engine-neutral document sessions (ADR 015 §3). Whether that crate is
  then renamed (the crate map's `clay_contract` proposal) is a separate naming decision.
- **Engine-specific, thin:** generated bindings, resource tables, store construction, bounds
  configuration, and the call adapter, in `clay_rt_wasmtime` and `clay_rt_wcmp`. Engine types
  never enter the shared crate (ADR 015 boundary correction).

### 3. Activation is async; presentation calls stay synchronous

- Compiling and instantiating move into activation, which is already async on both engines.
  `WasmPackage::from_bytes` and `clay_mdk::wasm::provider` change signature accordingly; no
  compatibility shim.
- `mount`, `dispatch`, and `unmount` stay synchronous (ADR 015 R5, sync dispatch). On `wcmp` they
  poll the guest call once and panic if it is still pending: our WIT contains no suspension
  point, so a pending call is a broken invariant, not a recoverable condition.
- Web-Worker guests remain rejected for the reason ADR 015 §1 records.

### 4. Publish the instance, not the runtime

Published provisions carry only the instance identity and its surface reference. The engine
runtime and store live in the guest's driver future and in the surface factory, which already
admit non-thread-safe state. Desktop adopts the same shape so both engines publish identical
provisions and `WasmMicro` stops owning a runtime.

### 5. Guest bounds on the web

- Memory and ingress bounds are kept wherever `wcmp` provides them; `DocumentLimits` apply on
  both engines.
- **Instruction bounds are not available in the browser, and we accept that for now.** On the web
  a guest is not preemptible; a runaway guest hangs its tab. This is a weaker isolation guarantee
  than desktop and is stated as such in user-facing trust documentation once web composition
  ships. Build-time fuel instrumentation of guests is the path if untrusted guests on the web
  become a requirement. Workers are not, because they break synchronous dispatch.
- Trap-on-fuel tests stay Wasmtime-only. Engine-neutral behavior tests run against both engines.

### 6. Resolve the `wasm-bindgen` pin by alignment, not vendoring

Move the Dioxus fork to `wasm-bindgen 0.2.126`, matching the flake's CLI, and ask `wcmp`
upstream to relax its exact pin (it exists only to match their test runner). Carry a patched
`wcmp` only if upstream refuses. Library and CLI versions are kept identical in the flake from
then on.

### 7. Shared composition root and component delivery

- The engine-agnostic part of `dayframe_desktop/composition.rs` (the runtime, the `Application`
  component, `linked_providers`, the configuration maps, the coherence test) moves into a shared
  composition crate. `dayframe_desktop` and `dayframe_web` become launch shells that select an
  engine and a renderer. This lands before parallel micro PRs edit the composition, so they do
  not edit two copies.
- Guest components are **fetched as `dx` assets** during activation, not embedded with
  `include_bytes!`. Activation is already async, a guest change does not rebuild the host, and it
  is the "micro delivered over the wire" direction composition already follows (no symbol per
  micro in the root). The `DAYFRAME_<NAME>_COMPONENT` file path remains the native loader.
- Gate lanes: Clippy for `wasm32-unknown-unknown` over the web crates, and a
  `wasm-bindgen-test` lane in headless Chromium that clicks the counter guest round trip. Web
  bundle size is tracked, because the bundle carries `wasmtime-environ` and a second `wasmparser`.

## Migration

1. **Spike** in a throwaway crate outside the workspace (the pin conflict keeps it out), against
   the real counter component: `wcmp::bindgen!` over our WIT; `wcmp` accepts our
   `wit-bindgen`-built component; activate, mount, and dispatch complete on the first poll
   natively, then in Chromium; web events serialize through `clay_surface::render`. This ADR
   becomes Accepted, amended, or Rejected on the spike's result.
2. Resolve the `wasm-bindgen` pin (§6).
3. Split the engine seam and move to instance-only publication (§2, §4) with desktop green.
4. `clay_rt_wcmp`, tested natively.
5. Shared composition crate (§7).
6. `dayframe_web` shell with asset-fetched components.
7. Web gate lanes.

## Consequences

- The browser becomes a link target plus one engine crate, as ADR 015 intended; no micro changes.
- Two engines keep the engine-neutral crate honest: anything engine-specific that leaks into it
  fails to build for one of them.
- Web guests are weaker-isolated than desktop guests until instruction bounds exist there.
- Activation signatures change on desktop too, for parity.

## Open questions

- Does `wcmp` expose any memory-growth cap? If not, is a per-guest memory ceiling enforced by
  instantiation-time import limits acceptable, or must it be upstreamed?
- Is `WebAssembly.promising` first-poll completion guaranteed for every non-suspending export in
  every target browser, or only observed? The panic-on-pending rule depends on the answer.
- Bundle size budget for the web host, given two `wasmparser` versions.
