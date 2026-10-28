# ADR 018: The browser guest engine — splitting the engine seam for `wcmp`

**Status:** Accepted. Migration steps 1–8 are implemented and verified; the
feasibility spike that proved `wcmp` out has been deleted now that the
production adapter and the `dayframe_web` shell carry it. **Amends:** ADR 015
§1 (Shape W), §8 (the future browser engine), and §Deferred
(`clay_engine_jco`), and executes the crate-map trigger D-c ("leave
`clay_guest_session` alone until a second engine needs the shared contract").
**Evidence:** the 2026-10-21 investigation of `wcmp` 0.1.1
(<https://github.com/dialog-db/wasm-component-model-polyfill>) against our host
stack; findings marked _unverified_ were read from documentation, not code. The
Dioxus prerequisite is published in `dman-os/dioxus` branch
`feat/dayframe-native-components` at `a3e1e68334b7dfa68cead38e9e8795ec7a6b534a`
(pinned in `Cargo.toml`/`Cargo.lock`): non-browser Wasm component support, the
direct native mutation-writer/state API, and pointer-event constructors.

Build logs, bundle sizes, and per-lane command results are kept in
`docs/scratch/dayframe-crate-map.md` §8 rather than here — this record states
the decisions.

## Context

ADR 015 Shape W puts the whole host (VDOM, composition, native micros) into one
wasm32 module in the JS realm and instantiates guests as components in the same
realm, called synchronously. It named "jco or equivalent" and deferred the
engine. The investigation established:

- **The host stack already builds for the browser.** Every Clay and micro crate
  except `clay_rt_wasmtime` passes Clippy for `wasm32-unknown-unknown`,
  including `clay_mdk`'s `native` and `view` features and `dayframe_web`.
- **Guest surfaces render on the web unchanged.** `clay_dioxus` projects a
  guest surface through ordinary Dioxus `rsx`, not the Blitz writer, so
  `dioxus-web` can draw it; the event and surface-factory closures need not be
  thread-safe.
- **`wcmp` fits our WIT.** It runs components on the browser's own engine with a
  Wasmtime-shaped API; `wcmp::bindgen!` accepts the options we use (`with:`,
  `trappable`, `HasSelf`, `add_to_linker`). Our WIT has no async functions,
  streams, or futures, so its suspension machinery is unused: per its source, a
  call that never suspends has finished when `WebAssembly.promising` returns. If
  that holds end to end, synchronous dispatch and `prevent_default` survive.
- **`wcmp` differs from Wasmtime in four ways that touch our interfaces:**
  1. everything is async: compile, instantiate, every guest call, and a fallible
     `Store::new`;
  2. there is no instruction fuel, so a looping guest freezes the tab and the
     `exhaust_fuel` trap tests have no web equivalent;
  3. no memory-growth limiter equivalent to `ResourceLimiter` was found
     (_unverified_); handle, record, and copy-size caps exist, and our own
     `DocumentLimits` port as-is;
  4. its store is not thread-safe, while published provisions must be
     `Send + Sync` and `WasmMicro` used to hold the runtime.
- **A version conflict blocked adoption.** `wcmp` and `wcmp-wasm-core-web`
  pinned `wasm-bindgen = 0.2.126` exactly, while the Dioxus fork needs 0.2.127
  and one lockfile serves every target. The flake's `wasm-bindgen-cli` was still
  0.2.126, which broke web builds on its own because library and CLI must match
  exactly. §6 records the resolution.

## Decision

### 1. The second engine is `wcmp`, as `clay_rt_wcmp`

`wcmp` is split into three crates rather than one crate with target-dependent
backend branches: `clay_rt_wcmp` owns the backend-neutral adapter,
`clay_rt_wcmp_wasmi` supplies the native Wasmi backend, and `clay_rt_wcmp_web`
supplies the browser's `WebAssembly` backend. The native crate is a root
workspace member and is tested under nextest; the Web crate belongs to the
all-Wasm `src/wasm_utils_rs` workspace so root feature unification cannot pull
native-only features into its build. The shared crate has no backend dependency
and compiles for both targets. `wcmp` replaces ADR 015's `clay_engine_jco`
placeholder.

### 2. Split the engine seam before adding the engine

`clay_rt_wasmtime` was roughly 1,600 lines of glue, most of it engine-neutral. A
second engine is the agreed trigger to split it rather than copy it:

- **Engine-neutral, shared:** wire/mutation conversions, surface and placement
  resource state, grant checks, document delivery, ingress budgets, and limits.
  This joined `clay_guest_session`, which already owns the WIT sources and
  engine-neutral document sessions (ADR 015 §3). Whether that crate is later
  renamed (the crate map's `clay_contract` proposal) is a separate naming
  decision.
- **Engine-specific, thin:** generated bindings, resource tables, store
  construction, bounds configuration, and the call adapter, in
  `clay_rt_wasmtime` and `clay_rt_wcmp`. Engine types never enter the shared
  crate (ADR 015 boundary correction).

### 3. Activation is async; presentation calls stay synchronous

- Compiling and instantiating move into activation, which is already async on
  both engines. `WasmPackage::from_bytes` and `clay_mdk::wasm::provider` change
  signature accordingly; no compatibility shim.
- `mount`, `dispatch`, and `unmount` stay synchronous (ADR 015 R5, sync
  dispatch). On `wcmp` they poll the guest call once and panic if it is still
  pending: our WIT contains no suspension point, so a pending call is a broken
  invariant, not a recoverable condition.
- Web-Worker guests remain rejected for the reason ADR 015 §1 records.

### 4. Publish the instance, not the runtime

Published provisions carry only the instance identity and its surface reference.
The engine runtime and store live in the guest's driver future and in the
surface factory, which already admit non-thread-safe state. Desktop adopts the
same shape so both engines publish identical provisions and `WasmMicro` stops
owning a runtime.

### 5. Guest bounds on the web

- Memory and ingress bounds are kept wherever `wcmp` provides them;
  `DocumentLimits` apply on both engines.
- **Instruction bounds are not available in the browser, and we accept that for
  now.** On the web a guest is not preemptible; a runaway guest hangs its tab.
  This is a weaker isolation guarantee than desktop and is stated as such in
  user-facing trust documentation once web composition ships. Build-time fuel
  instrumentation of guests is the path if untrusted guests on the web become a
  requirement. Workers are not, because they break synchronous dispatch.
- Trap-on-fuel tests stay Wasmtime-only. Engine-neutral behavior tests run
  against both engines.

### 6. `wasm-bindgen` 0.2.127 is the single pin

The earlier plan moved the Dioxus fork back to 0.2.126. Instead, `wcmp` is
consumed from the `dman-os/wasm-component-model-polyfill` fork at
`687e486c3bbf4c265f7edb5bc13e6c9c91ccee60`, which changes only its exact pin to
`=0.2.127`, the version the Dioxus fork and the lockfile already use. The root
`Cargo.toml` pins `wasm-bindgen = "=0.2.127"`, and the flake's
`wasm-bindgen-cli` moved to `wasm-bindgen-cli_0_2_127`, so the library, the CLI,
and `wcmp` move together. `x/dayframe-web.ts` refuses to build when the CLI and
`Cargo.lock` disagree. Asking upstream to relax the exact pin (it exists only to
match their test runner) remains open; the fork is dropped once they do.

### 7. Shared composition root and component delivery

- The engine-agnostic part of `dayframe_desktop/composition.rs` (the runtime,
  the `Application` component, `linked_providers`, the configuration maps, the
  coherence test) moved into `dayframe_composition`. `dayframe_desktop` and
  `dayframe_web` are launch shells that select an engine and a renderer. This
  landed before parallel micro PRs edit the composition, so they do not edit
  two copies.
- Guest components are **fetched as `dx` assets** during activation, not
  embedded with `include_bytes!`. Activation is already async, a guest change
  does not rebuild the host, and it is the "micro delivered over the wire"
  direction composition already follows (no symbol per micro in the root). The
  `DAYFRAME_<NAME>_COMPONENT` file path remains the native loader.
- The web shell is wasm-only by construction: its renderer, composition, and
  guest dependencies sit under `[target.'cfg(target_arch = "wasm32")']`, and the
  native binary is deliberately empty. Cargo builds every workspace binary for
  the native test and coverage lanes, and an empty native `main` keeps those
  lanes from pretending to run a browser.
- Web builds resolve features in isolation
  (`resolver.feature-unification="selected"`) because workspace unification
  pulls `dioxus-desktop` → `native-tls` → `openssl-sys` into wasm32, and use the
  `wasm-release` profile because a `debug_assertions` web build links Dioxus
  devtools, which dials `/_dioxus` against a plain static server.

### 8. Guests stay one lineage on `wasm32-wasip3`

The same `clay_mdk` guest sources build as `wasm32-wasip3` components (rustc
emits the component directly — no adapter crate) and are driven by
`clay_rt_wasmtime` (wasmtime-wasi `p3`) and `clay_rt_wcmp` (polyfill WASI host).
The guest pulls in `wasi:cli`, `wasi:clocks`, and `wasi:random` @0.3.0 only — no
filesystem or sockets. `clay_rt_wcmp::wasi` registers every one of those imports;
functions that are unimplemented (`exit`, `stdin.read-via-stream`) error when
called so a guest that starts using them fails loudly instead of silently.
`clay_mdk` overrides `__wasilibc_task_hook` and `__pthread_tsd_run_dtors` to
reimplement wasi-libc#884 (shared TLS for the program lifetime), because the
v34 sysroot still installs per-task TLS and wipes `#[thread_local]` state
between `activate` and `mount`.

### 9. Gate lanes

- Clippy for `wasm32-unknown-unknown` over the web crates, and the reachable
  contracts in `cargo x guardrails check-matrix` (`dayframe_web --bins` under
  `web`, `dayframe_composition`, `clay_mdk` feature subsets, native
  `clay_rt_wcmp_wasmi`, wasm32-only `clay_rt_wcmp_web`).
- A headless-Chromium lane that clicks the counter guest round trip. _Amended
  during implementation:_ it is the Playwright suite
  (`pnpm -C src/dayframe_web run e2e` / CI `e2e-dayframe-web-chromium`), not
  `wasm-bindgen-test` — the repo has no `wasm-bindgen-test` harness, and
  Playwright is the same shape the spike used.
- Web bundle size is printed after every build (raw + gzip), because the bundle
  carries `wasmtime-environ` and a second `wasmparser`.

## Migration

1. **Guest target on `wasm32-wasip3`:** `micro_counter` and `clay_mdk`'s
   `slots` example build as components; `x/check-dayframe.ts` prepares them
   under `DAYFRAME_COUNTER_COMPONENT` / `DAYFRAME_SLOTS_COMPONENT`.
   `clay_rt_wasmtime` drives them through wasmtime-wasi `p3`; `clay_rt_wcmp`
   registers the fail-loudly WASI host (§8). **Done.**
2. **Spike** against the real counter component: `wcmp::bindgen!` over our WIT;
   activate, mount, and dispatch complete on the first poll natively, then in
   Chromium. **Done.** The throwaway `clay_wcmp_spike` crate and
   `x/spike-wcmp-web.ts` were deleted once steps 5–7 landed; the lanes above
   cover what it proved.
3. Resolve the `wasm-bindgen` pin (§6). **Done.**
4. Split the engine seam and move to instance-only publication (§2, §4) with
   desktop green.
5. Implement the shared `wcmp` adapter and its thin Wasmi and Web backend
   crates; test the native backend under nextest and compile the Web backend in
   the Wasm workspace.
6. Shared composition crate (§7).
7. `dayframe_web` shell with asset-fetched components.
8. Web gate lanes (§9).

## Consequences

- The browser becomes a link target plus one engine crate, as ADR 015 intended;
  no micro changes.
- Two engines keep the engine-neutral crate honest: anything engine-specific
  that leaks into it fails to build for one of them.
- Web guests are weaker-isolated than desktop guests until instruction bounds
  exist there.
- Activation signatures change on desktop too, for parity.
- The `wasm-bindgen` exact pin now comes from a fork, so upstream moves must be
  re-pinned deliberately rather than absorbed by a version range.

## Open questions

- Does `wcmp` expose any memory-growth cap? If not, is a per-guest memory
  ceiling enforced by instantiation-time import limits acceptable, or must it be
  upstreamed?
- Is `WebAssembly.promising` first-poll completion guaranteed for every
  non-suspending export in every target browser, or only observed? The
  panic-on-pending rule depends on the answer; it is observed in Chromium only.
  Firefox and WebKit are untested.
- Bundle size budget for the web host, given two `wasmparser` versions.
