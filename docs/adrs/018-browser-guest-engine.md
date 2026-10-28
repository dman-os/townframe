# ADR 018: The browser guest engine — splitting the engine seam for `wcmp`

**Status:** Accepted. The feasibility spike (§Migration step 1) passed and
amended §6 (see §Spike result). Steps 1–7 including the `wasm32-wasip3` guest
target (§Migration step 1a) are done and verified (see §Migration result and
§Verification). **Amends:** ADR 015 §1 (Shape W), §8 (the future browser
engine), and §Deferred (`clay_engine_jco`). Executes the crate-map trigger D-c
("leave `clay_guest_session` alone until a second engine needs the shared
contract"). **Evidence:** the 2026-10-21 investigation of `wcmp` 0.1.1
(<https://github.com/dialog-db/wasm-component-model-polyfill>) against our host
stack. Findings marked _unverified_ were read from documentation, not code. The
Dioxus-side prerequisite is published in `dman-os/dioxus` as branch
`feat/dayframe-native-components` at `a3e1e68334b7dfa68cead38e9e8795ec7a6b534a`;
Townframe pins that commit in `Cargo.toml` and `Cargo.lock`. It contains only
the three Dayframe prerequisite commits on top of Dioxus `origin/main`:
non-browser Wasm component support, the direct native mutation-writer/state API,
and pointer-event constructors/re-exports.

## Context

ADR 015 Shape W puts the whole host (VDOM, composition, native micros) into one
wasm32 module in the JS realm and instantiates guests as components in the same
realm, called synchronously. It named "jco or equivalent" and deferred the
engine. The investigation established:

- **The host stack already builds for the browser.** Every Clay and micro crate
  except `clay_rt_wasmtime` passes Clippy for `wasm32-unknown-unknown`,
  including `clay_mdk`'s `native` and `view` features and `dayframe_web`.
- **Guest surfaces should render on the web unchanged.** `clay_dioxus` projects
  a guest surface through ordinary Dioxus `rsx`, not the Blitz writer, so
  `dioxus-web` can draw it; the event and surface-factory closures are not
  required to be thread-safe.
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
     `Send + Sync` and `WasmMicro` currently holds the runtime.
- **A version conflict blocks adoption today.** `wcmp` and `wcmp-wasm-core-web`
  pin `wasm-bindgen = 0.2.126` exactly; our Dioxus fork requires 0.2.127, and
  one lockfile serves every target. Separately, our lock was on 0.2.127 while
  the flake's `wasm-bindgen-cli` was 0.2.126; library and CLI must match
  exactly, so web builds were broken. The spike resolved both (§6).

## Decision

### 1. The second engine is `wcmp`, as `clay_rt_wcmp`

The second engine is `wcmp`, split into three crates rather than one crate with
target-dependent backend branches: `clay_rt_wcmp` owns the backend-neutral
adapter, `clay_rt_wcmp_wasmi` supplies the native Wasmi backend, and
`clay_rt_wcmp_web` supplies the browser's `WebAssembly` backend. The native
crate is a root workspace member and is tested under nextest; the Web crate
belongs to the all-Wasm `src/wasm_utils_rs` workspace so root feature
unification cannot pull native-only features into its build. The shared crate
has no backend dependency and compiles for both targets. Browser behavior
coverage remains an additional lane (§7). `wcmp` replaces ADR 015's
`clay_engine_jco` placeholder.

### 2. Split the engine seam before adding the engine

`clay_rt_wasmtime` is roughly 1,600 lines of glue, most of it engine-neutral. A
second engine is the agreed trigger to split it rather than copy it:

- **Engine-neutral, shared:** wire/mutation conversions, surface and placement
  resource state, grant checks, document delivery, and limits. This joins
  `clay_guest_session`, which already owns the WIT sources and engine-neutral
  document sessions (ADR 015 §3). Whether that crate is then renamed (the crate
  map's `clay_contract` proposal) is a separate naming decision.
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

### 6. Resolve the `wasm-bindgen` pin by moving `wcmp` forward

_Amended by the spike._ The original plan moved the Dioxus fork back to 0.2.126.
Instead, `wcmp` is consumed from the `dman-os/wasm-component-model-polyfill`
fork at `687e486c3bbf4c265f7edb5bc13e6c9c91ccee60`, which changes only its exact
pin to `=0.2.127`, the version the Dioxus fork and the lockfile already use. The
flake's `wasm-bindgen-cli` moved to `wasm-bindgen-cli_0_2_127` to match, and
`x/spike-wcmp-web.ts` fails the build if the CLI and `Cargo.lock` disagree. The
root `Cargo.toml` pins `wasm-bindgen = "=0.2.127"` so the library, the CLI, and
`wcmp` move together. Asking upstream to relax the exact pin (it exists only to
match their test runner) remains open; the fork is dropped once they do.

### 7. Shared composition root and component delivery

- The engine-agnostic part of `dayframe_desktop/composition.rs` (the runtime,
  the `Application` component, `linked_providers`, the configuration maps, the
  coherence test) moves into a shared composition crate. `dayframe_desktop` and
  `dayframe_web` become launch shells that select an engine and a renderer. This
  lands before parallel micro PRs edit the composition, so they do not edit two
  copies.
- Guest components are **fetched as `dx` assets** during activation, not
  embedded with `include_bytes!`. Activation is already async, a guest change
  does not rebuild the host, and it is the "micro delivered over the wire"
  direction composition already follows (no symbol per micro in the root). The
  `DAYFRAME_<NAME>_COMPONENT` file path remains the native loader.
- Gate lanes: Clippy for `wasm32-unknown-unknown` over the web crates, and a
  headless-Chromium lane that clicks the counter guest round trip. _Amended
  during implementation:_ that lane is the Playwright suite
  (`pnpm -C src/dayframe_web run e2e` / CI `e2e-dayframe-web-chromium`), not
  `wasm-bindgen-test` — the repo has no `wasm-bindgen-test` harness, and
  Playwright is the same shape the spike already used. Web bundle size is
  tracked, because the bundle carries `wasmtime-environ` and a second
  `wasmparser`.

## Migration

1. **Guest target on `wasm32-wasip3`** (Phase 1a): `micro_counter` and
   `clay_mdk`'s `slots` example build as `wasm32-wasip3` components;
   `x/check-dayframe.ts` prepares them under `DAYFRAME_COUNTER_COMPONENT` /
   `DAYFRAME_SLOTS_COMPONENT`. `clay_rt_wasmtime` drives them through
   wasmtime-wasi `p3` (`wasm_component_model_async`, async bindgen exports).
   `clay_rt_wcmp` registers a fail-loudly WASI host (`wasi:cli`, `wasi:clocks`,
   `wasi:random` @0.3.0) on the polyfill linker. **Done; see §Migration result —
   wasip3.**
2. **Spike** against the real counter component: `wcmp::bindgen!` over our WIT;
   `wcmp` accepts our `wit-bindgen`-built component; activate, mount, and
   dispatch complete on the first poll natively, then in Chromium; web events
   serialize through `clay_surface::render`. This ADR becomes Accepted, amended,
   or Rejected on the spike's result. **Done; see §Spike result.**
3. Resolve the `wasm-bindgen` pin (§6). **Done by the spike.**
4. Split the engine seam and move to instance-only publication (§2, §4) with
   desktop green. **Done; see §Migration result.**
5. Implement the shared `wcmp` adapter and its thin Wasmi and Web backend
   crates; test the native backend under nextest and compile the Web backend in
   the Wasm workspace. The throwaway spike remains unchanged until web
   composition replaces it. **Done; see §Migration result.**
6. Shared composition crate (§7). **Done; see §Migration result.**
7. `dayframe_web` shell with asset-fetched components. **Done; see §Migration
   result.**
8. Web gate lanes. **Done; see §Migration result.**

## Spike result

`src/clay_wcmp_spike` is a root workspace member rather than a throwaway crate
outside it, because resolving the pin (§6) removed the reason to keep it out.
Its one host compiles for both targets: Wasmi (`wcmp-wasm-core-wasmi`) natively,
the browser's own `WebAssembly` engine (`wcmp-wasm-core-web`) on
`wasm32-unknown-unknown`. That target-dependent backend selection is confined to
the throwaway spike; the production adapter uses the three-crate split in §1.

- **`wcmp::bindgen!` accepts our WIT** (`clay_guest_session/wit`, world
  `micro-client`) with `with:` mappings of the presentation resources onto host
  types, `trappable` imports, and `HasSelf` in `add_to_linker`. One
  generated-code difference from Wasmtime: `call_dispatch` takes the event
  record by reference.
- **`wcmp` accepts our `wit-bindgen`-built counter component** as-is, natively
  and in Chromium.
- **First-poll completion holds.** Activation is awaited; `mount`, `dispatch`,
  and `unmount` go through `first_poll`, which polls once with a no-op waker and
  panics on `Pending` (§3). The native nextest test drives activate, mount,
  click dispatch, and unmount through it; the Playwright test
  (`pnpm -C src/clay_wcmp_spike run e2e`) loads the counter in headless
  Chromium, waits for it to mount through `clay_dioxus` on `dioxus-web`, clicks
  it three times, and fails on any page or console error, which is where a
  `first_poll` panic surfaces.
- **Web events serialize through `clay_surface::render`** unchanged: the clicks
  reach the guest as `clay_surface` events and its mutations redraw the button.
- **Build constraints found:**
  - The root workspace sets `resolver.feature-unification = "workspace"`, which
    pulls desktop-only features (`dioxus-desktop` → `native-tls` →
    `openssl-sys`) into wasm32 builds. Web builds of the spike pass
    `--config 'resolver.feature-unification="selected"'`; the `dayframe_web`
    shell will need the same until that setting changes.
  - A `debug_assertions` web build links Dioxus devtools, which dial `/_dioxus`
    and log console errors under a plain static server, so the spike builds with
    `--profile wasm-release`.
- **Bundle size** (`wasm-release`, line-table debug info, no `wasm-opt`): the
  host module is 7.0 MB (1.8 MB gzipped), carrying `wasmtime-environ` 50 and
  `wasmparser` 0.261 alongside the Dioxus web runtime. This is the baseline for
  the bundle-size budget question below.
- **Not exercised**, by the migration order: document bindings (the counter runs
  standalone), ingress budgets, handle caps, and memory-growth limits. Those
  wait for the shared seam (step 3).

Gate lanes added: `clay_wcmp_spike` compiles for native and
`wasm32-unknown-unknown` in `cargo x guardrails check-matrix`, its native test
runs in the workspace nextest lane, and the `e2e-wcmp-spike-chromium` CI job
runs the Playwright test. The `.agents/skills/playwright` skill documents how
the flake supplies browsers to Playwright.

## Migration result

Steps 1a and 3–7 are done. `src/clay_wcmp_spike` is unchanged. Status is
Accepted.

### wasip3 guest target

Guests are one lineage across both host engines: the same `clay_mdk` sources
build for `wasm32-wasip3` (components rustc emits directly — no adapter) and are
driven by `clay_rt_wasmtime` (wasmtime-wasi `p3`) and `clay_rt_wcmp` (polyfill
WASI host).

**WASI import surface** (from `wasm-tools component wit` on the prepared counter
component):

- `wasi:cli/{environment,exit,types,stdin,stdout,stderr,terminal-input,terminal-output,terminal-stdin,terminal-stdout,terminal-stderr}@0.3.0`
- `wasi:clocks/{types,monotonic-clock,system-clock}@0.3.0`
- `wasi:random/insecure-seed@0.3.0`

No filesystem or sockets. `monotonic-clock.wait-for` is the only `async func`.
The component also exports
`townframe:dayframe/{document-events,micro-lifecycle,presentation-events}@0.1.0`.

**Guest-side task hooks.** wasi-sdk v34 (rustc 1.100.0-nightly 2026-09-17) still
implements per-task TLS: each Start `posix_memalign`s a fresh TLS block, so
`#[thread_local]` values written during `activate` vanish by `mount` ("host
drove a guest micro before activating it"). Upstream fixed this in wasi-libc#884
(shared TLS for the program lifetime). `clay_mdk` provides strong overrides of
`__wasilibc_task_hook` and `__pthread_tsd_run_dtors` that reimplement the
post-#884 contract: TLS installed once via `__wasm_init_tls`, the static
`__init_stack_pointer` acquired on Start / released on Finish, realloc on the
hook stack returning NULL, no TSD dtor sweeps. `.cargo/config.toml` passes
`-C link-arg=--allow-multiple-definition` (bare; wasm-component-ld rejects
`-Wl,`) so the strong definitions win over libc.a's weak ones.

**Host-side WASI.** `clay_rt_wcmp::wasi` registers every import above on the
polyfill linker. stdout/stderr write through a stream consumer (lines to process
stdio under a component label); `get-environment` answers empty (deterministic
across engines); clocks and insecure-seed are implemented; `exit` and
`stdin.read-via-stream` error when called so a guest that starts using them
fails loudly rather than silently.

### Engine seam

`clay_guest_session` owns the engine-neutral half of the old `clay_rt_wasmtime`
glue: document sessions and `Limits`, the delivery driver (`drive`),
presentation surface and placement state, grant checks, ingress budgets, and
mutation mapping. `clay_rt_wasmtime` and `clay_rt_wcmp` keep generated bindings,
resource tables, store construction, bounds configuration, and the call adapter.
Both call `clay_guest_session::drive` from the incarnation driver. Engine types
do not enter the shared crate.

Activation is async on both engines, with no compatibility shim.
`WasmRuntime::activate` and `WcmpRuntime::activate` compile and instantiate
during activation and return the driver future. `mount`, `dispatch`, and
`unmount` stay synchronous. On `wcmp`, `first_poll` polls the guest call once
with a no-op waker and panics if it is still `Pending`.

`clay_mdk::wasm::WasmMicro` publishes the incarnation `Instance`, the surface
`ComponentRef`, and a test-only fuel `AtomicU64`. The runtime and store stay in
the driver future and the surface factory. Desktop composition still selects
`clay_rt_wasmtime`. Fuel exhaustion remains a Wasmtime test control and has no
`wcmp` equivalent.

### `wcmp` crates

Three crates, not one crate with target branches:

- `clay_rt_wcmp` is the backend-neutral adapter. It depends on `wcmp` but not on
  a backend. `WcmpRuntime` is `!Send` (`Rc<RefCell<...>>`), which the surface
  factory already admits.
- `clay_rt_wcmp_wasmi` is a root workspace member. It builds an engine with
  `wcmp-wasm-core-wasmi` and is what nextest runs.
- `clay_rt_wcmp_web` is a member of the `src/wasm_utils_rs` workspace, so root
  `resolver.feature-unification = "workspace"` cannot pull `dioxus-desktop` into
  its build. It builds an engine with `wcmp-wasm-core-web` and re-exports the
  shared runtime. Nothing in `dayframe_web` calls it.

### Shared composition and shells (§7)

`dayframe_composition` owns the engine-neutral half of the old
`dayframe_desktop/composition.rs`: `CompositionRuntime::new(guest_providers)`,
the `Application` component, the native provider linkage (settings, sample), the
configuration maps, and the coherence test. `dayframe_desktop` and
`dayframe_web` are thin shells: each supplies `guest_providers` (Wasmtime
`clay_mdk::wasm::provider` versus browser `clay_mdk::wcmp::provider`) and builds
its runtime with `desktop_runtime()` / `web_runtime()`.

`clay_mdk` gained feature `wcmp` and module `wcmp`, the browser-engine twin of
`wasm`: same authoring vocabulary and published provision id `wasm.controller`,
package factory supplied asynchronously inside activation. `clay_rt_wasmtime` is
now a `cfg(not(target_arch = "wasm32"))` dependency of `clay_mdk`, so one
feature set compiles for both targets.

Guest components are fetched during activation: desktop still reads
`DAYFRAME_<NAME>_COMPONENT`; `dayframe_web` fetches `counter.component.wasm` as
a static asset placed next to the host module by `x/dayframe-web.ts` (never
`include_bytes!`).

Gate lanes: `cargo x guardrails check-matrix` covers `dayframe_web`,
`dayframe_composition`, `clay_mdk`, and the shared micro crates for `native` and
`wasm32-unknown-unknown` (the `Dependency.target` cfg filter keeps
`cfg(not(target_arch = "wasm32"))` deps out of wasm boundary checks).
`x/dayframe-web.ts` prints raw + gzip bundle size after `wasm-bindgen`. CI runs
`e2e-dayframe-web-chromium` (`pnpm -C src/dayframe_web run e2e`), which drives
the shared composition + counter guest through headless Chromium via Playwright.

### Verification

Recorded 2026-10-27 against rustc 1.100.0-nightly (2026-09-17), with
`CARGO_TARGET_DIR` scoped to `townframe-target-p3n-2` and the prepared
`DAYFRAME_COUNTER_COMPONENT` / `DAYFRAME_SLOTS_COMPONENT` artifacts:

- Native clippy, `--locked --all-targets -D warnings`, packages
  `dayframe_composition`, `dayframe_desktop`, `clay_mdk` with features
  `clay_mdk/wasmtime,clay_mdk/wcmp`, and `clay_rt_wcmp`/`clay_rt_wcmp_wasmi`
  with `clay_rt_wcmp/test-utils`: `CLIPPY_EXIT=0`.
- Native nextest: engine packages (`clay_guest_session`, `clay_rt_wasmtime`,
  `clay_rt_wcmp`, `clay_rt_wcmp_wasmi`, `clay_mdk`, features
  `clay_mdk/wasmtime`, `clay_rt_wcmp/test-utils`) — 75 passed, 0 skipped
  (`clay_guest_session` 7, `clay_rt_wasmtime` 9, `clay_rt_wcmp_wasmi` 8,
  `clay_mdk` 51). Shell packages (`dayframe_composition`, `dayframe_desktop`,
  `clay_mdk`, features `clay_mdk/wasmtime,clay_mdk/wcmp`) — 61 passed, 0 skipped
  (includes the desktop composition and settings/counter e2e flows).
  Trap-on-fuel stays Wasmtime-only
  (`execution_and_linear_memory_are_bounded_per_incarnation`).
- `cargo x guardrails check-matrix` (unfiltered): `MATRIX_EXIT=0`, 85 clippy
  invocations covering native + `wasm32-unknown-unknown` + `wasm32-wasip3`
  contracts, including `clay_mdk` feature subsets (`dioxus,in_process,wit` and
  the wasm32 sets without `wasmtime`), `dayframe_web` (`--bins` under `web`),
  `dayframe_desktop` (`--bins` under `desktop`), `clay_rt_wcmp` (native with and
  without `test-utils`, wasm32 bare), `clay_rt_wcmp_web` (wasm32 only), and
  `clay_rt_wcmp_wasmi` (native only). This is a compile lane, not a browser
  behavior lane.
- Web build `x/dayframe-web.ts --build`: exit 0. Bundle size
  `host=9915808 gzip=2473568 guest=11963437`. The manganis `bundled_path`
  placeholder in `site/dayframe_web_bg.wasm` is rewritten in place to
  `tailwind.css`.
- Playwright Chromium e2e, `DAYFRAME_WEB_PORT=4523`, flake-pinned browsers:
  `1 passed (4.1s)` — counter mounts and three clicks round-trip with zero
  console/page errors. The browser WASI host reads `Date.now`/`performance.now`
  (std time panics on `wasm32-unknown-unknown`); before that fix the counter
  activation asserted in `dayframe_composition`.
- `cargo x guardrails api --all --check`: exit 0.
  `./x/agent-build.ts -- cargo x guardrails
  doctor`: exit 0.
  `deno fmt --check x/`, `deno lint x/`, `deno check x/dayframe-web.ts`: exit 0.
- `cargo x guardrails lint`: fails only on the three pre-existing legacy
  ceilings (`parse_hledger.rs` 336>330, `label_engine.rs` 412>411,
  `daybook_types/manifest.rs` 585>574) — out of scope for this ADR.
- `nix flake check` still needs every new/changed `.rs` file tracked in the
  flake source; this worktree has no VCS mutation, so the store copy can miss
  untracked modules and rustfmt fails on unresolved `mod` paths.
- Firefox, WebKit, document bindings on the web, ingress budgets, handle caps,
  and memory-growth limits under `wcmp` remain open.

## Consequences

- The browser becomes a link target plus one engine crate, as ADR 015 intended;
  no micro changes.
- Two engines keep the engine-neutral crate honest: anything engine-specific
  that leaks into it fails to build for one of them.
- Web guests are weaker-isolated than desktop guests until instruction bounds
  exist there.
- Activation signatures change on desktop too, for parity.

## Open questions

- Does `wcmp` expose any memory-growth cap? If not, is a per-guest memory
  ceiling enforced by instantiation-time import limits acceptable, or must it be
  upstreamed?
- Is `WebAssembly.promising` first-poll completion guaranteed for every
  non-suspending export in every target browser, or only observed? The
  panic-on-pending rule depends on the answer. The spike observed it in Chromium
  only; Firefox and WebKit are untested.
- Bundle size budget for the web host, given two `wasmparser` versions.
