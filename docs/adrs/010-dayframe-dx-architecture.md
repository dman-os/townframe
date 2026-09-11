# ADR 010: dayframe_dx Architecture — Pieces, Faces, and the Composition Root

**Status:** Accepted.

## Context

`dayframe_dx` is the Rust/Dioxus re-implementation of `daybook_compose` (the Kotlin Multiplatform app), targeting Android, iOS, and desktop (and eventually the browser). It runs Dioxus `0.8.0-alpha.1` patched to the local `../dioxus` tree, with the official `dioxus-components`-style shared `ui` crate and `web`/`desktop`/`mobile` platform crates mirroring `../dioxus-test`.

The node architecture is driven by the multiprocessing analysis in `docs/DEVDOC/log.md` (2026-07-04): daybook repos are a web of on-disk formats (SQLite, Iroh blobs, Redb, Keyhive, Keyring) guarded by a single checkout lock file, which blocks local multiprocessing (CLI while the app is open), cloud scalability (relays, multi-tenancy), wasm deployment, and writing multiple apps against the same repo. Decided there: go all-in on wasm for server/cloud pieces (wasip3 components, FaaS-shaped, scale-to-zero), default to IPC-daemon-per-repo where pieces cannot be multi-writer, rewrite `daybook_ffi` into a `libdaybook` that hides daemon-vs-local actors, and separate event lanes (GUI-driving events processed by every opener; derived-state events crash-resistant and deduplicated across instances).

Additional context:

* `daybook_ffi` today is a uniffi bridge (runtime, repos, listener bridge, camera) serving the Kotlin app; it stays as-is for `daybook_compose`.
* `irpc` + `irpc-iroh` are already workspace dependencies, used in-process-adjacent by `big_sync`, `big_repo`, and `daybook_core/sync.rs`. IRPC is designed for efficient in-process actors as well as cross-process ones.
* IRPC mandates `postcard` wire compatibility for all messages: interfaces are forced to be plain data (scalars/strings/Vecs/maps/enums/Option/Result). No handles, no pointers can be smuggled across the boundary.
* Wasm is a major aspiration and is **imminent for server nodes** (relay-style nodes), not just the far-future browser. Full wasm browser nodes are explicitly *not* wanted; browser thin clients would carry a small subset of node features.
* BoltFFI (boltffi.dev) is the chosen replacement direction for uniffi-style platform bindings; it generates native-idiomatic Kotlin/Swift (and more) from annotated Rust with XCFramework/jniLibs packaging.
* ADR 009 already defers "the final WIT/wRPC resource shape", and `wash-runtime` (wasmCloud) is already a workspace dependency; a local `../wrpc` checkout exists.

## Decision

### 1. Pieces are the unit; faces are projections; processes are placement

`daybook_core` will be decomposed over time into **pieces** — small, individually addressable services (read/query, sync, storage, relay, wflow, …), each with a defined interface. A **node** is a process composition: some set of pieces hosted together, exposing **many faces** (a BFF-like GUI face, a CLI face, a server/relay face, a thin-client face) — all projections over the same piece graph rather than distinct servers.

Consumers (CLI, GUI, future web thin client) call pieces directly and do **not** boot a full node per invocation. A full `rt` + `sync` bring-up is only needed for the paths that actually need those pieces; pure data-query paths require no active event loops at all.

### 2. The composition root decides daemon vs in-proc; nothing else does

When a consumer starts, the **bootup sequence** either opens a connection to an existing node daemon or starts one in-process. Only the IRPC wiring differs; all other CLI/GUI code must behave identically in both cases. This is a dependency-injection boundary: app code only ever sees interfaces against a connected piece graph. The daemon-attach case additionally needs a small handshake (version compatibility, checkout hosting discovery, coordination with single-instance pieces) that the in-proc case does not — that lives inside the composition root, not the app.

### 3. IRPC is the substrate; postcard-typed interfaces are the contract

IRPC spans in-proc and cross-process with the same call semantics. A fake piece for tests is just another in-proc service implementing the same message contract — no mocking framework. The forced `serde_postcard` data-only surface is considered a feature: it keeps piece interfaces wit/component-model-friendly (see §6) and prevents interface rot by construction.

### 4. BoltFFI is exclusively platform integration

BoltFFI (not uniffi, not the node layer) will generate the Kotlin/Swift host-side bindings for **platform code only**: lifecycle, deep links, notifications, share sheets, camera, biometrics, and the winit gap. `daybook_ffi`/uniffi continues to serve `daybook_compose` untouched. The bridge mirrors the `listener_bridge.rs` shape: typed platform events → channel → app state, with a minimal exported surface.

For the winit gap, the tactic is: boltffi-hack first (app-shaped gaps are contained), and fork `winit`/dioxus upstream only when the same gap recurs across features (the `[patch.crates-io]` mechanism makes the fork the established pattern; the bar is "blocks two features").

### 5. Renderer duality: Blitz is the ambition, wry is prod-ready today

`dioxus-native`/Blitz compatibility is currently low, so the shared component library must render correctly on **both** Blitz and wry/webview. Shared UI uses the CSS subset both engines honor (CSS variables for tokens; avoid Blitz-partial features like `@layer`, `@property`, modern color functions in the shared library). Shipping on wry is an accepted fallback if Blitz does not pan out; dioxus's webview path keeps the app prod-ready today without rewriting.

i18n and Tailwind are **deferred** (see Deferred decisions) — strings stay plain (`format!` + constants, no literals scattered in rsx) so a later `.ftl` retrofit is mechanical.

### 6. Wasm/wrpc: server-first, adopt when the time comes

Server node pieces go wasip3 components over wRPC when we get there (local `../wrpc` checkout exists; wasmCloud machinery is already in the tree). Because IRPC interfaces are postcard/data-shaped, the irpc↔wrpc mapping is expected to be low-impedance (wit records vs plain-data messages); no migration work is scheduled now — this is deferred, with elbow grease expected when needed. Browser thin clients reuse the same faces against a smaller-hosted feature subset; the client side never forks per backend.

### 7. Testing is tiered from day one

1. Core logic: existing Rust tests (nextest).
2. Component tests: headless virtualdom + `kittest` (AccessKit) or Dioxus harness in the shared UI crate — the bulk of the app.
3. Native layout/input edge cases: `blitz-test-harness` (`Harness::from_component`, tick/pump).
4. Platform smoke: desktop via CDP-driven windows; Android via adb install + logcat assertions (bounded output).
5. Web: Playwright once wasm lands (mirroring dioxus's own `playwright-tests`).

§3 means UI tests can exercise real pieces in-proc without a mock layer once they exist.

### 8. Crate dicing: no shared boundary crate

No single "client" crate separates node from UI/CLI. Pieces live in their own crates and adapt organically for server/CLI/GUI needs. Each app is a **composition root ("stamp")** wiring a subset of pieces; `dayframe_dx` itself remains one crate with module seams (`components/`, `views/`, `platform/`) that can spin out when a seam earns its own crate. The piece inventory is TBD and will be figured out as the decomposition proceeds.

## Consequences

* CLI and GUI share identical piece-call code; the bootup sequence is the only divergent surface, keeping daemon and in-proc modes honest test subjects.
* The UI-to-node boundary is no longer a "client/server" seam, removing the need to draw a precise sand line on what logic lives where: some UI-friendly derived logic may live node-side (BFF style) without architectural ceremony.
* Platform integration stays contained behind BoltFFI, so a BoltFFI maturity failure is recoverable (small surface, uniffi pattern known).
* Renderer-agnostic shared UI keeps webview shipping viable while Blitz develops.
* The postcard-only IRPC surface keeps the option of wrpc/wasip3 components open without pre-committing server infrastructure.

## Deferred decisions

- **Piece inventory** (the concrete list and boundaries of the daybook_core decomposition) — TBD, decided as we go.
- **IRPC ↔ wRPC mapping verification** — a toy spike converting one piece interface is due only when we start server-node work; no action now.
- **First deployment mode per platform** — in-proc vs daemon for the dayframe_dx MVP, and the attach-handshake protocol spec (part of the composition root work).
- **i18n**: adopt `dioxus-i18n`/Fluent `.ftl` catalogs when its Dioxus 0.8-alpha compatibility is verified (today it targets 0.7); catalog layout in `ui` assets; trigger is UI string volume making retrofits costly.
- **Tailwind**: requires a Blitz/Stylo smoke test of Tailwind v4 output (cascade layers, `@property`, oklch) before shared UI bets on it; otherwise hand-rolled CSS via design tokens.
- **BoltFFI pinning and adoption** — exact version/rev pin and the first real platform integration (deep links or lifecycle) to prove the toolchain; daybook_compose stays on uniffi.
- **Wasm node feature subsets** — which pieces run as wasip3 components, which stay native, and the browser thin-client cut.
