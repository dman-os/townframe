# Dayframe crate map — what exists, who uses it, what to rename

Status: Living map — updated as crates move; layout decisions settle in ADR 014/015
Owner: operator + dayframe track
Scope: Dependency graph and naming of the `dayframe_*`/`clay_*`/`micro_*` workspace crates.
Updated: 2026-10-22 (post-merge crate hierarchy)

Companion to `dayframe-micros.md` (the design log). That file records *decisions*; this one records
*shape*: the real dependency graph, what each crate actually contains, and the naming/hierarchy
problems with evidence. Read this before moving any crate.

All line numbers and consumer lists were taken from the tree on the turn this file was written.
Re-verify before acting: the crate set moves.

## 1. The graph as built today

Arrows are "depends on". `[opt]` = behind a cargo feature. `(dev)` = dev-dependency only.

```
dayframe_desktop ──> clay_dioxus[wasmtime] ──[opt]──> clay_engine_wasmtime ──> clay_guest_session
       │                    │                                    │                      │
       │                    ├──> clay_compose ──> clay_host ──> clay_collection          │
       │                    ├──> clay_data ─────┘        └────> clay_collection          │
       │                    ├──> clay_host                                               │
       │                    └──> clay_surface                                            │
       ├──> clay_compose                                                                │
       ├──> clay_data <────────────────────────────────────────────────────────────────┘
       ├──> clay_host
       ├──> clay_mdk[native,wasmtime] ──[opt]──> clay_host, clay_data, clay_compose, clay_dioxus
       │                              └────────> clay_collection, clay_surface
       ├──> micro_settings ──> clay_mdk[native], micro_settings_manifest
       ├──> micro_settings_manifest ──> clay_mdk[native]; (dev) clay_host
       ├──> micro_sample ──> clay_mdk[native], micro_settings_manifest
       │              (dev) clay_compose, clay_dioxus, micro_settings
       ├──> micro_counter_manifest ──> clay_mdk[no-default]
       └──(dev) clay_surface

micro_counter ──> clay_mdk[wit,dioxus], micro_counter_manifest      (wasm32 only)
```

Two facts fell out of this that drove everything below (the first is since resolved — phase 3,
§8; the snapshot above is kept as the evidence it was written from):

- **`clay_mdk[native]` depended on `clay_dioxus`, which depends on `clay_compose`/`clay_host`/
  `clay_data`.** So a "native micro" transitively linked the whole host and the Dioxus renderer.
  That was why native and Wasm micros could not be isomorphic, and why `micro_sample` (native)
  looked nothing like `micro_counter` (wasm).
- **`clay_guest_session` has exactly one consumer** (`clay_engine_wasmtime`) yet holds the shared
  `.wit` contracts. The contract and one engine's host-side session glue are the same crate.

## 2. Inventory

LOC is source / tests. "Contains" is what is actually in the crate, not what its name suggests.

| crate | LOC | actually contains | consumers | problem |
|---|---|---|---|---|
| `clay_collection` | 640 / 977 | `Replica`, `Ledger`, `View`, `Batch`, `Query`, `DemandToken` — the substrate | host, data, mdk | none; this is the good one |
| `clay_host` | 1055 / 850 | **three** concerns in one file: instance lifecycle+mounts+cleanup (22–593), hosted collections (595–1054), inline tests (1055+) | compose, data, dioxus, guest_session, engine_wasmtime, mdk, desktop | one 1905-line `lib.rs`; collections deserve their own module |
| `clay_data` | 1117 / ~700 | facet/document **model** (`FacetRoute`, `FacetValue`, `Freshness`, `DocumentSnapshot`), the **service** (`DocumentService`, `DocumentAccess/Watch/Grant`), and `MockDocuments` | compose, dioxus, guest_session, engine_wasmtime, mdk, desktop | name says nothing; three responsibilities; not superseded by `clay_collection` — it is built on it |
| `clay_compose` | 1957 / ~700 | `micros` (registry, activation, provisions, owned-future driver) + `flags` (enablement) | dioxus, mdk, desktop, (dev) micro_sample | "compose" describes the *act*, not the *thing*; it is the micro core |
| `clay_dioxus` | 1609 / ~900 | (a) Dioxus renderer adapter (`SurfaceWriter`, `ComponentMount`, `MountedSurface`), (b) `NativeComponents` registry + `use_component_recovery`, (c) `ApplicationRuntime`, (d) `wasmtime.rs` provider assembly | mdk, desktop, (dev) micro_sample | (c) is host orchestration, (d) is engine work — neither is Dioxus-specific |
| `clay_surface` | 1443 / 1130 | `wire` (transport values + sink/session), `replay`, `tree` (validated retained arena), `render` (private; projects arena into a host `VirtualDom`) | dioxus, engine_wasmtime, mdk, (dev) desktop | doc-comment claims "engine-neutral", but `render.rs` imports `dioxus::prelude` + `dioxus_html::PlatformEventData` |
| `clay_guest_session` | 298 / ~200 | the two `.wit` files **and** host-side `DocumentSession`/`Access`/`Watch`/`Delivery`/`Limits` | engine_wasmtime only | named "guest", lives host-side, sole consumer is one engine |
| `clay_engine_wasmtime` | 1572 / ~500 | `WasmRuntime`, `WasmPackage`, surface/placement resources, mutation conversion, `documents.rs` | dioxus[opt] | fine, but the name should match its siblings |
| `clay_mdk` | 1510 / 1393 | guest side: `bindings` (wit-bindgen), `wire`, `guest` (`GuestDom`), `mirror`; authoring side: `manifest`, `component`; host side: `native` | every `micro_*` | "mdk" hides that it is *two* SDKs (guest + native authoring) in one crate behind features |
| `micro_settings_manifest` | ~110 / ~40 | the Settings **contract**: `SettingsSection(s)` provision + a `Collection`-backed store | micro_settings, micro_sample, desktop | contract crate — correct idea, inconsistent with `micro_sample` |
| `micro_settings` | ~120 / 0 | `SettingsMicro` + `SettingsView` | desktop, (dev) micro_sample | |
| `micro_sample` | ~200 / ~200 | inline `manifest()` + `SampleMicro` + view | desktop | manifest inline here, separate crate for Settings — pick one |
| `micro_counter_manifest` | ~30 / 0 | `CONFIG_KEY/NAME/TAG` consts | micro_counter, desktop | |
| `micro_counter` | ~120 / 0 | the Wasm counter guest | nobody (built as a wasm artifact) | `#![cfg(target_arch = "wasm32")]` gates the whole crate |
| `dayframe_desktop` | 953 / ~570 | `composition.rs` wiring + `main.rs` | bin | names `micro_settings::SettingsMicro`, `micro_settings_manifest::{SettingsSection,SettingsSections}`, `micro_sample`, `micro_counter_manifest` directly |

Nothing here is dead. `clay_compose::flags` has 6 real consumers; `clay_surface::render` is private
and used by `tree.rs`. The problem is placement and naming, not staleness.

## 3. Confirmed defects, with evidence

1. **Path dependencies everywhere.** 45 `path = "../…"` edges across 13 manifests while
   `[workspace.dependencies]` already existed and already carried `ui`, `daybook_types`,
   `utils_rs`. **Fixed this turn**: all clay/micro/dayframe edges now go through
   `[workspace.dependencies]`; `cargo metadata --locked` resolves.
2. **`#[path]` submodule-that-is-a-sibling-file.** Five sites, two of them cross-crate.
   **All five removed this turn**:
   - `src/clay_surface/tree.rs:1015` → `#[path = "tree_tests.rs"] mod tests;`
     now `src/clay_surface/tree/tests.rs`.
   - `src/clay_dioxus/wasmtime.rs:105` → `#[path = "wasmtime_tests.rs"] mod tests;`
     now `src/clay_dioxus/wasmtime/tests.rs`.
   - `src/dayframe_desktop/composition.rs:292` → `#[path = "counter_tests.rs"] mod counter_tests;`
     now `src/dayframe_desktop/composition/counter_tests.rs`.
   - `src/dayframe_desktop/composition.rs:15` → `#[path = "../micro_counter/fixture.rs"]`
     — reached into **another crate's** source file.
   - `src/clay_mdk/tests.rs:16` → the same cross-crate `#[path = "../micro_counter/fixture.rs"]`.
     Found only after the first move broke it; the fixture had two cross-crate consumers, not one.

   The repo already had the correct style beside them: `clay_dioxus/application.rs` +
   `application/tests.rs`, and `clay_compose/micros.rs` + `micros/{composition,provisions}.rs` +
   `micros/composition/tests.rs`. So this was inconsistency, not a missing convention.
   Rule now enforced: **a file is a sibling module; a submodule gets a directory. No `#[path]`.**
   `rg -n '#\[path' src/` returns nothing.

   `micro_counter/fixture.rs` was an orphan: `micro_counter/lib.rs` is `#![cfg(target_arch =
   "wasm32")]` and declares no modules, so that file was never part of its own crate — it existed
   only to be `#[path]`-included by two native consumers. It is now a real module in each
   (`src/dayframe_desktop/composition/counter_fixture.rs`, `src/clay_mdk/tests/counter_fixture.rs`).

   **Deliberately not shared yet.** The same "read the prepared artifact named by
   `DAYFRAME_COUNTER_COMPONENT`" logic exists in four places: those two, plus inline copies in
   `clay_dioxus/wasmtime/tests.rs` and `clay_engine_wasmtime`'s `mod tests`. Sharing it today
   would need either a new cross-layer dependency edge (`clay_mdk` → `clay_engine_wasmtime`) or
   demo plumbing on a production SDK surface (`clay_mdk` is the one crate both consumers already
   reach). Both are worse than 20 duplicated lines, and both would pre-empt decision **D-c**: the
   correct home is the WIT contract crate. Each copy carries a doc comment saying so.
3. **`clay_host` is one 1905-line file** holding instance lifecycle, mounts, cleanup/recovery,
   *and* the entire hosted-collection bridge, plus 850 lines of inline tests.
4. **`clay_dioxus` knows about Wasmtime**: `Cargo.toml:11` `wasmtime = ["dep:clay_engine_wasmtime"]`,
   and `wasmtime.rs:6` re-exports `WasmPackage`. Provider assembly for an engine is not a
   renderer concern. **Fixed (phase 3, see §8)**: the provider assembly moved to `clay_mdk::wasm`;
   `clay_dioxus` is now `Cargo.toml` + `lib.rs` with no features, depending only on `clay_host`,
   `clay_surface`, `dioxus`, and `tokio(sync)`.
5. **`ApplicationRuntime` lives in the renderer crate** (`clay_dioxus/application.rs`) while
   importing `clay_compose::micros` and `clay_host`. Only `NativeComponents`/`ComponentMount`
   in that file are genuinely Dioxus. **Fixed (D-h, see §8)**: it lives in
   `clay_micro_core::application`, generic over `RendererRegistry`.
6. **`clay_surface` is not engine-neutral**: `render.rs:8-10` uses `dioxus::prelude::*`,
   `dioxus_core::ListenerCallback`, `dioxus_html::PlatformEventData`. `wire`/`replay`/`tree` are.
7. **`clay_guest_session`** is host-side session/lease plumbing plus the shared `.wit`, with one
   consumer. The name points the wrong way across the boundary.
8. **`micro_counter/lib.rs:1` is `#![cfg(target_arch = "wasm32")]`** — an arch gate as the crate's
   module boundary. A guest crate should be gated by *target/role*, and the native twin should be
   the same source.
9. **The composition root names micro symbols.** `dayframe_desktop/composition.rs` imports
   `micro_settings::SettingsMicro`, `micro_settings_manifest::{SettingsSection,SettingsSections}`,
   `micro_sample`, `micro_counter_manifest::*`. A micro that arrives over the wire has no symbols
   to name, so this shape cannot generalise — it is the missing order of indirection.
10. **Native and Wasm micros are not isomorphic.** `micro_counter` implements the WIT guest
    (`Guest`, thread-locals, `export!`); `micro_sample` implements a plain `activate(&Context)`
    and a Dioxus view. Two authoring models for one concept.

## 4. Naming table

Bevy is the reference the operator named. The useful Bevy precedents: one `App`/plugin registry
(`bevy_app`) rather than a separate "compose" crate; `bevy_ecs` renderer-agnostic vs
`bevy_render`/`bevy_winit` backends; backend crates named `<domain>_<technology>`.

| current | proposed | why | Bevy analogue |
|---|---|---|---|
| `clay_collection` | **keep** | says what it is | `bevy_ecs` query/world layer |
| `clay_host` | **keep**, split into modules | the host is the thing that knows everything | `bevy_app::App` |
| `clay_compose` | `clay_micro_core` (or fold into `clay_host`) | shared micro *types + registry*, not an act | `bevy_app::{Plugin,PluginGroup}` |
| `clay_data` | `clay_documents` | it is the facet/document model + service | `bevy_asset` (domain-named) |
| `clay_dioxus` | `clay_rt_dioxus` | what remains after (c)/(d) move out is the Dioxus renderer adapter | `bevy_render` |
| `clay_engine_wasmtime` | `clay_rt_wasmtime` | sibling of the above | `bevy_winit` |
| — (future) | `clay_rt_jco` | JS/Node engine slot | — |
| `clay_guest_session` | split: `.wit` + bindings → `clay_contract`; host-side session → `clay_rt_wasmtime::session` | the contract is shared; the session is one engine's | `bevy_render::render_resource` vs backend |
| `clay_surface` | **keep** name; move `render` behind a `dioxus` feature or into `clay_rt_dioxus` | the wire/tree are neutral, the projection is not | `bevy_render` vs `bevy_ui` |
| `clay_mdk` | **keep**; split `guest/` vs `native/` into real submodules, and make `native` not depend on the renderer | two SDKs in one crate is fine if the seams are visible | `bevy_ecs` + `bevy_app` macros |
| `ApplicationRuntime` | move to `clay_host` (or `clay_micro_core`) | it orchestrates instances/registry, not rendering | `App` |
| `NativeComponents` | `clay_rt_dioxus` | Dioxus context-based renderer registry | `RenderPlugin` state |
| `ComponentRef` / `ComponentMount` | rename to `SurfaceRef` / `SurfaceMount` **or** document the collision | Bevy's `Component` is ECS data; ours is a UI component handle. Same word, different meaning, in a codebase that will also have collections | — |
| `Instance` | keep, but note it is a *micro instance*, not an ECS entity | | `Entity` (deliberately not the same) |
| `Mount` | keep | no Bevy analogue; it is a placement lease | — |
| `Entry`/`DocumentWatch`/`Mount` leases | keep names, keep once-only release semantics | already decided in the design log | — |
| `flags` / enablement | **keep the name** (operator: terrible but don't change it) | | |

## 5. Target hierarchy

```
clay_collection      substrate: replica + overlay ledger + view + batch protocol   (no deps)
clay_contract        the .wit files and generated bindings                          (no deps)
clay_host            instances, mounts, cleanup, collections, ApplicationRuntime,
                     capabilities registry — knows everything, renderer-agnostic     -> collection
clay_micro_core      shared micro types + registry/activation (if not folded in)    -> host
clay_documents       facet/document model + service + grants                        -> collection, host
clay_surface         wire + replay + validated tree (engine-neutral)                -> contract
clay_rt_dioxus       Dioxus renderer adapter, NativeComponents, surface projection  -> host, surface
clay_rt_wasmtime     Wasmtime engine + host-side guest session + provider assembly  -> host, surface, contract, documents
clay_rt_jco          (future)                                                       -> …
clay_mdk             guest authoring (wit/dioxus guest) + native authoring surface   -> collection, surface, documents
micro_*              one crate per micro + its contract crate; no host symbols
dayframe_desktop     configuration-driven composition root; names no micro symbols
```

The invariant that makes this readable: **nothing under `clay_rt_*` is imported by `clay_host`,
`clay_collection`, `clay_documents`, or `clay_micro_core`.** Backends depend inward, never the
reverse. The former violation — `clay_mdk[native] → clay_dioxus → clay_compose → clay_host`, the
structural reason native micros could not look like Wasm micros — is gone as of phase 3 (§8):
`clay_mdk::native` names only `RendererRegistry`, and the one renderer-bound backend module
(`clay_mdk::wasm`, which projects guest wire surfaces through the Dioxus adapter) says so in its
feature declaration.

## 6. Decisions needed before executing

These are the points where the feedback was ambiguous or self-contradictory. Each needs one answer.

- **D-a. `clay_compose`: fold into `clay_host`, or rename to `clay_micro_core`?** The feedback said
  both ("shouldn't be a separate crate, should be in clay host" and "the shared types inside clay
  compose make sense, call it clay_micro_core"). Folding removes a crate but puts the micro registry
  next to instance lifecycle in an already-large host; renaming keeps the seam.
- **D-b. `clay_data`: rename to `clay_documents`, or split model/service/mock apart first?** It is
  *not* superseded by `clay_collection` — 6 crates use it. Renaming is cheap; splitting
  `MockDocuments` out of the production module is the part that actually improves readability.
- **D-c. `clay_guest_session`: split into `clay_contract` + `clay_rt_wasmtime::session`, or just
  rename?** Splitting is correct but touches the WIT generation path (`x/wit-fetch.ts`,
  `api_utils_rs` bindings) and therefore the wasm build.
- **D-d. `ApplicationRuntime` → `clay_host`?** It is the single biggest readability win and the
  biggest diff: `clay_mdk::native`, `dayframe_desktop`, and `clay_dioxus` all name it.
- **D-e. Isomorphism target.** Should `micro_sample` be rebuilt as a WIT guest (same source, two
  targets, via an MDK `init_micro!`-style macro), or should native micros keep a distinct
  `activate(&Context)` shape and only the *manifest/provision* vocabulary be shared? The first is
  the "ultimate taste" the feedback described; it is also the largest single piece of work here.
- **D-f. Configuration-driven composition.** How far this turn: a descriptor table the desktop root
  reads (still linking the crates), or a real registry keyed by `MicroId` strings with dynamic
  provider lookup so a wire-delivered micro needs no symbol? The second needs the Bevy-style
  `Plugin`-registry decision from D-a first.

## 7. Suggested phases

Each phase is independently verifiable with `x/check-dayframe.ts` (216 tests today).

1. **Mechanical, no naming decisions**
   - [x] workspace dependencies;
   - [x] remove all five `#[path]` sites, giving `clay_surface/tree/tests.rs`,
     `clay_dioxus/wasmtime/tests.rs`, `dayframe_desktop/composition/{counter_tests,counter_fixture}.rs`,
     and `clay_mdk/tests/counter_fixture.rs`;
   - [x] split `clay_host/lib.rs` (1905 lines) into `lib.rs` (28, module decls + re-exports),
     `instance.rs` (583), `collection.rs` (480), `tests.rs` (849);
   - [x] reorder each module top-down (types → constructors → public API → internals → tests), starting
     with `micro_counter`, `clay_docs`, `clay_host` — done for those three (see §8); extend the
     same convention to other modules opportunistically as they are touched.
2. **Renames** — `clay_compose → clay_micro_core`, `clay_data → clay_docs`,
   `clay_engine_wasmtime → clay_rt_wasmtime` are **done**. The `ApplicationRuntime` move is **done**
   (via D-h, see §8): it now lives in `clay_micro_core::application`, generic over a host-side
   `RendererRegistry`. Still open: the `clay_dioxus` name (see D-g) and `clay_surface::render`
   behind a feature.
3. **Boundary fix**: break `clay_mdk[native] → clay_rt_dioxus`, so the native authoring surface is
   renderer-agnostic. **Done** (see §8): `native` is generic over `RendererRegistry`; the Dioxus
   view glue moved to `clay_mdk::view`; the wasm provider assembly moved to `clay_mdk::wasm`;
   `clay_dioxus` is a pure renderer adapter.
4. **Isomorphism** (D-e) and **configuration-driven composition** (D-f). **Done** for the shape
   the operator chose (see §8): guest-side `clay_mdk::guest::Context` + `export_micro!` give the
   Wasm micro the same `activate(&Context)` entry as the native ones (no WIT change); the
   provider registry is keyed by `MicroId` and contributions route through an erased
   `Contributable` contract, so `dayframe_desktop` no longer names any contribution type.

Phases 1, 3 and 4 are complete; phase 2 is complete except `clay_surface::render` behind a
feature. D-g is **closed as "keep `clay_dioxus`"** (operator confirmed).

## 8. Execution log

**Operator decisions (this turn).** D-a: rename `clay_compose` → `clay_micro_core` (not folded into
the host). D-b: rename `clay_data` → **`clay_docs`** — "where the supporting code for the doc
service stuff lives" (not `clay_documents`, and not merged into `clay_collection`). D-c: leave
`clay_guest_session` alone until `clay_rt_jco` exists, i.e. until a second engine actually needs the
shared contract. D-d: move `ApplicationRuntime` to `clay_host`.

**Landed.** The three renames above; `Cargo.lock` updated with `cargo update --workspace --offline`
so only those 3 packages changed and 218 third-party deps stayed pinned. All 45 internal `path`
edges now resolve through `[workspace.dependencies]`. All five `#[path]` sites removed;
`rg -n '#\[path' src/` is empty. `clay_host` split into `instance`/`collection`/`tests`.

The split needed no logic change, only visibility: `State`, `InstanceRecord`, `MountRecord`,
`Recovery`, `CollectionState` and the fields the invariant tests assert on (`State.{instances,
ending,closing,dependencies,recovery}`, `Recovery.pending`, `CollectionState.{replica,
subscriptions}`, `HostedCollection.state`, `CollectionReader.reader_id`, `Entry.id`, `Instance.id`)
became `pub(crate)`. Those tests read host internals deliberately — they assert that instance
values do not retain the host and that revocation prunes the right sender — so `pub(crate)` is the
correct reach, not a widening of the public API. `lib.rs` re-exports the same public names, so no
consumer's `use clay_host::…` changed.

**Blocked: D-d.** `ApplicationRuntime` has exactly one Dioxus coupling, but it is load-bearing:
`use crate::NativeComponents` — a field (`native: NativeComponents`), a construction
(`NativeComponents::new(&host)`), and an accessor (`pub fn native() -> NativeComponents`).
`NativeComponents` cannot follow it into the host: `Renderer = Rc<dyn Fn(&Arc<Mount>) -> Element>`,
`publish(impl Fn() -> Element)`, and `publish_surface` building `rsx! { MountedSurface { … } }` are
all Dioxus. Moving the runtime as-is would drag the renderer into the authority kernel, which is
the opposite of the goal. Everything else in `application.rs` (and all of `application/tests.rs`)
is Dioxus-free — it only names `clay_micro_core::micros` and `clay_host`.

So D-d needs one more decision, **D-h**: introduce a renderer-registry abstraction in the host and
make the runtime generic over it (`ApplicationRuntime<R: RendererRegistry>`, with
`clay_dioxus` implementing it for `NativeComponents`). That is precisely the "missing order of
indirection" the operator described, and it is also what unblocks phase 3 — but it is an API
change, not a move, so it was not done unilaterally.

**Landed: D-h (operator approved).** The renderer-registry seam is in. `clay_host` gained
`pub trait RendererRegistry: Clone { fn for_host(&Host) -> Self; }` (in `instance.rs`, re-exported
from `lib.rs`) — the kernel still names no concrete renderer. `application.rs` moved out of
`clay_dioxus` into `clay_micro_core::application` and became `ApplicationRuntime<R: RendererRegistry>`
(with `Driver<'a, R>` generic alongside it): the field is `native: R`, construction is
`R::for_host(&host)`, and `native()` returns `R`. This is the acyclic home the D-d blocker predicted
— `clay_micro_core` already depends on `clay_host`, so the runtime could not live in the host
without inverting that edge; the trait lets the host *define the seam* while the composition crate
*holds the runtime*.

`clay_dioxus/application.rs` is now a thin adapter, not the runtime: `impl RendererRegistry for
NativeComponents` (`for_host = NativeComponents::new`) plus
`pub type ApplicationRuntime = clay_micro_core::application::ApplicationRuntime<NativeComponents>`.
Because that alias hides the generic parameter, **none of the four external consumers changed an
import** — `clay_mdk/native.rs`, `dayframe_desktop/composition.rs`, `micro_sample/tests.rs`, and
`clay_dioxus/wasmtime/tests.rs` still `use …::application::ApplicationRuntime`, and `.native()` still
hands back a `NativeComponents`.

The moved unit tests (26 `ApplicationRuntime::new()` sites, all Dioxus-free) pin the generic to a
local `StubRegistry` via `type ApplicationRuntime = super::ApplicationRuntime<StubRegistry>`. That
stub is a *second* `RendererRegistry` implementor, so the tests prove the seam is real rather than
single-impl, and they avoid a `clay_micro_core → clay_dioxus` dev-dep cycle. Their self-references
were rewritten `clay_micro_core::micros → crate::micros` (a crate cannot name itself as an extern).
This also unblocks phase 3: the native authoring surface no longer has to depend on the renderer to
reach the runtime.

**New decision, D-g: what is `clay_dioxus` called?** The operator said `clay_rt_native`; this map
proposed `clay_rt_dioxus`. They are not the same claim. After `ApplicationRuntime` and
`wasmtime.rs` leave it, what remains (`SurfaceWriter`, `ComponentMount`, `MountedSurface`,
`NativeComponents`, `use_component_recovery`) is Dioxus-specific and renders *both* native/Blitz
and web. "native" also already means something else in this codebase — `clay_mdk`'s `native`
feature is the in-process backend as opposed to the Wasm guest — so `clay_rt_native` would overload
a word that is already taken. Deferred rather than guessed.

**Landed: top-down module reorder (phase 1 complete).** Operator chose the phase-1 remainder as the
next task. `micro_counter/lib.rs`, `clay_host/instance.rs`, `clay_host/collection.rs`, and
`clay_docs/lib.rs` were rewritten top-down: whole-item moves only, no body edits, grouped
types → constructors/main impls → trait impls/Drop → internals, with `// ── … ──` section banners
(the box-drawing banner style already used across the repo). Two consolidations fell out of the
reorder in `clay_docs`: the two `impl DocumentService` blocks merged into one (`new` first, then
the grant/mock surface) and the two `impl DocumentAccess` blocks merged (`check`, `watch`,
`mutate`, `document`); stray types found their section (`DocumentRef`, `Facet<T>`, `Right`, and the
private `Authority`/`WatchLease`/`State`/`CacheEntry`/`WatchRecord`/`PendingMutation` records), and
`#[cfg(test)] mod tests;` moved to the bottom of `lib.rs`, matching `clay_host`/`clay_collection`.
No API changed: the public surface, visibility, and every method body are verbatim.

**Verification.** `x/check-dayframe.ts --no-fail-fast -j 2`: **216/216 tests passed across 15
binaries**, 0 skipped, and its all-target/all-feature `--no-deps` Clippy gate exited 0 — run after
the renames, again after the `clay_host` split, again after the D-h `ApplicationRuntime` move
(60/60 in `clay_micro_core` alone, including the relocated `application::tests` on `StubRegistry`),
and again after the top-down reorder (`micro_counter` additionally re-checked under
`--target wasm32-unknown-unknown`; `clay_host` 23/23 and `clay_docs` 20/20 scoped runs).
Scoped `cargo clippy --locked --no-deps
--all-targets --all-features` over all 15 packages and `cargo fmt -- --check` over the same set are
clean. The three `dioxus-html` `file_data.rs` unused-`path` warnings from the wasm preparation
stage are the known fork baseline, not a workspace-clean claim.

**Environment churn, not defects.** The worktree rotation removed `../dioxus` again (restored with
`ln -sfn` to the intact `dayframe-gen-aa578c2c49e78545-10-a` workspace, no Dioxus edits) and wiped
the generated WIT deps, which broke `api_utils_rs` until `x/wit-fetch.ts` was rerun. One background
`setsid` check was killed mid-suite at 208/216 by the harness; the foreground rerun is the result
recorded above. No VCS commands were run.

**Landed: phase 3 — `clay_mdk::native` is renderer-agnostic.** The boundary break, done as one
turn of interface changes rather than shims:

- `native.rs` is generic over the host's seam: `Context<R: RendererRegistry>` (activation,
  registry, roots, granted configs), `provider<R: RendererRegistry + 'static>`, `output<T, R>`.
  `publish_component(impl Fn() -> R::Render)` wraps the host reference in the MDK's
  `ComponentRef::from_host`, so a micro's published presentation travels as MDK vocabulary
  everywhere — `SettingsSection.detail` already was. `compose_descriptor`/`routine_facets`
  became `pub(crate)`: the wasm backend shares them without widening the `daybook_types` surface.
- Micros constrain only the element they write:
  `micro_settings::activate<R: RendererRegistry<Render = Element>>`, `micro_sample::activate`
  likewise. No alias trait — the explicit bound is the contract, and it is what keeps the MDK
  silent about which renderer is installed.
- New `clay_mdk::view` (feature `view`): `ComponentSlot` and `use_collection` moved here from
  `native`. These are the two pieces that must resolve host state *through* the installed Dioxus
  renderer; micro views (`micro_settings/view.rs`) import `clay_mdk::view` and never `clay_dioxus`.
- `clay_dioxus/wasmtime.rs` moved to `clay_mdk::wasm` (feature
  `wasmtime = ["view", "dep:clay_rt_wasmtime"]`): `provider(application, manifest, package,
  contribute)` is the engine-backed twin of `native::provider`, and the guest's `ConfigGrant` is
  now converted through the same `routine_facets` path the native backend uses. The wasm module
  stays concrete on `NativeComponents` on purpose — projecting a wire surface into a host tree
  *is* the Dioxus adapter's job; a generic parameter there would be ceremony.
  `WasmMicro::surface()` now returns the MDK `ComponentRef`; host-side mounts unwrap with
  `.host()` (`dayframe_desktop/composition.rs`, `counter_tests.rs`).
- Feature rework in `clay_mdk`: `native` (host-side authoring, renderer-agnostic), `view` (its
  Dioxus half), `wasmtime` (engine providers), alongside the existing guest-side `wit`/`dioxus`.
  `dayframe_desktop` requests `["native","view","wasmtime"]`, `micro_settings`
  `["native","view"]`, `micro_sample` `["native"]`; feature unification is what enables the MDK's
  own wasm tests under the gate's nextest run.
- **Breaking removals/renames** (reported, not shimmed): `clay_micro_core::micros::{MicroManifest<C>,
  ConfigManifest}` deleted — once the wasm provider merged into the MDK, `clay_mdk::manifest` is
  the only manifest vocabulary and the compose-side copy was dead weight.
  `ApplicationRuntime::native()` renamed `renderer()` (field `renderer: R`) — the old name asserted
  a concrete backend the generic runtime does not have. `clay_dioxus` lost its `ApplicationRuntime`
  alias and its whole `[features]` section; consumers spell
  `ApplicationRuntime<NativeComponents>` through local aliases (`dayframe_desktop/composition.rs`
  uses `DesktopApplication`, because its Dioxus root component is already named `Application`;
  `micro_sample/tests.rs` and `clay_mdk/wasm/tests.rs` use `Application`).
- `micro_sample/tests.rs` composes against the real `NativeComponents` (dev-dep on `clay_dioxus`
  kept) rather than a stub registry: its assertions are about binding lifecycle through the same
  registry the desktop root installs. The `StubRegistry` in `clay_micro_core`'s own tests remains
  the second implementor proving the seam is not single-impl.

**Verification (phase 3).** `x/check-dayframe.ts --no-fail-fast -j 2` rerun after the boundary
break: **216/216 tests across 15 binaries**, 0 skipped, scoped all-features `--no-deps` clippy
exit 0; `cargo fmt --check` clean over the six touched crates.

Two traps recorded for the next rotation. A stale cargo fingerprint resurfaced mid-turn: the
`dayframe_desktop` graph reused a pre-D-h `clay_host` rmeta and reported phantom `E0437`/`E0407`
("`Render` is not a member of trait `RendererRegistry`") while the same crate compiled clean under
`-p clay_dioxus`; `cargo clean -p clay_host -p clay_dioxus` fixed it — when a trait member is
"not a member" in one graph but fine in another, suspect the cache before the code. And
workspace-wide `cargo clippy --all-features` fails in `wflow_sdk` (unfetched WIT deps: package
`townframe:wflow` not found) — pre-existing, outside the gate's package list, same family as the
`api_utils_rs` rotation wipe; its fix is a per-crate `wash wit fetch`, not a gate change.

**Landed: phase 4 (D-e, D-f, D-g).** Operator choices: D-e "guest-side `Context` first" (same
`activate(&Context)` shape on both sides, no WIT change), D-f "provider registry keyed by `MicroId`
with generic contribution routing", D-g "keep `clay_dioxus`".

- **D-e — `clay_mdk::guest`.** The Wasm guest now has the same authoring entry as a native micro:
  `pub struct Context { configs, presentation, changes }` with `config(name) ->
  Option<&ConfigDocument>` and `publish_component(impl Fn() -> Element + 'static)` (asserts a
  single publication per incarnation). `#[macro_export] export_micro!(activate)` generates the
  `Guest` impl and `bindings::export!`, so `micro_counter/lib.rs` is `fn activate(&Context) ->
  Result<(), DocumentError>` + `export_micro!(activate)` + a plain `fn counter(Option<Facet<u64>>)
  -> Element` — the rsx template is byte-identical, pinned by `expected_counter_batches`. The
  guest `Runtime` (activate/changed/lagged/completed/mount/unmount/dispatch) is private; `lagged`
  auto-resyncs the mirror and matches errors explicitly instead of discarding them. `GuestDom`
  moved to `guest/dom.rs`. New hooks `use_document_changes()` / `use_facet(&Facet<T>) ->
  MirrorSnapshot` read a `DocumentChanges(Signal<u64>)` context provided by `root(RootProps)`.
  `mirror.rs` grew the plumbing those hooks need: `MirrorState.notifier` (set once via
  `DocumentMirror::set_notifier`, fired from `changed/lagged/completed`), `MirrorWatch::release`,
  `resync_with`; `ConfigDocument` is `Clone` over `Rc<DocumentBinding>` with `facet::<T>(tag,
  key_id)`; `Facet<T>` carries `watch/current/resync/refresh/state/write`.
  **Constraint pinned by test** (`wasm_one_incarnation_shares_state_across_live_surfaces_and_remounts`):
  hooks that hold per-document state must live in the `render` closure (root scope) — a child
  component's state is per mounted surface, so putting the facet watch in a child would split
  state across surfaces of the same incarnation.
- **D-f — erased provisions + `Contributable`.** `clay_micro_core` gained the type-erased layer the
  typed API is sugar over: `Published::get_erased(version)`, `Provisions::get_erased(instance,
  contract, version)`, `MicroActivation::{descriptor(), require_erased, publish_erased(id,
  version, Arc<dyn Any + Send + Sync>)}`, `MicroRegistry::output_erased`,
  `ApplicationRuntime::output_erased`. `clay_mdk::manifest::MicroManifest` is **non-generic**
  (`contributions: Vec<Contribution { provision: String, payload: serde_json::Value }>`; serde
  derives throughout, `MicroId` transparent). `clay_mdk::native` adds `trait Contributable:
  Provision { fn contribute(&self, contributor: &Instance, payload: &serde_json::Value, surface:
  ComponentRef) -> Result<(), AccessError> }`; published values are stored as a private
  `Envelope { value, accept }` under the contract id (replaces `Provided<T>`), and
  `Context::publish_contributable<T: Contributable>` registers the acceptor. `pub(crate)
  Context::contribute(&[Contribution], ComponentRef)` is what `clay_mdk::wasm::provider` now calls —
  it panics if a contribution names a contract the micro did not require or one that accepts
  nothing (programming error in the manifest, not a runtime condition).
  `micro_settings_manifest::SettingsSections` implements `Contributable` (decodes `DeclaredSection
  { title }`); `micro_settings::activate` publishes via `publish_contributable`;
  `micro_counter_manifest::manifest()` declares `Contribution { provision: "settings.sections",
  payload: {"title": "Counter (Wasm)"} }` as data. `dayframe_desktop/composition.rs` now has
  `fn linked_providers(&DesktopApplication) -> [MicroFactory; 3]` and imports no contribution
  type — a micro delivered over the wire needs no symbol in the desktop crate.
  `clay_mdk/wasm/tests.rs` was rewritten around a local `Shelf` contributable contract
  (`shelf_provider`, `contribution(reject)`, `composed(manifest)`, `configuration()`, `shelved`),
  so the routing is proven against a second contract, not just settings.
- **Breaking changes (reported, not shimmed):** `clay_mdk::wasm::provider(application, manifest,
  package)` lost its `contribute` callback; `MicroManifest<C>` → `MicroManifest`;
  `micro_counter_manifest::Contribution` enum removed; `Provided<T>` replaced by erased
  `Envelope`; the counter's activation error string is now plain `"DocumentError::Denied"`
  (test updated). Not changed: `WasmMicro` is still a typed core `ComposeProvision`, so
  `app.output::<WasmMicro>()` keeps the typed path — the erased API is for contributions, not for
  the host's own view of a wasm instance.

**Verification (phase 4).** `x/check-dayframe.ts --no-fail-fast -j 2` after D-e+D-f: **216/216
tests across 16 binaries**, 0 skipped (counter and slots components rebuilt from the ported
guest), scoped all-features `--no-deps` clippy exit 0 over all 15 packages; `cargo fmt --check`
clean over the seven touched crates. `micro_counter` additionally clippy-clean under
`--target wasm32-unknown-unknown`. Rotation churn repeated (`../dioxus` symlink, `wash wit fetch`
in `api_utils_rs` plus re-tagging the two bare fences in the fetched `wasmcloud-postgres` WIT);
no VCS commands were run.

**Landed: parallel-PR prep (operator review follow-ups).** Four base fixes so two major-micro PRs
diverge only where they must:

- `linked_providers` returns `Vec<MicroFactory>` (was `[MicroFactory; 3]`): a new micro PR appends
  one line instead of editing array arity; both `counter_fixture.rs` copies are deleted.
- Malformed contribution payloads map to `AccessError::Denied` (was `.expect(...)` in
  `SettingsSections::contribute`): manifest payloads are wire-delivered data, not invariants.
  `DeclaredSection` gained `serde(deny_unknown_fields)`; the `Contributable` docs now state the
  contract (reject, don't panic), and the settings test pins wrong-shape and unknown-field denial.
- `x/check-dayframe.ts` derives its package list from `cargo metadata` (prefix `clay_`/`micro_`/
  `dayframe_`) instead of a hardcoded allowlist: a new micro crate joins the gate by naming
  convention. This picked up `dayframe_web`/`dayframe_mobile`/`dayframe_showcase` (216 tests
  across 18 binaries; those three have no tests but are now clippy-gated). The script also
  builds every prepared guest component from a `components` table and exports
  `DAYFRAME_<NAME>_COMPONENT` per guest — adding a wasm micro is one table entry.
- `WasmPackage::from_env(name)` in `clay_rt_wasmtime` (absolute-path + readability checks, error
  text pointing at the prepare step) and `clay_mdk::wasm::component_package(name)` (process-wide
  cached compile per guest name — one `Engine`/`Component` per process, reused across
  incarnations and test binaries' consumers). `dayframe_desktop`, `clay_mdk`'s two test suites,
  and `clay_rt_wasmtime`'s own tests all load through it; the duplicated fixture loaders are
  gone.

**Verification.** Full gate after the four fixes: 216/216 tests, clippy exit 0 over the derived
18-package set, fmt clean. Rotation churn repeated again this turn (the `../dioxus` symlink was
recreated at the wrong depth once — it must sit next to the worktree root, `c1b9fa66b59c4220/
dioxus`, not inside it; the `wasmcloud-postgres` WIT refetch needed its two opening fences
re-tagged ` ```text ` — note the *closing* fences must stay bare, CommonMark forbids info strings
on them).

**Landed: test ramp-up on the settled base (operator: property/snapshot-first, judgment over
letter).** Three audit agents inventoried clay_host/clay_micro_core, clay_collection/clay_mdk, and
clay_surface/micros/dayframe_desktop, proposed test classes, and — after sign-off — implemented
them in disjoint scopes. `proptest` joined `[workspace.dependencies]`; `insta` (already
workspace-level) gained dev-dep edges in clay_micro_core, clay_mdk, clay_surface, and the three
manifest crates. Gate: **282/282 tests across 18 binaries** (was 216), clippy exit 0, fmt clean;
every agent diff was re-read and its scoped nextest+clippy rerun independently.

What landed, by theme. **Model/oracle properties:** random host op sequences (start/close/bind/
publish/foreign/close-host) against an in-memory model pinning liveness, terminal close reasons,
and dependents-before-provider cleanup at arbitrary depth (`clay_host/tests.rs`); collection
contribution streams vs a contributor→entries model; a three-hop collection chain vs a BTreeMap
oracle with relay-replay reconstruction (`clay_collection/oracle.rs`, `#[cfg(test)]`); random
SurfaceTree mutation streams vs a model DOM, apply≡replay on every accepted batch, and
unfiltered streams never panicking and failing terminally (`clay_surface/tree/tests/property.rs`);
typed↔erased provision publication duality; reconcile idempotence/metamorphosis; supersession
fencing. **Snapshots (insta yaml):** the canonical manifest wire JSON for all three micro crates,
`DeclaredSection`, the report-stream grammar over a full lifecycle, the persisted composition
shape, and the render vocabulary (tags/attributes/listeners — vocabulary drift is now a
reviewable diff). **Panic-fence matrices:** adapter misuse (undeclared publish/require, double
publish, post-commit publish), mirror notifier exactly-once, `MirrorSnapshot::value` wrong-shape
panic pinned as the settled contract, close/panic semantics in clay_collection. **Composition
coherence:** one test statically pins that every configured instance names a linked provider,
binds declared requirements, routes bindings to real contracts, and declares parseable
contribution payloads — the drift surface two parallel PRs would otherwise only hit at runtime.
**Gardening:** tautological flags test folded, duplicate unknown-outcome test merged, vestigial
binding asserted, settings-manifest monolith split into focused tests (payload-denial assertions
preserved), one shallow replay example subsumed by the property and removed.

**One production change slipped the test-only mandate and was accepted on review:**
`MirrorWatch::release` is now idempotent (`Live | Lagged` guard, was `!= Revoked`) — a double
release previously double-withdrew and double-notified, breaking the exactly-once notifier
contract the new tests pin. One line, mechanism comment at the site, reported here.

**Findings filed as open product questions (not silently pinned):** text-node id mappings leak
in `SurfaceTree::drop_subtree` (fails closed today — latent, not exploitable); duplicate
*provision* ids in a descriptor are never validated (requirements are); `await_close` on unknown
ids silently returns while foreign identity panics; `try_start` during an active close cascade
succeeds and the fresh instance escapes the pending drain; `Failed` retry keys on supplier
instance identity, not configuration; `Provisions::resolve` conflates never-published/unknown/
withdrawn into `Revoked`; contribution payload validation happens at bind time (deny → rollback)
rather than fail-fast at activation; manifest version-bump discipline on breaking wire changes
is unspecified. Also of note: a superseded configuration revision is *never activated at all*
(pinned by the fencing tests).

### 2026-10-22 — merge `origin/main` and guardrail adoption

- Merged `origin/main` (the agent-guardrails refactor) into the phase 1–4 branch. Conflict
  resolutions: workspace `Cargo.toml` (our new members plus the desktop profile overrides),
  `flake.nix` (main's `src/nix/*` split, with `at-spi2-core` re-applied to both devshell input
  lists and `XDG_DATA_DIRS`), `.gitignore` (`specs` and `result`), and `Cargo.lock` regenerated
  with `cargo metadata`.
- Added a `guardrails.toml` for every new crate. Roles: `clay_surface`, `clay_collection`,
  `clay_docs`, `clay_host`, and `ui` are foundation; `clay_micro_core`, `clay_rt_wasmtime`, and
  `clay_guest_session` are runtime; `clay_mdk` and `clay_dioxus` are interface; `micro_*` are
  plugins; `dayframe_*` are applications. `allowed_dependencies` list only the path
  dependencies each crate actually uses.
- Renamed the `clay_mdk` platform feature `native` → `in_process`: a Cargo feature must not
  encode a platform. `dayframe_mobile` selects `desktop`, not `native`.
- Split large inline test modules into sibling files. To fit the function-size and type-size
  budgets, `SurfaceTree`'s validation and `WriteMutations` impls moved to `tree/validate.rs` and
  `tree/write.rs`; `WasmRuntime::mount` delegates guest mounting to `mount_guest_surface`;
  `ArcIndex` derives its layout in `ArcGeometry` and renders through `markers`/`accessibility`;
  `TileGrammarFoundation` builds its columns via per-column helpers; the desktop e2e test is
  split into a DOM-flow and a lifecycle-flow test over a shared `SettingsFixture`.
- Renumbered ADR `010-dayframe-dx-architecture` to `016` because main already owns
  `010-distributed-triage`; the scratch docs now carry the required `Status:`/`Owner:`/
  `Scope:`/`Updated:` block.
- Kept main's workspace-wide `resolver.feature-unification` instead of removing it. The Wasm
  guests now live in the root workspace, so unification leaks the native closure (`tokio/net`,
  `mio`) into `--target wasm32-unknown-unknown`; `x/check-dayframe.ts` therefore builds the two
  guest artifacts with `--config resolver.feature-unification="selected"`. That is the same
  isolation `guardrails check-matrix` applies per configuration (see `CONTRIBUTING.md`).

### 2026-10-22 — ADRs locked in before merge

- **ADR 017 (Accepted)** records the `clay_collection` substrate as built: the chained hop,
  the `Batch` waist, `Version { incarnation, seq }` epochs, segregated whole-row overlays and
  their retirement rules, opaque caller-authored demand, coverage, explicit roles and
  type-state write capability, lifecycle ordering, host-held contribution collections with
  sibling override collections, and facet-as-row documents. Its "Not yet implemented" list is
  the decided-but-unbuilt remainder (collection WIT, id namespacing, node adapter, write-path
  versions, backpressure, tombstone compaction).
- **ADR 018 (Proposed)** records the `wcmp` browser-engine investigation, previously only in
  conversation: engine-seam split into `clay_guest_session` + thin `clay_rt_wasmtime`/
  `clay_rt_wcmp`, async activation with synchronous presentation calls, instance-only
  publication, accepted lack of web fuel, `wasm-bindgen` alignment, shared composition crate,
  asset-fetched components, and web gate lanes. Accepted or rejected on the spike's result.
- **ADR 015** is now Accepted with as-built crate names (`clay_micro_core`, `clay_docs`,
  `clay_rt_wasmtime`, `clay_collection`, MDK feature `in_process`), the as-built MDK entry
  shapes and manifest-crate rule, a new rule **R9 "we do not break microspace"**, and
  amendment pointers where ADR 017 supersedes §4 wording (accepted overlays stay until base
  catches up; releasing a query does not evict rows). ADR 014 lists its amending ADRs.
- Open items from the parallel-PR prep review (provision-id validation, `await_close` on
  unknown ids, close-cascade `try_start`, `Failed` retry keying, `Provisions::resolve`
  conflation, bind-time payload validation, manifest version-bump discipline) stay tracked in
  §8 above; only the last is architectural and is now named in ADR 015 R9/§9.
