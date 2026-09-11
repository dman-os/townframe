# dayframe_dx — App Model

Source of truth for the re-implementation: `src/daybook_compose` (Kotlin
Multiplatform + Compose). This document maps the current app, derives the GUI
face contract (per ADR 010), and records the re-design directions. Generated
uniffi bindings (`commonMain/.../uniffi/`) were not read; the app-facing repos
are used as the interface surface.

## 1. Current runtime & navigation

### Lifecycle (App.kt + welcome/runtime.kt)

`Loading → Welcome → OpeningRepo → Ready | Error`, plus explicit shutdown.

- **Welcome**: list of known repos → select/open; clone from URL (QR scan, URL
  input, destination pick, initial-sync phase); create repo; forget repo.
- **Ready**: builds an `AppContainer` holding every repo handle
  (`DrawerRepoFfi`, `TablesRepoFfi`, `DispatchRepoFfi`, `ProgressRepoFfi`,
  `InitRepoFfi`, `SqliteLocalStateRepoFfi`, `PlugsRepoFfi`, `ConfigRepoFfi`,
  `BlobsRepoFfi`, `SyncRepoFfi?`, `RtFfi?`, `CameraPreviewFfi`) scoped to the
  repo context; screen ViewModels are keyed by container.
- Camera preview FFI preload is deferred until after bootstrap (avoids FFI init
  races).
- Camera is a major surface for Kotlin/Swift platform code: the capture pipeline should lean on native host code via the BoltFFI platform bridge (ADR 010 §4).

### Navigation (DaybookNavigation.kt)

8 destinations on a stack:
`Home, Capture, Tables, Progress, Settings, Drawer, DocEditor,
CloneShare`
(modal dialog). Per-destination chrome spec (top bar, back).

### Adaptive layout (AdaptiveAppLayout)

- `<600dp`: bottom navigation, list-only content.
- `<840dp`: navigation rail, list-only.
- `≥840dp`: permanent navigation drawer, list+detail.

Modals use a `BigDialog` strategy (full-screen on narrow, dialog on wide). A
dockable-pane system (`dockable/`) exists for richer layouts.

## 2. Screens & flows

- **Home** — widget soup today: a "WIP permissions" widget + menu (settings,
  clone, new doc, camera, mic, drawer). Three actions (new doc / camera / mic)
  all route to `Capture`.
- **Drawer** — the doc list + selected-doc editing: split (detail side-by-side)
  on expanded, single-pane on compact; `DrawerViewModel` holds doc list, loaded
  docs, selection, bundles; `DocEditorStoreViewModel` services sessions.
- **DocEditor** (largest surface, ~2.3k lines) — block-based editor:
  `EditorSessionController`, block details dialog, facet sidebar, selection
  state, facet display hints.
- **Tables** — windows / tables / tabs / panels workspace. The routed
  `TablesScreen` is currently a debug dump (plain text listing), while a rich
  tabbed chrome UI exists in `tables/` (`TablesRail`, `TabSelectionList`,
  `FloatingBottomBar`, `expanded.kt`/`compact.kt`) — split personalities.
- **Capture** — camera preview (platform `expect/actual`), QR overlays,
  frame-sample submission, OCR/QR bridges (`CameraQrOverlayBridge`,
  `CameraPreviewQrBridge` with session tokens).
- **Progress** — task list with states (`ACTIVE`…), per-task timeline of typed
  updates (`Amount`, …), detail pane, tag-prefixed queries
  (`listByTagPrefix("/mltools/model")`).
- **Settings** — config via `ConfigViewModel`: facet display hints per key,
  meta-table key configs, mltools provisioning (OCR / embed / LLM backend
  summaries + model download tasks).
- **Clone/Share** — dialog for URL-based clone/share of repos.

## 3. The data face (what the GUI needs — piece contract input)

Entities are `Uuid`-keyed: `Doc`, `Window`, `Table`, `Tab`, `Panel`,
`ProgressTask` + update timeline, `FacetKey` → `WellKnownFacet` (Note,
Body(order) blocks, Blob(digest/len/mime), image metadata, tags, facet-ref URLs
with commit heads, dmeta timestamps), facet display hints, plug custom-view
descriptors.

Every repo exposes the same shape the IRPC face must provide:

- list / get-by-id / set / create / delete per entity;
- typed event listener (e.g.
  `TablesEvent.ListChanged/WindowAdded/Changed/Deleted/…`, drawer events, config
  events, progress events);
- targeted refresh vs full refresh, coalesced in the UI (60 ms
  `CoalescingIntentRunner`), with optimistic local updates.

This is exactly the ADR 010 "event lanes" material: GUI-driving events (lane 1)
vs derived-state events (lane 2) should split along these listener surfaces when
the piece inventory lands.

## 4. Cross-cutting patterns worth keeping / killing

| Keep                                                                                                 | Kill                                                                                          |
| ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Coalesced targeted refresh (60 ms intent runner)                                                     | Debug-dump `TablesScreen`                                                                     |
| Error/Loading/Data tri-state per screen                                                              | "Capture" as one route for 3 intents                                                          |
| Typed event listeners, optimistic updates                                                            | Legacy permissions (overlay, storage read/write)                                              |
| Facet model + display hints                                                                          | `MagicWandService` (1.9k-line Android foreground-service hack — the daemon story replaces it) |
| Dockable panes, adaptive breakpoints                                                                 | Home widget soup                                                                              |
| Session-controller editor pattern (pure-logic, testable — see `EditorSessionControllerOrderingTest`) |                                                                                               |

## 5. Re-design directions (substantial)

1. **The Workspace is the product.** Tables windows/tables/tabs/panels — the
   tabbed-chrome UX is the long-term desktop shell. Dayframe_dx should make one
   coherent workspace (command palette, tabs, dockable panes) instead of two
   personalities and a debug screen.
2. **Capture as intent flow**: separate New Document (text/blank), Camera, and
   Mic intents with a single capture pipeline (frame → QR/OCR → facet/blob
   attachment).
3. **Home becomes a real surface**: recent docs, per-task progress, quick
   actions — not a menu of links. Widgets stay as the extension seam.
4. **Editor**: keep block/facet data model; re-design session UX around the
   testable `EditorSessionController`; the display-hint mechanism becomes the
   renderer-agnostic styling seam (ADR 010 §5).
5. **Plugs as UI extension points** (`FacetContentHost` → plugin custom views
   via `RtFfi`): needs a decision for Dioxus — plugin-provided RSX vs an
   embedded view host. This is an open question, not a solved one.
6. **Permissions modernization**: camera/mic/notifications only; the platform
   bridge (BoltFFI) owns this, not the UI.
7. **Mltools/AI surfaces** (OCR/embed/LLM backends + model downloads with
   progress) keep the progress/task model — good candidate for a node-side face
   (BFF-style) later.

## 6. Open questions (deeper reading still needed)

- DocEditor internals (block model, cursors, IME) — the biggest re-design risk
  surface.
- `MagicWandService` semantics before deciding its daemon replacement behavior.
- Welcome flow details (known-repo store, clone/QR specifics).
- iosApp (2 Swift files) and `wasmJsMain` will not be ported anytime soon (decision; both surfaces stay thin).
- Plug view protocol (`RtFfi` custom views) — contract to carry into the face.
