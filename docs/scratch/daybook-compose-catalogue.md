# daybook_compose catalogue

Status: Draft, read-only research (nothing built or type-checked)
Owner: operator + claytxt lane
Updated: 2026-11-18
Scope: What `src/daybook_compose` (Kotlin Multiplatform, 89 .kt files, 5 of them generated uniffi) does, what in the Rust/Dayframe stack covers each piece, and what blocks deleting it. Surveyed at `origin/main` (014c965a). Paths are relative to `src/daybook_compose/composeApp/src/` unless they start with `src/`, `x/`, `.github/` or `docs/`.

Reading note: "Rust side" means crates on `origin/main`: `dayframe_{composition,desktop,mobile,native,web,showcase}`, `micro_*`, `clay_*`, `big_*`, `day_cli`. There are no `intxt_*` crates on `origin/main` (the intxt phone viewer lives on `feat/phone-viewer-app`, see `docs/scratch/overnight-2026-11-18.md`), so nothing below is credited to them.

## 0. Headline

- The Kotlin app is the only UI over the `daybook_core` stack (drawer, tables, progress, plugs, rt/wflow, sync, mltools) through `daybook_ffi`. Dayframe/Clay is built on a different stack (`big_cabinet`, `clay_docs`, `big_node`), so "ported" below means "behaviour exists on the new stack", not "same backend".
- Dayframe today is a micro shell (frame, navigation, panels, settings sections, toolbar, counter demo). It has no doc list, editor, capture, progress, plug management or pairing UI.
- Status counts (by item rows in sections 1 and 2): Ported 4, Partial 10, Not ported 17, Obsolete 9 (total 40 rows; section 7 lists them).

## 1. Inventory and status: user-facing

Navigation keys are `DaybookNavKey` in `commonMain/.../navigation/DaybookNavigation.kt`: Home, Capture, Tables, Progress, Settings, Drawer, DocEditor, CloneShare (dialog). The pre-app flow is the welcome flow (`welcome/`).

| # | Item | Where | What it does | Status | Rust side / gap |
|---|---|---|---|---|---|
| 1 | Home screen | `home/home.kt`, `App.kt` (`defaultHomeScreenConfig`) | Widget list: "WIP permissions" widget plus a menu (settings, clone, new doc, camera, mic, drawer) | Obsolete | On the kill list in `docs/scratch/dayframe-app-model.md` §4; the shell micro's home panel and `micro_toolbar` are the replacement (`src/micro_shell/lib.rs`, `src/micro_toolbar/lib.rs`) |
| 2 | Capture screen: text mode | `capture/screens/CaptureScreen.kt` (`CaptureMode.TEXT`) | Creates a doc (`drawerRepo.add`) and opens the editor; mode persisted via tables state | Not ported | No new-doc surface on `origin/main` |
| 3 | Capture screen: camera mode | same file, `capture/`, `androidMain/.../capture/CameraPreview*.kt`, `desktopMain/.../CameraPreview.jvm.kt` | Live preview, QR overlay, frame sampling, `saveImage` as a blob doc facet | Not ported | ADR 016 §4 assigns camera to a BoltFFI platform bridge; none exists |
| 4 | Capture screen: mic mode | same file, line ~374 | Placeholder only ("Mic mode placeholder"); no recorder | Obsolete | Never implemented; `RECORD_AUDIO` is declared but unused |
| 5 | Drawer screen (doc list) | `drawer/DrawerScreen.kt` (`DocList`), `DrawerViewModel.kt` | Doc list with facet-type labels, selection, split detail on wide screens | Not ported | Data lives in `big_cabinet`/`clay_docs`; no list UI in Dayframe (intxt lane) |
| 6 | Doc editor | `ui/DocEditor.kt` (2.3k lines), `ui/editor/*`, `DocEditorStoreViewModel.kt`, `DaybookEditorSemantics.kt` | Block/facet editor: note editing, selection, block quick/menu actions, details dialog, drag handles, snackbars | Not ported | Biggest gap. `EditorSessionController` ordering test (`commonTest/.../EditorSessionControllerOrderingTest.kt`) is worth carrying as a spec |
| 7 | Facet content host | `ui/FacetContentHost.kt`, `ui/facets.kt` | Renders Note, Image(blob), generic, and plugin custom views | Not ported | Image/note rendering absent; plugin views are superseded (#9) |
| 8 | Plugin view renderer (ViewSpec) | `ui/view/DaybookViewRenderer.kt` | Renders `ViewNodeV1` (card, section, badge, amount, list, action group, button) produced by plug routines via `RtFfi` | Obsolete | Superseded by Clay micro presentations (`clay_surface`, `clay_dioxus`); the ViewSpec protocol is the old plug path |
| 9 | Tables screen (debug dump) | `App.kt` `TablesScreen` | Plain-text listing of windows/tables/tabs | Obsolete | Marked "Kill" in app model |
| 10 | Tables tabbed chrome | `tables/*` (rail, tab list, floating bottom bar, resizable drawer, `expanded.kt`/`compact.kt`) | Windows/tables/tabs/panels workspace UI, adaptive nav (bottom bar under 600dp, rail under 840dp, permanent drawer above) | Partial | `micro_shell` owns navigation stack, back and size-based arrangement (`HostInputs`); `micro_layout` arranges panels. No windows/tables/tabs data model or tab UI |
| 11 | Dockable panes | `dockable/*` | Draggable dock regions and dividers | Partial | `micro_layout` panel arrangement, no drag-dock |
| 12 | Progress screen | `progress/ProgressScreen.kt`, `ProgressViewModel.kt`, `ProgressUi.kt` | Task list with state/duration, per-task timeline of typed updates, detail pane, dismiss; tag-prefix queries (`/mltools/model`, `/sync/full`) | Not ported | No progress UI or Rust-side equivalent outside `daybook_core::progress` |
| 13 | Settings: shell + section list/detail | `settings/SettingsScreen.kt` | Two-section list/detail (Plugs, MLTools), responsive | Partial | `micro_settings` owns a section vocabulary and mounts section panels via the shell (`src/micro_settings/lib.rs`); new sections are Clay micros |
| 14 | Settings: Plugs list and import wizard | `SettingsScreen.kt` (`PlugsSettingsPane`, `ImportPlugWizardDialog*`) | Lists installed plugs with counts; import wizard (choose directory, preview, import) | Not ported | Plug manifests/routines still on `daybook_core`/`plug_*`; Clay installs micros by catalogue (`dayframe_composition::InstalledMicro`), not this flow |
| 15 | Settings: MLTools provisioning | `SettingsScreen.kt` (`MltoolsSettingsPane`), `ConfigViewModel.kt` | OCR/embed/LLM backend summary, "provision mobile defaults", model-download task rows | Not ported | `mltools` crate is only used by `daybook_core` and `daybook_ffi` |
| 16 | Settings: facet display hints | `ConfigViewModel.kt` (`setFacetDisplayHint`), `ui/editor/EditorFacetKeys.kt` | Per-facet-key display hint config consumed by the editor | Not ported | Dies with the editor unless intxt keeps hints |
| 17 | Welcome: known repos, open, forget | `welcome/welcome.kt` (`WelcomeScreen`, `WelcomeRepoDetailScreen`), `welcome/runtime.kt` | Lists known repos, opens one, forgets one, usability check | Partial | `day init`, `day daemon`, `day status` (`src/day_cli/cmds/`) and the node dir (`src/big_node/lib.rs`); no GUI repo picker (node model has one data dir) |
| 18 | Welcome: create repo | `welcome.kt` (`CreateRepoScreen`) | Create a new repo at a chosen location | Partial | `day init` (`src/day_cli/cmds/init.rs`); no GUI |
| 19 | Welcome: clone from URL / QR scan | `welcome.kt` (`CloneUrlScreen`, `CloneQrScannerScreen`, `CloneLocationScreen`, `CloneSyncScreen`) | Enter ticket URL or scan QR, choose destination, watch initial full sync with per-peer progress | Partial | `day peers add <ticket>` (`src/day_cli/cmds/peers.rs`) and `big_node` peers/tickets (`src/big_node/peers.rs`, `info.rs`). No QR scan, no first-sync progress UI |
| 20 | Clone/Share dialog | `welcome.kt` (`CloneShareDialogContent`), `BigDialog.kt` | Shows own ticket URL as text and QR PNG (`getTicketWithQrPng`), copy to clipboard | Partial | Node can emit a ticket (`day status`, `big_node::NodeInfo`); no QR rendering or share UI |
| 21 | BigDialog / adaptive layout | `BigDialog.kt`, `layouts/*`, `App.kt` (`AdaptiveAppLayout`), `ChromeState.kt` | Full-screen vs dialog modals, chrome spec (top bar, back) | Ported | `micro_shell` frame, back and `HostInputs` sizing; Wry/WebView paths per `docs/scratch/clay-on-the-node.md` |
| 22 | Theme | `theme/Theme.kt`, `AppConfig.theme` | Dark/light Material theme | Ported | Dayframe design tokens (`docs/design-language/`, `dayframe_showcase`) |
| 23 | Loading and error screens, runtime state machine | `App.kt` (`LoadingScreen`, `ErrorScreen`), `AppRuntimeViewModel.kt`, `FfiRuntimeState.kt` | Loading, Welcome, OpeningRepo, Ready, Error, graceful shutdown | Partial | `dayframe_native::Shell` has loading/failure/mount; no welcome state |
| 24 | Desktop shell: window, SIGINT/SIGTERM graceful shutdown | `desktopMain/.../main.kt` | Window sizing, signal handlers, shutdown hook closing the FFI ctx | Ported | `src/dayframe_desktop/main.rs`, `dayframe_composition::lifecycle` |
| 25 | iOS app | `iosMain/*`, `../../iosApp/` (2 Swift files) | Compose iOS host | Obsolete | Decision in `docs/scratch/dayframe-app-model.md` §6: not ported anytime soon |
| 26 | Web (wasmJs) app | `wasmJsMain/*` | Compose-for-web host, stub camera | Obsolete | `src/dayframe_web` |
| 27 | Desktop packaging | `x/build-appimage-dayb.ts`, `tools/appimage-builder/daybook-jvm.AppImageBuilder.yml` | AppImage of the JVM app plus `libdaybook_ffi.so` | Not ported | No Dayframe desktop packaging exists on `origin/main` |
| 28 | Kotlin tests | `commonTest/*`, `desktopTest/*` (5 files: facet parsing, editor ordering, doc editor / settings smoke, view renderer) | Compose UI and logic tests | Obsolete | Go with the app; only the editor ordering test is a spec worth keeping (#6) |

## 2. Inventory and status: Android integration and background

Manifest: `androidMain/AndroidManifest.xml`. Only one activity (MAIN/LAUNCHER) is declared; there are **no** share targets, intent filters for VIEW/SEND, deep links, widgets, content providers or boot receivers. The Rust Android app is `src/dayframe_mobile/` (INTERNET only, Wry WebView by default since 2026-11-18).

| # | Item | Where | What it does | Status | Rust side / gap |
|---|---|---|---|---|---|
| 29 | MainActivity and runtime permissions | `androidMain/.../MainActivity.kt` | Edge-to-edge, requests CAMERA / RECORD_AUDIO / POST_NOTIFICATIONS / storage via `PermissionsContext`, tracks overlay permission | Not ported | `dayframe_mobile/MainActivity.kt` is a thin Dioxus host; ADR 016 §4 puts permissions in a BoltFFI bridge that does not exist yet |
| 30 | MagicWandService overlay | `androidMain/.../MagicWandService.kt` (1.9k lines), `MagicWandLifecycleOwner.kt` | Foreground service (types remoteMessaging/camera/microphone/specialUse) drawing a draggable "puck" overlay with Camera/Notes quick actions, persistent notification with Stop action, `SYSTEM_ALERT_WINDOW` | Obsolete | "Kill" in app model §4 ("the daemon story replaces it") |
| 31 | Persistent/FGS notification and stop receiver | `MagicWandService$StopServiceReceiver`, manifest receiver `ACTION_STOP_SERVICE` | Notification channel and stop action for #30 | Obsolete | Dies with #30 |
| 32 | Android keyring init | `androidMain/.../io/crates/keyring/Keyring.kt` | `System.loadLibrary("daybook_ffi")` and `initializeNdkContext` so the Rust keyring store works | Partial | Secrets are in `src/secrets_rs`; Dayframe Android stores under app data dir (`dayframe_mobile/main.rs`) and has no Android keystore binding |
| 33 | Camera: CameraX preview and vendor extensions | `androidMain/.../capture/CameraPreview.android.kt`, `CameraPreviewEnhanced.kt`, `CameraExtensionsState.kt` | CameraX preview, extensions (HDR/night etc.), frame analysis | Not ported | See #3 |
| 34 | Camera: desktop/iOS/web | `desktopMain/.../CameraPreview.jvm.kt` (nokhwa via `daybook_ffi/camera.rs`), `iosMain`, `wasmJsMain` | Platform previews, QR analyzer FFI (`CameraQrAnalyzerFfi`) | Not ported | Rust `nokhwa` path exists only inside `daybook_ffi` |
| 35 | File pickers | `welcome.kt`, `SettingsScreen.kt` (FileKit dialogs) | Choose clone destination and plug directory | Not ported | No picker in Dayframe |
| 36 | Clipboard / QR image decode | `welcome.kt`, `ImageDecode*.kt` | Copy ticket, decode PNG | Not ported | See #20 |
| 37 | Coalesced refresh and event listeners | `CoalescingIntentRunner.kt` (60 ms), repo `on*Event` listeners | Targeted refresh, optimistic updates | Ported | Collection watches and projections in `clay_collection`/`clay_docs`; pattern kept in app model, not code |
| 38 | Background sync | `SyncRepoFfi` in `AppContainer` (`App.kt`), `daybook_core::sync`, `big_sync_core` uniffi | Sync service started with the repo | Partial | `big_node` runs sync for `day daemon`; Dayframe Android must attach to/hold the node (`docs/scratch/clay-on-the-node.md`), landed only on branches |
| 39 | Plug runtime and workflows (`RtFfi`) | `daybook_ffi/rt.rs`, `daybook_core::rt`, `wflow*` | Runs plug routines, dispatches workflows (OCR, embeddings, labels) | Not ported | Ledger: workflow engine "still needed", decision pending (cartridge feature or wflow) |
| 40 | Blob store, image tools, embeddings | `BlobsRepoFfi`, `daybook_core::{blobs,imgtools,index}` | Encrypted blob pin pipeline, thumbnails, embeddings | Not ported | `big_blobs` is the replacement; ledger says compare before deleting `daybook_*` blob pipeline |


## 3. Non-UI pieces the Kotlin app owns

| Piece | Where | Notes |
|---|---|---|
| uniffi bindings (generated) | `commonMain/.../uniffi/{daybook_ffi,core/daybook_core,types/daybook_types,big_sync/big_sync_core}.kt`, `uniffi/types/WellKnownFacet.kt` | Generated by `x/gen-ffi-dayb.ts` from `target/debug/libdaybook_ffi.so`; marked generated in `.gitattributes`, excluded in `.coderabbit.yaml` |
| FFI crate | `src/daybook_ffi/` (`lib.rs`, `ffi.rs`, `camera.rs`, `rt.rs`, `listener_bridge.rs`, `repos/{blobs,config,dispatch,drawer,init,plugs,progress,sqlite_local_state,sync,tables}.rs`, `uniffi-bindgen.rs`, `public-api.txt`) | Serves only the Kotlin app: nothing else depends on it |
| uniffi cargo features/derives | `src/daybook_core` (`uniffi` feature, `uniffi.toml`, derives in `app.rs`, `config.rs`, `plugs.rs`, `tables.rs`, `progress.rs`, `local_state.rs`, `repos.rs`, `drawer/types.rs`, `rt/*`), `src/daybook_types` (`doc.rs`, `view.rs`, `manifest.rs`, `macros.rs`, `lib.rs`), `src/big_sync_core` (`ids.rs`, `lib.rs`) | Only needed for the Kotlin bindings; `daybook_types` is also used by clay/dayframe crates and must stay (without the feature) |
| Rust crates called through FFI | `daybook_core` (drawer, tables, progress, plugs, config, blobs, sync, rt, local_state), `big_sync_core`, `mltools` (hf_hub), `daybook_types`, `am_utils_rs`, `utils_rs`, optional `nokhwa` | `daybook_core` stays alive for `daybook_cli` and `plug_{test,plabels,dayledger}_manifest`, `wasm_utils_rs` |
| Storage | Rust side: `daybook_core` SQLite local state, repo config (known repos), Automerge drawer; Kotlin side has no storage of its own except Navigation3 saved state | No Kotlin-side database |
| Config | `AppConfig(theme)` in `App.kt`; repo config via `getRepoConfig`; mltools config JSON parsed in `ConfigViewModel.kt` | |
| Build | `build.gradle.kts`, `composeApp/build.gradle.kts`, `settings.gradle.kts`, `gradle/`, `gradlew*`, `config/detekt/`, `composeApp/detekt-baseline-main.xml`, `kotlin-js-store/`, `iosApp/` | Targets: android, desktop (JVM), iosArm64/Simulator, wasmJs |

## 4. Claytext concerns (what AGENTS.md refers to)

AGENTS.md (line 50) says the Kotlin app "stays in the tree until its remaining claytext concerns are settled". No document defines "claytext concerns" by that name. `grep -ri claytext docs/ .agents/` finds only `docs/scratch/nodule-runtime-ledger.md` (the #84 freeze: "claytext can run on nodules"), and the sentence was introduced by commit d5e5156e (2026-11-10, "docs: take the Kotlin app off the validation path"), whose body says only "The code stays until its claytext concerns are settled". The concrete related decisions are:

- **Operator answer 2026-11-12** (commit 4e73e635, `docs/scratch/nodule-runtime-ledger.md` D46 on `feat/claytxt-node` and siblings, not on `origin/main`): "Dayframe replaces the Kotlin app. `daybook_ffi` and `daybook_compose` go together once Dayframe covers it, and the `daybook_*`/`plug_*` removal order stands." Order: move clay/dayframe off `daybook_types`; decide workflows (cartridge feature or wflow); port or drop the plug routines; drop `daybook_ffi` with `daybook_compose`; then delete the rest. Still needed from `daybook_*` per that ledger: workflow engine (`rt.rs` + `wflow*`), plug routines (plabels, dayledger), blob pin/encryption pipeline, sync bootstrap/clone/tickets, tables/progress/index and facet cache, image tools and embeddings.
- **Docs that treat Kotlin as the spec to re-implement**: `docs/adrs/016-dayframe-dx-architecture.md` (Dayframe is "the re-implementation of `daybook_compose`"; `daybook_ffi` stays as-is for Kotlin; BoltFFI for platform code), `docs/scratch/dayframe-app-model.md` (kill/keep table, open questions: editor internals, MagicWand semantics, welcome flow details, plug view protocol).
- **Open items I can infer** (these are my reading, not a recorded list; the operator should confirm what "claytext concerns" means): (a) the editor, doc list and capture have no Clay micro yet (the intxt/node lane owns anchors and datums); (b) Clay on the node (`docs/scratch/clay-on-the-node.md`) is accepted but the phone/desktop host composition that attaches to the node is not on `origin/main`; (c) plug import/runtime/ViewSpec has no Clay equivalent; (d) the "micros must replace plug views" path (`FacetContentHost` to Clay presentations) is undecided; (e) camera/permissions need the BoltFFI platform bridge.
- `docs/scratch/agent-guardrails-open-items.md` already states the Kotlin app "is being deleted and replaced with Rust" and puts every Kotlin item out of scope.

## 5. To delete safely

### 5a. Blockers (priority order)

1. **Confirm what "claytext concerns" means** and record it (AGENTS.md sentence has no referent).
2. **Decide the fate of what only the Kotlin app can reach**: doc list, editor, capture (text/camera), progress, plug import, MLTools, repo create/clone/open, ticket QR. Each is "Not ported" above (#2, #3, #5, #6, #12, #14, #15, #19, #20). Either port, or the operator declares them dropped.
3. **Doc editor** (#6, #7, #16): the largest and riskiest surface; keep `EditorSessionControllerOrderingTest` semantics as a spec.
4. **Plug runtime and workflows** (#39, #40): `daybook_core::rt`/`wflow*` and plug routines are listed as still needed; deleting `daybook_ffi` is fine but `daybook_core` must stay until that decision lands.
5. **Platform bridge** (#29, #33, #34, #35): camera, permissions, file pickers, QR scan, keystore on Android have no Rust-side replacement (ADR 016 §4 BoltFFI is a direction, not code).
6. **Desktop packaging** (#27): no AppImage path for Dayframe desktop.

Non-blockers: Home (#1), mic (#4), MagicWand (#30, #31), Tables debug screen (#9), ViewSpec renderer (#8), iOS (#25), wasmJs (#26), Kotlin tests (#28) are Obsolete.

### 5b. References to remove or update when deleting

| Area | Reference |
|---|---|
| Tree | `src/daybook_compose/` (incl. `iosApp/`, `gradle/`, `config/detekt/`, `kotlin-js-store/`) |
| Rust crates | `src/daybook_ffi/` (root `Cargo.toml` workspace member `./src/daybook_ffi`, `Cargo.lock` entries, `src/daybook_ffi/public-api.txt`, `guardrails.toml`); `uniffi` features/derives in `daybook_core`, `daybook_types`, `big_sync_core` (and `daybook_core/uniffi.toml`); `nokhwa` dep goes with `daybook_ffi` |
| x/ scripts | `x/check-dayb.ts`, `x/dev-d-dayb.ts`, `x/run-d-dayb.ts`, `x/build-a-dayb.ts`, `x/run-a-dayb.ts`, `x/build-appimage-dayb.ts`, `x/gen-ffi-dayb.ts`, `x/gradle.ts`, `x/lint-kt.ts` (verify `x/dayframe.ts` Gradle mentions are about `dx`, not this app: it is) |
| CI | `.github/workflows/`: no Kotlin or Gradle job exists; `builds.yml` has only `DAYBOOK_COMPOSE_PROFILE` (naming only, builds `daybook_cli`, rename optional); `checks.yml` mentions android only for disk cleanup |
| Nix | `src/nix/devshells.nix` Android SDK/NDK and `GRADLE_OPTS` (keep: `dayframe_mobile` needs the NDK; only the Gradle/aapt2 option may go if `x/dayframe.ts android` does not need it, check first); `src/nix/checks.nix` comment mentioning detekt/gradle; `src/nix/packages.nix` comment |
| Repo config | `.gitattributes` (uniffi linguist-generated line), `.coderabbit.yaml` (uniffi path exclusion), `.pre-commit-config.yaml` (commented-out detekt hook), `tools/appimage-builder/daybook-jvm.AppImageBuilder.yml` |
| Guardrails | `src/xtask/guardrails/repository.rs` (skips `daybook_compose` in its walk; remove the entry after deletion) |
| Docs | `AGENTS.md` (Kotlin app sentence, `check-dayb.ts` mention), `CONTRIBUTING.md` (`dev-d-dayb.ts`, `build-a-dayb.ts` commands), `README.md` lines 23 and 35 (Compose Multiplatform), `docs/adrs/014`, `016` (re-implementation framing and "stays on uniffi"), `docs/scratch/dayframe-app-model.md`, `docs/scratch/agent-guardrails-open-items.md`, `docs/DEVDOC/{todo,log}.md` (historical, leave), ledger D46 |
| Not Kotlin-app | `src/dayframe_mobile/MainActivity.kt` is the new app's host and stays |

## 6. Rust crates that exist only to serve the Kotlin app

| Crate / feature | Why |
|---|---|
| `src/daybook_ffi` | Sole consumer is the Kotlin app (no other crate depends on it) |
| `uniffi` feature in `daybook_core`, `daybook_types`, `big_sync_core` | Only enabled by `daybook_ffi` |
| `nokhwa` optional dep and `daybook_ffi/camera.rs` | Camera preview/QR analyzer for Kotlin only |

Not on this list: `daybook_core` (used by `daybook_cli`, plug manifests, `wasm_utils_rs`), `daybook_types` (used by clay/dayframe), `mltools` (used by `daybook_core`).

## 7. Final tally

Rows 1-40 (sections 1 and 2):

| Status | Count | Rows |
|---|---|---|
| Ported | 4 | 21, 22, 24, 37 |
| Partial | 10 | 10, 11, 13, 17, 18, 19, 20, 23, 32, 38 |
| Not ported | 17 | 2, 3, 5, 6, 7, 12, 14, 15, 16, 27, 29, 33, 34, 35, 36, 39, 40 |
| Obsolete | 9 | 1, 4, 8, 9, 25, 26, 28, 30, 31 |
