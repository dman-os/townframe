# Dayframe shell PR — ledger

Status: Living ledger. Operator decisions are marked as such; everything else is the PR owner's working state.
Owner: Claude (PR owner) with Sonnet subagents; operator reviews.
Scope: Branches `feat/dayframe-shell` and `feat/dayframe-android`; FDR 006, ADR 032, `clay_dioxus_kit`, `micro_shell`, `dayframe_mobile`.
Updated: 2026-11-05 (S1, S2, S2b and S3 landed)

Branch `feat/dayframe-shell`, stacked on PR #79 (`feat/better-collections`). Design: FDR 006
(shell grammar) and ADR 032 (shell micro and component kit), both accepted 2026-11-05.

## Operator decisions (2026-11-05)

- Most micros ship as Wasm; native only where needed. Wasmtime runs on Android.
- The shell is one native micro (`micro_shell` + backend-free `micro_shell_manifest`); its
  vocabulary is reused by other Clay compositions (a planned agentic-IDE app; build-time-configured
  images).
- Panel-instance identity is internal to the shell; start with what works (IntelliJ: tool windows
  vs editor tabs). The shell owns back/forward.
- Component library: `clay_dioxus_kit` (framework-named; other UI frameworks would get sibling
  kits). The PR owner sets its conventions.
- First consumer: Settings (home → Settings → section details).
- Android (`dayframe_mobile`) is in scope as a stacked/parallel PR whose job includes finding
  Dioxus-on-Android constraints.
- Testing: the web build is the broad integration surface (Playwright, Bombadil invariants);
  platform-specific tests per platform. The full `x/check-dayframe.ts` gate is released during the
  dev phase.
- Sync: general configuration syncs; open-context state (open tabs, their contents) stays on the
  device. The noun for an OS window is deferred.
- Command palette on mobile: micro-chosen; default two-finger swipe down.

## Workstreams

| Id | Item | Branch / worktree | State |
|----|------|-------------------|-------|
| S1 | `clay_dioxus_kit` foundation: conventions, Sections, ListDetails, Sheet, NavButton, showcase | `feat/dayframe-shell` (main checkout) | done (KIT-001..090 in the crate README) |
| S2 | `micro_shell` + manifest, navigation stack, panels collection, chrome button, sheets; Settings migration | `feat/dayframe-shell` | done: see "S2 notes"; native screenshots beyond home blocked on pointer input |
| S2b | The shell is the whole frame: edge-to-edge `Application`, toolbar/layout/lifecycle as panels and a Settings section, kit `Drawer`/`AppBar`/`ListRow`, insets, dev seed | `feat/dayframe-shell` | done except native screenshots: see "S2b notes" |
| S3 | Android: `dayframe_mobile` on the composition, Wasmtime on Android, constraints doc | `feat/dayframe-android` (isolated worktree) | done: APK builds; never run (no device); see `dayframe-android.md` |
| S4 | Web tests: Playwright flows, Bombadil invariants | `feat/dayframe-shell` | after S2's first flow |

## S1 review (PR owner)

- Accepted: headless/skin split (`headless/<name>.rs` + root skin), controlled props with domain callbacks,
  `WidthClass` breakpoints only in `width.rs`, host-passed `extent`, drag via a window overlay (no pointer
  capture), keyboard resize opt-in (`tabindex` panics Blitz), VirtualDom test harness that enforces the
  `clay_surface` wire vocabulary.
- Decided: on compact a sheet is full height (FDR 006 "full-screen on a phone"); the kit gets no
  `public-api.txt` yet (no `clay_*` crate has one).
- Open: how a guest learns whether its renderer takes keyboard focus (shell/host capability, S2); Sheet focus
  trap/restore (blocked on a focus API in Blitz; required before web ships it); design-language gaps listed in
  the kit README (breakpoints, scrim, layering, sheet sizes, motion, hit targets) need proper rules.
- To pin: the native showcase panics at startup with a live AT-SPI bus (stylo `data.rs:187` via blitz-dom
  `accessibility.rs:190`); `border` utilities draw nothing on Blitz; rounded `overflow-hidden` does not clip.
- The SIGTERM that killed S3's first `dx build` was S1's `pkill`.

## S3 findings (Android, from source; nothing has run)

- No IME composition with the `native-activity` backend (key events only): blocks editor/chat text input;
  GameActivity is the fix and is large.
- Accessibility is not compiled in (AccessKit's Android adapter is off in Blitz).
- Insets/edge-to-edge unhandled (`safe_area()` zeros), so content sits under system bars at target SDK 36.
- System back maps to `BrowserBack`, but predictive back may swallow it at SDK 36 unless the manifest opts out.
- Touch drags turn into scroll pans past 2 px unless `touch-action: none` (resize handles need it); no
  system-gesture exclusion rects, so edge drawers fight the back gesture.
- Debug APK 192 MiB (`libmain.so` 400 MB stripped); release size unknown.
- New crate `dayframe_native` shares the native composition root between desktop and mobile.

## S2 notes (2026-11-05)

What landed: `micro_shell_manifest` (contract), `micro_shell` (native micro: stack, frame, home,
sidebar, nav button, calls), Settings as a list panel, the composition mounting the shell as the
frame, desktop and web launchers feeding it the viewport and platform back, desktop and Playwright
flows, ADR 032 "First slice as built".

Decisions and rejected alternatives:

- **Opening a row.** A list panel row names `Panel.details`, a presentation the shell mounts keyed by
  `(panel, row)`; that presentation asks the shell which row is open (`opened` call). Rejected: the
  list passing a capability (calls carry data, not authority), the shell reading the list's collection
  (no binding), a Dioxus context (in-process only), Settings keeping `selected` itself (diverges from
  the stack after back/forward).
- **Stack.** History-like; overlays (sheets, compact sidebar) exist only on top and are discarded for
  good; another row of the open list replaces the row's entry; implicit home base. Pure model in
  `micro_shell/navigation.rs`, 9 tests. Not persisted: it lives in the shell incarnation, so it is
  device-local by construction and starts at home on restart. A persisted version needs a store path
  that never syncs (not the install/config documents).
- **Sheets are panels** with the `sheet` socket; entries naming a retracted panel are pruned by a task
  watching the panels collection.
- **Roots.** The shell registers one unpinned root (`shell.roots`); `Application` shows pinned roots
  (toolbar, layout) above it and the lifecycle panel below, as before. Settings is no longer a root,
  no longer eager or locked (`enabled_on_admission`, on demand); the shell is eager and locked. The
  "Open/Close Settings" toggle is gone. Layout and toolbar stay as pinned roots: mounting them as
  panels is later work (the toolbar has no list panel shape; the layout arranges panels itself).
- **Requirements.** Settings `requires` the shell's `navigate`, `opened` and `nav-button`;
  `InstalledMicro::satisfied_by(&shell)` binds each to the shell publication of the same name, so
  each launcher says it once.
- **Host inputs.** `micro_shell::HostInputs` (a Dioxus context `Application` provides): width of the
  frame (each owner of chrome subtracts its own padding), a platform-back counter, a `can_back`
  signal written by the shell, and `keyboard`. Rejected: measuring the DOM (Blitz panics).
- **Platform back.** Desktop: winit `KeyboardInput` `BrowserBack` (what S3 found Android's
  `KEYCODE_BACK` becomes) and the mouse back button, from the window event stream, because Blitz
  delivers neither to a focused element. Web: `popstate` with one guard history entry kept in front
  of the user while the shell can go back (the last back to home leaves one spare entry, so the
  next press is a no-op before the one after leaves the page). Not wired: Alt+Left, Escape. The
  Android launcher (`dayframe_mobile`, another branch) reuses the desktop mapping.
- **Keyboard capability for `Sections` (ADR 032 open item).** For the shell's own layout it is
  `HostInputs.keyboard` (web true, Blitz false), done. For guests, proposal: renderer facts as a
  small record in the guest's `initial-context` (`keyboard-focus`, later `pointer-capture`), surfaced
  as `Context::renderer()` and read by a hook; not implemented (needs a WIT change).
- **Back button placement.** The covered list's back button is out of reach on a compact host, so a
  details panel needs its own. The shell provides a `Placement` context around each mounted panel
  and the nav button renders nothing for a details panel beside its list on a wide host.
  Settings' section view carries the button in its header.
- **Kit fix.** `ListDetails` remounted the list whenever details opened or closed (different root
  structure), which would have released and restarted a provider on every open. The list is now the
  same element in every arrangement (single-pane `Sections` when wide); harness test added.
- **Dioxus finding.** A component that only forwards an `Element` prop (`children`) did not re-render
  when the element inside it changed key; `PlacedSlot` takes the `Capability` instead.
- **Sidebar.** Wide: a pane (hideable; the button then offers to show it). Compact: a `Sheet`, the
  kit having no left drawer. The sidebar lists Home and every list panel and mounts panels with the
  `sidebar` socket.

Verification run: `cargo clippy --all-targets` on `micro_shell_manifest micro_shell
micro_settings_manifest micro_settings clay_dioxus_kit dayframe_composition dayframe_desktop
micro_counter_manifest micro_sample`; `cargo nextest run` on the same (102 passed, Wasm fixtures via
`x/check-dayframe.ts --prepare-only` env); `cargo clippy -p dayframe_web --target
wasm32-unknown-unknown --config 'resolver.feature-unification="selected"'`; `cargo x guardrails
lint | api --all --check | doctor`; `cargo x guardrails check-matrix` for `micro_shell` (native,
wasm32-unknown-unknown), `micro_shell_manifest` (native, wasm32-unknown-unknown, wasm32-wasip3),
`micro_settings` (native, wasm32-unknown-unknown), `micro_settings_manifest` (wasm32-wasip3);
`pnpm -C src/dayframe_web run e2e` (5 passed). The full `x/check-dayframe.ts` gate was not run.

Native screenshots: a raw `cargo build` binary has no stylesheet (assets come from the `dx` build, not
run here for disk), so the native window is unstyled; it shows home wide and at 420 px, with the
width classification and sidebar behaviour correct. Settings and a section need a click (Blitz takes
no keyboard focus; AT-SPI is off by the startup-panic workaround), so those were captured in Chromium
at 420 and 1280 px instead. Open question for the operator: add a dev seed to start the native shell
on a given stack, or accept web captures for these states.

Design-language gaps: no left drawer (compact sidebar is a bottom `Sheet`); no app-bar component
(each panel hand-rolls `flex items-center gap-2` around the nav button); the frame height is a
560 px constant in `Application` (FOUND-008) pending a viewport-height input; list rows are plain
bordered buttons until the tile grammar lands; no selected-row state in Settings' list (the open
row is not highlighted).

Product questions: should going home from the sidebar push (back returns to where you were, as
built) or reset the stack? Should the shell's open-context stack persist across restarts on a
device? Should the toolbar and layout become panels (sidebar and sheet sockets) rather than pinned
roots?

## S2b notes (2026-11-05)

What landed: kit `AppBar`, `Drawer`, `ListRow` (+ `ListDetails { current }`); `HostInputs` height,
typed `Insets` and `start`; `Application` mounts the shell root at the window size, no card,
heading or 560 px constant; `RootRow.pinned` removed; the toolbar is a `sidebar` panel, the layout a
list panel (it requires the nav button); the lifecycle controls are a locked built-in micro
(`dayframe_composition::lifecycle`) contributing a Settings section; Settings rows are `ListRow`s
(Settings no longer requires `navigate`); `DAYFRAME_SHELL_OPEN` seed (documented in the native
dev-loop skill); `tailwind.css` regenerated.

Decisions and rejected alternatives:

- **Lifecycle section.** A composition-owned micro reading `CompositionRuntime` from the Dioxus
  context `Application` provides. Rejected: a cell holding the runtime inside the micro (supervisor
  to runtime reference cycle); the composition auto-adding it (breaks catalogs without Settings).
- **Insets** are applied by `Application` (it pads the frame and hands the shell the remaining size),
  so neither shell nor kit reads them. Android fills `HostInputs::insets`.
- **Toolbar** in the sidebar socket rather than a list panel: it has no list shape and belongs
  wherever navigation is.
- **Nav button on a wide host.** Back only while details are open beside the list, otherwise
  "open sidebar", so a hidden sidebar can always be brought back (found by the Playwright spec).
- **ListRow context** is in-tree only; guests cannot highlight the open row until the shell reports
  it reactively (needs a subscribable `opened`).
- **Packages** may require shell publications: both launchers apply `satisfied_by(shell)` to every
  discovered package.

Findings:

- Returning to the Wasm layout in the browser (leave it, come back) remounts a new guest
  incarnation whose panels stay "Loading section..." for roughly 30 s or more on `wcmp`, matching
  the 30 s collection long poll the desktop layout scenario guards with a paused clock. Not fixed
  here; the Playwright spec carries a 180 s timeout and a comment. Needs instrumentation in the
  guest session collection feed.
- Lifecycle, the layout and a Settings section share the main/details positions, so the browser
  suite cannot see a provider disappear beside a mounted sibling or the `Active` state; the desktop
  e2e tests (`runtime.state()`) carry those.

Verification: see the S2b report; `cargo x guardrails lint` clean after extracting `use_start` and
generalising `open_panel`; `check-matrix` finished for `micro_shell`, `micro_shell_manifest`,
`micro_settings`, `micro_settings_manifest`, `micro_toolbar_manifest`, `micro_layout_manifest`,
`micro_layout`, `clay_dioxus_kit`, `dayframe_composition`. `clay_mdk` tests need
`DAYFRAME_COUNTER_COMPONENT` and a context-component fixture.

Design-language gaps: motion (drawer does not slide), selected-row role, app-bar metrics,
directionality (leading = left), scrim, safe-area vocabulary.

Product questions: going home from the sidebar: push or reset? Persist the stack? Should lifecycle
be reachable beside the layout (a sheet)? Should a hidden wide sidebar persist across restarts
(it is device-local state today)?
