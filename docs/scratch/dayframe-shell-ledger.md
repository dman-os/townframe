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

## Operator play-test notes (themelab, Wry, from 2026-11-11)

Collected while the operator plays; nothing here is being fixed until the list is gone through
together.

| Id | Defect | What is known | State |
|----|--------|---------------|-------|
| P1 | The Sheet and Drawer specimens in the themelab's Components tab aren't interactive. | Not reproducible on a fresh build. Read-through of the kit's Sheet/Drawer (scrim `onclick`, stacking, `--df-layer-sheet`, tailwind classes) and the specimens found no wiring fault; the new owner-loop tests (`sheet::tests::an_owner_can_dismiss_the_sheet_by_scrim_or_escape_and_reopen_it`, `drawer::tests::an_owner_can_dismiss_the_drawer_by_scrim_or_escape_and_reopen_it`: scrim dismiss, Open reopens, Escape dismiss, reopen, through a signal owner like the specimens) pass; a fresh non-hot-patched Wry build renders the open Sheet specimen correctly (scrim over the Open button, bottom panel). Real pointer clicks could not be injected, and the report came from a hot-patched lab, so the operator's next play on a fresh build confirms. **Escape gap (real):** the panel `div` is not focusable and nothing focuses it, so Escape never reaches `onkeydown` when the panel has no focusable child (the Sheet specimen). Fix attempted (`tabindex=-1` + `onmounted` `set_focus(true)`, WAI-ARIA dialog pattern) and reverted: dioxus-desktop's `set_focus` fails in Wry (`OperationFailed(Deserialize("invalid type: null, expected a boolean"))`) because `interpreter.setFocus` in `native.ts` returns nothing while `desktop/src/element.rs` queries a `bool`; with the `.expect` it panics the lab on mount. Needs a fork fix (`setFocus` returns `true`) before it can land; the patch is kept at scratchpad `focus-on-mount.patch` (commit 3ae417d3 holds the original). | owner tests landed; Escape blocked on the dioxus-desktop fork |
| P3 | (Expected, on `feat/clay-standard-dioxus` after merging the egress lane, `106ef590`.) A Sections handle drag that leaves the Sections box may lose its capture surface. | The egress policy refuses raw `style` strings and `position: fixed` inline, so Sections' gesture overlay was rewritten as per-property styles and became `position: absolute` inside the Sections box instead of viewport-wide. Unverified in a browser; dioxus has no pointer-capture API, so the overlay is the only capture surface. | open |
| P4 | Regression: the desktop (Wry) theme lab rendered a blank white window after the dioxus repin to `ba8feef7` (`clay/shadow-roots`). | Cause: the fork patch declared `eventOrigin`/`walkParent` in both `core.ts` and `native.ts`; desktop concatenates `core.js` and `native.js` into one module script, so the duplicate declarations failed the whole script. The fork's harness loaded the files separately, so it missed this. Fixed in fork `34ca2b0f` (helpers only in `native.ts`; harness now also runs the concatenated desktop form in Chromium and WebKit). Repinned on `feat/clay-standard-dioxus` (`c195b237`) and merged into `feat/pierre-dioxus-host` (`b2336974`); verified 2026-11-11 by Wry screenshots of the Components and Diffs tabs. | fixed |
| P2 | ArcIndex crashes the lab. | Cause found: `ArcIndex` captured its items with `use_signal(\|\| items)`, so after a lab tab switch (Components has 1 section, Tokens 5) it kept the first tab's labels and count, with focus and announced state carried across. No panic reproduced through tab switches (stale lengths are only `get`/clamp reads); the stale strip is the likely "misbehaves while navigating". A real panic exists for long lists: `pull_for` calls `f64::clamp(-top, height - top - span)` and `span > height` past ~40 breakpoints at the engaged spacing (1.35x), so a 60-item list panics on the first press. Fixed in 4ca4a1ed: items are the prop (no signal; the live-region text derives from the announced index), focus is clamped to the count, `spacing(height, count)` shrinks past `REFERENCE_COUNT` so any list spans `SPAN_FRAC` of the window, and `ThemeLabNav` keys `ArcIndex` by tab. Tests: `rendered::the_strip_follows_the_items_the_parent_passes` (VirtualDom, labels and aria-valuemax follow tab changes), `long_lists_fit_and_pulling_never_panics` (1..500 items), `every_tab_lists_a_section` (the `assert!(!items.is_empty())` stays; no tab can pass an empty list). | fixed |

## Workstreams

| Id | Item | Branch / worktree | State |
|----|------|-------------------|-------|
| S1 | `clay_dioxus_kit` foundation: conventions, Sections, ListDetails, Sheet, NavButton, showcase | `feat/dayframe-shell` (main checkout) | done (KIT-001..090 in the crate README) |
| S2 | `micro_shell` + manifest, navigation stack, panels collection, chrome button, sheets; Settings migration | `feat/dayframe-shell` | done: see "S2 notes"; native screenshots beyond home blocked on pointer input |
| S2b | The shell is the whole frame: edge-to-edge `Application`, toolbar/layout/lifecycle as panels and a Settings section, kit `Drawer`/`AppBar`/`ListRow`, insets, dev seed | `feat/dayframe-shell` | done except native screenshots: see "S2b notes" |
| S3 | Android: `dayframe_mobile` on the composition, Wasmtime on Android, constraints doc | `feat/dayframe-android` (isolated worktree) | done: first run on a device 2026-11-12 (ZTE 8050, Android 13, arm64): the shell launches, Home → Settings navigates, and the Wasm counter micro runs under Wasmtime (enabled from Micro lifecycle, three taps → 3, writes accepted); see `dayframe-android.md` |
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
  guest session collection feed. (Fixed in S5: the cause was in the `wcmp` driver, not the feed.)
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

## S6 notes (2026-11-05): `nested or concurrent __wasilibc_task_hook Start` on `wcmp` (Bombadil finding 2)

- **Root cause.** The MDK's task hook assumed a guest instance never has two tasks holding a stack at once (one static
  stack, "a held stack means two tasks are running at once, which the component model never does"). That is false for a
  callback task suspended in the *middle* of its callback: a blocking built-in (a synchronous `subtask.cancel` of a host
  import, `thread.yield`, `waitable-set.wait`) parks the task's thread with its frames on its stack, no hook fires, and the
  component model lets a synchronous-typed export ignore the instance's entry gate, so the engine runs the host's next
  sync entry meanwhile. In the layout this is: the viewer hides the sample panel, the layout drops the futures awaiting the
  panel (`subtask.cancel` of a host import, which `wcmp` can only finish on the next turn that polls host tasks, so the
  built-in blocks), and the host's document delivery (`call_changed`) is already waiting; it starts inside the blocked
  task's turn, finds the stack held, panics, and the incarnation is withdrawn ("Component withdrawn.").
- **Engine difference.** `wcmp` admits the sync export (documented in `call_concurrent.rs`: "a synchronous export's task
  ignores the entry gate ... inside the first's nested turn when the first blocks"). Wasmtime 50 answers `Pending` on
  the first poll of the same call (confirmed by a probe: `call_mount` returns "suspended on its first poll" while a
  relay task waits in `waitable-set.wait`), and Wasmtime's host-import cancel does not block at all, so the overlap never
  occurs there. So the bug is in `clay_mdk`'s hook (a wrong invariant), not in the `wcmp` fork or the driver; the fork
  behaves as its documentation says. The driver's `first_poll` is unrelated (S4 already ruled out the S5 ring).
- **Evidence.** (1) The flake is a race: the S4 scenario failed 1 in 6 serial runs and 15 of 48 under 8-way load; paused
  clock never (0 of 40), because the race is host task interleaving. (2) The failure vanished from every build that
  touched the hook's code or statics (a 12-entry ring, a stack canary): 0 of 190; so instrumentation had to be host-side.
  (3) Host-side ring of every `context.get/set` plus the origin (source line) of every blocking built-in, in the fork
  (branch `s6-hook-trace`, commit `b0dffa8`, instrumentation only): the failing START of thread T1.35 comes right after
  thread T0.3 resumed (`prev_context_0` was its parked state), ran, and stopped mid-execution with its stack pointer deep
  in the init stack (1047888, not back at 1048576) at a `try_block` whose origin is `executor/cancel.rs:186`
  (`subtask.cancel`); no BLOCK/FINISH hook for T0.3 follows; the host's realloc tasks (T2.10-16) and T1.35's START do.
- **Fix** (`clay_mdk`): `task_stacks.rs` hands the static stack to the first task and a heap-allocated 512 KiB stack
  (16 at most, crash beyond) to any task that starts while it is held; freed by the stack pointer the finishing task
  carries (an unknown one crashes). wit-bindgen's per-task state moves from the single `CURRENT` pointer to a cell beside
  each stack (found from the address of a local in `get/set_task_context`), because with two live tasks one pointer cannot
  be right and no hook fires for the mid-callback park. BLOCK/RESUME still park the state in context slot 0. Rejected:
  context slot 1 for the state (the toolchain's `wasmparser` accepts only `context.get/set 0`: "immediate must be zero").
- **Regression coverage.** The desktop scenario `e2e::layout::hiding_a_native_panel_leaves_the_layout_mounted` (wcmp over
  Wasmi, six hide/show rounds): 11 of 12 runs fail without the fix under 4-way load, 0 of 48 with it under 8-way load. It
  is not deterministic and a paused clock does not make it so (the clock is not what varies), so it is the scenario and
  not the gate; the hook's side is pinned deterministically by the unit tests of `task_stacks` (overlapping tasks get
  distinct stacks, per-stack state, refusal of unknown stacks). A deterministic engine-level scenario (a fixture whose call
  blocks in `waitable-set.wait` while the host mounts) failed without the fix on both engines with exactly this panic, but
  I could not make it complete with the fix on `wcmp` (the blocked task never resumes after the gate opens, with or
  without the mount: a separate `wcmp` question, see Open) and Wasmtime refuses the sync entry by design, so it was dropped.
- **Bombadil, and why the CI step stays non-blocking.** Local runs after the fix: `e2e:all` green once, then 4 full Bombadil
  runs (2 widths each): 3 green, 1 red; a 420-only loop: red on its 2nd run; the same 420-only loop at `HEAD` (hook unfixed) was red 2 of 8,
  one being Finding 3 below (fixed in S8) and one the sample's greeting panic below. So the fault Finding 2 describes is gone
  but a different violation remains at the same rate, and flipping the step to blocking would make CI flaky. Left as the
  brief's last item undone on purpose.

### Finding 3 (fixed in S8): hiding a shown panel withdrew the layout in the browser engine

Not a transient and not the layout's `Loading` branch: after "Show" then "Hide" of the sample panel the layout's region is
permanently "Component withdrawn." (no app bar, no toggles). `navigationAlwaysReachable` only caught it at a phone width because
at a wide width the sidebar's own "Hide sidebar" button keeps the property true over a dead layout. The S6 hypothesis (a bare
`Loading layout…`) was wrong; that branch did lack the app bar and was fixed separately. Root cause and fix: S8 notes.

### Finding 4 (fixed): the sample's greeting field panicked on a multi-byte character

Bombadil, typing `🀃|Ä0RｒbE…` into the sample detail's Greeting field (browser build): `micro_sample/view.rs:65` did
`String::truncate(120)` at a byte offset inside a character, which panics (`is_char_boundary`), failing
`noConsoleErrors`. The clamp is now by characters (`clamp_greeting`, limit 120) with a unit test (a string whose byte 120
is inside a four-byte character; fails with the old code).

### Open (S6)

- `wcmp`: a callback task parked in `waitable-set.wait` (via `wit_bindgen::block_on`) or `thread.yield` (blocking) on the
  Wasmi provider did not resume after the host released what it waited for, in a lifecycle-conformance probe (not
  committed). Unexplained; not needed by anything in the app today.

## S8 notes (2026-11-05): the layout withdrawn after hiding a panel in the browser (Bombadil finding 3)

- **Reproduction.** Deterministic in the browser build, no Bombadil needed: enable the layout, open Layout, "Show Sample micro
  panel", "Hide Sample micro panel": the layout's region becomes "Component withdrawn." at every width, no console output. The
  desktop scenario `hiding_a_native_panel_leaves_the_layout_mounted` passes because Wasmi completes the call that the browser
  engine suspends. Now a Playwright spec (`e2e/layout.spec.ts`, "hiding a shown panel leaves the layout mounted ...", 420 and 1200
  px, three rounds each).
- **Chain, by temporary panics in the web build** (all reverted): the layout instance is closed `Requested` by its own driver;
  `clay_guest_session::drive` closed it on `DeliveryOutcome::Trapped`; that came from `Err(Suspended)` in `clay_rt_wcmp`'s
  `deliver`: the guest's `document-events.changed` callback returned `Pending` on its first poll. `first_poll` dropped the half-run
  call and the host withdrew the incarnation. No trap, no dependency close, no supervisor close was involved.
- **Why it suspends.** The callback renders (`guest.rs` `mutate` -> `dom.update`), which drops the sample's `PresentationSlot`
  and so cancels the slot's pending host call. In `wcmp`, a synchronous `subtask.cancel` of a host callee only marked the host
  task aborted and then waited for a *later* turn to drop its future (`executor/cancel.rs`, `abort_host_callee`); on a provider
  that hands waits to its driver (the browser) that wait suspends the calling thread, which made the sync export `Pending`.
  Wasmtime and the native test backends resolve it within the built-in.
- **Fix (wcmp fork, branch `s8-sync-host-cancel`, rev `a5db353b`).** A synchronous cancel takes the aborted host task out of the
  set and drops it at once, with the store lent as in a turn, so the subtask is resolved before the built-in returns
  (`HostTaskSet::take_aborted`). Pinned in the root `Cargo.toml`/`Cargo.lock` and `src/wasm_utils_rs`. Confirmed by the same
  probe failing before and passing after, then by the Playwright spec. The fork's regression test
  (`it_completes_a_synchronous_cancel_of_a_host_callee_on_the_first_poll`) polls the call once and asserts it is `Ready`; it
  passes on the native test backends before the fix too, because only the browser's provider defers the wait, so it documents the
  contract and does not by itself reproduce the browser failure.
- **No more silent withdrawals.** A sync export that returns `Pending` on its first poll now panics naming the export and the
  incarnation (`first_poll`). Guest traps (`presentation.mount/dispatch/unmount`, a document delivery, a failed store drive) are
  recorded with their message by `Host::record_fault` and the supervisor reports it as the micro's `Faulted` cause instead of
  "ended outside supervision". `DeliveryOutcome::Trapped` carries the cause (the Wasmtime runtime too). The Wasmtime runtime keeps
  its own `Suspended` handling.
- **`micro_layout`.** The `Loading layout…` state now renders inside the same frame as every other state, so it keeps the app
  bar's sidebar/back button (`framed`, with a unit test).
- **Property fix.** After the kit's motion rules the drawer's scrim fades in from opacity 0, and `checkVisibility({opacityProperty})`
  made `overlaysBelongToTheirPanel` fail within seconds on every 420 px run (the "Dismiss Sidebar" scrim read as absent). The
  frame no longer reads opacity.
- **Bombadil.** Ten consecutive 60 s runs at 420 px green after the fixes. The CI step is blocking now.

## S7 notes (2026-11-05): `invalid SlotMap key` at `blitz-dom form.rs:38` (compact scenario)

- **Root cause (Blitz, `blitz-dom 0.3.0-beta.4`).** `DocumentMutator::process_added_subtree` records every added
  `button|input|select|textarea|fieldset|object|output` in `form_nodes`; `flush` (run when the mutator drops) calls
  `reset_form_owner(id)` on each, which does `self.nodes[id]` (`form.rs:38`). Nothing removes an id from `form_nodes` when
  the node is dropped (`remove_and_drop_node`, `remove_and_drop_all_children`), so a form control that is added and dropped
  within one mutator lifetime leaves a freed slot-map key behind and `flush` panics. The autofocus step in the same `flush`
  guards the same hazard with `get_node(id).is_some()`; the form step lacks it. (`controls_to_form` also keeps entries for
  dropped controls, harmless but a leak.)
- **Why our tree reaches it, only compact.** One `render_immediate` batch is applied by one mutator. In the failing batch
  (traced with a patched copy of `blitz-dom` that logs adds and drops) Dioxus builds the Home panel's templates, one of whose
  roots holds the app bar's `Open sidebar` hamburger button (`NodeId(24)`, in-doc), clones them into the real tree
  (`Open sidebar`, `Open Layout`, `Open Settings` = 43, 51, 53) and then `Remove`s the original template subtree
  (`NodeId(19)`, 10 nodes, containing 24). The hamburger exists only below the side-by-side width, which is why the wide
  scenarios pass. This is the normal Dioxus template protocol, not a usage error in the shell or kit, so no change in our
  code avoids it honestly.
- **Fix (dman-os fork).** Upstream `main` (104 commits past beta.4) has no fix. `dman-os/blitz` branch
  `form-nodes-dropped-in-batch` (`ffaf2bd0`, from tag `v0.3.0-beta.4`): dropping a node unqueues it from `form_nodes`
  (both mutator drop paths), and `remove_node_from_tree` forgets a dropped control's `controls_to_form` entry (node ids are
  slab indices and get reused, so skipping dead ids in `flush` alone could reset an unrelated node). The root `Cargo.toml`
  patches every `blitz-*` crate to that rev, since blitz-dom reaches its siblings by path inside the fork's workspace; the
  lock gains the fork's vendored `accesskit_xplat` and `stylo_taffy`. No upstream PR (owner's call).
- **Regression.** `dayframe_desktop::e2e::blitz_flush_skips_a_button_dropped_in_the_same_batch` (create a `button`, append
  it to the root, drop it, drop the mutator) panicked on beta.4 and passes on the pin; the compact scenario passes.
  `cargo nextest run -p dayframe_desktop`: 16/16.

## S5 notes (2026-11-05): the 30 s layout stall on `wcmp`

- **Root cause.** The C7c doorbell works, but under `wcmp` nothing ticked the store after it rang. A guest task spawned from a
  synchronous entry (`mount`, `dispatch`, `unmount`, a document delivery) is adopted when the guest's executor is next polled; the
  doorbell wakes that executor with a stream write inside the store (wit-bindgen's inter-task wakeup), which involves no host
  waker. `clay_rt_wcmp` runs a store's event loop only in `WcmpIncarnation::tick`, driven by `IncarnationWake`, and the sync
  entries never rang it: the new task (a layout panel's `PresentationSlot`) waited for an unrelated wake, which for a layout is its
  collection feed's long poll (30 s). Wasmtime is unaffected (its store is driven by the host's own concurrent runtime), which is
  why C7c's paused-clock test passed. The feed and the host side of the collection were not involved.
- **Evidence.** New in-crate scenario `dayframe_desktop::e2e::layout::returning_to_the_wcmp_layout_mounts_its_panels_without_waiting_for_a_timer`
  runs the real `Application` with the packages on `wcmp` over Wasmi (the desktop composition now takes the package engine as a
  parameter: `composition::assemble`), under `start_paused`. Per-checkpoint virtual times before the fix: first mount of the two
  panels +30 s, return to the layout +30 s more (60 s total); the same walk on Wasmtime: 0.4 s wall, no virtual time.
- **Fix.** `clay_rt_wcmp::first_poll`, the one place every sync entry goes through, now rings the incarnation's waker after the
  call returns, so the driver ticks the store once per entry and the doorbell's wake is delivered. One extra bounded
  `run_until_stalled` per entry, no loop. The test above fails (60 s) without the ring and passes with it.
- **Spec.** `e2e/layout.spec.ts` lost its 180 s timeout and the comment calling the latency a finding.
- **dman-os wcmp fork.** Untouched; the fault was in this repo's driver.

## S4 notes (2026-11-05): Bombadil on the web build

**Decision: keep.** It earned its place on the first day: two real faults in the shell stack that the scripted Playwright flows
had not walked into, found in minutes, with a repro trace each. It is wired in non-blocking; the second fault was fixed in S6 but a third (transient) violation remains.

- **What it is.** Bombadil 0.7.10 (Antithesis, MIT, Rust) explores a page by clicking, typing, scrolling, reloading and going
  back, and checks temporal-logic properties (`always`, `eventually(...).within(...)`, `next`) over the states it reaches. The
  specification is a TypeScript module whose extractors run inside the page. It drives Chromium itself over CDP (`CHROME` names the
  binary); it is not built on Playwright.
- **Install, reproducible.** The npm package `@antithesishq/bombadil`, pinned `0.7.10` in `src/dayframe_web/package.json` and
  `pnpm-lock.yaml`. It ships the prebuilt linux-x64 binary and the TypeScript types, so no cargo build and no Nix build; the
  existing `pnpm install --frozen-lockfile --filter @townframe/dayframe_web` installs it. Rejected: `cargo install` of a rev (a
  cold Rust build in CI for no gain), the release binary by `curl` (a second pin to keep beside the lockfile).
- **Harness.** `e2e/bombadil.spec.ts` (tagged `@bombadil`) spawns Bombadil per width (420 and 1024) against Playwright's
  `webServer`, with `CHROME=chromium.executablePath()` so it drives the flake's pinned browser. `pnpm run e2e` skips it,
  `pnpm run bombadil` runs only it, `pnpm run e2e:all` both; `x/dayframe.ts bombadil` wraps the script.
  `BOMBADIL_TIME_LIMIT` (default `60s`) is the exploration per width. Exit 2 (a violation) fails the test with the property, the
  state and the `--reproduce` command in the message; the trace is in the test's output directory.
- **Seeds.** Bombadil 0.7.10 has no seed option (checked in `--help` and the source). Runs are not seeded; a run is reproduced from
  its trace (`--reproduce <dir>`, best effort) and `bombadil browser shrink` minimizes one.
- **Properties** (`src/dayframe_web/bombadil/shell.ts`, roles and accessible names only, each with its FDR 006 / ADR 032 reason at
  the site): `oneMainRegion`, `navigationAlwaysReachable`, `backReturnsToTheList` (a click that lands on "Back" while a details
  region is open must leave that list on screen and the details gone), `overlaysBelongToTheirPanel`, `loadingSectionsFinish`
  (`eventually(no "Loading section…").within(15 s)`), `shellBoots`, plus Bombadil's defaults for page errors, rejected promises,
  console errors and HTTP errors. An action generator waits while the shell has not booted, otherwise Bombadil stops with "no
  actions available" during the Wasm compile. Mutation check: each of the first five was made to fail on purpose (inverted
  predicate) and Bombadil reported it within 40 s, so none is vacuous; the 1 ms bound on `loadingSectionsFinish` fires, so
  "Loading section…" does occur during exploration.
- **Property mistakes.** None needed fixing after the fact: the first violation was a real dead end, not a property error. The
  design choices that avoided false alarms: outermost panel regions only (a guest's own sections are regions too), regions inside
  a dialog excluded, a list covered by details is `aria-hidden` on a phone (so it does not count), and the nav-button property
  accepts "Hide sidebar" because a wide window with the sidebar showing has no nav button in the panel by design.
- **Sheets.** No micro opens a sheet yet, so the sheet half of `overlaysBelongToTheirPanel` is exercised only through the
  drawer (the compact sidebar dialog) until one does.

### Finding 1 (fixed): an unavailable or loading panel had no way out

A panel's own app bar places the sidebar/back button. Packages are admitted disabled, so on a fresh install home's "Layout" opens a
panel whose micro has no presentation: a placeholder with no button. On a phone that is a dead end except for the platform back; on
a wide window with the sidebar hidden, the same. Found at the first run (`navigationAlwaysReachable`). Fix (`c91afeb2`):
`PresentationSlot` takes optional `placeholder_chrome`, rendered above the Loading and Unavailable placeholders, and the shell passes
an app bar with the panel's title and the button. Rejected: a Dioxus context for the chrome (nested slots inside a mounted panel,
such as the layout's own, would each draw a second app bar); asking the micro (it is the absent party). Regression:
`e2e/shell.spec.ts` "a panel that is unavailable still offers the way back or the sidebar" fails without the fix.

### Finding 2 (fixed in S6): hiding a native panel in the Wasm layout takes the whole layout down on `wcmp`

Repro in the browser: enable the layout in Settings, open Layout, "Show Sample micro panel", then "Hide Sample micro panel": the
layout's region becomes "Component withdrawn." and stays. Bombadil reached it on about one run in four (it needs the lifecycle toggle,
then the layout, then show and hide). Hiding the Counter's panel (a Wasm guest) is fine; it is the native sample's.

Evidence so far:
- Same on the desktop composition with the packages on `wcmp` over Wasmi (the browser's engine), and not on Wasmtime: a scenario
  `Layout::start_on(wasmi_runtime().await, &["dayframe.sample"])`, click `Hide Sample micro panel`, wait for the panel to go, wait
  for `Show Sample micro panel`, passes on Wasmtime and hangs on Wasmi. It is not committed because a hanging test cannot be
  skipped; the body is the four lines above in `dayframe_desktop/e2e/layout.rs` style.
- The guest panics: `internal error: entered unreachable code: nested or concurrent __wasilibc_task_hook Start`
  (`clay_mdk/lib.rs`, `task_stack_alloc`). The call that hits it is a document delivery (`document_events::call_changed`, the
  arrangement facet echoing the write) made after the hide's `dispatch`, so the shared task stack was still marked in use after
  `dispatch` returned. The host then closes the incarnation; the shell's `MountedComponent` for the layout shows its mount not live
  (`signal=false mount=false path=true`, instrumented and reverted).
- Not the S5 ring: the same panic with `waker.wake_by_ref()` removed from `first_poll`; no host-side nesting of `first_poll`
  (instrumented depth counter, reverted).
- Next step: record the hook sequence (`INIT_STACK_IN_USE`) into a static ring, not `eprintln!` (the hook runs on a 1024-byte
  stack; printing from it deadlocked the run), and dump it at the panic; suspect a sync export whose `prev_context_0` is 0 at
  `SYNC_FINISH` (`task_stack_free` then keeps the stack held), which only the `wcmp` hosts produce.

### Cost and CI

Bombadil is one dev dependency with no browser download (`pnpm add` took 12.5 s here) and adds nothing to `pnpm run e2e`. Measured
locally against a running server, `pnpm run bombadil` takes 2.0 minutes (2 x 60 s of exploration). The CI step runs after the e2e step
in the same job, where the web server restarts first (the build is already in the target directory; not measured, assume 1 to 2
minutes), so expect roughly 3 to 4 minutes added. It stays `continue-on-error` after
Finding 2 was fixed (S6): see Finding 3 there.

### Open

- Findings 2 and 3 are fixed (S6, S8); the CI step is blocking.
- Bombadil's output directory holds the trace for `--reproduce`; the e2e artifact upload already collects `test-results/`, but
  Playwright clears it at the start of each run, so a run of the plain e2e step followed by the Bombadil step keeps only the latter's.
- Properties to add as the shell grows: forward after back, a resized pane never below its rule, the open row marked
  `aria-current` exactly while its details are open.

## K2 notes (kit gates, CI, Blitz pins)

Kit conventions made mechanical: `clay_dioxus_kit/conventions.rs` (a `cfg(test)` module; 11 tests over the crate's own
sources as text, comments stripped) plus what `guardrails.toml` already did. The README's new "Mechanical enforcement"
section maps every rule to what fails; rules not listed stay review-only. Checked: KIT-001 (guardrails refuses any workspace
crate; a test refuses any other runtime dependency), 002, 010/011 (headless names no class and imports no styled part; styled
uses semantic tokens only), 012, 040 (breakpoints only in `width.rs`; no DOM measuring or window queries), 052 (`tabindex`
only in the Sections opt-in), 071, 072, 080, 090, and that every cited `KIT-nnn` exists. Verified by mutation: a palette
utility, an arbitrary value, a stray breakpoint literal, `tabindex` and `get_client_rect` each fail with file and line, and a
workspace dependency fails `cargo x guardrails lint` (`ERROR_DEPENDENCY_BOUNDARY`). No xtask change: neither guardrails
mechanism reads source text, and a crate-local test does.

Gate-forced edits: `headless/nav_button.rs` mentioned Blitz without a `# Renderer limits` section (KIT-090): section added
(doc only). Findings left for K1 / a rule decision, not fixed: (1) KIT-011 says the one allowed bracket form is
`data-[state=...]`, but `sections.rs` uses `group-data-[state=dragging]:...`; the gate allows the `group-` form and the rule's
wording should say so. (2) The README describes skins as `<Component>Skin` structs; `NavButton` and `ListRow` pass a `const`
class string / a `class` prop instead (KIT-003/011 are review-only, so nothing fails). (3) `NavButton`'s chevron is drawn from
`border-b-2 border-l-2` and the showcase `Frame` outlines with `bg-border p-px` because of the S1 border report; see below.

CI: no gap found, no workflow change. `tests` runs `cargo llvm-cov nextest --workspace --all-targets` in `ci-desktop` after
`x/check-dayframe.ts --prepare-only`, so the kit (incl. the new conventions tests), `micro_shell` and the `dayframe_desktop`
e2e scenarios all run there (nextest `ci` profile, 3 retries; the new pins are deterministic). `rust-target-matrix` runs
`check-matrix` per target and covers the new crates: the kit declares native / wasm32-unknown-unknown / wasm32-wasip3,
`micro_shell` and `micro_shell_manifest` native + wasm32-unknown (+ wasip3 for the manifest), `dayframe_showcase` has no
`[builds]` and so defaults to native (xtask guardrails matrix.rs: empty builds means native). `pre-commit` runs guardrails lint / api /
doctor. Added CI minutes: none (the conventions tests run in ~0.05 s; the pins ~0.6 s, plus compiling three more blitz crates
as dev-dependencies of `dayframe_desktop`, which it already links).

Blitz pins, `dayframe_desktop/e2e/blitz_limits.rs` (document level, blitz fork rev ffaf2bd):

- (a) AT-SPI startup panic: reproduced without a bus. `BaseDocument::build_accessibility_tree()` on a document that has
  not had its first `resolve()` panics at stylo `data.rs:187` (`ElementStyles::primary()` unwraps `None`), reached from
  `blitz-dom/src/accessibility.rs:190` (`is_hidden_from_accessibility_tree`). The window asks for the tree
  (`InitialTreeRequested`, `blitz-shell/src/application.rs:82`) before the first resolve. A resolved document, and an element
  under `display:none`, are fine. Pinned as the defect. A fix needs one of: `get_primary()` in `accessibility.rs:190`
  (unstyled counts as visible), or `resolve()` before `build_accessibility_tree()` in the `InitialTreeRequested` arm. When
  fixed the first assertion flips; invert the test.
- (b) borders and (c) rounded `overflow-hidden`: NOT reproducible at the scene level. With the shipped `tailwind.css` painted
  through `blitz-paint` into the CPU renderer (`anyrender_vello_cpu` 0.18, the same `anyrender` 0.14 `PaintScene` blitz
  uses), `border`, `border-b-2 border-l-2 border-current` and the showcase `Frame` (nested rounded clips) all paint
  correctly. So the S1 observation lives below the scene: the native window renders with `anyrender_vello_hybrid` 0.11 (GPU;
  it has no headless image renderer, so no hermetic pin is possible), or in the `asset!` CSS pipeline `dx` applies to the
  stylesheet. Those tests are therefore regression guards of the scene, not pins of the defect. Still owed: a windowed
  GPU run of the showcase `Frame` to say which, and if it is the renderer, an upstream issue against vello_hybrid; if it is
  the asset pipeline, compare the `dx`-processed stylesheet against the source. Until then the `bg-border p-px` outline and the
  border-built chevron are unverified on the native renderer.
- Environment note: a fresh worktree needs the gitignored `src/*/wit/deps/` trees (copied from the main checkout here) before
  `dayframe_desktop` compiles; the first build in a new target dir took roughly 10 minutes.

## K1 notes (kit design language, `feat/dayframe-kit-design`)

What landed: kit rules `KIT-100` to `KIT-109` (README: width classes and what changes at each,
one z-order scale, the scrim, sheet sizes, app-bar metrics, 48 px hit targets, the selected row,
motion, safe areas), implemented in the kit and the foundation tokens. New tokens: `layer.*`
(number), `motion.*` (new `duration` and `cubicBezier` types in `x/design-tokens.ts`, with
self-tests), colour roles `scrim`, `selected`, `on-selected` (the Material adapter in `tokens.css`
maps `selected`); `ui/styles/motion.css` (keyframes and `animate-*`); `INTERACTION-007`
(reduced motion). Kit: `Layer` (public, mirrors `layer.*`, tested against the JSON), shared
`SCRIM`, `Drawer { width }` (drops itself on a wide host; the shell no longer repeats the check),
per-class `Sheet` skins (`w-96` medium, `w-128` expanded), `AppBar` (`h-14`, `data-state`
leading/bare, `Layer::AppBar`), `NavButton` `size-12`, `ListRow` `min-h-12` and `bg-selected`,
`Sections` handle 24 px on `Layer::Raised` with panes as `Layer::Content` contexts.

Decisions and rejected alternatives:

- **Layers as tokens, inline `var()`.** Headless parts write `z-index:var(--df-layer-x)` from the
  `Layer` enum; a test reads `semantic.tokens.json` so the two cannot drift, another fails on any
  numeric `z-index` in a headless part. Rejected: Rust constants alone (invisible to themes and
  hosts that render a toast).
- **No exit animations.** They need `animationend`, which Blitz and the guest wire do not deliver,
  and timers are forbidden. Enter only; documented in `KIT-108`.
- **Safe areas stay with the host.** A guest mounts the kit in its own tree and cannot read an
  inset; the host pads its frame (S2b). Cost: scrims stop at the safe rectangle.
- **Handle hit area 16 to 24 px**, not 48: it overlaps pane edges.
- The scrim role overrides Material's opaque `--color-scrim` in the Tailwind theme (the Dayframe
  token file loads after `material.css`), so `bg-scrim` is the 32% role, not black.

Findings: the S1 review says "on compact a sheet is full height", but the committed skin was and
remains `h-5/6` (a full-height sheet has no scrim to tap); kept 5/6, see questions. The committed
`assets/tailwind.css` is prettier-formatted; no prettier here, so the diff was generated as a
patch between two raw Tailwind runs (HEAD tree and this tree) and applied with whitespace
tolerance.

Product questions: (1) full-height compact sheet vs 5/6; (2) approve `INTERACTION-007` and the new
foundation tokens (`layer`, `motion`, `scrim`, `selected`); (3) should overlay scrims bleed under
the system bars once guests can learn insets; (4) a kit icon button for trailing app-bar actions.
## A1 notes (Android on an emulator)

- Branch `feat/dayframe-android` rebased onto `feat/dayframe-shell` (conflicts in the desktop composition and main: the shell micro,
  `HostInputs` and the `assemble(engine)` seam moved into `dayframe_native`: `native_runtime(.., engine: impl PackageEngine)`,
  `wasmtime_engine`, `Shell`, `use_host_inputs`). Desktop keeps `desktop_runtime` and `assemble` for the Wasmi e2e.
- Ran the APK on an API 36 x86_64 emulator: shell, SQLite, Wasmtime load, touch, sidebar, insets, system back. Details, paths and
  gaps in `docs/scratch/dayframe-android.md` (A1 section).
- Fixed: insets into `HostInputs` (JNI `WindowInsets`), predictive-back system back (MainActivity callback + `SYSTEM_BACK`),
  `x/dayframe.ts android --abi x86_64`.
- Pinned, not fixed: debug builds SIGSEGV in the emulator's Vulkan debug-label call (workaround `wrap.<pkg>` WGPU env); Wasm guest
  on Android still unconfirmed; arm64 and release builds not re-run after the shell rebase.
- Tests: `dayframe_native::tests::system_back_is_claimed_only_while_the_shell_can_go_back`; clippy clean for dayframe_desktop
  (all targets) and dayframe_native/dayframe_mobile on x86_64-linux-android.


## A2 notes (Android on the user's phone)

- The phone (ZTE 8050, API 33, Mali) was attached and then taken away before an APK existed; no device step ran. The arm64 debug
  build was stopped twice at the 15 GB disk floor (about 560 of 1925 crates compiled) and the release build was not started.
- Fixed: `x/dayframe.ts android` called `prepareDayframePackages()` without its new `{ testFixtures }` argument after the shell
  rebase, so it crashed before building anything.
- The exact adb checklist for the next phone session (Mali render, back key and gesture on API 33, sidebar edge swipe, Counter
  Wasm guest, Layout show/hide, IME, cold start, meminfo) is in `docs/scratch/dayframe-android.md` (A2 section).
- Disk: removed the emulator SDK and the worktree's host `debug/` target dir; the volume still sits near the floor, so an arm64
  build wants a quiet host and about 12 GB free.

## K5 notes: a guest trap in a sync export in the browser

- Cause: every guest thread entry on the browser backend runs through `WebAssembly.promising`; a trap in it is readable only from the promise (a microtask), so `clay_rt_wcmp::first_poll` saw `Pending` and panicked, then a RefCell panic followed. The web provider cannot catch it: no sync read of a rejection, and a JS frame between the stack and its suspender breaks JSPI (wcmp `host_suspension_provider.rs` ~680-687).
- Fix (fork `dman-os/wasm-component-model-polyfill`, branch `k5-sync-export-plain-call`, rev `f2c771462960004d2bfe92c2a55929ba7f7b8b8c`, pinned in the root and `src/wasm_utils_rs` Cargo.toml/lock): new capability `late_trap_reasons` (declared by the web backend with JSPI), `SuspendProvider::reports_traps_late`, and a sync-typed export (`may_not_suspend`, never suspends) runs as a plain core call under such a provider. Wasmi's provider is unchanged. The trait doc states the all-providers alternative (option A) and why it was not taken (conformance lanes are nix-only).
- Block inside a plain-call sync export: takes the seam's nested-turn fallback, which is what a sync export's own thread does on native for every block except a switch to another thread (`suspend_seam.rs` `try_block`). Verified by test only that a block fails with the same error text as on the unchanged provider (wasmi); a thread switch from a sync export on the plain path is unverified.
- Task hook: the `__wasilibc_task_hook` START/FINISH calls are synthesized into the lifted export's core adapter (clay_mdk `task_hook::run`), so they run however the export is entered; the stack is the guest's shadow stack in its own memory (`task_stacks.rs`), not the wasm call stack the plain call changes. Reasoned from file:line only, not run with a second suspended task.
- Found while writing the spec: `Supervisor::close` turned an incarnation its runtime had closed (fault recorded) into `Idle`, losing the cause once the section was withdrawn. It now keeps `Faulted(cause)` (`clay_host/supervisor.rs`, test `closing_an_incarnation_its_runtime_closed_keeps_its_cause_as_the_fault`).
- Spec `src/dayframe_web/e2e/trap.spec.ts` with the `clay_mdk` `trapper` example (served only with `DAYFRAME_WEB_TRAPPER=1`, set by `playwright.config.ts`). Before the pin: page errors `clay_rt_wcmp/lib.rs:207` (first_poll) and `:651` (RefCell already borrowed). After: passes.

## K7 Blitz focus (pinned dman-os blitz `ffaf2bd`, dioxus fork `a3e1e68`)

Paths: `B` = `~/.cargo/git/checkouts/blitz-1c9e528e0b0c1da5/ffaf2bd/packages/blitz-dom/src`, `D` = `~/.cargo/git/checkouts/dioxus-771b967bec7b64b0/a3e1e68/packages/native-dom/src` (the dioxus fork's `dioxus-native-dom`, not blitz's own copy). "Pinned by test" means a test in `src/dayframe_desktop/e2e/blitz_limits.rs` `mod focus`; its run result is recorded at the end of this section.

1. **Programmatic focus: yes.** `MountedData::set_focus(true)` -> `D/events.rs:299-310` -> `BaseDocument::set_focus_to` (`B/document.rs:1702-1731`). It never consults `is_focussable`, so a `tabindex="-1"` div (skipped by Tab) can be focused. `set_focus(false)` calls `clear_focus` (`document.rs:1689`) only when that node is the focused one (`events.rs:304`). The fork's own tests cover focus-during-render and removed-element no-ops (`D/dioxus_document.rs:~660-750`). Focusing a removed node: no-op (same tests). Removal of the focused node clears focus silently (`document.rs:947-950`).
2. **Focus events.** Blitz dispatches `blur`, `focusout` (old), then `focus`, `focusin` (new) from `generate_focus_events` (`B/events/focus.rs:5-40`), `focus`/`blur` do not bubble, `focusin`/`focusout` do (`blitz-traits events.rs:~445-448`), and Dioxus maps all four (`D/dioxus_document.rs:313-316`, names parse at `blitz-traits events.rs:203-206`). They are dispatched only from Tab (`events/keyboard.rs:26`), pointer-down focus (`events/pointer.rs:544,658,686,712`, `events/mod.rs:139,169`) and click-off clear (`pointer.rs:816`). **`set_focus` / blur from host code dispatch nothing**: `D/events.rs:302` and `:306` are "TODO: queue focus events somehow". Autofocus (`mutator.rs:911`) also dispatches none and is behind the `autofocus` cargo feature (`blitz-dom Cargo.toml:35`), not in dioxus-native defaults (`native/Cargo.toml` default list) nor enabled by `dayframe_desktop` (`features = ["scrollbars"]`): `autofocus` attribute is inert in our build (not run).
3. **Tab traversal.** Keydown default action (`events/mod.rs:210-216`) -> `focus_next_node`/`focus_prev_node` (`document.rs:1673-1686`), pure document order over the whole document (`traversal.rs:195-310`), wrapping. Focusable = `tabindex >= 0`, or no tabindex and `button/input/select/textarea/frame/iframe/summary` or `a[href]`, and not `disabled` (`node/element.rs:634-668`): so `tabindex=-1` is skipped, **positive tabindex is not hoisted**, `inert` is not read anywhere (`grep inert` hits only the Dioxus attribute list, `D/mutation_writer.rs:458`, and an unrelated comment), no visibility check. **`get_focussed_node_id` falls back to the root element when nothing is focused** (`document.rs:635-638`), so Tab with nothing focused starts from the root and lands on the first stop, dispatching `focus`/`focusin` there (an earlier reading, "does nothing", was refuted by the test run). It also means `generate_focus_events` sees the root as the old focus; the page saw no `blur` for it because the root carries no Dioxus id. Key events target the focused node, else the root (`events/driver.rs:224-232`) and bubble. **Containing Tab works from the guest side today**: a `keydown` handler that `prevent_default()`s cancels the engine traversal (`driver.rs:352-358`, `D/dioxus_document.rs:356`) and may call `set_focus` synchronously (pinned by test). No engine subtree/inert hook exists.
4. **Styling.** `:focus` matches (`B/stylo.rs:476`); `:focus-visible` and `:focus-within` are hard-coded `false` (`stylo.rs:477-478`). `FOCUSRING` is set with `FOCUS` on every focus (`node/node.rs:721`) but nothing reads it. So `focus:` utilities work, `focus-visible:` and `focus-within:` never do on Blitz. No focus ring is painted by the engine (no `FOCUSRING` reader in blitz-paint).
5. **Transitions/animations run, events do not exist.** Stylo animation sets advance every resolve (`stylo.rs:83-120`) and keep the shell redrawing (`document.rs:2066-2082`). No `transitionend`/`animationend`/`transitionrun`/`animationstart` anywhere (`grep -rn` over packages: no hit; `DomEventKind` has no such names). So KIT-108's reason stands for the events and is wrong that animations may not render.
6. **Stale claims.**
   - Skill "`tabindex` panics, so keyboard focus is unavailable" (SKILL.md "### `tabindex` panics"): the panic was the `doc_mut()` borrow during render; fixed in the dioxus fork with a regression test (`D/dioxus_document.rs`, `task_polled_during_render_can_access_document`). Corrected in the skill.
   - README KIT-052 and `headless/sections.rs` "setting `tabindex` panics Blitz" / "Blitz has no focus API": there is one (`set_focus`, Tab traversal). Corrected in the README. KIT-052's `tabindex` opt-in gate is not removed by this lane (a decision for the owner: the gate may now be unnecessary on Blitz; the test `conventions::tabindex_is_only_the_renderer_focus_gate` is untouched).
   - README line ~145 "Escape needs a focused element, which Blitz cannot give": it can (a focused element receives keydown; focus needs `set_focus`/Tab/click), but nothing focuses a freshly opened sheet unless the host does it.
   - README KIT-108 "Blitz does not evaluate prefers-reduced-motion": unverified here, left alone.

### Proposed shape: one focus capability over web and Blitz

Guest wire per-own-element handle (modelled on `MountedData`): `focus(el)`, `blur(el)`, plus a query `focused() -> Option<el>`, scoped to the micro's own elements. Both backends implement it with the engine call (web `HTMLElement.focus({preventScroll})`/`blur`, Blitz `set_focus_to`/`clear_focus`). Event delivery to the guest must be uniform, so the capability, not the engine, owns it: the host emits `focus`/`blur` (and `focusin`/`focusout`) itself after a programmatic call on Blitz (where the engine emits none) and does not double-emit on web (where the DOM does). Pin `set_focus_..._dispatches_no_focus_events` flips when Blitz starts emitting.

### What a FocusScope port needs and what is missing

| Need | Web | Blitz today | Gap / option |
|---|---|---|---|
| move focus into the scope on open | `focus()` | `set_focus` | none |
| restore to opener on close | `focus()` on the saved handle | `set_focus`; removal of the focused node clears silently (`document.rs:947`) | guest saves the opener handle before opening; no gap |
| contain Tab | browser Tab + sentinel/keydown | engine Tab walks the whole document, no inert | keydown `preventDefault` + first/last `set_focus` works (pinned). Needs the scope's tabbable list: a guest-side enumeration of its own focusable elements, or a capability query `tabbables(scope_el) -> [el]` answered by the host |
| Tab when nothing focused | n/a | starts at the root, lands on the first stop in the document | a scope must move focus in on open, as it would anyway |
| tabbable discovery | `:is(a[href],button,...)` heuristics | `is_focussable` is private to the engine (`node.is_focussable()` is pub: `node/node.rs:553`) | host-side `tabbables` query using `is_focussable` makes both backends agree; fork patch option: honour `inert` and `tabindex>0` order in `flush_is_focussable`/`next_node` |
| `aria-modal`/outside inert | `inert` | not honoured | fork patch: skip nodes under `[inert]` in `focus_next_node` (small, in `traversal.rs` filter) |
| `focus-visible` styling | works | never matches | fork patch: set `FOCUSRING` only for keyboard-initiated focus and match `FocusVisible`/`FocusWithin` in `stylo.rs:477-478`; otherwise skins use `focus:` |
| focus events for host-driven focus | DOM | none | fork patch to `D/events.rs:302,306` (queue events) or capability synthesises them |

### Decisions (coordinator) and results

- (a) Port upstream's keydown-based Tab containment as-is AND patch the Blitz fork to honour `inert` (Base UI relies on it). The patch is a later lane.
- (b) Fix the "TODO: queue focus events" in our dioxus fork's native-dom (`events.rs:302,306`) rather than synthesising events in the host. Later lane. The pin `set_focus_moves_and_clears_focus_but_dispatches_no_focus_events` flips when it lands.
- (c) Drop the KIT-052 `tabindex` gate only if the tests prove the panic is gone. Document-level evidence: `tab_walks_...` and `a_keydown_handler_can_trap_...` run elements with `tabindex` attributes, `set_focus` from a handler and from outside, without a panic. Not done in this lane: the gate (`Split::keyboard`, `conventions::tabindex_is_only_the_renderer_focus_gate`, shell.rs/main.rs/composition wiring) is also the web keyboard opt-in and removing it is a cross-crate API change needing its own builds; and no windowed (AT-SPI/native) run was done. Left as a follow-up.
- Test run (`cargo nextest run -p dayframe_desktop -E 'test(focus) | test(transition)'`): 6 passed. Two first-run failures corrected the source reading: Tab-from-nothing (above), and the transition test's own bug (swapping `.a` for `.b` dropped the `transition` property; the class must be `a b`). Transitions interpolate (mid-transition opacity strictly between 0.1 and 0.9 at t=0.5, 0 at t=2).

## T1 notes (typed declarations lane) - HANDOFF, stopped at step 1

Stopped because free space on /run/media/asdf/p3N fell to 4.2 GB (floor 5 GB; the fresh per-worktree target dir took 15 min and the disk was already shrinking from other builds). Nothing past step 1 was started.

Step 1 (datum_core extraction), state:
- New crate src/datum_core (member.rs, shape.rs, wire.rs moved with git mv; value.rs holds `Extra` and all DatumShape impls for std/serde_json types; foreign leaf impls for ObjKey/Timestamp/ChangeHash sit behind features `sync-core`/`jiff`/`automerge`; `schemars` feature gates check_shape/union_schema etc.). big_cabinet_core does `pub use datum_core::*` in datum/mod.rs and keeps impls for its own leaf types in datum/value.rs.
- macros/lib.rs: `runtime = ::path` argument (Args::datum_path, default ::big_cabinet_core::datum).
- Verified: `cargo clippy --all-targets --all-features -p datum_core -p big_cabinet_core -p macros` compiled datum_core, macros and the big_cabinet_core lib; the only error was a missing `use indexmap::IndexMap` in value.rs tests, fixed afterwards but NOT re-run. Unverified: big_cabinet_core/macros nextest, `cargo x guardrails api --all --check` (big_cabinet_core/public-api.txt needs regeneration, it lists datum::member::/shape:: paths), `cargo x guardrails lint`, check-matrix for datum_core on both wasm targets, the wasm_utils_rs and btress_sysadmin lockfiles (root Cargo.lock updated by the build).
- Not done: a macros unit test for `runtime =`.

Design decisions for steps 2-6 (from reading, nothing built):
- Traits in one clay_mdk module: PackageDecl{ID,VERSION,AUTHORITY}, CallDecl{NSID,Request,Response}, CollectionDecl{NSID,Key,Value,Selector,OFFERED}, ConfigDecl{BINDING,NSID(tag),KEY,Value}, CallPublication{NAME,Contract}, ComponentPublication{NAME}. Suffix `Decl` where the plain name is taken (wire struct clay_mdk::manifest::CollectionContract, context::Config, rows::Collection).
- Binding names cannot be derived from the contract and keep the snapshots byte-identical: counter binds the toolbar buttons as "toolbar", shell binds panels as "panels" (read) and "home-panel" (write), layout uses "shell-panels". Composition resolves by contract (dayframe_composition/bindings.rs), and only micro code and dayframe_composition/lifecycle.rs + micro_sample use binding names. So: the manifest builder takes the binding string explicitly once per request; Context::collection::<C>/registration::<C> look the binding up from the manifest's collection_requests by (contract NSID, access) (read prefers the Read request); needs a (nsid, access, binding) table in context.rs Shared, built in native.rs:467 and guest.rs:369.
- Typed attachments: keep `Attachment` untyped on row types (generic would fan out into rows.rs/views); give Attachment a human-readable serde form `{"$attachment": n}` and let the builder's contribution closure hand out `at.own::<Publication>()` which numbers attachments and fills AttachmentDeclaration. Shell `Panel.details`/`home` need skip_serializing_if so serialized rows match the hand-written snapshot payloads (hand payloads omit them).
- Requirement check (step 5): Call publications compare the contract NSID; presentation publications declare `contract: None` while requirements name e.g. NAV_BUTTON_CONTRACT, so those cannot be compared without changing the snapshots (presentation wire is out of scope); keep name-only for presentations and say so.
- Coordinator constraint: app/composition crates must not gain typed deps on wasm micro crates. Sites that reference wasm micro (counter, layout) consts and must be left as-is (keep those consts in the micro crates): dayframe_composition/tests.rs (micro_counter_manifest::ID, PACKAGE_VERSION, VIEW, CONFIG_TAG/KEY lines 1220-1221, INCREMENT 1287, COUNT 1302, sidecar()/manifest()), dayframe_desktop/composition.rs:267-268, dayframe_desktop/e2e.rs:285,296,517-518, dayframe_desktop/composition/tests.rs:34-90, dayframe_desktop/e2e/layout.rs:33-36,52,77,97, dayframe_web/shell.rs:211-212. So counter/layout manifest crates keep ID, PACKAGE_VERSION, VIEW, INCREMENT, COUNT, CONFIG_NAME/TAG/KEY as consts; C1 removes them.
- Disk: use CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0; the first clippy of three crates took 15 min on a cold target (/run/media/asdf/p3N/tmp/townframe-target-p3n-4/townframe-p3N-4/typed-decls, about 1+ GB at that point).

### T1 notes, update after the datum_core PR was verified in /.../worktrees/datum-core (commits 28af87ce, 94ef225d)
- Verified there: clippy --all-targets --all-features and nextest (326 passed) for datum_core, big_cabinet_core, macros; clippy for big_sync_core, big_plugs_core, big_nodule_core, big_blobs_core; check-matrix datum_core (native, wasm32-unknown-unknown, wasm32-wasip3); `guardrails lint` 0 diagnostics; `api --all --check` clean after blessing big_cabinet_core. The root lockfile is the only one that changes (the wasm_utils_rs and btress_sysadmin locks do not contain big_cabinet_core or big_sync_core).
- Changes vs 9129043d (reconcile when it merges): ObjKey's DatumShape impl moved into big_sync_core (new dep on datum_core) because big_sync_core has no wasm contract; no `sync-core` feature on datum_core; big_cabinet_core re-exports named items instead of a glob (lint ERROR_GLOB_EXPORT); datum_core has a test for `runtime = crate`.
- Design update from coordinator: calls will implement the shared `Operation` trait (request type is the contract; Output, error type, NSID const) once the node session moves it into a wasm-buildable crate. STUB: the call declaration and `publish_call`/`call` typing are the LAST step; do packages, collections, configs, components, the manifest builder and micro migrations first. No Context<M> type-list. "Plug" is "cartridge" in new docs.

## T2 notes (typed declarations lane, steps 2-5 done, step 6 and the final Playwright run NOT done)

Commits on feat/dayframe-typed-decls (all green when committed): datum_core matched to PR 82; declaration traits + generic Context; ManifestBuilder; counter, shell, toolbar, layout, settings manifests on the builder; micro_sample + trapper fixture; `satisfied_by` contract check.

Design as built:
- `clay_mdk/decl.rs`: `PackageDecl`, `CollectionDecl` (NSID, Key, Value), `DeclaredCollection` (schema NSIDs, OFFERED; owner side only), `ConfigDecl`, `ComponentDecl`, plus `RootRow`/`ShellRoots` (moved out of native.rs). Consts live on the declaration types, no `Context<M>`, no dyn.
- `Context::collection::<C>()`, `registration::<C>()`, `config::<C>() -> Result<Option<Facet<C::Value>>>`, `publish::<P>(render)`. Binding names are looked up from the manifest's collection requests by (contract NSID, access); read falls back to a write request; two requests with the same (NSID, access) panic. `Shared.requests` carries them (native.rs Granted, guest.rs).
- Still strings, deliberately, until the `Method` trait (formerly called Operation) exists: `publish_call`, `require`, `ManifestBuilder::call/requires`, `Attachments::by_name`, `RowAttachment::by_name`.
- `Attachment` serializes as `{"$attachment": n}` in human-readable formats and stays the CBOR tag otherwise. `ManifestBuilder::contributes::<C>(id, key, |at| row)` numbers attachments via `at.own::<P>()`.
- Wire manifests unchanged: all five manifest insta snapshots are byte-identical. ONE row snapshot changed: micro_shell_manifest panel_row_wire_shape_is_reviewable (hex drops the trailing `home: false` and the map length goes a5 -> a4), because `Panel.details`/`home` now skip serializing when default (needed so the typed Panel matches the hand-written manifest payloads). Readers all decode through the typed `Panel` with `#[serde(default)]`, nothing reads the raw field, and `a_panel_without_details_or_home_reads_as_neither` covers rows without it; old stored rows that carry `home: false` still decode.
- `InstalledMicro::satisfied_by(&MicroId, &Sidecar) -> Result<Self, String>`: errors if the provider publishes no publication of the requirement's binding name, or publishes a call under another contract NSID. Presentations are name-only (they declare no contract on the wire). Loader sites push the error; static sites `expect`. Tests: wrong contract refused, missing publication refused, shell satisfies Settings.
- `SettingsConfig` moved from micro_settings into micro_settings_manifest (it is the config facet value the manifest crate must name). micro_shell_manifest declaration types live in `declarations.rs` (module-items budget).
- Counter/layout consts that app crates reference (ID, PACKAGE_VERSION, VIEW, INCREMENT, COUNT, CONFIG_*) are untouched.

Step 6 handoff (not started; reasons and findings):
- Needs the row/key/selector types as datums: `#[macros::datum(runtime = ::clay_mdk::datum, authority = AUTHORITY)]` with `Extra` flatten for open objects, `schemars` derive behind a native feature, and `clay_mdk` re-exporting datum_core (`clay_mdk::datum`, with `const_format`), clay_mdk/guardrails.toml allowing datum_core, manifest crates allowing `macros`.
- RISK to check first: adding `#[serde(flatten)] extra: Extra` makes the CBOR row maps indefinite-length, which would change the row hex snapshots (settings/toolbar/layout/shell `*_row_wire_shape_*`). Decide with the user whether row hex may change or whether those rows stay closed objects.
- Then `DeclaredCollection` VALUE_SCHEMA/KEY_SCHEMA/SELECTOR_SCHEMA can become `<T as DatumMember>::ID` (assert equal to the current consts, they match the lowerCamel name rule: section, sectionKey, sectionSelector), and a native-only `schemas()` per manifest crate built with big_plugs_core `Declaration::of` / `Universe::collect`, carried in the sidecar as a flat `lexicons` member.
- Not run: dayframe_web Playwright suite, check-matrix. Verified: clippy --all-targets and nextest (250+ tests incl. clay_rt_wasmtime, clay_rt_wcmp_wasmi, dayframe_composition, dayframe_desktop 62) with prepared guest fixtures; `guardrails lint` 0 diagnostics; `api --all --check` clean.
