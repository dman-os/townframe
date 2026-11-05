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
  one being the new Finding 3 below and one the sample's greeting panic below. So the fault Finding 2 describes is gone
  but a different violation remains at the same rate, and flipping the step to blocking would make CI flaky. Left as the
  brief's last item undone on purpose.

### Finding 3 (open): the layout shows no controls for a moment after a panel is hidden at 420 px

`navigationAlwaysReachable` is violated about 200 ms after a click on "Hide Sample micro panel" or "Hide Counter (Wasm)
panel" at a phone width: frame `lists: [Layout]`, `navigation: []`, `clickablePoints: []` (not even the toggles), no console
error, no guest panic, and the screenshot of the next state shows the layout intact. It reproduces at `HEAD` (before the
hook fix) at the same rate, so it is not the hook. Evidence: Bombadil traces (the property text and frame above); the
desktop scenario with an assertion that the layout's `Panels` toolbar is present in every DOM render between the click and
the panel's removal (Wasmi, wide) did not see it, so the transient is specific to the browser engine/timing or the
compact layout. Hypothesis (unverified): the layout's `view` returns a bare `Loading layout…` without its app bar when the
arrangement facet reads `Loading`, and a write makes the facet read `Loading` for a moment (`FacetState::Loading` is also
what a released or revoked mirror reports); the Finding 1 fix (always give a placeholder its app bar) was applied to
`PresentationSlot` placeholders and not to this guest's own Loading branch. Next step: capture the guest's states with a
Bombadil `--reproduce` of a saved trace, or instrument `micro_layout::view` to log the facet state per render.

### Finding 4 (fixed): the sample's greeting field panicked on a multi-byte character

Bombadil, typing `🀃|Ä0RｒbE…` into the sample detail's Greeting field (browser build): `micro_sample/view.rs:65` did
`String::truncate(120)` at a byte offset inside a character, which panics (`is_char_boundary`), failing
`noConsoleErrors`. The clamp is now by characters (`clamp_greeting`, limit 120) with a unit test (a string whose byte 120
is inside a four-byte character; fails with the old code).

### Open (S6)

- `wcmp`: a callback task parked in `waitable-set.wait` (via `wit_bindgen::block_on`) or `thread.yield` (blocking) on the
  Wasmi provider did not resume after the host released what it waited for, in a lifecycle-conformance probe (not
  committed). Unexplained; not needed by anything in the app today.

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

- Finding 2 is fixed (S6); the CI step stays non-blocking until Finding 3 (S6 notes) is fixed.
- Bombadil's output directory holds the trace for `--reproduce`; the e2e artifact upload already collects `test-results/`, but
  Playwright clears it at the start of each run, so a run of the plain e2e step followed by the Bombadil step keeps only the latter's.
- Properties to add as the shell grows: forward after back, a resized pane never below its rule, the open row marked
  `aria-current` exactly while its details are open.

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
