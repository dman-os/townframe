# Dayframe shell PR — ledger

Status: Living ledger. Operator decisions are marked as such; everything else is the PR owner's working state.
Owner: Claude (PR owner) with Sonnet subagents; operator reviews.
Scope: Branches `feat/dayframe-shell` and `feat/dayframe-android`; FDR 006, ADR 032, `clay_dioxus_kit`, `micro_shell`, `dayframe_mobile`.
Updated: 2026-11-05 (S1 and S3 landed)

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
| S2 | `micro_shell` + manifest, navigation stack, panels collection, chrome button, sheets; Settings migration | `feat/dayframe-shell` | after S1's first components |
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
