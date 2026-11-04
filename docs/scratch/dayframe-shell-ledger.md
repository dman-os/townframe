# Dayframe shell PR — ledger

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
| S1 | `clay_dioxus_kit` foundation: conventions, Sections, ListDetails, Sheet, NavButton, showcase | `feat/dayframe-shell` (main checkout) | running |
| S2 | `micro_shell` + manifest, navigation stack, panels collection, chrome button, sheets; Settings migration | `feat/dayframe-shell` | after S1's first components |
| S3 | Android: `dayframe_mobile` on the composition, Wasmtime on Android, constraints doc | `feat/dayframe-android` (isolated worktree) | running |
| S4 | Web tests: Playwright flows, Bombadil invariants | `feat/dayframe-shell` | after S2's first flow |
