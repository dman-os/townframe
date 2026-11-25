---
name: daytxt-gui-loop
description: Look at and drive the daytxt shell as an agent without touching the operator's desktop - screenshots, element inspection and Playwright scripts against the web build in a headless WebKit/Chromium. Use for any "does the UI look right" or "what does this element resolve to" question, before reaching for a native window, sway or AT-SPI.
---

# daytxt GUI loop (agents)

The default way for an agent to see the daytxt GUI. It opens no window, takes no focus, uses no
`DISPLAY`/`WAYLAND_DISPLAY` and needs no compositor: `x/daytxt-web.ts` builds and serves the web
shell on a free localhost port and a headless Playwright browser (the flake's pinned farm, see the
`playwright` skill) drives it. WebKit is the default because the desktop is WebKitGTK; Chromium
and Firefox are `--browser`. The web build is a dev and Playwright artifact, never shipped.
The native app and `daytxt-native-dev-loop` are for what only the desktop has (Wry/Blitz
specifics, the node); the operator's own session is never driven by an agent.

## Commands

Run inside `nix develop` from the repo root (first run: `nix develop -c pnpm install --frozen-lockfile --filter @townframe/daytxt_web`; the first build is a cold wasm32 build of several minutes).

```sh
deno run --allow-all x/daytxt-gui.ts shot home out.png
deno run --allow-all x/daytxt-gui.ts shot settings out.png --browser chromium
deno run --allow-all x/daytxt-gui.ts shot theme-lab out.png --theme my.css
deno run --allow-all x/daytxt-gui.ts inspect "button" --route settings
deno run --allow-all x/daytxt-gui.ts run script.ts --route home
```

- `<route>` is `home`, `settings`, `theme-lab`, or a raw `DAYTXT_SHELL_OPEN` spec
  (`settings,settings/counter`, `@sidebar`). It is the page's `?open=` query, so the shell starts
  on that stack: no click paths. Micros are admitted enabled (`?enable=all`, the web
  `DAYTXT_ENABLE_MICROS=all`).
- `--theme file.css` applies the CSS as the user-layer theme through the theme lab (the only way a
  theme is written), then loads the route. Each run is a fresh browser profile, so nothing leaks
  between runs.
- `--width/--height` (default 1280x800; 420x800 is the compact phone layout), `--full-page`,
  `--verbose` (stream the build log).
- `inspect <selector>` prints up to 20 matches as JSON: tag, role, aria-label, text, `ct-*` classes,
  `data-*` attributes (`data-part`, `data-state`) and the computed styles the contract layers decide.
  Playwright CSS pierces the open shadow roots every micro mounts in.
- `run script.ts` takes a module whose default export is
  `async ({ page, goto, settle, context, browser }) => {...}` (Node strips the types; imports resolve
  from `src/daytxt_web`, so `import { expect } from "@playwright/test"` works).
  `goto(route)` navigates and waits for the shell; `DAYTXT_GUI_STRICT=1` fails the run on page errors.

## What it guarantees

- Ready means the shell said so: `html[data-daytxt-ready]` (set once the runtime is built and the
  shell mounted), a visible region, and no "Loading" status left. No sleeps. A refused route or a
  runtime that cannot start fails with the shell's own alert text.
- The server and browser are stopped on exit, failure and SIGINT/SIGTERM (the whole process tree,
  cargo included). A build failure prints the build's last lines.
- Page errors and `console.error` lines are printed to stderr after the run.

## Gotchas

- The web shell has no node: collections are the mock/IndexedDB-seeded ones, so anything that needs
  real node data (intxt cards, sync) cannot be shown. Next step: the desktop serving its node to the
  page over localhost.
- Known gap: the web server's CSP (`style-src-elem 'self'`) refuses the shell's host-document `<style>` for the user theme, so a theme restyles micro mounts (adopted sheets) but not the shell's own light-DOM chrome, and the console reports the violation (the page-error lines after a run). Not fixed here: relaxing the CSP weakens the egress backstop and needs a decision.
- Fonts and rendering differ from WebKitGTK in details; use it for layout, state and styling
  questions, not pixel parity.
- Do not run it while another build holds the same checkout's target busy for long; mbx will queue.
- Page `url()` fetches are blocked by the shell's CSP, as in the product.
