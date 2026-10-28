---
name: playwright
description: Drive a real browser against townframe's web pages (the btress sysadmin app, the clay_wcmp_spike browser host) from the terminal - e2e specs, one-off screenshots, codegen, and page-error triage. Use when a task needs Chromium/Firefox/WebKit, when a Playwright run cannot find its browser, or before adding a new Playwright package.
---

# Playwright in townframe

The flake owns the browsers. `src/nix/devshells.nix` builds a browser farm from
`pkgs.playwright-driver` (`playwrightBrowsers`) and exports it as `PLAYWRIGHT_BROWSERS_PATH`
(with `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1`) in the `dev` shell and in `ci-desktop`. Playwright
looks browsers up by *revision*, so the npm runner must be exactly the `playwright-driver`
version of the pinned nixpkgs (currently 1.63.0, chromium revision 1243). A different version
asks for a revision the farm does not have and dies at browser launch with "Executable doesn't
exist". Check the pair before blaming anything else:

```bash
nix develop .#ci-desktop -c bash -c 'ls "$PLAYWRIGHT_BROWSERS_PATH"; grep -rn "@playwright/test" src/*/package.json'
```

Never run `playwright install`: the farm is read-only and the downloaded builds do not run on
NixOS anyway.

## Packages

Each browser-facing crate that has specs is a pnpm workspace package (`pnpm-workspace.yaml`)
with its own `package.json` pinning `@playwright/test` to the driver version, a
`playwright.config.ts`, a `tsconfig.json`, and specs under `e2e/`:

- `src/btress_sysadmin`: the sysadmin login flow; CI starts its servers itself (see the
  `sysadmin e2e` job in `.github/workflows/checks.yml`).
- `src/clay_wcmp_spike`: ADR 018's browser host. Its config's `webServer` runs
  `x/spike-wcmp-web.ts`, which prepares the counter component, builds the wasm32 module, runs
  `wasm-bindgen` (checked against `Cargo.lock`'s version), and serves the page; the first run
  includes a cold wasm32 build.

Run specs from the package, through the package's own CLI, never a global or workspace-root
one (those resolve a different version and so a different browser revision):

```bash
nix develop .#ci-desktop -c pnpm install --frozen-lockfile --filter @townframe/clay_wcmp_spike
nix develop .#ci-desktop -c pnpm -C src/clay_wcmp_spike run e2e
```

A new package joins `pnpm-workspace.yaml` and gets an `importers:` entry in `pnpm-lock.yaml`
with the same `@playwright/test` version, so the lock keeps a single Playwright.

## One-off browsing

The package CLI also does ad-hoc work, using the same pinned browsers:

```bash
pnpm -C src/clay_wcmp_spike exec playwright screenshot http://127.0.0.1:4517/ /tmp/spike.png
pnpm -C src/clay_wcmp_spike exec playwright codegen http://127.0.0.1:4517/
```

Keep artifacts out of the checkout (`/tmp`, `$TMPDIR`); specs write `test-results/` and
`playwright-report/`, which are ignored.

## Triage

- Wasm pages report a Rust panic as a page error (`RuntimeError: unreachable` or a
  `panicked at` console line). Specs collect `pageerror` and `console` errors and assert there
  are none, so a panic fails the test immediately instead of surfacing as a locator timeout.
- A spec that times out waiting on the web server is usually still building: run the
  `webServer` command by hand (`deno run --allow-all x/spike-wcmp-web.ts --build`) to see the
  build's own output.
- Prefer role and label locators (`getByRole("button", { name: ... })`): they are what the
  Dayframe components expose for accessibility, and they survive markup changes.
