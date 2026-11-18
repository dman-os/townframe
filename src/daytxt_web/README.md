# Web

Web composition root for daytxt (ADR 018 §7). Shared design tokens and
components live in `../ui`; the engine-neutral composition runtime lives in
`../daytxt_core`. This shell selects the `wcmp` browser engine and fetches each
package's guest (the counter, the layout) as an asset during activation.

```sh
deno run --allow-all x/daytxt-web.ts            # build, then serve on HOST:PORT
deno run --allow-all x/daytxt-web.ts --build    # build only
pnpm -C src/daytxt_web run e2e                  # headless-Chromium counter and layout round trips
```

`x/daytxt-web.ts` prepares every package's `<name>.component.wasm`, builds the
wasm32 host with isolated feature unification, runs `wasm-bindgen`, copies the
guest next to the host module, and serves the site. Bundle size is printed after
the build (raw + gzip) for the ADR §7 budget.
