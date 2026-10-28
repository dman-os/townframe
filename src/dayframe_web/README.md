# Web

Web composition root for Dayframe (ADR 018 §7). Shared design tokens and components live in
`../ui`; the engine-neutral composition runtime lives in `../dayframe_composition`. This shell
selects the `wcmp` browser engine and fetches the counter guest as an asset during activation.

```sh
deno run --allow-all x/dayframe-web.ts            # build, then serve on HOST:PORT
deno run --allow-all x/dayframe-web.ts --build    # build only
pnpm -C src/dayframe_web run e2e                  # headless-Chromium counter click round trip
```

`x/dayframe-web.ts` prepares `counter.component.wasm`, builds the wasm32 host with isolated
feature unification, runs `wasm-bindgen`, copies the guest next to the host module, and serves
the site. Bundle size is printed after the build (raw + gzip) for the ADR §7 budget.