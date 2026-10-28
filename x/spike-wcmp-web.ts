#!/usr/bin/env -S deno run --allow-all

// Build and serve the ADR 018 `wcmp` spike page (`src/clay_wcmp_spike/web.rs`): the real counter
// component, compiled by the browser's own `WebAssembly` engine through `wcmp` and rendered by
// `clay_dioxus` on `dioxus-web`. `src/clay_wcmp_spike/playwright.config.ts` runs this as its web
// server; run it directly to look at the page.
//
//   deno run --allow-all x/spike-wcmp-web.ts            build, then serve on HOST:PORT
//   deno run --allow-all x/spike-wcmp-web.ts --build    build only

import { $, useDotenvTargetDir } from "./utils.ts";

const buildOnly = Deno.args.includes("--build");
const root = $.relativeDir("../");
await useDotenvTargetDir();

// The spike is a browser entrypoint inside the root workspace, so resolve its features in
// isolation; workspace unification would pull the desktop renderer's native-tls into wasm32. See
// CONTRIBUTING.md on `resolver.feature-unification`.
const isolated = ["--config", 'resolver.feature-unification="selected"'];

const prepared = await $`deno run --allow-all ${
  root.join("x/check-dayframe.ts")
} --prepare-only`.lines();
const counter = prepared
  .map((line) => line.match(/^DAYFRAME_COUNTER_COMPONENT=(.+)$/)?.[1])
  .find((path) => path !== undefined);
if (!counter) {
  throw new Error(
    `check-dayframe.ts printed no DAYFRAME_COUNTER_COMPONENT:\n${
      prepared.join("\n")
    }`,
  );
}

// `wasm-release`, as `cargo x guardrails check-matrix` builds it: a debug build also links Dioxus
// devtools, whose hot-reload socket finds no `dx serve` here and logs an error per retry.
await $`cargo build --locked ${isolated} -p clay_wcmp_spike --lib --target wasm32-unknown-unknown --profile wasm-release`;
const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
  .json<{ target_directory: string }>();
const target = $.path(metadata.target_directory);

// `wasm-bindgen` rejects a module built by another `wasm-bindgen` crate version; name the
// mismatch instead of letting it fail on a schema error.
const lockedBindgen = (await root.join("Cargo.lock").readText())
  .match(/\[\[package\]\]\nname = "wasm-bindgen"\nversion = "([^"]+)"/)?.[1];
const cliBindgen = (await $`wasm-bindgen --version`.text()).split(" ")[1];
if (lockedBindgen !== cliBindgen) {
  throw new Error(
    `wasm-bindgen CLI ${cliBindgen} does not match Cargo.lock's ${lockedBindgen}; use nix develop .#ci-desktop`,
  );
}

const site = target.join("clay_wcmp_spike/site");
await $`rm -rf ${site}`;
await Deno.mkdir(site.toString(), { recursive: true });
await $`wasm-bindgen --target web --no-typescript --out-dir ${site} --out-name clay_wcmp_spike ${
  target.join("wasm32-unknown-unknown/wasm-release/clay_wcmp_spike.wasm")
}`;
await Deno.copyFile(counter, site.join("counter.component.wasm").toString());
await site.join("index.html").writeText(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>wcmp spike: counter</title>
  </head>
  <body>
    <div id="main"></div>
    <script type="module">
      import init from "./clay_wcmp_spike.js";
      await init();
    </script>
  </body>
</html>
`);

if (!buildOnly) {
  const types: Record<string, string> = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".wasm": "application/wasm",
  };
  const hostname = Deno.env.get("HOST") ?? "127.0.0.1";
  const port = Number(Deno.env.get("PORT") ?? "0");
  Deno.serve({ hostname, port }, async (request) => {
    const path = decodeURIComponent(new URL(request.url).pathname);
    const name = path === "/" ? "index.html" : path.slice(1);
    // `wasm-bindgen` nests the crates' JS snippets under `snippets/`; nothing outside the site
    // directory is part of the page.
    const file = site.resolve(name);
    if (
      !file.toString().startsWith(`${site}/`) || !(await file.stat())?.isFile
    ) {
      return new Response("not found", { status: 404 });
    }
    const extension = name.slice(name.lastIndexOf("."));
    return new Response((await Deno.open(file.toString())).readable, {
      headers: {
        "content-type": types[extension] ?? "application/octet-stream",
      },
    });
  });
}
