#!/usr/bin/env -S deno run --allow-all

// Build and serve the `daytxt_web` shell (ADR 018 migration steps 6–7): the shared
// composition plus every package in `packages/index.json`. Each entry's manifest is the
// sidecar `<name>.manifest.json` beside the component (STAND-IN until `big_cartridges` delivers
// manifests) and is pinned with the BLAKE3 digest of the component bytes; the shell fetches a
// package only when an instance activates and verifies the digest before `wcmp` compiles it.
//
//   deno run --allow-all x/daytxt-web.ts            build, then serve on HOST:PORT
//   deno run --allow-all x/daytxt-web.ts --build    build only

import { prepareDaytxtPackages } from "./daytxt-packages.ts";
import { $ } from "./utils.ts";

const buildOnly = Deno.args.includes("--build");
const root = $.relativeDir("../");

// Web builds resolve features in isolation; workspace unification would pull the desktop
// renderer's native-tls into wasm32. See CONTRIBUTING.md on `resolver.feature-unification`.
const isolated = ["--config", 'resolver.feature-unification="selected"'];

// Only the shells' packages: the test-only guest fixtures are not served. The one exception is
// the `trapper` micro the e2e's sync-export trap spec opens, served when the e2e asks for it
// (`DAYTXT_WEB_TRAPPER=1`, which `playwright.config.ts` sets) and never in a plain serve.
const packageDirectory = (await prepareDaytxtPackages({
  testFixtures: false,
  trapper: Deno.env.get("DAYTXT_WEB_TRAPPER") === "1",
}))["DAYTXT_PACKAGE_DIR"];

// `wasm-release`, as `cargo x guardrails check-matrix` builds it: a debug build also links
// Dioxus devtools, whose hot-reload socket finds no `dx serve` here and logs an error per retry.
await $`cargo build --locked ${isolated} -p daytxt_web --bin daytxt_web --target wasm32-unknown-unknown --profile wasm-release`;
const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
  .json<{ target_directory: string }>();
const target = $.path(metadata.target_directory);

// The site (bindgen output, package index, tailwind, boot module) is assembled by
// `x/daytxt-site.ts`, the same script the `daytxt-web` Nix package runs.
const site = target.join("daytxt_web/site");
await $`rm -rf ${site}`;
await $`deno run --no-config --allow-read --allow-write --allow-run x/daytxt-site.ts --wasm ${
  target.join("wasm32-unknown-unknown/wasm-release/daytxt_web.wasm")
} --packages ${packageDirectory} --tailwind ${
  root.join("src/clay_dioxus_kit/assets/tailwind.css")
} --cargo-lock ${root.join("Cargo.lock")} --out ${site}`;

// Backstop for the guest surface vocabulary (`clay_surface` value policy): if a URL-bearing value
// ever slips past validation, the browser still refuses to fetch or run it. `img-src` also governs
// CSS `url()` images, so a `style:` value that reached the DOM fetches nothing foreign.
// `'wasm-unsafe-eval'` is what `WebAssembly.compile` of the host and every guest component needs.
// `style-src-attr 'unsafe-inline'` is for the Dioxus interpreter writing per-property styles.
// `form-action` and `base-uri` do not fall back to `default-src`, so they are set explicitly.
const contentSecurityPolicy = [
  "default-src 'none'",
  "script-src 'self' 'wasm-unsafe-eval'",
  "connect-src 'self'",
  "img-src 'self' blob: data:",
  "font-src 'self'",
  "style-src 'self'",
  "style-src-elem 'self'",
  "style-src-attr 'unsafe-inline'",
  "form-action 'none'",
  "base-uri 'none'",
  "object-src 'none'",
  "frame-src 'none'",
  "frame-ancestors 'none'",
  "worker-src 'self' blob:",
  "manifest-src 'self'",
].join("; ");

if (!buildOnly) {
  const types: Record<string, string> = {
    ".css": "text/css; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".wasm": "application/wasm",
  };
  const hostname = Deno.env.get("HOST") ?? "127.0.0.1";
  const port = Number(Deno.env.get("PORT") ?? "0");
  Deno.serve({ hostname, port }, async (request) => {
    const path = decodeURIComponent(new URL(request.url).pathname);
    const name = path === "/" ? "index.html" : path.slice(1);
    // Browsers probe this without a <link rel="icon">; an empty 204 keeps console clean.
    if (name === "favicon.ico") {
      return new Response(null, { status: 204 });
    }
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
        "content-security-policy": contentSecurityPolicy,
        "referrer-policy": "no-referrer",
        "x-content-type-options": "nosniff",
      },
    });
  });
}
