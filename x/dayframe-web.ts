#!/usr/bin/env -S deno run --allow-all

// Build and serve the `dayframe_web` shell (ADR 018 migration steps 6–7): the shared
// composition with the counter guest fetched as an asset, compiled by the browser's
// `WebAssembly` engine through `wcmp`.
//
//   deno run --allow-all x/dayframe-web.ts            build, then serve on HOST:PORT
//   deno run --allow-all x/dayframe-web.ts --build    build only

import { $, useDotenvTargetDir } from "./utils.ts";

const buildOnly = Deno.args.includes("--build");
const root = $.relativeDir("../");
await useDotenvTargetDir();

function matchesAt(
  buf: Uint8Array,
  offset: number,
  needle: Uint8Array,
): boolean {
  if (offset < 0 || offset + needle.length > buf.length) return false;
  for (let i = 0; i < needle.length; i++) {
    if (buf[offset + i] !== needle[i]) return false;
  }
  return true;
}

// Web builds resolve features in isolation; workspace unification would pull the desktop
// renderer's native-tls into wasm32. See CONTRIBUTING.md on `resolver.feature-unification`.
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

// `wasm-release`, as `cargo x guardrails check-matrix` builds it: a debug build also links
// Dioxus devtools, whose hot-reload socket finds no `dx serve` here and logs an error per retry.
await $`cargo build --locked ${isolated} -p dayframe_web --bin dayframe_web --target wasm32-unknown-unknown --profile wasm-release`;
const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
  .json<{ target_directory: string }>();
const target = $.path(metadata.target_directory);

const lockedBindgen = (await root.join("Cargo.lock").readText())
  .match(/\[\[package\]\]\nname = "wasm-bindgen"\nversion = "([^"]+)"/)?.[1];
const cliBindgen = (await $`wasm-bindgen --version`.text()).split(" ")[1];
if (lockedBindgen !== cliBindgen) {
  throw new Error(
    `wasm-bindgen CLI ${cliBindgen} does not match Cargo.lock's ${lockedBindgen}; use nix develop .#ci-desktop`,
  );
}

const site = target.join("dayframe_web/site");
await $`rm -rf ${site}`;
await Deno.mkdir(site.toString(), { recursive: true });
await $`wasm-bindgen --target web --no-typescript --out-dir ${site} --out-name dayframe_web ${
  target.join("wasm32-unknown-unknown/wasm-release/dayframe_web.wasm")
}`;
await Deno.copyFile(counter, site.join("counter.component.wasm").toString());
// `ui::ThemeRoot` loads `asset!("/assets/tailwind.css")`. Without `dx` the manganis
// `bundled_path` stays `BundledAsset::PLACEHOLDER_HASH`, so `Asset::resolve()` asks for
// `/assets/This should be replaced by dx…` (404 + console error → e2e fail). Rewrite the
// fixed-width `__ASSETS__` CBOR blob in place: patch bundled_path to `tailwind.css` and
// zero-pad the rest of the 4096-byte section so the wasm file size is unchanged.
const PLACEHOLDER =
  "This should be replaced by dx as part of the build process. If you see this error, make sure you are using a matching version of dx and dioxus and you are not stripping symbols from your binary.";
const MANGANIS_SECTION_SIZE = 4096;
{
  const hostWasmPath = site.join("dayframe_web_bg.wasm").toString();
  const wasm = await Deno.readFile(hostWasmPath);
  const ph = new TextEncoder().encode(PLACEHOLDER);
  const key = new TextEncoder().encode("bundled_path");
  const needle = new Uint8Array(key.length + 2 + ph.length);
  needle.set(key, 0);
  needle[key.length] = 0x58; // CBOR bytes, 1-byte length follows
  needle[key.length + 1] = ph.length;
  needle.set(ph, key.length + 2);
  let at = -1;
  for (let i = 0; i + needle.length <= wasm.length; i++) {
    let ok = true;
    for (let j = 0; j < needle.length; j++) {
      if (wasm[i + j] !== needle[j]) {
        ok = false;
        break;
      }
    }
    if (ok) {
      at = i;
      break;
    }
  }
  if (at < 0) {
    throw new Error(
      "dayframe_web_bg.wasm: manganis PLACEHOLDER_HASH not found (already patched, or dioxus changed the blob format)",
    );
  }
  // Map header `a3` sits 2 bytes before `74 absolute_source_path`; walk back from the key.
  const srcKey = new TextEncoder().encode("absolute_source_path");
  let mapStart = -1;
  for (let i = at; i >= 3; i--) {
    if (
      wasm[i] === 0xa3 && wasm[i + 1] === 0x74 &&
      matchesAt(wasm, i + 2, srcKey)
    ) {
      mapStart = i;
      break;
    }
  }
  if (mapStart < 0) {
    throw new Error(
      "dayframe_web_bg.wasm: BundledAsset map header (a3) not found",
    );
  }
  const newPath = "tailwind.css";
  const newPathBytes = new TextEncoder().encode(newPath);
  // bytes short form: major 2 | length (length < 24) → 0x40 | len
  const newValLen = 1 + newPathBytes.length;
  const oldValLen = 2 + ph.length;
  const delta = oldValLen - newValLen;
  const regionEnd = mapStart + MANGANIS_SECTION_SIZE;
  // Shift everything after the value left by `delta`, then zero-pad the tail of the
  // fixed-width section so the wasm file length stays identical.
  const after = at + needle.length;
  wasm.copyWithin(after - delta, after, regionEnd);
  wasm[at + key.length] = 0x40 | newPathBytes.length;
  wasm.set(newPathBytes, at + key.length + 1);
  wasm.fill(0, regionEnd - delta, regionEnd);
  await Deno.writeFile(hostWasmPath, wasm);
}
await Deno.mkdir(site.join("assets").toString(), { recursive: true });
await Deno.copyFile(
  root.join("src/ui/assets/tailwind.css").toString(),
  site.join("assets/tailwind.css").toString(),
);

// Bundle-size tracking (ADR 018 §7): the host module carries `wasmtime-environ` and a second
// `wasmparser` next to the Dioxus web runtime. Print raw + gzip so regressions are visible.
const hostWasm = site.join("dayframe_web_bg.wasm");
const hostBytes = await Deno.stat(hostWasm.toString()).then((s) => s.size);
const gzip = new Deno.Command("gzip", {
  args: ["-c", "-9", hostWasm.toString()],
  stdout: "piped",
}).outputSync().stdout;
console.log(
  `bundle-size dayframe_web host=${hostBytes} gzip=${gzip.length} guest=${
    (await Deno.stat(counter)).size
  }`,
);

await site.join("index.html").writeText(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Dayframe</title>
  </head>
  <body>
    <div id="main"></div>
    <script type="module">
      import init from "./dayframe_web.js";
      await init();
    </script>
  </body>
</html>
`);

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
      },
    });
  });
}
