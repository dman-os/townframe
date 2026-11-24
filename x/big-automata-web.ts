#!/usr/bin/env -S deno run --allow-all

// Build and serve the `big_automata_web` browser host, beside the native peer it syncs from.
//
//   deno run --allow-all x/big-automata-web.ts            build, start the peer, serve on HOST:PORT
//   deno run --allow-all x/big-automata-web.ts --build    build only
//
// The peer is `big_automata_iroh`'s `web_fixture` example: an in-process iroh relay and a node
// serving a part store through it. Its first stdout line (relay URL, node id, the part and its
// object count) is served as `/fixture.json`, which the page reads to connect. The peer runs
// until its stdin closes, so this script's death (however it ends) takes the peer along.

import { $ } from "./utils.ts";

const buildOnly = Deno.args.includes("--build");
const root = $.relativeDir("../");

// Web builds resolve features in isolation (see CONTRIBUTING.md on
// `resolver.feature-unification`); the native fixture build uses the workspace's.
const isolated = ["--config", 'resolver.feature-unification="selected"'];

await $`cargo build --locked ${isolated} -p big_automata_web --bin big_automata_web --target wasm32-unknown-unknown --profile wasm-release`;
await $`cargo build --locked -p big_automata_iroh --example web_fixture`;
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

const site = target.join("big_automata_web/site");
await $`rm -rf ${site}`;
await Deno.mkdir(site.toString(), { recursive: true });
await $`wasm-bindgen --target web --no-typescript --out-dir ${site} --out-name big_automata_web ${
  target.join("wasm32-unknown-unknown/wasm-release/big_automata_web.wasm")
}`;
await site.join("index.html").writeText(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>big_automata web host</title>
  </head>
  <body>
    <p>status: <span id="status">connecting</span></p>
    <p>objects: <span id="objects"></span> of <span id="expected"></span></p>
    <script type="module">
      import init from "./big_automata_web.js";
      await init();
    </script>
  </body>
</html>
`);

if (!buildOnly) {
  const peer = new Deno.Command(
    target.join("debug/examples/web_fixture").toString(),
    { stdin: "piped", stdout: "piped", stderr: "inherit" },
  ).spawn();
  // Held for the process's life: closing it is the peer's cue to stop.
  const peerStdin = peer.stdin.getWriter();
  const lines = peer.stdout.pipeThrough(new TextDecoderStream()).getReader();
  let first = "";
  while (!first.includes("\n")) {
    const chunk = await lines.read();
    if (chunk.done) throw new Error("the fixture exited before printing");
    first += chunk.value;
  }
  const fixture = first.slice(0, first.indexOf("\n"));
  JSON.parse(fixture); // a malformed line fails here, not in the page
  console.log(`fixture ${fixture}`);
  globalThis.addEventListener("unload", () => peerStdin.close());

  const types: Record<string, string> = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
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
    if (name === "fixture.json") {
      return new Response(fixture, {
        headers: { "content-type": types[".json"] },
      });
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
  // The peer's end is the script's end: a crashed fixture must not leave a page syncing from
  // nothing.
  const status = await peer.status;
  throw new Error(`the fixture exited (${status.code})`);
}
