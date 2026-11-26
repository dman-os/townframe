#!/usr/bin/env -S deno run --allow-all

// Build the intxt viewer (`src/intxt_web`, a Dioxus fullstack app) as one `wasi:http` component with
// its hydrating client embedded in it. The two halves are one crate built twice:
//
//   1. the client, `--features web` for wasm32-unknown-unknown, run through `wasm-bindgen` and
//      `wasm-opt -Oz` into a directory of `intxt_web.js`, `intxt_web_bg.wasm` and the snippets;
//   2. the component, `--features server` for wasm32-wasip3, whose `build.rs` compiles that
//      directory in (`INTXT_WEB_CLIENT_DIR`) and serves it under `/assets/`.
//
//   deno run --allow-all x/intxt-web.ts              build; prints where the component is
//   deno run --allow-all x/intxt-web.ts --out FILE   also copy the component to FILE
//
// Serve it with the unmodified host: `cargo run -p intxt_web_host -- --component <component>`.

import { $ } from "./utils.ts";

const args = Deno.args;
const outIndex = args.indexOf("--out");
const out = outIndex >= 0 ? args[outIndex + 1] : undefined;
const root = $.relativeDir("../");
const manifest = "src/intxt_web/Cargo.toml";

// The crate is its own single-crate workspace with its own lockfile and target directory.
const metadata =
  await $`cargo metadata --locked --no-deps --format-version 1 --manifest-path ${manifest}`
    .json<{ target_directory: string }>();
const target = $.path(metadata.target_directory);

// `wasm-release` has no debug info, which wasm-bindgen would carry into the shipped file.
await $`cargo build --locked --manifest-path ${manifest} --no-default-features --features web --bin intxt_web --target wasm32-unknown-unknown --profile wasm-release`;

// wasm-bindgen's CLI and the library the client links must be the same version.
const lockedBindgen = (await root.join("src/intxt_web/Cargo.lock").readText())
  .match(/\[\[package\]\]\nname = "wasm-bindgen"\nversion = "([^"]+)"/)?.[1];
const cliBindgen = (await $`wasm-bindgen --version`.text()).split(" ")[1]
  .trim();
if (lockedBindgen !== cliBindgen) {
  throw new Error(
    `wasm-bindgen CLI ${cliBindgen} does not match src/intxt_web/Cargo.lock's ${lockedBindgen}; use nix develop`,
  );
}

const client = target.join("intxt_web_client");
await $`rm -rf ${client}`;
await Deno.mkdir(client.toString(), { recursive: true });
await $`wasm-bindgen --target web --no-typescript --out-dir ${client} --out-name intxt_web ${
  target.join("wasm32-unknown-unknown/wasm-release/intxt_web.wasm")
}`;
const bindgenOutput = client.join("intxt_web_bg.wasm");
const optimized = client.join("intxt_web_bg.opt.wasm");
// The features rustc enables for this target, and no more: `--all-features` also lets binaryen write
// the compact-imports encoding, which browsers older than 2025 refuse to compile.
const features = [
  "bulk-memory",
  "mutable-globals",
  "nontrapping-float-to-int",
  "reference-types",
  "sign-ext",
  "multivalue",
].map((feature) => `--enable-${feature}`);
await $`wasm-opt -Oz ${features} ${bindgenOutput} -o ${optimized}`;
await $`mv ${optimized} ${bindgenOutput}`;

await $`env INTXT_WEB_CLIENT_DIR=${client} cargo build --locked --manifest-path ${manifest} --lib --target wasm32-wasip3 --release`;
// The shipped component has no DWARF (most of the linked file) and keeps its name section, so a
// trap's frames still name their functions; the linked build stays beside it for file:line. As
// `x/daytxt-packages.ts` does.
const linked = target.join("wasm32-wasip3/release/intxt_web.wasm");
const component = target.join("intxt_web.wasm");
await $`wasm-tools strip ${linked} -o ${component}`;
if (out) {
  await Deno.copyFile(component.toString(), out);
}
const size = (path: { toString(): string }) =>
  Deno.statSync(path.toString()).size;
console.log(
  `component ${component} (${
    size(component)
  } bytes), client wasm ${size(bindgenOutput)} bytes, glue ${
    size(client.join("intxt_web.js"))
  } bytes`,
);
