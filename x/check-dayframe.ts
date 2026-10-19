#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const prepareOnly = Deno.args.includes("--prepare-only");
if (prepareOnly && Deno.args.length !== 1) {
  throw new Error("--prepare-only cannot be combined with nextest arguments");
}

const root = $.relativeDir("../");
const dotenvPath = root.join(".env");
if (!Deno.env.get("CARGO_TARGET_DIR") && await dotenvPath.exists()) {
  const dotenvTarget = (await dotenvPath.readText())
    .match(/^(?:export\s+)?CARGO_TARGET_DIR\s*=\s*["']?([^"'\r\n#]+)["']?/m)
    ?.[1]
    ?.trim();
  if (dotenvTarget) {
    Deno.env.set("CARGO_TARGET_DIR", root.resolve(dotenvTarget).toString());
  }
}

await $`cargo build --locked -p dayframe_guest --example counter --target wasm32-unknown-unknown`;
const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
  .json<{ target_directory: string }>();
const target = $.path(metadata.target_directory);
const directory = target.join("dayframe");
await Deno.mkdir(directory.toString(), { recursive: true });
const component = directory.join("counter.component.wasm");
const core = target.join(
  "wasm32-unknown-unknown",
  "debug",
  "examples",
  "counter.wasm",
);
await $`wasm-tools component new ${core} --output ${component}`;

console.log(`DAYFRAME_COUNTER_COMPONENT=${component}`);

if (!prepareOnly) {
  const packages = [
    "dayframe_presentation",
    "dayframe_app",
    "dayframe_guest",
    "dayframe_engine_wasmtime",
    "dayframe_settings_contract",
    "dayframe_settings",
    "dayframe_sample",
    "dayframe_counter",
    "desktop",
  ].flatMap((name) => ["-p", name]);
  const env = { DAYFRAME_COUNTER_COMPONENT: component.toString() };
  await $`cargo nextest run --locked ${packages} ${Deno.args}`.env(env);
  await $`cargo clippy --locked --no-deps --all-targets --all-features ${packages}`
    .env(env);
}
