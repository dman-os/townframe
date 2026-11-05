#!/usr/bin/env -S deno run --allow-read --allow-write --allow-run --allow-env
// Builds the wrapper component around an unmodified Pierre `highlights.wasm`.
//
//   deno run -A bench/highlight/component/build.ts <highlights.wasm> <out-dir>
//
// Upstream update: drop the new `highlights.wasm` in place and rerun this; nothing in the adapter
// names Pierre's code, only its control block (byte 0 language, byte 1 mode, u32@2 length, u32@6
// output pointer, u32@10 output length, input at 81920), so a layout change in Pierre shows up
// as the differential test (golden records from the direct path) failing, not as a silent drift.
//
// Writes into <out-dir>:
//   wrapper.component.wasm      Pierre's module byte for byte + adapter; imports `ident`
//   ident.component.wasm        the `ident` provider (unicode-ident)
//   highlighter.component.wasm  the wrapper with `ident` plugged: imports nothing, exports
//                               `townframe:highlights/highlight@0.1.0`
import { dirname, fromFileUrl, join, resolve } from "jsr:@std/path@1";

const here = dirname(fromFileUrl(import.meta.url));
const [pierrePath, outArg] = Deno.args;
if (!pierrePath || !outArg) {
  throw new Error("usage: build.ts <highlights.wasm> <out-dir>");
}
const out = resolve(outArg);
await Deno.mkdir(out, { recursive: true });

async function run(cmd: string, args: string[], cwd?: string) {
  console.log("+", cmd, ...args);
  const { code } = await new Deno.Command(cmd, {
    args,
    cwd,
    stdout: "inherit",
    stderr: "inherit",
  }).output();
  if (code !== 0) throw new Error(`${cmd} exited ${code}`);
}

const pierre = await Deno.readFile(resolve(pierrePath));
// The WAT component text format has no binary-module form, so embed the module as text:
// `wasm-tools print` of a core module parses back to the identical bytes, and the check after the
// build confirms the module is in the component verbatim.
const printed = new TextDecoder().decode(
  (await new Deno.Command("wasm-tools", {
    args: ["print", resolve(pierrePath)],
    stdout: "piped",
  }).output()).stdout,
);
if (!printed.startsWith("(module\n")) {
  throw new Error("unexpected `wasm-tools print` shape");
}
const fields = printed.slice("(module".length, printed.lastIndexOf(")"));
const wat = (await Deno.readTextFile(join(here, "wrapper.wat.in")))
  .replace("@PIERRE@", () => fields);
const watPath = join(out, "wrapper.wat");
await Deno.writeTextFile(watPath, wat);

const wrapper = join(out, "wrapper.component.wasm");
await run("wasm-tools", ["parse", watPath, "-o", wrapper]);
await run("wasm-tools", ["validate", "--features", "all", wrapper]);

function indexOf(haystack: Uint8Array, needle: Uint8Array): number {
  outer: for (
    let i = haystack.indexOf(needle[0]);
    i >= 0;
    i = haystack.indexOf(needle[0], i + 1)
  ) {
    for (let j = 1; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) continue outer;
    }
    return i;
  }
  return -1;
}
if (indexOf(await Deno.readFile(wrapper), pierre) < 0) {
  throw new Error("the wrapper does not embed highlights.wasm byte for byte");
}

// The binary must really implement the committed WIT's `wrapper` world.
await run("wasm-tools", [
  "component",
  "targets",
  "--world",
  "wrapper",
  join(here, "wit"),
  wrapper,
]);

await run("cargo", [
  "build",
  "--release",
  "--target",
  "wasm32-unknown-unknown",
  "--manifest-path",
  join(here, "ident/Cargo.toml"),
]);
// The target directory is the environment's (`./x/agent-build.ts` sets CARGO_TARGET_DIR).
const target = Deno.env.get("CARGO_TARGET_DIR") ?? join(here, "ident/target");
// wasm32-unknown-unknown rather than wasip3: the wasip3 std takes `context.get` (a component-model
// async builtin) even for a pure function, which a plain wasmtime engine refuses to compile.
await run("wasm-tools", [
  "component",
  "new",
  join(target, "wasm32-unknown-unknown/release/highlights_ident.wasm"),
  "-o",
  join(out, "ident.component.wasm"),
]);

const highlighter = join(out, "highlighter.component.wasm");
await run("wac", [
  "plug",
  "--plug",
  join(out, "ident.component.wasm"),
  wrapper,
  "-o",
  highlighter,
]);
await run("wasm-tools", ["validate", "--features", "all", highlighter]);
console.log("wrote", highlighter);
