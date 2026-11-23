#!/usr/bin/env -S deno run --no-config --allow-read --allow-write --allow-run
// Assembles the static `daytxt_web` site from a cargo-built `daytxt_web.wasm` and a package
// directory (`<name>.manifest.json` + `<name>.component.wasm` pairs). Called by
// `x/daytxt-web.ts` (the dev loop) and by the `daytxt-web` Nix package, so the site has one
// implementation.
//
// It has no imports and shells out only to `wasm-bindgen` and `b3sum` (both on PATH), so it runs
// inside the Nix sandbox, where `jsr:` imports cannot be fetched. Run it with `--no-config`.
//
//   daytxt-site.ts --wasm <cargo daytxt_web.wasm> --packages <dir> --tailwind <tailwind.css>
//                    --out <site dir> [--cargo-lock <Cargo.lock>]
//
// `--cargo-lock` asserts that the `wasm-bindgen` CLI matches the lock's `wasm-bindgen` crate.
// The caller owns the output directory's emptiness: `--out` must not exist or be empty.
//
// The Content-Security-Policy is an HTTP header and so belongs to whatever serves the site; see
// `x/daytxt-web.ts`.

function arg(name: string, required = true): string | undefined {
  const at = Deno.args.indexOf(name);
  const value = at < 0 ? undefined : Deno.args[at + 1];
  if (required && value === undefined) throw new Error(`missing ${name}`);
  return value;
}

async function run(command: string, args: string[]): Promise<string> {
  const output = await new Deno.Command(command, {
    args,
    stdout: "piped",
    stderr: "inherit",
  }).output();
  if (!output.success) {
    throw new Error(`${command} ${args.join(" ")} exited with ${output.code}`);
  }
  return new TextDecoder().decode(output.stdout);
}

const wasmInput = arg("--wasm")!;
const packageDirectory = arg("--packages")!;
const tailwind = arg("--tailwind")!;
const site = arg("--out")!;
const cargoLock = arg("--cargo-lock", false);

if (cargoLock !== undefined) {
  const lockedBindgen = (await Deno.readTextFile(cargoLock))
    .match(/\[\[package\]\]\nname = "wasm-bindgen"\nversion = "([^"]+)"/)?.[1];
  const cliBindgen = (await run("wasm-bindgen", ["--version"])).split(" ")[1]
    .trim();
  if (lockedBindgen !== cliBindgen) {
    throw new Error(
      `wasm-bindgen CLI ${cliBindgen} does not match Cargo.lock's ${lockedBindgen}; use nix develop .#ci-desktop`,
    );
  }
}

await Deno.mkdir(`${site}/packages`, { recursive: true });
await run("wasm-bindgen", [
  "--target",
  "web",
  "--no-typescript",
  "--out-dir",
  site,
  "--out-name",
  "daytxt_web",
  wasmInput,
]);

// One index entry per `<name>.manifest.json` beside its `<name>.component.wasm`, in name order
// (the order the desktop shell scans them in). The entry is pinned with the BLAKE3 digest of the
// component bytes, the digest big_blobs uses and `clay_mdk::package::Package::digest` computes;
// the shell verifies it before `wcmp` compiles the package. Deno's WebCrypto lacks BLAKE3, so
// `b3sum` computes it.
const packageNames = [...Deno.readDirSync(packageDirectory)]
  .flatMap((entry) => entry.name.match(/^(.+)\.manifest\.json$/)?.[1] ?? [])
  .sort();
let guestBytes = 0;
{
  const index = [];
  for (const name of packageNames) {
    const component = `${packageDirectory}/${name}.component.wasm`;
    const bytes = await Deno.readFile(component);
    guestBytes += bytes.length;
    index.push({
      location: `packages/${name}.component.wasm`,
      digest: (await run("b3sum", ["--no-names", component])).trim(),
      sidecar: JSON.parse(
        await Deno.readTextFile(`${packageDirectory}/${name}.manifest.json`),
      ),
    });
    await Deno.writeFile(`${site}/packages/${name}.component.wasm`, bytes);
  }
  await Deno.writeTextFile(
    `${site}/packages/index.json`,
    JSON.stringify(index),
  );
}

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

// `clay_dioxus_kit::ThemeRoot` loads `asset!("/assets/tailwind.css")`. Without `dx` the manganis
// `bundled_path` stays `BundledAsset::PLACEHOLDER_HASH`, so `Asset::resolve()` asks for
// `/assets/This should be replaced by dx…` (404 + console error → e2e fail). Rewrite the
// fixed-width `__ASSETS__` CBOR blob in place: patch bundled_path to `tailwind.css` and
// zero-pad the rest of the 4096-byte section so the wasm file size is unchanged.
const PLACEHOLDER =
  "This should be replaced by dx as part of the build process. If you see this error, make sure you are using a matching version of dx and dioxus and you are not stripping symbols from your binary.";
const MANGANIS_SECTION_SIZE = 4096;
{
  const hostWasmPath = `${site}/daytxt_web_bg.wasm`;
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
    if (matchesAt(wasm, i, needle)) {
      at = i;
      break;
    }
  }
  if (at < 0) {
    throw new Error(
      "daytxt_web_bg.wasm: manganis PLACEHOLDER_HASH not found (already patched, or dioxus changed the blob format)",
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
      "daytxt_web_bg.wasm: BundledAsset map header (a3) not found",
    );
  }
  const newPathBytes = new TextEncoder().encode("tailwind.css");
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
await Deno.mkdir(`${site}/assets`, { recursive: true });
await Deno.copyFile(tailwind, `${site}/assets/tailwind.css`);

// Bundle-size tracking (ADR 018 §7): the host module carries `wasmtime-environ` and a second
// `wasmparser` next to the Dioxus web runtime. Print raw + gzip so regressions are visible.
{
  const hostWasm = `${site}/daytxt_web_bg.wasm`;
  const hostBytes = (await Deno.stat(hostWasm)).size;
  const gzipped = (await new Deno.Command("gzip", {
    args: ["-c", "-9", hostWasm],
    stdout: "piped",
  }).output()).stdout.length;
  console.log(
    `bundle-size daytxt_web host=${hostBytes} gzip=${gzipped} guests=${guestBytes}`,
  );
}

// The boot module is a file, not an inline script, so a server's CSP can omit `'unsafe-inline'`
// from `script-src`: that omission is what stops a guest `javascript:` href from running.
await Deno.writeTextFile(
  `${site}/boot.js`,
  `import init from "./daytxt_web.js";
await init();
`,
);
await Deno.writeTextFile(
  `${site}/index.html`,
  `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>daytxt</title>
  </head>
  <body>
    <div id="main"></div>
    <script type="module" src="./boot.js"></script>
  </body>
</html>
`,
);
