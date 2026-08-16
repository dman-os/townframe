// The `jco componentize` componentize-js 0.19.3 fallback (used while the project
// is on WASI Preview 2 packages < 0.2.10) can't enable the
// `@unstable(feature = clocks-timezone)` feature. `wasi-cli`'s package.wit
// imports the gated `wasi:clocks/timezone` interface, so WIT resolution fails
// with "interface not found in package". btress_auth doesn't use timezone, so
// drop that gated import from the fetched wasi-cli package after `wash wit fetch`.
// Run via `pnpm run fetch:wit` (which calls this after the fetch).
import { readFile, writeFile, readdir } from "node:fs/promises";
import { join } from "node:path";

const depsDir = "wit/deps";
let patched = false;
for (const dir of await readdir(depsDir)) {
  if (!dir.startsWith("wasi-cli-")) continue;
  const p = join(depsDir, dir, "package.wit");
  const src = await readFile(p, "utf8");
  // Remove the `@unstable(feature = clocks-timezone)` annotation + the
  // `import wasi:clocks/timezone@<version>;` line that follows it.
  const next = src.replace(
    /\n[^\n]*@unstable\(feature = clocks-timezone\)\n[^\n]*import wasi:clocks\/timezone@[0-9.]+;/g,
    "",
  );
  if (next !== src) {
    await writeFile(p, next);
    patched = true;
    console.log(`patch-wit-deps: removed gated wasi:clocks/timezone import from ${p}`);
  }
}
if (!patched) {
  console.log("patch-wit-deps: no wasi-cli timezone import found (already patched or upstream changed)");
}