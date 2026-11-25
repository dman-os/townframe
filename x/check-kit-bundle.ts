#!/usr/bin/env -S deno run --allow-read --allow-write --allow-run

/**
 * Fails when the kit's generated stylesheets are stale: the token CSS
 * (`styles/tokens.generated.css`, from `styles/tokens/*.tokens.json`) or the Tailwind bundle
 * (`assets/tailwind.css`, from `tailwind.css`, every skin under `styles/kit/` and every class the
 * repository's sources name). The bundle is what the shell links, so a skin or token edited
 * without regenerating it ships the old look.
 *
 * It regenerates both into temporary files and compares, which costs about a second. With
 * `--write` it updates the committed files instead of failing.
 */

const kit = new URL("../src/clay_dioxus_kit/", import.meta.url);
const entry = new URL("tailwind.css", kit);
const bundle = new URL("assets/tailwind.css", kit);
const write = Deno.args.includes("--write");

async function run(command: string, args: string[]): Promise<void> {
  const { code, stderr } = await new Deno.Command(command, {
    args,
    cwd: new URL("../", import.meta.url),
    stdout: "inherit",
    stderr: "piped",
  }).output();
  if (code !== 0) {
    console.error(new TextDecoder().decode(stderr));
    throw new Error(`${command} ${args.join(" ")} exited with ${code}`);
  }
}

// The Tailwind bundle imports the generated tokens, so they are brought up to date first.
await run(Deno.execPath(), [
  "run",
  "--allow-read",
  "--allow-write",
  "x/design-tokens.ts",
  write ? "--write" : "--check",
]);

const scratch = await Deno.makeTempDir({ prefix: "kit-bundle-" });
try {
  const fresh = `${scratch}/tailwind.css`;
  await run("tailwindcss", [
    "-i",
    entry.pathname,
    "-o",
    write ? bundle.pathname : fresh,
  ]);
  if (!write) {
    const committed = await Deno.readTextFile(bundle);
    const regenerated = await Deno.readTextFile(fresh);
    if (committed !== regenerated) {
      const line = committed.split("\n").findIndex((text, index) =>
        text !== regenerated.split("\n")[index]
      ) + 1;
      console.error(
        `assets/tailwind.css is stale (first difference at line ${line}). ` +
          "Regenerate it: deno run --allow-all x/check-kit-bundle.ts --write",
      );
      Deno.exit(1);
    }
  }
} finally {
  await Deno.remove(scratch, { recursive: true });
}
console.log(
  write
    ? "regenerated the kit's token CSS and Tailwind bundle"
    : "the kit's token CSS and Tailwind bundle are current",
);
