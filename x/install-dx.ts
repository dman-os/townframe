#!/usr/bin/env -S deno run --allow-env --allow-read --allow-run --allow-write

import { fromFileUrl, join } from "jsr:@std/path@1.0.8";

const root = fromFileUrl(new URL("../", import.meta.url));
const cargoToml = await Deno.readTextFile(join(root, "Cargo.toml"));

const metadata = cargoToml.match(
  /\[workspace\.metadata\.townframe\.dioxus\]([\s\S]*?)(?=\n\[|$)/,
)?.[1];
if (!metadata) {
  throw new Error("Cargo.toml is missing [workspace.metadata.townframe.dioxus]");
}
const dioxusMetadata = metadata;

function metadataString(name: string): string {
  const value = dioxusMetadata.match(new RegExp(`^${name}\\s*=\\s*"([^"]+)"`, "m"))?.[1];
  if (!value) {
    throw new Error(`Cargo.toml Dioxus metadata is missing ${name}`);
  }
  return value;
}

const repository = metadataString("repository");
const revision = metadataString("revision");
const installRoot = Deno.env.get("DIOXUS_CLI_ROOT") ?? join(root, "target", "dioxus-cli");
const executableName = Deno.build.os === "windows" ? "dx.exe" : "dx";
const executable = join(installRoot, "bin", executableName);

const install = new Deno.Command("cargo", {
  args: [
    "install",
    "--locked",
    "--git",
    repository,
    "--rev",
    revision,
    "dioxus-cli",
    "--features",
    "no-downloads",
    "--root",
    installRoot,
    "--force",
  ],
  cwd: root,
  stdin: "inherit",
  stdout: "inherit",
  stderr: "inherit",
});
const installStatus = await install.output();
if (!installStatus.success) {
  Deno.exit(installStatus.code);
}

const versionStatus = await new Deno.Command(executable, {
  args: ["--version"],
  stdin: "inherit",
  stdout: "inherit",
  stderr: "inherit",
}).output();
if (!versionStatus.success) {
  Deno.exit(versionStatus.code);
}

console.error(`Installed Dioxus CLI ${revision} at ${executable}`);
