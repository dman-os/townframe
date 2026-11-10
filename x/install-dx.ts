#!/usr/bin/env -S deno run --allow-all

import $ from "jsr:@david/dax@0.45.0";

function installationSource(cargoToml: string): string[] {
  const patchSection = cargoToml.match(
    /^\[patch\.crates-io\][ \t]*\r?\n([\s\S]*?)(?=^\[|(?![\s\S]))/m,
  )?.[1];
  if (!patchSection) {
    throw new Error("Cargo.toml is missing [patch.crates-io]");
  }
  const patch = patchSection.match(/^dioxus\s*=\s*\{([^}\n]*)\}/m)?.[1];
  if (!patch) {
    throw new Error("Cargo.toml [patch.crates-io] is missing dioxus");
  }
  const field = (name: string) =>
    patch.match(new RegExp(`\\b${name}\\s*=\\s*"([^"]+)"`))?.[1];
  const path = field("path");
  const repository = field("git");
  const revision = field("rev");
  if (path) {
    if (repository || revision) {
      throw new Error("Cargo.toml dioxus patch mixes path and Git sources");
    }
    return ["--path", path];
  }
  if (!repository || !revision) {
    throw new Error(
      "Cargo.toml dioxus patch requires path or pinned git and rev",
    );
  }
  return ["--git", repository, "--rev", revision];
}

export function resolveInstallRoot(
  root: ReturnType<typeof $.path>,
  env: Record<string, string | undefined>,
) {
  const configuredRoot = env.DIOXUS_CLI_ROOT?.trim();
  if (configuredRoot) {
    return root.resolve(configuredRoot);
  }
  const targetDir = env.CARGO_TARGET_DIR?.trim();
  if (targetDir) {
    return root.resolve(targetDir).join("dioxus-cli");
  }
  return root.join("target", "dioxus-cli");
}

async function install(rootPath: string, installPath: string) {
  const root = $.path(rootPath);
  const source = installationSource(await root.join("Cargo.toml").readText());
  if (source[0] === "--path") {
    const manifest = root.resolve(source[1]).join("Cargo.toml");
    const metadata =
      await $`cargo metadata --locked --no-deps --format-version 1 --manifest-path ${manifest}`
        .cwd(root)
        .json<{ packages: { name: string; manifest_path: string }[] }>();
    const cli = metadata.packages.find((pkg) => pkg.name === "dioxus-cli");
    if (!cli) {
      throw new Error(
        `Dioxus workspace for ${manifest} has no dioxus-cli package`,
      );
    }
    source[1] = $.path(cli.manifest_path).parentOrThrow().toString();
  }
  const installRoot = $.path(installPath);
  const executableName = Deno.build.os === "windows" ? "dx.exe" : "dx";
  const executable = installRoot.join("bin", executableName);

  await $`cargo install --locked ${source} dioxus-cli --features no-downloads --root ${installRoot} --force --debug`
    .cwd(root);
  await $`${executable} --version`;
  $.logLight(`Installed Dioxus CLI from ${source.join(" ")} at ${executable}`);
}

if (import.meta.main) {
  const root = $.path(new URL("../", import.meta.url));
  await install(
    root.toString(),
    resolveInstallRoot(root, Deno.env.toObject()).toString(),
  );
}

Deno.test("Dioxus CLI install root prefers DIOXUS_CLI_ROOT, then CARGO_TARGET_DIR, then the repo target", () => {
  const root = $.path("/workspace/repo");
  const cases: [Record<string, string>, string][] = [
    [
      {
        DIOXUS_CLI_ROOT: "/custom/dx-root",
        CARGO_TARGET_DIR: "/elsewhere/target",
      },
      "/custom/dx-root",
    ],
    [
      { CARGO_TARGET_DIR: "/elsewhere/target" },
      "/elsewhere/target/dioxus-cli",
    ],
    [{}, "/workspace/repo/target/dioxus-cli"],
  ];
  for (const [env, expected] of cases) {
    const actual = resolveInstallRoot(root, env).toString();
    if (actual !== expected) {
      throw new Error(
        `install root for ${
          JSON.stringify(env)
        } was ${actual}, expected ${expected}`,
      );
    }
  }
});

Deno.test("Dioxus CLI source follows the local workspace patch", () => {
  const actual = installationSource(`
[patch.crates-io]
dioxus = { path = "../fork with spaces/packages/dioxus" }
[profile.dev]
opt-level = 0
`);
  if (
    JSON.stringify(actual) !==
      JSON.stringify(["--path", "../fork with spaces/packages/dioxus"])
  ) {
    throw new Error(`incorrect local source: ${JSON.stringify(actual)}`);
  }
});

Deno.test("Dioxus CLI source preserves a pinned Git patch", () => {
  const repository = "https://github.com/dioxuslabs/dioxus";
  const actual = installationSource(`
[patch.crates-io]
dioxus = { git = "${repository}", rev = "pinned-revision" }
`);
  const expected = ["--git", repository, "--rev", "pinned-revision"];
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`incorrect Git source: ${JSON.stringify(actual)}`);
  }
});

Deno.test("Dioxus CLI source rejects absent, ambiguous and unpinned patches", () => {
  for (
    const cargoToml of [
      "[workspace]\n",
      '[patch.crates-io]\nother = { path = "../other" }\n',
      '# [patch.crates-io]\ndioxus = { path = "../other" }\n',
      '[patch.crates-io]\n[profile.dev]\ndioxus = { path = "../other" }\n',
      '[patch.crates-io]\ndioxus = { git = "repository" }\n',
      '[patch.crates-io]\ndioxus = { path = "local", git = "repository", rev = "pin" }\n',
    ]
  ) {
    let rejected = false;
    try {
      installationSource(cargoToml);
    } catch {
      rejected = true;
    }
    if (!rejected) {
      throw new Error(`invalid source was accepted: ${cargoToml}`);
    }
  }
});

Deno.test("Dioxus CLI installs the metadata-selected local package with spaced paths", async () => {
  const temporary = await Deno.makeTempDir({ prefix: "dayframe-cli-" });
  const priorTargetDir = Deno.env.get("CARGO_TARGET_DIR");
  try {
    const directory = $.path(temporary);
    const app = directory.join("app");
    const fork = directory.join("fork with spaces");
    const cli = fork.join("tools", "cli");
    const library = fork.join("library");
    for (const path of [app, cli, library]) {
      await Deno.mkdir(path.toString(), { recursive: true });
    }
    await app.join("Cargo.toml").writeText(
      '[patch.crates-io]\ndioxus = { path = "../fork with spaces/library" }\n',
    );
    await fork.join("Cargo.toml").writeText(
      '[workspace]\nmembers = ["library", "tools/cli"]\nresolver = "3"\n',
    );
    await library.join("Cargo.toml").writeText(
      '[package]\nname = "dioxus"\nversion = "0.0.0"\nedition = "2024"\n[lib]\npath = "lib.rs"\n',
    );
    await library.join("lib.rs").writeText("");
    await cli.join("Cargo.toml").writeText(
      '[package]\nname = "dioxus-cli"\nversion = "0.0.0"\nedition = "2024"\n[[bin]]\nname = "dx"\npath = "main.rs"\n[features]\nno-downloads = []\n',
    );
    await cli.join("main.rs").writeText(
      '#[cfg(not(feature = "no-downloads"))]\ncompile_error!("no-downloads feature is required");\nfn main() { println!("fixture dx"); }\n',
    );
    await fork.join("Cargo.lock").writeText(
      'version = 4\n[[package]]\nname = "dioxus"\nversion = "0.0.0"\n[[package]]\nname = "dioxus-cli"\nversion = "0.0.0"\n',
    );
    Deno.env.delete("CARGO_TARGET_DIR");
    const destination = directory.join("dx root");
    await install(app.toString(), destination.toString());
    const executable = destination.join(
      "bin",
      Deno.build.os === "windows" ? "dx.exe" : "dx",
    );
    const result = await $`${executable} --version`.text();
    if (result.trim() !== "fixture dx") {
      throw new Error(`wrong CLI installed: ${result}`);
    }
    if (await app.join("target").exists()) {
      throw new Error("install unexpectedly created app/target");
    }
  } finally {
    if (priorTargetDir === undefined) {
      Deno.env.delete("CARGO_TARGET_DIR");
    } else {
      Deno.env.set("CARGO_TARGET_DIR", priorTargetDir);
    }
    await Deno.remove(temporary, { recursive: true });
  }
});
