#!/usr/bin/env -S deno run --allow-all

import $ from "jsr:@david/dax@0.45.0";
import { resolveInstallRoot } from "./install-dx.ts";

const scriptDir = $.path(new URL(".", import.meta.url));
const root = (await scriptDir.join("x", "install-dx.ts").exists())
  ? scriptDir
  : scriptDir.parent()!;
const dotenvPath = root.join(".env");
const dotenvText = (await dotenvPath.exists())
  ? await dotenvPath.readText()
  : "";
if (!Deno.env.get("CARGO_TARGET_DIR")) {
  const dotenvTarget = dotenvText
    .match(/^(?:export\s+)?CARGO_TARGET_DIR\s*=\s*["']?([^"'\r\n#]+)["']?/m)
    ?.[1]
    ?.trim();
  if (dotenvTarget) {
    Deno.env.set("CARGO_TARGET_DIR", root.resolve(dotenvTarget).toString());
  }
}
const installRoot = resolveInstallRoot(root, Deno.env.toObject(), dotenvText);

const executableName = Deno.build.os === "windows" ? "dx.exe" : "dx";
const executable = installRoot.join("bin", executableName);

if (!(await executable.exists())) {
  $.logStep(
    "Local Dioxus CLI not found. Building and installing via x/install-dx.ts...",
  );
  const installScript = root.join("x", "install-dx.ts");
  await $`deno run --allow-all ${installScript}`.cwd(root);
}

const result = await $`${executable} ${Deno.args}`.noThrow();
Deno.exit(result.code);
