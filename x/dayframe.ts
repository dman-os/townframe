#!/usr/bin/env -S deno run --allow-all

// One entry point for running Dayframe locally. `--help` lists the commands.

import { prepareDayframePackages } from "./dayframe-packages.ts";
import { $, useDotenvTargetDir } from "./utils.ts";

const root = $.relativeDir("../");
const [command, ...rest] = Deno.args;

const help = `usage: deno run --allow-all x/dayframe.ts <command> [flags]

  desktop     build the Wasm packages and run the native desktop shell
                --webview        webview renderer (AT-SPI interaction checks)
                --fresh          start from an empty store in a new temp file
                --store <path>   use this store file (DAYFRAME_STORE_PATH)
  web         build and serve dayframe_web (--build: build only)
  showcase    run the design showcase (dayframe_showcase) natively
  packages    only (re)build the Wasm packages and print the DAYFRAME_* env lines
  test        x/check-dayframe.ts, then the dayframe_web Playwright e2e
                --no-e2e         skip the browser part
  bombadil    explore dayframe_web with Bombadil and check the shell invariants
                (src/dayframe_web/bombadil/shell.ts); BOMBADIL_TIME_LIMIT=<n>s sets
                the exploration time per width, default 60s
`;

function flag(name: string): boolean {
  return rest.includes(name);
}

function dx(...args: string[]) {
  return $`deno run --allow-all ${root.join("x/dx.ts")} serve --desktop ${args}`
    .cwd(root);
}

switch (command) {
  case "desktop": {
    await useDotenvTargetDir();
    const env = await prepareDayframePackages({ testFixtures: false });
    const storeFlag = rest.indexOf("--store");
    if (flag("--fresh") && storeFlag >= 0) {
      throw new Error("--fresh and --store are mutually exclusive");
    }
    let store: string | undefined;
    if (storeFlag >= 0) {
      store = rest[storeFlag + 1];
      if (!store) throw new Error("--store needs a path");
    } else if (flag("--fresh")) {
      // A path that does not exist yet: the app creates it, so the run starts empty.
      store = `${await Deno.makeTempDir({ prefix: "dayframe-store-" })}/store`;
    }
    if (store) env["DAYFRAME_STORE_PATH"] = $.path(store).resolve().toString();
    console.log(`package dir: ${env["DAYFRAME_PACKAGE_DIR"]}`);
    console.log(`store path: ${env["DAYFRAME_STORE_PATH"] ?? "(app default)"}`);
    await dx(
      "-p",
      "dayframe_desktop",
      "--renderer",
      flag("--webview") ? "webview" : "native",
      "--hot-patch",
      "false",
    ).env(env);
    break;
  }
  case "web":
    await $`deno run --allow-all ${root.join("x/dayframe-web.ts")} ${
      flag("--build") ? ["--build"] : []
    }`;
    break;
  case "showcase":
    await dx("-p", "dayframe_showcase", "--renderer", "native");
    break;
  case "packages":
    await useDotenvTargetDir();
    await prepareDayframePackages({ testFixtures: true });
    break;
  case "test":
    await $`deno run --allow-all ${root.join("x/check-dayframe.ts")}`;
    if (!flag("--no-e2e")) {
      await $`pnpm -C src/dayframe_web run e2e`.cwd(root);
    }
    break;
  case "bombadil":
    await $`pnpm -C src/dayframe_web run bombadil`.cwd(root);
    break;
  case "--help":
  case "-h":
  case undefined:
    console.log(help);
    break;
  default:
    console.error(`unknown command: ${command}\n\n${help}`);
    Deno.exit(2);
}
