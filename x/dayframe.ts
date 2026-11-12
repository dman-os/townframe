#!/usr/bin/env -S deno run --allow-all

// One entry point for running Dayframe locally. `--help` lists the commands.

import { prepareDayframePackages } from "./dayframe-packages.ts";
import { $ } from "./utils.ts";

const root = $.relativeDir("../");
const [command, ...rest] = Deno.args;

const help = `usage: deno run --allow-all x/dayframe.ts <command> [flags]

  desktop     build the Wasm packages and run the desktop shell in a webview (Wry)
                --native         the Blitz renderer instead
                --fresh          start from an empty store in a new temp file
                --store <path>   use this store file (DAYFRAME_STORE_PATH)
  web         build and serve dayframe_web (--build: build only)
  showcase    run the design showcase (dayframe_showcase) in a webview (Wry)
                --native         the Blitz renderer instead
  showcase-web  serve the design showcase for the browser (Dioxus web, wasm-release) on
                127.0.0.1:PORT, to compare its diff code in Firefox against Wry
                --port <n>       default 8765
                the lab's PR #84 view fetches pr84.diff from beside the page: it is copied
                from DAYFRAME_LAB_DIFF or /tmp/pr84.diff into the gitignored
                src/dayframe_showcase/public/ at serve time. Start choices are URL
                parameters: ?tab=diffs&diffs_view=pr84, ?kit=nav
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
    // One binary carries both launchers; the variable picks Blitz (see dayframe_desktop/main.rs).
    if (flag("--native")) env["DAYFRAME_RENDERER"] = "blitz";
    if (store) env["DAYFRAME_STORE_PATH"] = $.path(store).resolve().toString();
    console.log(`package dir: ${env["DAYFRAME_PACKAGE_DIR"]}`);
    console.log(`store path: ${env["DAYFRAME_STORE_PATH"] ?? "(app default)"}`);
    await dx(
      "-p",
      "dayframe_desktop",
      ...(flag("--native")
        ? ["--renderer", "native", "--features", "blitz"]
        : ["--renderer", "webview"]),
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
    await dx(
      "-p",
      "dayframe_showcase",
      "--renderer",
      flag("--native") ? "native" : "webview",
    ).env(flag("--native") ? { DAYFRAME_RENDERER: "blitz" } : {});
    break;
  case "showcase-web": {
    const portFlag = rest.indexOf("--port");
    const port = portFlag >= 0 ? rest[portFlag + 1] : "8765";
    if (!port) throw new Error("--port needs a number");
    const diff = $.path(Deno.env.get("DAYFRAME_LAB_DIFF") ?? "/tmp/pr84.diff");
    const served = root.join("src/dayframe_showcase/public");
    await Deno.mkdir(served.toString(), { recursive: true });
    if (await diff.exists()) {
      await diff.copyFile(served.join("pr84.diff"));
    } else {
      console.warn(
        `${diff} is missing: the PR #84 view will say how to produce it`,
      );
    }
    // Isolated feature resolution: workspace unification would pull the desktop renderer into
    // the wasm32 build (CONTRIBUTING.md, `resolver.feature-unification`).
    await $`deno run --allow-all ${
      root.join("x/dx.ts")
    } serve --web -p dayframe_showcase --no-default-features --features web --profile wasm-release --addr 127.0.0.1 --port ${port} --open false --interactive false --hot-reload false --hot-patch false`
      .cwd(root)
      .env({ CARGO_RESOLVER_FEATURE_UNIFICATION: "selected" });
    break;
  }
  case "packages":
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
