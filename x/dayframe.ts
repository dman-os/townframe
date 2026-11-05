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
  android     build the Android APK (dayframe_mobile)
                --abi <arm64|x86_64>  target ABI, default arm64 (aarch64-linux-android);
                                 x86_64 is what an emulator on this host runs
                --release        release profile (unsigned unless Dioxus.toml configures signing)
                --install        adb install the APK on the attached device
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
  case "android": {
    await useDotenvTargetDir();
    const env = await prepareDayframePackages();
    // The APK carries its packages as assets: `dayframe_mobile` bundles `packages/` with
    // `asset!`, so stage the prepared counter and layout beside the crate (gitignored).
    const staged = root.join("src/dayframe_mobile/packages");
    await Deno.mkdir(staged.toString(), { recursive: true });
    for (const name of ["counter", "layout"]) {
      for (const suffix of ["manifest.json", "component.wasm"]) {
        await $`cp ${
          $.path(env["DAYFRAME_PACKAGE_DIR"]).join(`${name}.${suffix}`)
        } ${staged.join(`${name}.${suffix}`)}`;
      }
    }
    const abiFlag = rest.indexOf("--abi");
    const abi = abiFlag >= 0 ? rest[abiFlag + 1] : "arm64";
    const androidTargets: Record<string, string> = {
      arm64: "aarch64-linux-android",
      x86_64: "x86_64-linux-android",
    };
    const androidTarget = androidTargets[abi];
    if (!androidTarget) {
      throw new Error(
        `--abi must be one of ${Object.keys(androidTargets)}, got ${abi}`,
      );
    }
    // dx parses the triple and prints it back as `x86_64-unknown-linux-android`, so it looks
    // up its linker under that name (a cargo `target.<triple>.linker` or the matching
    // `CARGO_TARGET_*_LINKER`) and, failing that, builds the NDK clang name from it, which
    // does not exist (the NDK ships `x86_64-linux-android<api>-clang`). aarch64 prints back
    // unchanged and needs nothing. Alias the NDK's own linker under dx's spelling.
    const ndkLinker = Deno.env.get("CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER");
    if (abi === "x86_64" && ndkLinker) {
      Deno.env.set(
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_ANDROID_LINKER",
        ndkLinker,
      );
    }
    // `dx build` assembles through Gradle into the target dir; `--release` also needs a
    // signing config in Dioxus.toml to yield a signed APK.
    await $`deno run --allow-all ${
      root.join("x/dx.ts")
    } build --platform android --target ${androidTarget} --renderer native -p dayframe_mobile ${
      flag("--release") ? ["--release"] : []
    }`.cwd(root);
    const metadata =
      await $`cargo metadata --locked --no-deps --format-version 1`
        .json<{ target_directory: string }>();
    const apks =
      await $`find ${metadata.target_directory} -name '*.apk' -path '*dayframe_mobile*' -printf '%T@ %p\n'`
        .text();
    const apk = apks.trim().split("\n").filter(Boolean).sort().pop()?.split(
      " ",
    )[1];
    if (!apk) {
      throw new Error("dx reported success but no dayframe_mobile APK exists");
    }
    console.log(`apk: ${apk}`);
    if (flag("--install")) {
      // `adb install` fails with "no devices/emulators found" when nothing is attached; that is
      // the right error, not something to paper over.
      await $`adb install -r ${apk}`;
    }
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
