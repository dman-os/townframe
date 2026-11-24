#!/usr/bin/env -S deno run --allow-all

// One entry point for running daytxt locally. `--help` lists the commands.

import { prepareDaytxtPackages } from "./daytxt-packages.ts";
import { $ } from "./utils.ts";

const root = $.relativeDir("../");
const [command, ...rest] = Deno.args;

const help = `usage: deno run --allow-all x/daytxt.ts <command> [flags]

  desktop     build the Wasm packages and run the desktop shell in a webview (Wry)
                --native         the Blitz renderer instead
                --fresh          start from an empty store and an empty node directory (temp)
                --store <path>   use this store file (DAYTXT_STORE_PATH)
  android     build the Android APK (daytxt_mobile)
                --abi <arm64|x86_64>  target ABI, default arm64 (aarch64-linux-android);
                                 x86_64 is what an emulator on this host runs
                --release        release profile (unsigned unless Dioxus.toml configures signing)
                --guest-profile <dev|wasm-release>  cargo profile of the bundled Wasm guests,
                                 default wasm-release; dev builds faster but its 15 to 17 MB components
                                 compile slowly on a phone and burn about 10x the fuel
                --install        adb install the APK on the attached device
  web         build and serve daytxt_web (--build: build only)
  showcase    run the design showcase (claytxt_showcase) in a webview (Wry)
                --native         the Blitz renderer instead
  showcase-web  serve the design showcase for the browser (Dioxus web, wasm-release) on
                127.0.0.1:PORT, to compare its diff code in Firefox against Wry
                --port <n>       default 8765
                the lab's PR #84 view fetches pr84.diff from beside the page: it is copied
                from DAYTXT_LAB_DIFF, when set, into the gitignored
                src/claytxt_showcase/public/ at serve time. Start choices are URL
                parameters: ?tab=diffs&diffs_view=pr84, ?kit=nav
  packages    only (re)build the Wasm packages and print the DAYTXT_* env lines
  test        x/check-daytxt.ts, then the daytxt_web Playwright e2e
                --no-e2e         skip the browser part
  bombadil    explore daytxt_web with Bombadil and check the shell invariants
                (src/daytxt_web/bombadil/shell.ts); BOMBADIL_TIME_LIMIT=<n>s sets
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
    const env = await prepareDaytxtPackages({
      testFixtures: false,
      intxtGuest: true,
    });
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
      store = `${await Deno.makeTempDir({ prefix: "daytxt-store-" })}/store`;
      // The desktop's node is the one `day` uses (DAY_NODE, else the platform data directory):
      // a fresh run gets a fresh node directory too, not the operator's own.
      env["DAY_NODE"] = await Deno.makeTempDir({ prefix: "daytxt-node-" });
    }
    // One binary carries both launchers; the variable picks Blitz (see daytxt_desktop/main.rs).
    if (flag("--native")) env["DAYTXT_RENDERER"] = "blitz";
    if (store) env["DAYTXT_STORE_PATH"] = $.path(store).resolve().toString();
    console.log(`package dir: ${env["DAYTXT_PACKAGE_DIR"]}`);
    console.log(`store path: ${env["DAYTXT_STORE_PATH"] ?? "(app default)"}`);
    await dx(
      "-p",
      "daytxt_desktop",
      ...(flag("--native")
        ? ["--renderer", "native", "--features", "blitz"]
        : ["--renderer", "webview"]),
      "--hot-patch",
      "false",
    ).env(env);
    break;
  }
  case "android": {
    const guestProfileFlag = rest.indexOf("--guest-profile");
    const guestProfile = guestProfileFlag >= 0
      ? rest[guestProfileFlag + 1]
      : "wasm-release";
    if (guestProfile !== "dev" && guestProfile !== "wasm-release") {
      throw new Error(
        `--guest-profile must be dev or wasm-release, got ${guestProfile}`,
      );
    }
    const env = await prepareDaytxtPackages({
      testFixtures: false,
      intxtGuest: true,
      guestProfile,
    });
    // The APK carries its packages as assets: `daytxt_mobile` bundles `packages/` with
    // `asset!`, so stage the prepared counter, layout and review beside the crate (gitignored).
    const staged = root.join("src/daytxt_mobile/packages");
    await Deno.mkdir(staged.toString(), { recursive: true });
    for (const name of ["counter", "layout", "review"]) {
      for (const suffix of ["manifest.json", "component.wasm"]) {
        await $`cp ${
          $.path(env["DAYTXT_PACKAGE_DIR"]).join(`${name}.${suffix}`)
        } ${staged.join(`${name}.${suffix}`)}`;
      }
    }
    // intxt's nodule handler is not a micro: the phone's node installs it as a cartridge at boot
    // (`daytxt_native`'s `install_intxt`). `prepareDaytxtPackages` built it
    // beside the packages; stage it with them.
    await $`cp ${$.path(env["DAYTXT_PACKAGE_DIR"]).join("intxt.guest.wasm")} ${
      staged.join("intxt.guest.wasm")
    }`;
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
    } build --platform android --target ${androidTarget} --renderer webview -p daytxt_mobile ${
      flag("--release") ? ["--release"] : []
    }`.cwd(root);
    const metadata =
      await $`cargo metadata --locked --no-deps --format-version 1`
        .json<{ target_directory: string }>();
    const apks =
      await $`find -H ${metadata.target_directory} -name '*.apk' -path '*daytxt_mobile*' -printf '%T@ %p\n'`
        .text();
    const apk = apks.trim().split("\n").filter(Boolean).sort().pop()?.split(
      " ",
    )[1];
    if (!apk) {
      throw new Error("dx reported success but no daytxt_mobile APK exists");
    }
    // `src/daytxt_mobile/AndroidManifest.xml` replaces dx's generated manifest to opt in to
    // predictive back (dx's template has no hook for application attributes). Check the shipped
    // APK's manifest rather than trust that dx still honors `android_manifest`: without the
    // attribute, API 33 ignores `MainActivity`'s back callback and back at Home does nothing.
    const shippedManifest = await $`apkanalyzer manifest print ${apk}`.text();
    if (
      !/android:enableOnBackInvokedCallback="true"/.test(shippedManifest)
    ) {
      throw new Error(
        `${apk} lacks android:enableOnBackInvokedCallback="true"; ` +
          "dx did not use src/daytxt_mobile/AndroidManifest.xml (Dioxus.toml android_manifest)",
      );
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
    await $`deno run --allow-all ${root.join("x/daytxt-web.ts")} ${
      flag("--build") ? ["--build"] : []
    }`;
    break;
  case "showcase":
    await dx(
      "-p",
      "claytxt_showcase",
      "--renderer",
      flag("--native") ? "native" : "webview",
    ).env(flag("--native") ? { DAYTXT_RENDERER: "blitz" } : {});
    break;
  case "showcase-web": {
    const portFlag = rest.indexOf("--port");
    const port = portFlag >= 0 ? rest[portFlag + 1] : "8765";
    if (!port) throw new Error("--port needs a number");
    const served = root.join("src/claytxt_showcase/public");
    await Deno.mkdir(served.toString(), { recursive: true });
    const override = Deno.env.get("DAYTXT_LAB_DIFF");
    const diff = override ? $.path(override) : served.join("pr84.diff");
    if (override && await diff.exists()) {
      await diff.copyFile(served.join("pr84.diff"));
    } else if (!(await diff.exists())) {
      console.warn(
        `${diff} is missing: the PR #84 view will say how to produce it`,
      );
    }
    // Isolated feature resolution: workspace unification would pull the desktop renderer into
    // the wasm32 build (CONTRIBUTING.md, `resolver.feature-unification`).
    await $`deno run --allow-all ${
      root.join("x/dx.ts")
    } serve --web -p claytxt_showcase --no-default-features --features web --profile wasm-release --addr 127.0.0.1 --port ${port} --open false --interactive false --hot-reload false --hot-patch false`
      .cwd(root)
      .env({ CARGO_RESOLVER_FEATURE_UNIFICATION: "selected" });
    break;
  }
  case "packages":
    await prepareDaytxtPackages({ testFixtures: true });
    break;
  case "test":
    await $`deno run --allow-all ${root.join("x/check-daytxt.ts")}`;
    if (!flag("--no-e2e")) {
      await $`pnpm -C src/daytxt_web run e2e`.cwd(root);
    }
    break;
  case "bombadil":
    await $`pnpm -C src/daytxt_web run bombadil`.cwd(root);
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
