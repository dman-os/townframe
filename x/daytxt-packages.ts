import { $ } from "./utils.ts";

// Builds the Wasm guests and lays out the package directory the desktop shell scans. Shared by
// `check-daytxt.ts` and `daytxt.ts`; the target dir comes from `cargo metadata`. Prints the
// `DAYTXT_*` lines and returns them, with the package directory under `DAYTXT_PACKAGE_DIR`.
//
// `testFixtures` also builds the guests only tests load (the collection probe and the
// `clay_mdk` example fixtures). The shells need just the packages; building the fixtures for
// them doubled the cold build that the web e2e server waits on.
//
// `trapper` builds the `clay_mdk` `trapper` example (a micro whose view traps its guest on a
// click) and adds it to the package directory beside the counter and the layout, for the web
// e2e that pins a sync-export trap faulting only that micro. It is not part of `testFixtures`:
// those are components tests load by environment variable, while this one is a package the
// shell lists and so changes what every shell shows.
//
// `intxtGuest` also builds intxt's nodule handler component (the `wasm_utils_rs` workspace's
// `intxt_nodule_guest`, always release) and stages it as `intxt.guest.wasm` in the package
// directory, where the desktop host and the Android staging read it. It is not a micro, so the
// shell's package scan skips it; the node installs it as a cartridge at boot. `testFixtures`
// builds the same component for the tests that install intxt and exports its path as
// `INTXT_GUEST_WASM` (nothing is staged into the package directory).
//
// `guestProfile` is the cargo profile the guests build with. `wasm-release` (the default) is the
// workspace's optimized guest profile, the one `daytxt_web` also builds with. `dev` is opt-in, for
// faster rebuilds while iterating: it emits 15 to 17 MB unoptimized components that Wasmtime needs
// seconds to compile on a phone, and they burn about 10x the fuel (see below).
export async function prepareDaytxtPackages(
  {
    testFixtures,
    trapper = false,
    intxtGuest = false,
    guestProfile = "wasm-release",
  }: {
    testFixtures: boolean;
    trapper?: boolean;
    intxtGuest?: boolean;
    guestProfile?: "dev" | "wasm-release";
  },
): Promise<
  Record<string, string>
> {
  // Guests are wasm-only configurations: resolve their features in isolation. See
  // CONTRIBUTING.md on `resolver.feature-unification`.
  const isolated = ["--config", 'resolver.feature-unification="selected"'];

  // Guests build optimized (`wasm-release`), as plugs (`cargo x plug build-oci`, `--release`) and
  // `daytxt_web` already do. The host meters every guest call with fuel, and an unoptimized
  // guest burns about 10x the fuel of an optimized one (the lab's first diff render: ~95M in the
  // dev profile, ~9.6M optimized, against a 50M per-call budget), so a dev build would measure
  // the budget against code no user runs.
  //
  // One cargo invocation per artifact kind, so cargo schedules the guests together.
  const guests = testFixtures
    ? [
      "micro_counter",
      "micro_layout",
      "micro_lab",
      "micro_review",
      "micro_theme_lab",
      "micro_collection_probe",
    ]
    : [
      "micro_counter",
      "micro_layout",
      "micro_lab",
      "micro_review",
      "micro_theme_lab",
    ];
  await $`cargo build --locked ${isolated} ${
    guests.flatMap((guest) => ["-p", guest])
  } --target wasm32-wasip3 --profile ${guestProfile}`;
  if (testFixtures) {
    await $`cargo build --locked ${isolated} -p clay_mdk --example sleeper --example egress --example dispatch --example overbudget --example lifecycle --example context --target wasm32-wasip3 --profile ${guestProfile}`;
  }
  if (trapper) {
    await $`cargo build --locked ${isolated} -p clay_mdk --example trapper --target wasm32-wasip3 --profile ${guestProfile}`;
  }
  // cargo names the `dev` profile's output directory `debug`; other profiles use their own name.
  const profileDirectory = guestProfile === "dev" ? "debug" : guestProfile;
  const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
    .json<{ target_directory: string; packages: { name: string }[] }>();
  const target = $.path(metadata.target_directory);
  const directory = target.join("daytxt");
  await Deno.mkdir(directory.toString(), { recursive: true });

  // Prepared guest components, keyed by the guest name `component_package`/env vars use.
  const components: Record<string, [string, string]> = {
    counter: [
      `wasm32-wasip3/${profileDirectory}/micro_counter.wasm`,
      "counter",
    ],
    layout: [`wasm32-wasip3/${profileDirectory}/micro_layout.wasm`, "layout"],
    lab: [`wasm32-wasip3/${profileDirectory}/micro_lab.wasm`, "lab"],
    review: [`wasm32-wasip3/${profileDirectory}/micro_review.wasm`, "review"],
    themeLab: [
      `wasm32-wasip3/${profileDirectory}/micro_theme_lab.wasm`,
      "theme-lab",
    ],
    ...(testFixtures
      ? {
        collectionProbe: [
          `wasm32-wasip3/${profileDirectory}/micro_collection_probe.wasm`,
          "collection-probe",
        ],
        sleeper: [
          `wasm32-wasip3/${profileDirectory}/examples/sleeper.wasm`,
          "sleeper",
        ],
        egress: [
          `wasm32-wasip3/${profileDirectory}/examples/egress.wasm`,
          "egress",
        ],
        dispatch: [
          `wasm32-wasip3/${profileDirectory}/examples/dispatch.wasm`,
          "dispatch",
        ],
        overbudget: [
          `wasm32-wasip3/${profileDirectory}/examples/overbudget.wasm`,
          "overbudget",
        ],
        lifecycle: [
          `wasm32-wasip3/${profileDirectory}/examples/lifecycle.wasm`,
          "lifecycle",
        ],
        context: [
          `wasm32-wasip3/${profileDirectory}/examples/context.wasm`,
          "context",
        ],
      }
      : {}),
    ...(trapper
      ? {
        trapper: [
          `wasm32-wasip3/${profileDirectory}/examples/trapper.wasm`,
          "trapper",
        ],
      }
      : {}),
  };
  const componentEnv: Record<string, string> = {};
  // Every guest ships without its DWARF and keeps its `name` section; the build as rustc linked it
  // is kept as the guest's debug side file (docs/scratch/guest-faults-and-logs.md §3). Stripping
  // removes custom sections only, so the side file has the shipped component's core modules, in
  // the same order with the same code, plus their DWARF: a trap frame's (core module, offset)
  // resolves against it. The DWARF is ~94% of a micro (review: 69.6 MB linked, ~4.7 MB shipped).
  // The side file is named by the shipped component's BLAKE3 digest, the package identity
  // `clay_mdk::package::Package::digest` computes, so no second hash names a guest. Deno's
  // WebCrypto lacks BLAKE3, so `b3sum` computes it (as `x/daytxt-site.ts` does).
  const debugDirectory = directory.join("debug");
  await Deno.mkdir(debugDirectory.toString(), { recursive: true });
  // A side file is a whole unstripped component (70 MB for review), and every rebuild with
  // changed source writes a new one. Only the side files of the components `directory` holds
  // after this run are kept: a fault from an older build would not resolve after a rebuild
  // anyway. Components an earlier run left (test fixtures this run did not build) keep theirs.
  const kept = new Set<string>();
  for (const name in components) {
    const [relative, variableName] = components[name];
    const artifact = directory.join(`${variableName}.component.wasm`);
    const linked = target.join(relative);
    // rustc's wasm32-wasip3 target already emits a component; no adapter step. `strip`'s default
    // keeps `name` and `component-type` and removes every other custom section.
    await $`wasm-tools strip ${linked} -o ${artifact}`;
    const digest = (await $`b3sum --no-names ${artifact}`.text()).trim();
    await $`cp ${linked} ${debugDirectory.join(`${digest}.wasm`)}`;
    kept.add(digest);
    const variable = `DAYTXT_${
      variableName.toUpperCase().replaceAll("-", "_")
    }_COMPONENT`;
    componentEnv[variable] = artifact.toString();
    console.log(`${variable}=${artifact}`);
  }
  for (const entry of Deno.readDirSync(directory.toString())) {
    if (entry.isFile && entry.name.endsWith(".component.wasm")) {
      const component = directory.join(entry.name);
      kept.add((await $`b3sum --no-names ${component}`.text()).trim());
    }
  }
  for (const entry of Deno.readDirSync(debugDirectory.toString())) {
    if (!kept.has(entry.name.replace(/\.wasm$/, ""))) {
      await Deno.remove(debugDirectory.join(entry.name).toString());
    }
  }

  // The counter, the layout, the lab, the review micro and the theme lab are also packages: the desktop shell scans a package directory for
  // `<name>.manifest.json` sidecars paired with `<name>.component.wasm`, so prepare that
  // directory beside the raw fixtures. STAND-IN until `big_cartridges` delivers manifests; each
  // sidecar is emitted from the package's manifest crate.
  const packagesDirectory = directory.join("packages");
  await Deno.mkdir(packagesDirectory.toString(), { recursive: true });
  for (
    const [name, manifestCrate] of [
      ["counter", "micro_counter_manifest"],
      ["layout", "micro_layout_manifest"],
      ["lab", "micro_lab_manifest"],
      ["review", "micro_review_manifest"],
      ["theme-lab", "micro_theme_lab_manifest"],
    ]
  ) {
    await $`cp ${directory.join(`${name}.component.wasm`)} ${
      packagesDirectory.join(`${name}.component.wasm`)
    }`;
    await $`cargo run --locked -q -p ${manifestCrate} --example emit_manifest -- ${
      packagesDirectory.join(`${name}.manifest.json`)
    }`;
  }
  if (trapper) {
    await $`cp ${directory.join("trapper.component.wasm")} ${
      packagesDirectory.join("trapper.component.wasm")
    }`;
    // A native example of `clay_mdk` (built for the host, not wasm32-wasip3).
    await $`cargo run --locked -q -p clay_mdk --example emit_trapper_manifest -- ${
      packagesDirectory.join("trapper.manifest.json")
    }`;
  }
  if (intxtGuest || testFixtures) {
    await $`cargo build --locked --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release`;
    const guestWorkspace =
      await $`cargo metadata --locked --no-deps --format-version 1 --manifest-path src/wasm_utils_rs/Cargo.toml`
        .json<{ target_directory: string }>();
    const built = $.path(guestWorkspace.target_directory).join(
      "wasm32-wasip3/release/intxt_nodule_guest.wasm",
    );
    if (intxtGuest) {
      await $`cp ${built} ${packagesDirectory.join("intxt.guest.wasm")}`;
    }
    if (testFixtures) {
      // `daytxt_native`'s interim e2e installs intxt from this component; without the variable it
      // looks in the guest workspace's own `target/`, which a CI runner never populated.
      componentEnv["INTXT_GUEST_WASM"] = built.toString();
      console.log(`INTXT_GUEST_WASM=${built}`);
    }
  }
  componentEnv["DAYTXT_PACKAGE_DIR"] = packagesDirectory.toString();
  console.log(`DAYTXT_PACKAGE_DIR=${packagesDirectory}`);
  return componentEnv;
}
