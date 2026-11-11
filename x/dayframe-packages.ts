import { $ } from "./utils.ts";

// Builds the Wasm guests and lays out the package directory the desktop shell scans. Shared by
// `check-dayframe.ts` and `dayframe.ts`; the target dir comes from `cargo metadata`. Prints the
// `DAYFRAME_*` lines and returns them, with the package directory under `DAYFRAME_PACKAGE_DIR`.
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
export async function prepareDayframePackages(
  { testFixtures, trapper = false }: {
    testFixtures: boolean;
    trapper?: boolean;
  },
): Promise<
  Record<string, string>
> {
  // Guests are wasm-only configurations: resolve their features in isolation. See
  // CONTRIBUTING.md on `resolver.feature-unification`.
  const isolated = ["--config", 'resolver.feature-unification="selected"'];

  // One cargo invocation per artifact kind, so cargo schedules the guests together.
  const guests = testFixtures
    ? ["micro_counter", "micro_layout", "micro_lab", "micro_collection_probe"]
    : ["micro_counter", "micro_layout", "micro_lab"];
  await $`cargo build --locked ${isolated} ${
    guests.flatMap((guest) => ["-p", guest])
  } --target wasm32-wasip3`;
  if (testFixtures) {
    await $`cargo build --locked ${isolated} -p clay_mdk --example sleeper --example overbudget --example lifecycle --example context --target wasm32-wasip3`;
  }
  if (trapper) {
    await $`cargo build --locked ${isolated} -p clay_mdk --example trapper --target wasm32-wasip3`;
  }
  const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
    .json<{ target_directory: string; packages: { name: string }[] }>();
  const target = $.path(metadata.target_directory);
  const directory = target.join("dayframe");
  await Deno.mkdir(directory.toString(), { recursive: true });

  // Prepared guest components, keyed by the guest name `component_package`/env vars use.
  const components: Record<string, [string, string]> = {
    counter: ["wasm32-wasip3/debug/micro_counter.wasm", "counter"],
    layout: ["wasm32-wasip3/debug/micro_layout.wasm", "layout"],
    lab: ["wasm32-wasip3/debug/micro_lab.wasm", "lab"],
    ...(testFixtures
      ? {
        collectionProbe: [
          "wasm32-wasip3/debug/micro_collection_probe.wasm",
          "collection-probe",
        ],
        sleeper: ["wasm32-wasip3/debug/examples/sleeper.wasm", "sleeper"],
        overbudget: [
          "wasm32-wasip3/debug/examples/overbudget.wasm",
          "overbudget",
        ],
        lifecycle: [
          "wasm32-wasip3/debug/examples/lifecycle.wasm",
          "lifecycle",
        ],
        context: ["wasm32-wasip3/debug/examples/context.wasm", "context"],
      }
      : {}),
    ...(trapper
      ? { trapper: ["wasm32-wasip3/debug/examples/trapper.wasm", "trapper"] }
      : {}),
  };
  const componentEnv: Record<string, string> = {};
  for (const name in components) {
    const [relative, variableName] = components[name];
    const artifact = directory.join(`${variableName}.component.wasm`);
    // rustc's wasm32-wasip3 target already emits a component; no adapter step.
    await $`cp ${target.join(relative)} ${artifact}`;
    const variable = `DAYFRAME_${
      variableName.toUpperCase().replaceAll("-", "_")
    }_COMPONENT`;
    componentEnv[variable] = artifact.toString();
    console.log(`${variable}=${artifact}`);
  }

  // The counter, the layout and the lab are also packages: the desktop shell scans a package directory for
  // `<name>.manifest.json` sidecars paired with `<name>.component.wasm`, so prepare that
  // directory beside the raw fixtures. STAND-IN until `big_plugs` delivers manifests; each
  // sidecar is emitted from the package's manifest crate.
  const packagesDirectory = directory.join("packages");
  await Deno.mkdir(packagesDirectory.toString(), { recursive: true });
  for (
    const [name, manifestCrate] of [
      ["counter", "micro_counter_manifest"],
      ["layout", "micro_layout_manifest"],
      ["lab", "micro_lab_manifest"],
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
  componentEnv["DAYFRAME_PACKAGE_DIR"] = packagesDirectory.toString();
  console.log(`DAYFRAME_PACKAGE_DIR=${packagesDirectory}`);
  return componentEnv;
}
