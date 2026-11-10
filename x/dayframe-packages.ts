import { $ } from "./utils.ts";

// Builds the Wasm guests and lays out the package directory the desktop shell scans. Shared by
// `check-dayframe.ts` and `dayframe.ts`; Prints the
// `DAYFRAME_*` lines and returns them, with the package directory under `DAYFRAME_PACKAGE_DIR`.
//
// `testFixtures` also builds the guests only tests load (the collection probe and the
// `clay_mdk` example fixtures). The shells need just the packages; building the fixtures for
// them doubled the cold build that the web e2e server waits on.
export async function prepareDayframePackages(
  { testFixtures }: { testFixtures: boolean },
): Promise<
  Record<string, string>
> {
  // Guests are wasm-only configurations: resolve their features in isolation. See
  // CONTRIBUTING.md on `resolver.feature-unification`.
  const isolated = ["--config", 'resolver.feature-unification="selected"'];
  // `DAYFRAME_GUEST_PROFILE=release` builds the guests optimized: the Pierre scroll spike's
  // timings are meaningless against the unoptimized debug guests the shells otherwise package.
  const release = Deno.env.get("DAYFRAME_GUEST_PROFILE") === "release";
  const profileFlags = release ? ["--release"] : [];
  const profileDir = release ? "release" : "debug";

  // One cargo invocation per artifact kind, so cargo schedules the guests together.
  const guests = testFixtures
    ? [
      "micro_counter",
      "micro_layout",
      "micro_pierre_spike_scroll",
      "micro_collection_probe",
    ]
    : ["micro_counter", "micro_layout", "micro_pierre_spike_scroll"];
  await $`cargo build --locked ${isolated} ${
    guests.flatMap((guest) => ["-p", guest])
  } ${profileFlags} --target wasm32-wasip3`;
  if (testFixtures) {
    await $`cargo build --locked ${isolated} -p clay_mdk --example sleeper --example lifecycle --example context ${profileFlags} --target wasm32-wasip3`;
  }
  const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
    .json<{ target_directory: string; packages: { name: string }[] }>();
  const target = $.path(metadata.target_directory);
  const directory = target.join("dayframe");
  await Deno.mkdir(directory.toString(), { recursive: true });

  // Prepared guest components, keyed by the guest name `component_package`/env vars use.
  const components: Record<string, [string, string]> = {
    counter: [`wasm32-wasip3/${profileDir}/micro_counter.wasm`, "counter"],
    layout: [`wasm32-wasip3/${profileDir}/micro_layout.wasm`, "layout"],
    pierreSpikeScroll: [
      `wasm32-wasip3/${profileDir}/micro_pierre_spike_scroll.wasm`,
      "pierre-spike-scroll",
    ],
    ...(testFixtures
      ? {
        collectionProbe: [
          `wasm32-wasip3/${profileDir}/micro_collection_probe.wasm`,
          "collection-probe",
        ],
        sleeper: [
          `wasm32-wasip3/${profileDir}/examples/sleeper.wasm`,
          "sleeper",
        ],
        lifecycle: [
          `wasm32-wasip3/${profileDir}/examples/lifecycle.wasm`,
          "lifecycle",
        ],
        context: [
          `wasm32-wasip3/${profileDir}/examples/context.wasm`,
          "context",
        ],
      }
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

  // The counter and the layout are also packages: the desktop shell scans a package directory for
  // `<name>.manifest.json` sidecars paired with `<name>.component.wasm`, so prepare that
  // directory beside the raw fixtures. STAND-IN until `big_plugs` delivers manifests; each
  // sidecar is emitted from the package's manifest crate.
  const packagesDirectory = directory.join("packages");
  await Deno.mkdir(packagesDirectory.toString(), { recursive: true });
  for (
    const [name, manifestCrate] of [
      ["counter", "micro_counter_manifest"],
      ["layout", "micro_layout_manifest"],
      ["pierre-spike-scroll", "micro_pierre_spike_scroll"],
    ]
  ) {
    await $`cp ${directory.join(`${name}.component.wasm`)} ${
      packagesDirectory.join(`${name}.component.wasm`)
    }`;
    await $`cargo run --locked -q -p ${manifestCrate} --example emit_manifest -- ${
      packagesDirectory.join(`${name}.manifest.json`)
    }`;
  }
  componentEnv["DAYFRAME_PACKAGE_DIR"] = packagesDirectory.toString();
  console.log(`DAYFRAME_PACKAGE_DIR=${packagesDirectory}`);
  return componentEnv;
}
