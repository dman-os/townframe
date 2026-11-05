import { $ } from "./utils.ts";

// Builds the Wasm guests and lays out the package directory the desktop shell scans. Shared by
// `check-dayframe.ts` and `dayframe.ts`; callers run `useDotenvTargetDir()` first. Prints the
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

  // One cargo invocation per artifact kind, so cargo schedules the guests together.
  const guests = testFixtures
    ? [
      "micro_counter",
      "micro_layout",
      "micro_collection_probe",
      "micro_highlight_spike",
    ]
    : ["micro_counter", "micro_layout"];
  await $`cargo build --locked ${isolated} ${
    guests.flatMap((guest) => ["-p", guest])
  } --target wasm32-wasip3`;
  if (testFixtures) {
    await $`cargo build --locked ${isolated} -p clay_mdk --example sleeper --example lifecycle --example context --target wasm32-wasip3`;
  }
  const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
    .json<{ target_directory: string; packages: { name: string }[] }>();
  const target = $.path(metadata.target_directory);
  const directory = target.join("dayframe");
  await Deno.mkdir(directory.toString(), { recursive: true });

  // The wrapper around Pierre's unmodified `highlights.wasm` (H1 spike): rebuilt from the module
  // at `PIERRE_HIGHLIGHTS_WASM` by `bench/highlight/component/build.ts`.
  const highlightDirectory = directory.join("highlight");
  const pierreWasm = Deno.env.get("PIERRE_HIGHLIGHTS_WASM") ??
    $.path(
      import.meta.resolve(
        "../bench/highlight/node_modules/@pierre/highlights/dist/highlights.wasm",
      ),
    )
      .toString();
  if (testFixtures) {
    await $`deno run --allow-read --allow-write --allow-run --allow-env bench/highlight/component/build.ts ${pierreWasm} ${highlightDirectory}`;
    // The spike micro imports `highlight`; `wac plug` satisfies that import with the wrapper.
    await $`wac plug --plug ${
      highlightDirectory.join("highlighter.component.wasm")
    } ${target.join("wasm32-wasip3/debug/micro_highlight_spike.wasm")} -o ${
      highlightDirectory.join("micro_highlight_spike.composed.wasm")
    }`;
  }
  // Prepared guest components, keyed by the guest name `component_package`/env vars use.
  const components: Record<string, [string, string]> = {
    counter: ["wasm32-wasip3/debug/micro_counter.wasm", "counter"],
    layout: ["wasm32-wasip3/debug/micro_layout.wasm", "layout"],
    ...(testFixtures
      ? {
        collectionProbe: [
          "wasm32-wasip3/debug/micro_collection_probe.wasm",
          "collection-probe",
        ],
        highlightSpike: [
          "dayframe/highlight/micro_highlight_spike.composed.wasm",
          "highlight-spike",
        ],
        sleeper: ["wasm32-wasip3/debug/examples/sleeper.wasm", "sleeper"],
        lifecycle: [
          "wasm32-wasip3/debug/examples/lifecycle.wasm",
          "lifecycle",
        ],
        context: ["wasm32-wasip3/debug/examples/context.wasm", "context"],
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
