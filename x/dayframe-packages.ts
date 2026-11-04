import { $ } from "./utils.ts";

// Builds the Wasm guests and lays out the package directory the desktop shell scans. Shared by
// `check-dayframe.ts` and `dayframe.ts`; callers run `useDotenvTargetDir()` first. Prints the
// `DAYFRAME_*` lines and returns them, with the package directory under `DAYFRAME_PACKAGE_DIR`.
export async function prepareDayframePackages(): Promise<
  Record<string, string>
> {
  // Guests are wasm-only configurations: resolve their features in isolation. See
  // CONTRIBUTING.md on `resolver.feature-unification`.
  const isolated = ["--config", 'resolver.feature-unification="selected"'];

  await $`cargo build --locked ${isolated} -p micro_counter --target wasm32-wasip3`;
  await $`cargo build --locked ${isolated} -p micro_layout --target wasm32-wasip3`;
  await $`cargo build --locked ${isolated} -p micro_collection_probe --target wasm32-wasip3`;
  await $`cargo build --locked ${isolated} -p clay_mdk --example sleeper --target wasm32-wasip3`;
  await $`cargo build --locked ${isolated} -p clay_mdk --example lifecycle --target wasm32-wasip3`;
  await $`cargo build --locked ${isolated} -p clay_mdk --example context --target wasm32-wasip3`;
  const metadata = await $`cargo metadata --locked --no-deps --format-version 1`
    .json<{ target_directory: string; packages: { name: string }[] }>();
  const target = $.path(metadata.target_directory);
  const directory = target.join("dayframe");
  await Deno.mkdir(directory.toString(), { recursive: true });

  // Prepared guest components, keyed by the guest name `component_package`/env vars use.
  const components: Record<string, [string, string]> = {
    counter: ["wasm32-wasip3/debug/micro_counter.wasm", "counter"],
    layout: ["wasm32-wasip3/debug/micro_layout.wasm", "layout"],
    collectionProbe: [
      "wasm32-wasip3/debug/micro_collection_probe.wasm",
      "collection-probe",
    ],
    sleeper: ["wasm32-wasip3/debug/examples/sleeper.wasm", "sleeper"],
    lifecycle: ["wasm32-wasip3/debug/examples/lifecycle.wasm", "lifecycle"],
    context: ["wasm32-wasip3/debug/examples/context.wasm", "context"],
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
