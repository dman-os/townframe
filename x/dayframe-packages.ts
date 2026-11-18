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
//
// `reviewFixture` builds the review micro with its `fixture` cargo feature, so its guest serves
// the in-memory `Fixture::showcase` node when no configuration is granted (the web composition has
// no node), and serves it in place of the plain review package. The feature is compiled only into
// this build; the shipped guest has no fixture code. For the web e2e's review specs
// (`DAYFRAME_WEB_REVIEW_FIXTURE=1`).
//
// `guestProfile` is the cargo profile the guests build with. `wasm-release` (the default) is the
// workspace's optimized guest profile, the one `dayframe_web` also builds with. `dev` is opt-in, for
// faster rebuilds while iterating: it emits 15 to 17 MB unoptimized components that Wasmtime needs
// seconds to compile on a phone, and they burn about 10x the fuel (see below).
export async function prepareDayframePackages(
  { testFixtures, trapper = false, reviewFixture = false, guestProfile = "wasm-release" }: {
    testFixtures: boolean;
    trapper?: boolean;
    reviewFixture?: boolean;
    guestProfile?: "dev" | "wasm-release";
  },
): Promise<
  Record<string, string>
> {
  // Guests are wasm-only configurations: resolve their features in isolation. See
  // CONTRIBUTING.md on `resolver.feature-unification`.
  const isolated = ["--config", 'resolver.feature-unification="selected"'];

  // Guests build optimized (`wasm-release`), as plugs (`cargo x plug build-oci`, `--release`) and
  // `dayframe_web` already do. The host meters every guest call with fuel, and an unoptimized
  // guest burns about 10x the fuel of an optimized one (the lab's first diff render: ~95M in the
  // dev profile, ~9.6M optimized, against a 50M per-call budget), so a dev build would measure
  // the budget against code no user runs.
  //
  // One cargo invocation per artifact kind, so cargo schedules the guests together.
  const guests = testFixtures
    ? ["micro_counter", "micro_layout", "micro_lab", "micro_review", "micro_collection_probe"]
    : ["micro_counter", "micro_layout", "micro_lab", "micro_review"];
  // The fixture variant of the review guest is its own invocation (cargo names the output by
  // package, so it must not share a build with the plain one) and replaces it in the list.
  const plainGuests = reviewFixture
    ? guests.filter((guest) => guest !== "micro_review")
    : guests;
  await $`cargo build --locked ${isolated} ${
    plainGuests.flatMap((guest) => ["-p", guest])
  } --target wasm32-wasip3 --profile ${guestProfile}`;
  if (reviewFixture) {
    await $`cargo build --locked ${isolated} -p micro_review --features fixture --target wasm32-wasip3 --profile ${guestProfile}`;
  }
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
  const directory = target.join("dayframe");
  await Deno.mkdir(directory.toString(), { recursive: true });

  // Prepared guest components, keyed by the guest name `component_package`/env vars use.
  const components: Record<string, [string, string]> = {
    counter: [`wasm32-wasip3/${profileDirectory}/micro_counter.wasm`, "counter"],
    layout: [`wasm32-wasip3/${profileDirectory}/micro_layout.wasm`, "layout"],
    lab: [`wasm32-wasip3/${profileDirectory}/micro_lab.wasm`, "lab"],
    review: [`wasm32-wasip3/${profileDirectory}/micro_review.wasm`, "review"],
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
        egress: [`wasm32-wasip3/${profileDirectory}/examples/egress.wasm`, "egress"],
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

  // The counter, the layout, the lab and the review micro are also packages: the desktop shell scans a package directory for
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
