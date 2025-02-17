// export { sophon } from "@ghjk/ts";
// import { file } from "@ghjk/ts";
// import * as ports from "@ghjk/ports_wip";
import { file, $ } from "https://raw.githubusercontent.com/metatypedev/ghjk/v0.2.1/mod.ts";
import * as ports from "https://raw.githubusercontent.com/metatypedev/ghjk/v0.2.1/ports/mod.ts";

import * as std_url from "jsr:@std/url@0.215.0";

const DOCKER_CMD = "docker";
const RUST_VERSION = "1.84.1";

const installs = {
  rust: ports.rust({ 
    version: RUST_VERSION, 
    profile: "default",
    components: ["rust-src"],
    targets: ["wasm32-unknown-unknown"] 
  })
}

// This export is necessary for typescript ghjkfiles
const ghjk = file({
  defaultEnv: Deno.env.get("CI") ? "ci" : Deno.env.get("OCI") ? "oci" : "dev",
  // allows usage of ports that depend on node/python
  enableRuntimes: true,
  allowedBuildDeps: [
    // ports.rust({ version: "nightly-2025-01-26" }),
    // ports.node({ version: "20.8.0" }),
    installs.rust
  ],
});

export const sophon = ghjk.sophon;

ghjk.env("main")
  .vars({
    CARGO_BUILD_JOBS: 8,
  })
  .install(
    installs.rust,
    ports.pipi({ packageName: "pre-commit" })[0],
  );

ghjk.env("dev")
  .install(
    ports.cargobi({ crateName: "cargo-leptos", locked: true }),
    ports.cargobi({ crateName: "leptosfmt", locked: true }),
    ports.cargobi({ crateName: "trunk", locked: true }),
  )
  .vars({
    // ...Object.fromEntries(
    //   [
    //     await $.path(
    //       import.meta.resolve("./.env.compose"),
    //     ).readText()
    //   ]
    //     .join("\n")
    //     .split("\n")
    //     .filter((line) => !/^#/.test(line))
    //     .filter((line) => line.length > 0)
    //     .map((line) => line.split("=").map((str) => str.trim())),
    // )
  })

ghjk.task(
  "greet", 
  ($) => $`bash -c 'env'`,
  { inherit: "dev" },
)

ghjk.task(
  "dev-gran", 
  ($) => $`trunk serve`,
  { inherit: "dev", workingDir: "./src/granary_web", vars: {
    TRUNK_SERVE_PORT: 3000
  } },
)

ghjk.task(
  "compose-up", 
  ($) => $.raw`${DOCKER_CMD} compose ${
    $.argv
      .map(prof => `--profile ${prof}`)
      .join(' ')
    } up -d`, 
  { workingDir: "./tools" }
)

const allProfiles = async ($) => (await $`${DOCKER_CMD} compose config --profiles`.text())
      .split('\n');

ghjk.task(
  "compose-down", 
  async ($) => $.raw`${DOCKER_CMD} compose ${
    ($.argv.length ? $.argv : await allProfiles($))
      .map(prof => `--profile ${prof}`)
      .join(' ')
  } down -v`,
  { workingDir: "./tools" }
)
ghjk.task(
  "compose-logs", 
  async ($) => $.raw`${DOCKER_CMD} compose ${
    (await allProfiles($))
      .map(prof => `--profile ${prof}`)
      .join(' ')
    } logs ${$.argv}`,
  { workingDir: "./tools" }
)
