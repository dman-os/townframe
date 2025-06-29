export { sophon } from "@ghjk/ts";
import { file } from "@ghjk/ts";
import * as ports from "@ghjk/ports_wip";

import * as std_url from "jsr:@std/url@0.215.0";

const DOCKER_CMD = Deno.env.get("DOCKER_CMD") ?? "podman";
const RUST_VERSION = "1.84.1";

const installs = {
  rust: ports.rust({ 
    version: RUST_VERSION, 
    profile: "default",
    components: ["rust-src"],
    targets: ["wasm32-unknown-unknown"] 
  }),
  py: ports.cpy_bs({
    version: "3.12.9",
    releaseTag: "20250212"
  }),
  node: ports.node({ version: "22.14.0" }),
}

// This export is necessary for typescript ghjkfiles
const ghjk = file({
  defaultEnv: Deno.env.get("CI") ? "ci" : Deno.env.get("OCI") ? "oci" : "dev",
  // allows usage of ports that depend on node/python
  enableRuntimes: true,
  allowedBuildDeps: [
    installs.node,
    installs.rust, 
    installs.py
  ],
});

export const sophon = ghjk.sophon;

ghjk.env("main")
  .vars({
    CARGO_BUILD_JOBS: 8,
  })
  .install(
    installs.rust,
    installs.node,
    ports.pnpm(),
    ports.pipi({ packageName: "pre-commit" })[0],
    ports.cargobi({ crateName: "kanidm_tools", locked: true }),
    ports.cargobi({ crateName: "cargo-nextest", locked: true }),
  );

ghjk.env("dev")
  .install(
    ports.cargobi({ crateName: "cargo-leptos", locked: true }),
    ports.cargobi({ crateName: "leptosfmt", locked: true }),
    ports.cargobi({ crateName: "trunk", locked: true }),
    ports.pipi({ packageName: "uv" })[0],
    ports.pipi({ packageName: "aider-chat" })[0],
    ports.npmi({ packageName: "eas-cli" })[0],
  )
  .vars({
    KANIDM_URL: "https://localhost:8443",
    KANIDM_SKIP_HOSTNAME_VERIFICATION: "true",
    KANIDM_ACCEPT_INVALID_CERTS: "true",
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
)

ghjk.task(
  "flyway",
  ($) =>
    $`${DOCKER_CMD} compose --profile cli
          run -e DB_NAME=$DB_NAME -e MIG_DIR=$MIG_DIR --rm flyway ${$.argv}`,
  {
    workingDir: "./tools",
  },
);

ghjk.task(
  "psql",
  ($) =>
    $`${DOCKER_CMD} compose --profile db
          exec postgres psql -d postgres 
          -v SEARCH_PATH=zitadel,spicedb,granary,btress ${$.argv}`,
  {
    workingDir: "./tools",
  },
);

ghjk.task(
  "psql-tty",
  ($) =>
    // FIXME: compose exec doesn't suupport -i
    $`${DOCKER_CMD} exec -i townframe_postgres_1 psql -U postgres
          -v SEARCH_PATH=zitadel,spicedb,granary,btress ${$.argv}`,
  {
    workingDir: "./tools",
  },
);

ghjk.task(
  "kanidmd",
  ($) =>
    $`${DOCKER_CMD} compose --profile auth exec kanidmd kanidmd ${$.argv}`,
  {
    workingDir: "./tools",
  },
);

ghjk.task(
  "kanidm-recover", 
  async ($) => {
    const out = await $`ghjk x kanidmd recover-account idm_admin -o json`.text();
    const pass = out.match(/"password":"([^"]*)"/)![1]!;
    console.log({pass})
    {
      const path = $.workingDir.join(".env");
      let envRaw = await path.readText();
      const prefix = 'KANIDM_ADMIN_PASSWORD=';
      let lineAdded = false;
      await path.writeText(
          [
            ...envRaw.split('\n')
              .slice(0, -1)
              // .filter(line => line.length)
              .map(line => {
                if (line.startsWith(prefix)) {
                  lineAdded = true;
                  return prefix+pass;
                } else {
                  return line;
                }
              }),
            lineAdded ? '' : prefix+pass
          ]
          .join('\n')
      )
    }
    // await $`kanidm login -D idm_admin`
  },
)

ghjk.task(
  "kanidm-login", 
  async ($) => {
    await $`kanidm login -D idm_admin`
  },
  { dependsOn: ["kanidm-recover"] }
)

ghjk.task(
  "kanidm-seed", 
  async ($) => {
    await $`cargo x seed-kanidm`
  },
  { dependsOn: ["kanidm-login"] }
)


ghjk.task(
  "dev-gran", 
  ($) => $`trunk serve`,
  { 
    workingDir: "./src/granary_web", 
    vars: {
      TRUNK_SERVE_PORT: 3000
    } 
  },
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

// const allProfiles = async ($) => (await $`${DOCKER_CMD} compose config --profiles`.text())
//       .split('\n');
// FIXME: https://github.com/containers/podman-compose/issues/1052
const allProfiles = ($) => Promise.resolve(["auth", "db", "cli"]);

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

ghjk.task(
  "db-mig",
  { dependsOn: ["db-mig-btress"] }
)

ghjk.task(
  "db-mig-btress",
  ($) => $`ghjk x flyway migrate`.env({
    DB_NAME: "btress",
    MIG_DIR: $.workingDir.join("./src/btress_api/migrations").toString()
  })
)

ghjk.task(
  "db-seed",
  { dependsOn: ["db-seed-btress"] }
)

ghjk.task(
  "db-seed-btress",
  ($) => $`ghjk x psql-tty -d btress < ./src/btress_api/fixtures/000_test_data.sql`,
  {
    dependsOn: ["db-mig-btress"],
    vars: {
      DB_NAME: "btress",
      MIG_DIR: "./src/btress_api/migrations"
    }
  }
)

ghjk.task(
  "test",
  ($) => $`cargo nextest run -p btress_api`,
  { 
    vars: {
      // required so that `.env.test` is loaded
      DOTENV_ENV: "test"
    }
  }
)
