export { sophon } from "@ghjk/ts";
import { file } from "@ghjk/ts";
import { sedLock } from "@ghjk/ts/std.ts";
import jdk_temurin from "./tools/jdk_temurin.port.ts";
import rust from "./tools/rust.port.ts";
import * as ports from "@ghjk/ports_wip";

const DOCKER_CMD = Deno.env.get("DOCKER_CMD") ?? "podman";
const RUST_VERSION = "nightly-2025-09-01";
const GHJK_VERSION = "v0.3.2";

const installs = {
  rust: rust({
    version: RUST_VERSION,
    profile: "default",
    components: ["rust-src"],
    targets: ["wasm32-unknown-unknown", "wasm32-wasip2"],
  }),
  py: ports.cpy_bs({
    version: "3.12.9",
    releaseTag: "20250212",
  }),
  node: ports.node({ version: "22.14.0" }),
};

// This export is necessary for typescript ghjkfiles
const ghjk = file({
  defaultEnv: Deno.env.get("CI") ? "ci" : Deno.env.get("OCI") ? "oci" : "dev",
  // allows usage of ports that depend on node/python
  enableRuntimes: true,
  allowedBuildDeps: [
    installs.node,
    installs.rust,
    installs.py,
  ],
});

ghjk.env("main")
  .vars({
    CARGO_BUILD_JOBS: 8,
  })
  .install(
    installs.node,
    ports.pnpm(),
    ports.pipi({ packageName: "pre-commit" })[0],
    ports.cargobi({ crateName: "kanidm_tools", locked: true }),
    ports.cargobi({ crateName: "cargo-nextest", locked: true }),
    ports.cargobi({ crateName: "cargo-ndk", locked: true }),
    ports.mold({ replaceLd: true }),
    // ports.cargobi({ crateName: "cargo-edit", locked: true }),
  );

ghjk.env("dev")
  .install(
    ports.cargobi({ crateName: "wash", locked: true }),
    ports.cargobi({ crateName: "wac-cli", locked: true }),
    ports.cargobi({ crateName: "wasmtime-cli", locked: true }),
    // ports.cargobi({ crateName: "wasi-virt", locked: true }),
    // ports.cargobi({ crateName: "spin-cli", locked: true }),
    ports.cargobi({ crateName: "wit-deps-cli", locked: true }),
    ports.cargobi({ crateName: "wasm-opt", locked: true }),
    ports.cargobi({ crateName: "wasm-tools", locked: true }),
    ports.cargobi({ crateName: "cargo-leptos", locked: true }),
    ports.cargobi({ crateName: "leptosfmt", locked: true }),
    ports.cargobi({ crateName: "trunk", locked: true }),
    ports.pipi({ packageName: "uv" })[0],
    // ports.pipi({ packageName: "aider-chat" })[0],
    // expo router
    ports.npmi({ packageName: "eas-cli" })[0],
    // ports.npmi({ packageName: "@google/gemini-cli" })[0],
    jdk_temurin({ version: "21.0.8\\+9.0.LTS" }),
  )
  .vars({
    // java tooling is not great with wayland scaling
    // so we force it
    // FIXME: this only seems to load on hot reload
    // also, it doesn't support fractions
    GDK_SCALE: 2,
    KANIDM_URL: "https://localhost:8443",
    KANIDM_SKIP_HOSTNAME_VERIFICATION: "true",
    KANIDM_ACCEPT_INVALID_CERTS: "true",
    WASMCLOUD_OCI_ALLOWED_INSECURE: "localhost:5000"
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
  });

ghjk.task(
  "greet",
  ($) => $`bash -c 'env'`,
);

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
  ($) => $`${DOCKER_CMD} compose --profile auth exec kanidmd kanidmd ${$.argv}`,
  {
    workingDir: "./tools",
  },
);

ghjk.task(
  "kanidm-recover",
  async ($) => {
    const out = await $`ghjk x kanidmd recover-account idm_admin -o json`
      .text();
    const pass = out.match(/"password":"([^"]*)"/)![1]!;
    console.log({ pass });
    {
      const path = $.workingDir.join(".env");
      let envRaw = await path.readText();
      const prefix = "KANIDM_ADMIN_PASSWORD=";
      let lineAdded = false;
      await path.writeText(
        [
          ...envRaw.split("\n")
            .slice(0, -1)
            // .filter(line => line.length)
            .map((line) => {
              if (line.startsWith(prefix)) {
                lineAdded = true;
                return prefix + pass;
              } else {
                return line;
              }
            }),
          lineAdded ? "" : prefix + pass,
        ]
          .join("\n"),
      );
    }
    // await $`kanidm login -D idm_admin`
  },
);

ghjk.task(
  "kanidm-login",
  async ($) => {
    await $`kanidm login -D idm_admin`;
  },
  { dependsOn: ["kanidm-recover"] },
);

ghjk.task(
  "kanidm-seed",
  async ($) => {
    await $`cargo x seed-kanidm`;
  },
  { dependsOn: ["kanidm-login"] },
);

ghjk.task(
  "dev-gran",
  ($) => $`trunk serve`,
  {
    workingDir: "./src/granary_web",
    vars: {
      TRUNK_SERVE_PORT: 3000,
    },
  },
);

ghjk.task(
  "compose-up",
  ($) =>
    $.raw`${DOCKER_CMD} compose ${$.argv
      .map((prof) => `--profile ${prof}`)
      .join(" ")
      } up -d`,
  { workingDir: "./tools" },
);

// const allProfiles = async ($) => (await $`${DOCKER_CMD} compose config --profiles`.text())
//       .split('\n');
// FIXME: https://github.com/containers/podman-compose/issues/1052
const allProfiles = ($) => Promise.resolve(["auth", "db", "cli"]);

ghjk.task(
  "compose-down",
  async ($) =>
    $.raw`${DOCKER_CMD} compose ${($.argv.length ? $.argv : await allProfiles($))
      .map((prof) => `--profile ${prof}`)
      .join(" ")
      } down -v`,
  { workingDir: "./tools" },
);
ghjk.task(
  "compose-logs",
  async ($) =>
    $.raw`${DOCKER_CMD} compose ${(await allProfiles($))
      .map((prof) => `--profile ${prof}`)
      .join(" ")
      } logs ${$.argv}`,
  { workingDir: "./tools" },
);

ghjk.task(
  "db-mig",
  { dependsOn: ["db-mig-btress"] },
);

ghjk.task(
  "db-mig-btress",
  ($) =>
    $`ghjk x flyway migrate`.env({
      DB_NAME: "btress",
      MIG_DIR: $.workingDir.join("./src/btress_api/migrations").toString(),
    }),
);

ghjk.task(
  "db-mig-daybook",
  ($) =>
    $`ghjk x flyway migrate`.env({
      DB_NAME: "daybook",
      MIG_DIR: $.workingDir.join("./src/daybook_api/migrations").toString(),
    }),
);

ghjk.task(
  "db-seed",
  { dependsOn: ["db-seed-btress"] },
);

ghjk.task(
  "db-seed-btress",
  ($) =>
    $`ghjk x psql-tty -d btress < ./src/btress_api/fixtures/000_test_data.sql`,
  {
    dependsOn: ["db-mig-btress"],
    vars: {
      DB_NAME: "btress",
      MIG_DIR: "./src/btress_api/migrations",
    },
  },
);

ghjk.task(
  "test-rust",
  ($) => $`cargo nextest run -p tests_http`,
  {
    desc: "Run rust tests",
    vars: {
      // required so that `.env.test` is loaded
      DOTENV_ENV: "test",
    },
  },
);

ghjk.task("lock-sed", async ($) =>
  sedLock($.workingDir, {
    lines: {
      "./.ghjk/deno.jsonc": [
        [
          /(https:\/\/raw.githubusercontent.com\/metatypedev\/ghjk\/)[^\/]+(\/.+)/,
          GHJK_VERSION,
        ],
      ],
      // "./README.md": [
      //   [/(GHJK_VERSION=).*()/, GHJK_VERSION],
      // ],
    },
    ignores: [
      // TODO: std function for real ignore handling
      ...(await $.path(".gitignore").readText())
        .split("\n")
        .map((l) => l.trim())
        .filter((line) => line.length > 0)
        .filter((line) => !/^#/.test(line))
        .map((l) => `${l}${l.endsWith("*") ? "" : "*"}`),
      ...(await $.path(".ghjk/.gitignore").readText())
        .split("\n")
        .map((l) => l.trim())
        .filter((line) => line.length > 0)
        .filter((line) => !/^#/.test(line))
        .map((l) => `.ghjk/${l}${l.endsWith("*") ? "" : "*"}`),
    ],
  }));

ghjk.task(
  "check-dayb",
  async ($) => {
    // Stream gradle combined stdout/stderr and reveal output only when a failure marker appears.
    // Worse-is-better: no swallow-catches, fail fast on non-zero exit, minimal code paths.
    // Match clear failure markers only: 'BUILD FAILED', 'FAILURE:' lines, kotlin 'e:' compiler errors, or 'Exception' words
    const failureRe = /(^|\n)(FAILED|FAILURE:|^e:|\bFAILED\b|\bException\b)/im;

    // Run via shell to ensure gradle wrapper is executed correctly
    const proc = Deno.run({ cmd: ["/bin/sh", "-lc", "./gradlew check"], stdout: "piped", stderr: "piped", cwd: $.workingDir.toString() });
    const decoder = new TextDecoder();

    const outReader = proc.stdout.readable.getReader();
    const errReader = proc.stderr.readable.getReader();

    let buffer = "";
    let revealed = false;

    // collect all output into buffer; do not reveal mid-stream to avoid false positives
    const handle = (chunk: Uint8Array | null) => {
      if (!chunk) return;
      const s = decoder.decode(chunk);
      buffer += s;
    };

    // pump both streams concurrently
    const pump = async (reader: ReadableStreamDefaultReader<Uint8Array>) => {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        handle(value || null);
      }
    };

    await Promise.all([pump(outReader), pump(errReader)]);

    const status = await proc.status();
    outReader.releaseLock();
    errReader.releaseLock();
    proc.close();

    if (status.code !== 0 || failureRe.test(buffer)) {
      // print full output on failure
      console.log(buffer);
      throw new Error("gradle check reported FAILED/ERROR");
    }

    return "";
  },
  {
    desc: "Run gradle check task on app.",
    workingDir: "./src/daybook_compose/",
  },
);

ghjk.task(
  "build-a-dayb",
  ($) => $`./gradlew installDebug`,
  {
    desc: "Build and install daybook_compose",
    workingDir: "./src/daybook_compose/",
  },
);

ghjk.task(
  "run-a-dayb",
  ($) => $`adb shell am start -n org.example.daybook/.MainActivity`,
  { workingDir: "./src/daybook_compose/" },
);

ghjk.task(
  "run-d-dayb",
  ($) => $`./gradlew run`.env({
    LD_LIBRARY_PATH: `${$.env.LD_LIBRARY_PATH}:${$.workingDir.join("../../target/debug/").toString()}`
  }),
  {
    dependsOn: "gen-ffi-dayb",
    workingDir: "./src/daybook_compose/",
  },
);

ghjk.task(
  "dev-d-dayb",
  ($) => $`./gradlew desktopRunHot -PmainClass=org.example.daybook.MainKt --auto`.env({
    LD_LIBRARY_PATH: `${$.env.LD_LIBRARY_PATH}:${$.workingDir.join("../../target/debug/").toString()}`
  }),
  {
    dependsOn: "gen-ffi-dayb",
    workingDir: "./src/daybook_compose/",
  },
);

ghjk.task(
  "gen-ffi-dayb",
  ($) => $`cargo build -p daybook_core 
    && cargo run 
      -p daybook_core 
      generate --library ${$.workingDir.join("./target/debug/libdaybook_core.so")}
      --language kotlin 
      --out-dir ${$.workingDir.join("./src/daybook_compose/composeApp/src/commonMain/kotlin/")}
      --no-format`,
  {
    desc: "Generate uniffi kotlin bindings for the daybook_core crate",
  },
);

ghjk.task(
  "scrcpy",
  ($) => $`scrcpy --no-audio -S`,
  { desc: "Start scrcpy to mirror an adb accessible android phone" },
);

ghjk.task(async function dev($) {
  await $`wash build`;
}, {
  workingDir: "./src/btress_api"
});

ghjk.task("build-btress-http", async ($) => {
  await $`cargo b -p btress_api -p btress_http --target wasm32-wasip2`
  await $`wac plug --plug btress_api.wasm btress_http.wasm -o btress_http_plugged.wasm`
    .cwd("./target/wasm32-wasip2/debug/")
}, {
});

ghjk.task("deploy-btress", async ($) => {
  await $`wash build`.cwd("./src/btress_http/");
  await $`wash app deploy ./src/btress_http/local.wadm.yaml --replace`
}, {
  // dependsOn: "build-btress"
});

ghjk.task("build-daybook-http", async ($) => {
  await $`cargo b -p daybook_api -p daybook_http --target wasm32-wasip2`
  await $`wac plug --plug daybook_api.wasm daybook_http.wasm -o daybook_http_plugged.wasm`
    .cwd("./target/wasm32-wasip2/debug/")
}, {
});

ghjk.task("deploy-daybook", async ($) => {
  await $`wash build`.cwd("./src/daybook_http/");
  await $`wash app deploy ./src/daybook_http/local.wadm.yaml --replace`
}, {
  // dependsOn: "build-daybook"
});

ghjk.task(
  "restate",
  ($) =>
    $`${DOCKER_CMD} compose --profile cli
          run --rm restate-cli ${$.argv}`,
  {
    workingDir: "./tools",
  },
);
