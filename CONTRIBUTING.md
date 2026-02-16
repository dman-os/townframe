# CONTRIBUTING

- Avoid adding dependencies if possible
  - `wc -l Cargo.lock` is around 10k lines. Let's keep it that way.
- Always audit LLM code in an editor with an IDE present.
  - Assume you'll need to slightly improve every result.

## Repo guide

- `./src/daybook_core/`: Rust core for daybook.

  - `./src/daybook_core/rt/wash_plugin/`: wash plugin for daybook host support.

  - Uses automerge (through [`samod`](https://lib.rs/samod)) and SQLite (through [`sqlx`](https://lib.rs/sqlx)) for storage.

- `./src/daybook_types/`: Core types for daybook.

- `./src/daybook_cli/`: CLI app for daybook.

- `./src/daybook_compose/`: Compose multplatform app for daybook.

    - Confirmed to run on desktop and android.

- `./src/daybook_ffi/`: [uniffi](https://lib.rs/uniffi) based bindigns for kotlin.

    - `./x/gen-ffi-dayb.ts` to re-generate the bindings and build the library.

- `./src/daybook_wflows/`: wflows for daybook.

- `./src/btress_api/`: Supporting WASI API for all apps.

  - `./src/btress_http/`: Http wrapper for `btress_api` that runs on wasmcloud.

- `./src/tests_http/`: E2e tests for the APIs through http.

- `./src/utils_rs/`: General purpose utilities.

  - `./src/api_utils_rs/`: Utilities for writing WASI APIs.

- `./src/mltools/`: ML stack.

- `./src/macros/`: Proc-macro utilities.

- `./src/infra/`: Terraform IaC for deployment.

- `./src/xtask/`: General purpose scripts.

  - Includes the `cargo x gen` command used to do codegen.

    - Is source of truth for the interfaces for the WASI apis and handles all the boilerplate.

- `./src/granary_web/`: Web app for granary (haitus).

- `./src/wflow/`: the top level crate for wflow.

  - `./src/wflow_core/`: the core types and logic.
 
  - `./src/wflow_tokio/`: tokio implementation for the wflow engine.

  - `./src/wflow_webui/` (hiatus): web ui for wflow.

  - `./src/wflow_sdk/`: sdk for writing wflows.

  - `./src/wflow_ingress_http/`: http api for wflows.

  - `./src/test_wflows/`: wflows used for tests in `wflow`.

- `./x/`: contans a lot of necessary scripts.

  - The nix flake will put them in your PATH for ease of invoking.

- `./flake.nix`: Nix flake with development environments.

  - Provides four specialized dev shells for different use cases.
  
    - For CI workflows, use the specialized shells (`ci-rust`, `ci-android`, or `ci-desktop`) for faster builds.

- `./tools/compose.yml`: docker compose file for supporting services.

  - `profiles` are used to group services together and operate on the groups.

  - Scripts `./x/compose-up.ts` and `./x/compose-logs.ts` operate on this file.

## Style guide

- Avoid crates with a `src/` directory. 
  - The source files and `Cargo.toml` should reside in the same root.
  - Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the `interlude` module of the crate.
- Don't use single char variable names.
- DHashMaps shouldn't not be used for sync across tasks/threads. 
  - They easily deadlock if modified across multiple tasks.
  - They're only a good fit for single modifier situation where a normal HashMap won't due to do async problems.
- Do not use the cargo integration tests features.
  - I.e. avoid making tests in crate_root::tests.