# CONTRIBUTING

- Avoid adding dependencies if possible
  - `wc -l Cargo.lock` is around 10k lines. Let's keep it that way.

## Repo guide

- `./src/daybook_core/`: Rust core for daybook.

  - `./src/daybook_core/rt/wash_plugin/`: wash plugin for daybook host support.

  - Uses automerge (through [`samod`](https://lib.rs/samod)) and SQLite (through [`sqlx`](https://lib.rs/sqlx)) for storage.

- `./src/daybook_types/`: Core types for daybook.

- `./src/daybook_cli/`: CLI app for daybook.

- `./src/daybook_compose/`: Compose multplatform app for daybook.

    - Confirmed to run on desktop and android.

- `./src/daybook_ffi/`: [uniffi](https://mozilla.github.io/uniffi-rs/latest/) based bindings for kotlin.

    - `./x/gen-ffi-dayb.ts` to re-generate the bindings and build the library.

- `./src/daybook_wflows/`: wflows for daybook.

- `./src/btress_api/`: Supporting WASI API for all apps.

  - `./src/btress_http/`: Http wrapper for `btress_api` that runs on wasmcloud.

- `./src/tests_http/`: E2e tests for the APIs through http.

- `./src/utils_rs/`: General purpose utilities.

  - `./src/api_utils_rs/`: Utilities for writing WASI APIs.

- `./src/mltools/`: ML stack.

- `./src/macros/`: Proc-macro utilities.

- `./src/wflow/`: the top level crate for wflow, a durable workflows impl.

  - `./src/wflow_core/`: the core types and logic.
 
  - `./src/wflow_tokio/`: tokio implementation for the wflow engine.

  - `./src/wflow_webui/` (hiatus): web ui for wflow.

  - `./src/wflow_sdk/`: sdk for writing wflows.

  - `./src/wflow_ingress_http/`: http api for wflows.

  - `./src/test_wflows/`: wflows used for tests in `wflow`.

- `./src/infra/`: Terraform IaC for deployment.

- `./src/xtask/`: Scripts in rust.

- `./x/`: contans a lot of necessary scripts.

  - The nix flake will put them in your PATH for ease of invoking.

- `./flake.nix`: Nix flake with development environments.

- `./tools/compose.yml`: docker compose file for supporting services.

  - `profiles` are used to group services together and operate on the groups.

  - Scripts `./x/compose-up.ts` and `./x/compose-logs.ts` operate on this file.

## Style guide

- Avoid crates with a `src/` directory. 
  - The source files and `Cargo.toml` should reside in the same root.
  - Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the `interlude` module of the crate.
- Don't use single char variable names, they make harder to use `sed` for replacement.
- DHashMaps shouldn't not be used for sync across tasks/threads. 
  - They easily deadlock if modified across multiple tasks.
  - They're only a good fit for single modifier situation where a normal HashMap won't due to do async problems.
- Do not use the cargo integration tests features.
  - I.e. avoid making tests in crate_root::tests.
- Git submodules? I'm using `jj` :'/

## Useful command snippets

```bash
# run a hot reload instance daybook desktop
./x/dev-d-dayb.ts
# run the daybook cli
DAYB_REPO_PATH=/tmp/repo1 cargo r -p daybook_cli --help
# run the xtask CLI
cargo x --help

# test rust code
RUST_LOG_TEST=info cargo nextest run
# lint rust code
cargo clippy --all-targets --all-features
# run pre commit hooks
prek -a
# type check kotlin app 
./x/check-dayb.ts
```
