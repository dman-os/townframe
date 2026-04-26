# CONTRIBUTING

Feel free to throw yourself or tokens at the code though I'd personally appreciate help the most in design and research.

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

- `./src/plug_test/`: plug used for testing.

- `./src/plug_plabels/`: plug providign pseudo-labelling and classification utils.

- `./src/plug_dayledger/`: personal finance plug.

## Style guide

- Avoid crates with a `src/` directory. 
  - The source files and `Cargo.toml` should reside in the same root.
  - Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the `interlude` module of the crate.
- Don't use single char variable names, they make harder to use `sed` for replacement.
- DHashMaps shouldn't not be used for sync across tasks/threads. 
  - They easily deadlock if modified across multiple tasks.
  - They're only a good fit for single modifier situation where a normal HashMap won't work due to do async problems.
- Do not use the cargo integration tests features.
  - I.e. avoid making tests in crate_root::tests.
- Git submodules? I'm using `jj` :'/
- Prefer `futures_buffered::BufferedStreamExt::buffered_unordered` over
  `futures::StreamExt::buffer_unordered` for unordered buffered async stream work.
- Avoid adding dependencies if possible
  - `wc -l Cargo.lock` is around 10k lines. Let's keep it that way.

- Always collapse if statements per https://rust-lang.github.io/rust-clippy/master/index.html#collapsible_if
- Always inline format! args when possible per https://rust-lang.github.io/rust-clippy/master/index.html#uninlined_format_args
- Use method references over closures when possible per https://rust-lang.github.io/rust-clippy/master/index.html#redundant_closure_for_method_calls
- Avoid bool or ambiguous Option parameters that force callers to write hard-to-read code such as foo(false) or bar(None). Prefer enums, named methods, newtypes, or other idiomatic Rust API shapes when they keep the callsite self-documenting.
- When you cannot make that API change and still need a small positional-literal callsite in Rust, follow the argument_comment_lint convention:
  - Use an exact /*param_name*/ comment before opaque literal arguments such as None, booleans, and numeric literals when passing them by position.
  - Do not add these comments for string or char literals unless the comment adds real clarity; those literals are intentionally exempt from the lint.
  - The parameter name in the comment must exactly match the callee signature.
- When possible, make match statements exhaustive and avoid wildcard arms.
- Newly added traits should include doc comments that explain their role and how implementations are expected to use them.
- When writing tests, prefer comparing the equality of entire objects over fields one by one.

## Useful command snippets

```bash
# enter nix devShell (large, provisions android studio)
nix develop .
# show alternative dev shells
nix flake show
# run a hot reload instance daybook desktop
./x/dev-d-dayb.ts
# build and install to connected android device
./x/build-a-dayb.ts
# run the daybook cli
DAYB_REPO_PATH=/tmp/repo1 cargo r -p daybook_cli --help
# run the xtask CLI
cargo x --help

# test rust code
# nextest is preferred test runner
RUST_LOG_TEST=info cargo nextest run
# lint rust code
cargo clippy --all-targets --all-features
# run pre commit hooks
prek -a
# type check kotlin app 
./x/check-dayb.ts

# build a plug OCI package
cargo x build-plug-oci --plug-root ./src/plug_test/
```
