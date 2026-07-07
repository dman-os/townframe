# CONTRIBUTING

Feel free to throw yourself or tokens at the code though I'd personally appreciate help the most in design and research.

## Useful command snippets

```sh
# enter nix devShell (large, provisions android studio)
nix develop .
# show alternative dev shells
nix flake show
# run a hot reload instance daybook desktop
./x/dev-d-dayb.ts
# build and install to connected android device
./x/build-a-dayb.ts
# run the daybook cli
DAYB_REPO_PATH=/tmp/repo1 cargo r -p daybook_cli -- --help
# run the xtask CLI
cargo x --help

# test rust code
# CRITICAL: nextest is preferred test runner
RUST_LOG_TEST=info cargo nextest run
# lint rust code
cargo clippy --all-targets --all-features
# run pre commit hooks
prek -a
# type check kotlin app 
./x/check-dayb.ts

# build a plug OCI package
# goes into ./target/oci
cargo x build-plug-oci --plug-root ./src/plug_test/
```

## Repo guide

- `./src/utils_rs/`: General purpose utilities.
  - `./src/api_utils_rs/`: Utilities for writing WASI APIs.
  - `./src/am_utils_rs/`: Automerge utilities.
  - `./src/sqlx_utils_rs/`: Sqlx utilities.
- `./src/wflow/`: Top level crate for wflow, a durable workflows impl.
  - `./src/wflow_core/`: Core types and logic.
  - `./src/wflow_tokio/`: Tokio implementation for the wflow engine.
  - `./src/wflow_sdk/`: Sdk for writing wflows.
  - `./src/test_wflows/`: Wflows used for tests in `wflow`.
- `./src/mltools/`: ML abstractions.
- `./src/pauperfuse/`: Worktrees and FUSE.
- `./src/big_sync/`: Set reconcilliation algorithms.
  - `./src/big_sync_core/`: Sans-io core for the big_sync impls.
  - `./src/big_sync/worker.rs`: Tokio based driver.
  - `./src/big_sync/rpc.rs`: [Irpc](https://lib.rs/irpc) based rpc.
  - `./src/big_sync/part_store/sqlite.rs`: Sqlite based partition store.
- `./src/big_repo/`: Automerge-repo alternative that supports keyhive and big_sync.
- `./src/infra/`: Terraform IaC for deployment.
- `./src/xtask/`: Scripts in rust.
- `./x/`: Scripts in deno typescript.
  - The nix flake will put them in your PATH for ease of invoking.
- `./flake.nix`: Nix flake with development environments.
  - Default devshell loads `./.env` file into your env if found.
- `./tools/compose.yml`: docker compose file for supporting services.
  - `profiles` are used to group services together and operate on the groups.
  - Scripts `./x/compose-up.ts` and `./x/compose-logs.ts` operate on this file.

### Daybook

- `./src/daybook_core/`: Rust core for daybook.
  - `./src/daybook_core/rt/wash_plugin/`: Wash plugin for daybook host support.
- `./src/daybook_types/`: Core types for daybook.
- `./src/daybook_cli/`: CLI app for daybook.
- `./src/daybook_compose/`: Compose multplatform app for daybook.
  - Tested on desktop and android only.
- `./src/daybook_ffi/`: [uniffi](https://mozilla.github.io/uniffi-rs/latest/) based bindings for kotlin.
  - `./x/gen-ffi-dayb.ts` to re-generate the bindings and build the library.
- `./src/daybook_types/`: Core types for daybook.
- `./src/plug_test/`: Plug used for testing.
- `./src/plug_plabels/`: Plug providign pseudo-labelling and classification utils.
- `./src/plug_dayledger/`: Personal finance plug.
- `./src/daybook_pdk/`: Plug development kit. 
  - Supporting code for writing plugs goes here.

### Dead code

The following are not in use and possibly dead code.

- `./src/daybook_wflows/`: wflows for daybook.
- `./src/daybook_sql/`: wit bindings for sql.

## Style guide

- Avoid crates with a `src/` directory. 
  - The source files and `Cargo.toml` should reside in the same root.
  - Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the `interlude` module of the crate.
  - `utils_rs` provides a prelude that exports a lot of common things expected to go into other crate `interlude`s.
    - `Res`, an alias for `eyre::Result` for generic errors.
    - `ferr!`, an alias of `eyre::eyre!` for quick error messages.
    - Instead of `unwrap` or generic, usless messages for `expect` or `ferr!` like "xxx channel was closed", prefer the common expect_tags consts seen in the `mod expect_tags` contained in `./src/utils_rs/lib.rs` like `ERROR_CHANNEL`, `ERROR_JSON`, `ERROR_IMPOSSIBLE` (for unreachable cases).
- Don't use single char variable names, they make harder to use `sed` for replacement.
- DHashMaps shouldn't not be used for sync across tasks/threads. 
  - They easily deadlock if modified across multiple tasks.
  - They're only a good fit for single modifier situation where a normal HashMap won't work due to do async problems.
- Do not use the cargo integration tests features.
  - I.e. avoid making tests in crate_root::tests.
- Git submodules? I'm using `jj` which doesn't support submodules :'/
- Prefer `futures_buffered::BufferedStreamExt::buffered_unordered` over
  `futures::StreamExt::buffer_unordered` for unordered buffered async stream work.
- Avoid adding dependencies if possible.
- Always use #[expect(...)] instead of #[allow(...)] for suppressing lints.
  - The expect attribute will warn if the lint is no longer triggered, helping to keep the codebase clean.

- If a function always clones a parameter or does an allocation, replacing an argument by value instead of reference.
  - For example, a &str that's immediately turned into a String.
  - This embues the function signature with more information about the cost.
  - If the clone/allocation is on an optional branch, a ref arg is fine.
  - This obviously doesn't apply to cases where the value is transformed in the function changing semantics.
    - For example, a serde_json::Value that's turned into a string.
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
- Adding a key to a hash map that shouldn't have seen that key before, add an `assert!(old.is_none(), "fishy")`
