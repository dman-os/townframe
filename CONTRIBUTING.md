# CONTRIBUTING

Feel free to throw yourself or tokens at the code though I'd personally
appreciate help the most in design and research.

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

# build the daybook CLI as a sandboxed Nix package (no devshell, no .env)
nix build .#daybook-cli
./result/bin/daybook_cli --help
# packaged CLI end-to-end test (init, documents, persistence) in an empty env
nix build .#daybook-cli.tests.round-trip
# hermetic source-only checks: nixfmt, statix/deadnix, rustfmt (every .rs
# file), deno fmt/lint/check for ./x, biome, JSON/TOML/YAML validity. Runs as a
# pre-commit hook; cache hits make an unchanged tree free. clippy, nextest and
# the cargo x checks stay in the devshell (they need ./target).
nix flake check
# apply the formatting those checks enforce (nixfmt, rustfmt, deno fmt, biome)
nix fmt

# test rust code
# CRITICAL: nextest is preferred test runner
RUST_LOG_TEST=info cargo nextest run
# lint rust code
cargo clippy --all-targets --all-features
# run AST, architecture, API snapshot, and doc guardrails
cargo x guardrails lint
cargo x guardrails api --all --check
cargo x guardrails doctor
cargo x guardrails check-matrix --dry-run
cargo x guardrails check-matrix --target wasm32-wasip3
cargo x guardrails check-matrix --target wasm32-unknown-unknown
# run pre commit hooks
prek -a
# type check kotlin app 
./x/check-dayb.ts

# build a plug OCI package
# goes into ./target/oci
cargo x plug build-oci --plug-root ./src/plug_test_manifest/
```

## Dual targets: host vs guest

Two targets describe one product, not two products:

- **Host** (`wasm32-unknown-unknown`): the browser-side modules —
  `dayframe_web`, `clay_rt_wcmp_web`. They run the polyfill engine (`wcmp`) on the
  page's own `WebAssembly`. No WASI.
- **Guest** (`wasm32-wasip3`): micro components (`micro_counter`, `clay_mdk`
  examples) that both engines instantiate. rustc emits a component directly (no
  adapter). The guest import surface is the WASI 0.3 set listed in ADR 018
  §Migration result — wasip3 — plus the `townframe:dayframe` WIT imports.

Native is the third lane: the same guest crates' host-side tests
(`clay_rt_wasmtime`, `clay_rt_wcmp_wasmi`) drive the prepared components under
nextest. Feature unification must stay isolated per target: guest builds and
matrix checks pass `--config resolver.feature-unification="selected"`.
`x/check-dayframe.ts --prepare-only` rebuilds and exports `DAYFRAME_*_COMPONENT`
paths for tests and the web shell.

## Rust target contracts

The root workspace owns native applications and portable libraries.
`src/wasm_utils_rs/Cargo.toml` owns Wasm applications and components without
moving them out of `src/`. All three lockfiles are committed (root,
`src/wasm_utils_rs`, `src/btress_sysadmin`); update and review each affected
resolution. Native `--workspace --all-features` never includes Wasm entrypoints.

Portable libraries declare supported targets and feature combinations in their
crate-local `guardrails.toml` under `[builds.<target>]`.
`cargo x guardrails check-matrix` runs one isolated Cargo invocation per
configuration: no defaults, each declared feature alone, all declared features,
and the crate defaults merged in — deduplicated, so a crate with no features
costs one run per target. `powerset = true` opts a contract into every subset
instead. `required_features` describes target-specific applications such as
Leptos SSR versus hydration. New features must be added to a contract or
`cargo x guardrails lint` reports `ERROR_FEATURE_CONTRACT`. Features named after
platforms are rejected (`ERROR_PLATFORM_FEATURE`): a platform is a target or a
separate crate, never an additive feature. A Wasm build may only depend on
crates whose contract covers that target (`ERROR_TARGET_BOUNDARY`); native
dev-dependencies such as `daybook_core`'s test harness are exempt because they
are never built for the guest.

Use `cargo x guardrails check-matrix --package utils_rs --dry-run` to inspect
commands and omit `--dry-run` to execute them. CI checks native, WASI, and
browser contracts in separate jobs. Format with `nix fmt`; it runs rustfmt over
every `.rs` file in all three workspaces (root, `src/wasm_utils_rs`,
`src/btress_sysadmin`), including files outside any module tree. Build guest
artifacts with
`cargo build --locked --manifest-path src/wasm_utils_rs/Cargo.toml --target wasm32-wasip3 --release`;
browser builds require the browser target and their declared features. Guest
workspace dev/release and shared-library `wasm-release` profiles use
optimization level 3. `src/btress_sysadmin` is a separate single-crate
workspace: cargo-leptos assumes each package lives under its workspace root and
failed with "Could not remove base" when the crate was a member of
`src/wasm_utils_rs`, whose root is a sibling directory.

Each manifest crate declares
`[package.metadata.townframe] routines = "../plug_name_routines"`.
`cargo x plug build-oci --plug-root src/plug_name_manifest` runs its native
`manifest` binary, builds the sibling routines component, and packages both
under the existing logical plug ID. Wasmtime is not a dependency of xtask.

## Build directories and feature resolution

Cargo runs through [mbx](https://mr-boxington.jdx.dev/) (mr-boxington), pinned
as a flake input. The dev shell puts mbx's Cargo shim ahead of the toolchain's
`cargo` and unsets stdenv's `CC`/`CXX`, so host C compilations reach mbx's
compiler shims. mbx keys each compilation by its inputs, with checkout paths
normalized, and restores matching outputs from one store on the machine. A
second worktree therefore builds warm, which sccache could not do: it got 0%
Rust hits across worktree paths because its `SCCACHE_BASEDIRS` only applies to
C/C++. Identical compilations running at the same time wait for one result, and
a machine-wide scheduler admits work against the CPU count and a memory budget
learned per compilation, holding new compilations while the kernel reports
memory pressure. On a 2026-11-09 trial, `xtask` took 62s cold and 15s from a
second path (290 hits, 2 misses), and the `plug_test_routines` guest 79s and 18s.

Each workspace root (the root, `src/wasm_utils_rs`, and `src/btress_sysadmin`)
gets a managed target: `target` is a symlink into the mbx cache, which mbx
prunes when the checkout is gone, unused, or over budget. Setting
`CARGO_TARGET_DIR` opts a checkout out of this, so leave it unset;
`cargo x guardrails doctor` reports it. Code that locates build output reads
`CARGO_TARGET_DIR` and falls back to the workspace's `target`, which is that
symlink.

Two settings would defeat the cache, and are absent on purpose.
`split-debuginfo = "unpacked"` makes every native link (build scripts, proc
macros, test binaries) unshareable across checkouts, and the doctor reports it.
An existing `RUSTC_WRAPPER` makes mbx step aside and cache nothing.
`mbx explain --last` names the cause of every miss and bypass in the last build.

The cache directory is machine configuration, in mbx's global config
(`mbx settings set cache_dir <dir>`). Keep it on the filesystem that holds the
checkouts: restores reflink or hard link the cached object, and fall back to
copying across filesystems. mbx's Cargo shim and cache agent require the exact
same mbx version, so every worktree on a machine must pin the same tag.

`.cargo/config.toml` sets `[resolver] feature-unification = "workspace"` (with
the `[unstable] feature-unification = true` gate it needs on nightly; stable
Cargo warns and ignores it) so alternating `-p` selections do not rebuild shared
dependencies under different feature sets. That is only safe because Wasm
entrypoints live in their own workspace: unification would otherwise pull
native-only features into `wasm32` builds. Unification still applies to every
member of the workspace you invoke, so a portable crate checked _from the root
workspace_ inherits the native closure —
`cargo clippy -p daybook_types --target wasm32-wasip3 --no-default-features --features wit`
fails in `uniffi_core`, and `-p utils_rs --target wasm32-unknown-unknown` fails
in `mio`, because `daybook_core` enables those features. Two consequences: check
guest contracts through `cargo x guardrails check-matrix`, which passes
`--config resolver.feature-unification="selected"` so each configuration is
resolved in isolation, and build guests through `src/wasm_utils_rs/Cargo.toml`,
whose members are all Wasm so unification cannot leak native features.

## Documentation guardrails

`cargo x guardrails lint` checks Markdown under `docs/`, except
`docs/DEVDOC/log.md`, as the `@docs` policy target:

- Relative Markdown link and image targets must exist (`ERROR_DOC_LINK`).
  External URLs, fragment anchors, root docs, and Rust doc comments are not
  covered by this check.
- ADR filename prefixes must be unique (`ERROR_ADR_ID`), and each ADR must
  contain a line starting with `Status:` or `**Status:**` (`ERROR_ADR_STATUS`).
  Status values are not yet validated.
- Scratch notes need `Status:`, `Owner:`, `Updated:`, and `Scope:` in the first
  25 lines (`ERROR_DOC_METADATA`). These are plain Markdown fields; date
  validity and staleness are not checked.
- Each directory under `.agents/skills/` needs `SKILL.md` and its name
  referenced in `AGENTS.md` (`ERROR_SKILL_DOC`).

Existing debt is itemized in `docs/guardrails.toml`; a clean lint run means no
unexempted violations, not that every historical doc has been repaired. After
fixing an exempted finding, run `cargo x guardrails bless --package @docs` to
lower/remove its exemption, then rerun `cargo x guardrails lint`. Do not raise
ceilings or add blanket exemptions to pass the check. Follow-ups and deferred
decisions are tracked in
[the guardrail tracker](docs/scratch/agent-guardrails-open-items.md).

## Generated files and merge conflicts

Resolve source and manifest conflicts first, then regenerate derived files
instead of hand-merging them. Regenerate SQLite query/migration bindings with
`cargo x sql gen --all` and verify with `cargo x sql gen --check --all`. For
intentional public-interface changes, regenerate snapshots with
`cargo x guardrails api --all --bless`, review the contract diff, then run
`cargo x guardrails api --all --check`.

Both root and guest `Cargo.lock` files are tracked. Resolve dependency intent in
the manifests, use Cargo to regenerate each affected workspace's lockfile, and
review both resolutions. Do not silently renumber applied migrations or accepted
ADR identities to resolve collisions. Shared-dependency lockfile-divergence
checking remains a tracked follow-up.

## Repo guide

- `./src/utils_rs/`: Portable shared utilities (native, WASI, browser).
- `./src/native_utils_rs/`: Native filesystem and HTTP utilities.
- `./src/wasm_utils_rs/`: Wasm workspace and home for guest-specific utilities.
  Its workspace owns guest crates at sibling paths, with a separate lockfile and
  `opt-level = 3` in dev and release profiles.
  - `./src/am_utils_rs/`: Automerge utilities.
  - `./src/sqlite_rs/`: Shared SQLite runtime, typed queries, and embedded
    migrations.
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
- `./src/big_repo/`: Automerge-repo alternative that supports keyhive and
  big_sync.
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
- `./src/daybook_ffi/`: [uniffi](https://mozilla.github.io/uniffi-rs/latest/)
  based bindings for kotlin.
  - `./x/gen-ffi-dayb.ts` to re-generate the bindings and build the library.
- `./src/daybook_types/`: Core types for daybook.
- `./src/plug_*_manifest/`: Portable plug types, shared logic, native `manifest`
  binaries, and native e2e tests. Members of the native workspace.
- `./src/plug_*_routines/`: Wasm WIT bindings and workflows. Members of the Wasm
  workspace; depend on their manifest crate, never the reverse.
- `./src/daybook_pdk/`: Plug development kit.
  - Supporting code for writing plugs goes here.

## Style guide

- Avoid crates with a `src/` directory.
  - The source files and `Cargo.toml` should reside in the same root.
  - Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the
  `interlude` module of the crate.
  - `utils_rs` provides a prelude that exports a lot of common things expected
    to go into other crate `interlude`s.
    - `Res`, an alias for `eyre::Result` for generic errors.
    - `ferr!`, an alias of `eyre::eyre!` for quick error messages.
    - Instead of `unwrap` or generic, usless messages for `expect` or `ferr!`
      like "xxx channel was closed", prefer the common expect_tags consts seen
      in the `mod expect_tags` contained in `./src/utils_rs/lib.rs` like
      `ERROR_CHANNEL`, `ERROR_JSON`, `ERROR_IMPOSSIBLE` (for unreachable cases).
- Don't use single char variable names, they make harder to use `sed` for
  replacement.
- DHashMaps shouldn't not be used for sync across tasks/threads.
  - They easily deadlock if modified across multiple tasks.
  - They're only a good fit for single modifier situation where a normal HashMap
    won't work due to do async problems.
- Do not use the cargo integration tests features.
  - I.e. avoid making tests in crate_root::tests.
- Git submodules? I'm using `jj` which doesn't support submodules :'/
- Prefer `futures_buffered::BufferedStreamExt::buffered_unordered` over
  `futures::StreamExt::buffer_unordered` for unordered buffered async stream
  work.
- Avoid adding dependencies if possible.
- Always use #[expect(...)] instead of #[allow(...)] for suppressing lints.
  - The expect attribute will warn if the lint is no longer triggered, helping
    to keep the codebase clean.
- Unless the test is guaranteeing wall clock times as part of the contract,
  avoid introducing in test timeouts.
  - Nextest, the test runner, has a real bound on most tests that should prevent
    forever runs.
  - Putting timeouts in tests makes them flaky since it's been proven hard to
    get good timeout values that are reliable under different CI conditions.

- If a function always clones a parameter or does an allocation, replacing an
  argument by value instead of reference.
  - For example, a &str that's immediately turned into a String.
  - This embues the function signature with more information about the cost.
  - If the clone/allocation is on an optional branch, a ref arg is fine.
  - This obviously doesn't apply to cases where the value is transformed in the
    function changing semantics.
    - For example, a serde_json::Value that's turned into a string.
- Always collapse if statements per
  https://rust-lang.github.io/rust-clippy/master/index.html#collapsible_if
- Always inline format! args when possible per
  https://rust-lang.github.io/rust-clippy/master/index.html#uninlined_format_args
- Use method references over closures when possible per
  https://rust-lang.github.io/rust-clippy/master/index.html#redundant_closure_for_method_calls
- Avoid bool or ambiguous Option parameters that force callers to write
  hard-to-read code such as foo(false) or bar(None). Prefer enums, named
  methods, newtypes, or other idiomatic Rust API shapes when they keep the
  callsite self-documenting.
- When you cannot make that API change and still need a small positional-literal
  callsite in Rust, follow the argument_comment_lint convention:
  - Use an exact /_param_name_/ comment before opaque literal arguments such as
    None, booleans, and numeric literals when passing them by position.
  - Do not add these comments for string or char literals unless the comment
    adds real clarity; those literals are intentionally exempt from the lint.
  - The parameter name in the comment must exactly match the callee signature.
- When possible, make match statements exhaustive and avoid wildcard arms.
- Newly added traits should include doc comments that explain their role and how
  implementations are expected to use them.
- When writing tests, prefer comparing the equality of entire objects over
  fields one by one.
- Adding a key to a hash map that shouldn't have seen that key before, add an
  `assert!(old.is_none(), "fishy")`
