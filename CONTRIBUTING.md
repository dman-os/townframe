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

The OCI builder honors `CARGO_TARGET_DIR` for its child WASM compilation as well
as the host Cargo command. OCI output remains `./target/oci` unless `--out-root`
is supplied; artifact output is separate from the compilation cache.
Before child WASM compilation, the OCI builder runs the canonical
`x/wit-fetch.ts --plug-root <directory>` refresh for that plug. Guest bindings
consume generated `wit/deps`; without this refresh a changed host interface can
leave a successfully compiled guest on an incompatible ABI. The refresh also
preserves generated doc-comment sanitization. Use `./x/wit-fetch.ts` without
`--plug-root` to regenerate dependency bindings across all consumers.

`daybook_core` tests enable the `test-support` features of `big_repo`, `big_sync`,
and `secrets_rs` through dev-dependencies. Repository identity and key material
use the process-local in-memory keyring during tests, not a desktop secret
service or kernel keyring. A standalone runtime smoke that needs the same
isolation should explicitly enable `daybook_core/test-support`; ordinary
production builds retain the platform keyring. The in-memory keyring is not
cross-process persistence, so identity-reopen checks must run in one process.

Facet schema validators are cached by facet tag and complete schema content. Cache misses compile on Tokio's blocking pool: first-use draft meta-schema/regex compilation is CPU-bound and recursive, so it must not run on a deep asynchronous bootstrap poll stack. Cache hits neither recompile nor hop to a blocking worker. Native command acceptance boots its repository fixture on a separate task before polling the large execution/recovery scenario; increasing thread stacks is not required.

Native revocation acceptance pauses checkpoint admission only after observing the submitted task's own revision. Pausing an unrelated router or terminal revision first suspends the actor before submission and rejects the command instead of exercising the intended retained-checkpoint boundary.

The native sync ladder and two-node executable share `test_support::sync_node` lifecycle ownership. Install each service stop token before the next fallible bootstrap step; explicit stop and early-error teardown drain children before consuming RepoCtx. The headless task/processor transport fixture shares this owner without booting a Daybook runtime or obtaining Read. Both full and headless native boot run on a separate task: boxing the scenario reduces future storage, not the recursive archive-replay poll stack. Reuse RepoCtx's local-state repository instead of opening a second cache over the same files. Native admission helpers recheck only the established `CoordinationError::Pending` sampling race; missing metadata, invalid keys, revoked access, and other failures are not retried.

The memory `DaybookTestContext` also owns the named teardown guard, without booting native networking. Release facade Arc copies, drain runtime/repository children, and consume RepoCtx before deleting its TempDir. Runtime shutdown first cancels and joins the triage producer, then closes runtime admission: an in-flight triage plan may still dispatch, so reversing those steps causes an invariant failure rather than graceful shutdown.

Run the actual command/triage smoke with `OLLAMA_URL=http://localhost:11434 cargo run -p daybook_core --features test-support --example distributed_smoke`. It clones two native repositories, allocates only to the executor, checks the real guest's durable SQLite command effect, reopens the executor using retained authenticated pool control and checks its original signed terminal/receipt, then checks the processor label effect and durable slot settlement before accepting its signed terminal. The imported plug manifest enables only `test-label` for this scenario; OCR, embeddings, and other processors are not acceptance targets. Processor and pool metadata share one authority document. This is an in-process restart smoke, not cross-process keyring persistence.
The command waits inside the real guest while the producer proposes a successor. The fixture delivers the exact successor key document over native document sync, adopts signed current control, and resumes the same admitted attempt. It checks the original declaration, guest effect and receipt across rotation and executor reopen; control discovery alone does not promise readable key-document materialization. Native smoke reads retain only typed Pending sampling obligations, never authority denial or malformed data.

Public native connection success means the worker/RPC registration is complete, not merely reserved by another dial. Concurrent callers wait on registration state changes; setup failure is returned after cleanup. Native replication acceptance exercises concurrent dials without timers or polling.

`IrohSyncRepo::discover_task_pool(peer, pool, group)` and `discover_processor_domain(peer, processor, group)` retrieve bounded signed current transport control over `townframe/task-coordination/6`. Explicit identity/group sponsorship requires native Relay, not metadata Read or an executor session. Connected nodes exchange pool and processor control after attachment, connection registration, and native authority progress. Offers advance only an already-attached identity/group. Current control remains history-free; its common signed header binds register scope, incarnation, and publication-key URI. The independent exchange child may await a peer without blocking permission maintenance and stops before native transport dependencies.

When received ciphertext names a missing historical key source, native transport requests one bounded signed source certificate from the delivering authenticated peer. An original current certificate can attest its own publication key; a selected current editor can reattest already-admitted historical provenance. Native original-author Edit, local/requester Relay, exact scope/incarnation/key URI, and same-group source-document Relay are checked independently. Key provenance and its certificate commit together before ciphertext admission. No source certificate imports predecessor parts, subscriptions, private configuration, or Read. Retained Relays forward exact signatures after publisher disconnect; disconnected resume retains both selection and source evidence.

Attachment persists the certificate and selected binding atomically before permission installation. A provisioner-selected successor without a certificate clears the old selection's certificate. A Relay forwards the original certificate without minting an Edit declaration. `resume_task_pool(pool, group)` and `resume_processor_domain(processor, group)` re-admit retained control without dialing its publisher. Processor control also appends only a generation/reference locator to local plug configuration; execution activation remains node-local. Native processor acceptance proves offline cached-control reopen, template-free initial Relay discovery, automatic successor adoption, exact ciphertext carry-forward, higher-generation adoption within the same authority document, and replacement of a revoked leaf editor's certificate. Fresh historical-source bootstrap and physical retirement remain separate obligations.

A provisioned Relay may start without a signed certificate. Automatic exchange requests control without an offer in that case; the first matching certificate is durably retained for forwarding and resume. Equal already-authenticated selected control does not repeatedly reinstall permissions. The three-node late-offline acceptance checks a certificate-free Relay adopting the successor automatically, retaining exact producer ciphertext, and remaining unable to decrypt it.

Live scheduling follows strong transport-owner selection through an independent watch wake; native ingress never waits for an actor acknowledgement. Rotation replaces the part reader and loads the successor selector checkpoint while retaining admitted attempts, pending terminal effects, and unread revisions. A superseded SQL binding is not native revocation: only denial under the selected native authority cancels durable execution. Mixed current/predecessor delivery hints are admitted only when they all bind to one logical register. Signed control may arrive before key-document materialization; that missing dependency retains the operation as native Pending.

Native graph tests own internally multithreaded runtimes. Nextest accounts for those sync, headless transport, and native-owner fixtures with `threads-required = "num-cpus"`; this constrains concurrent graph allocation without increasing test timeouts or changing scenario assertions.

Named BigSync channels have opt-in correlation with `--features big_sync_core/channel-trace` and `RUST_LOG_TEST='warn,big_sync_core::mpsc=trace'`. Capture native nextest output with `--no-capture` into a file, then run `python3 .agents/skills/stress-sync-investigation/scripts/channel-edges.py <log>`. Records identify process, channel, message, channel labels, message type, and send/receive/failure/cancellation phase; payloads and keys are never logged. The collector streams the log, bounds retained correlation state, rejects unsupported records instead of accepting a partial summary, and emits bounded channel rows and unresolved message IDs with source line numbers. Unresolved observations may still be queued or outside the capture; they do not prove loss. Send completion can be logged after reception. Channel labels are not identities for individual cloned senders, IDs are local to one process run, and this instrumentation records neither queue linearization order nor deterministic replay inputs. Without the feature, channel layout remains the original payload type with no trace allocation or counters.

Automerge 0.12 and Autosurgeon 0.14 are upgraded together. Both Autosurgeon
crates are pinned to the published `dman-os/autosurgeon` fork, rebased onto
upstream 0.14 with tuple/historical-read/hydration fixes retained; no sibling
checkout is required. Drawer and native store writes set node authors, never
path-derived session actor IDs.
Keep fixed-width `VersionTag` IDs and legacy dmeta role IDs separate from node
authors; do not derive an author from an old unlabelled change's actor ID.

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
- Unless the test is guaranteeing wall clock times as part of the contract, avoid introducing in test timeouts.
  - Nextest, the test runner, has a real bound on most tests that should prevent forever runs.
  - Putting timeouts in tests makes them flaky since it's been proven hard to get good timeout values that are reliable under different CI conditions.

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
