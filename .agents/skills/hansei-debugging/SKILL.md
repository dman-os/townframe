---
name: hansei-debugging
description: Inspect suspended Tokio tasks, async await chains, channel waits, and mutex contention in Linux core dumps with Hansei. Use for Rust async hangs when a snapshot of the blocked state is more useful than execution replay.
---

# Hansei debugging for Tokio hangs

Hansei is a post-mortem debugger, not a live debugger or recorder. Use a core to locate a blocked await; a snapshot alone does not establish the causal history or prove a lost wakeup. Keep ordinary phase instrumentation for correlation. Use rr only when execution history is needed and its host prerequisites are satisfied.

## Verified baseline

Validated on Linux x86_64 with Hansei revision `088d7641d206869dcc257c2b5cff839ad991d743`, Rust 1.98.0, Tokio 1.53.1, and GDB 17.2. The upstream deadlock example decoded ten tasks, the dispatcher's contended Tokio mutex and wake queue, the reloader's unsent oneshot with its sender alive, source-level async backtraces, and three futures held in `FuturesUnordered`. Extraction to `.tinfo` and subsequent analysis using that file also worked.

At this revision, extraction supports Rust 1.97–1.98. Townframe's Rust 1.100 nightly is outside that range: the normal command refuses it. A trial with `--allow-unsupported` listed example tasks but lost detailed await-chain decoding. Such output is provisional, not a trusted diagnosis. Do not silently add `--force`, `--allow-unsupported`, or `--best-effort`, change the project's compiler, or widen Hansei's support range.

The real native test core was successfully captured through systemd-coredump and exported as a valid 684,486,656-byte ELF core. Direct extraction rejected its split-DWARF skeleton units. GNU `dwp` and LLVM `llvm-dwp` both packaged the original executable's `.dwo` files. Native extraction with the GNU package exited 137 under a 3 GiB memory limit; with the LLVM package and a 6 GiB limit it reached an explicit Hansei panic: `exegesis/src/cgu.rs:274: unexpected encoding for Base type: Encoding(DwAte(3))` (`DW_ATE_complex_float`). No native task state was decoded. This is an extractor blocker, not a diagnosis of the application.

## Safety and resources

- Core dumps, locals, and output may contain credentials, keys, and document data. Set `umask 077`, keep captures local, and never upload or paste arbitrary locals.
- Preserve the exact executable and matching DWARF before rebuilding. On Linux `--binary` names the original executable; a different debug build is not a replacement.
- Check free memory, swap, and disk before capture. Capture and analyze one large process at a time. Two concurrent native-test GDB captures caused memory pressure and incomplete cores on this host.
- For capture-only GDB use `--readnever`; Hansei, not GDB, needs DWARF. Do not enable dumping every excluded mapping for a Wasmtime process: direct native `generate-core-file` failed to finish within 180 seconds here and left no valid core.
- Where a user systemd manager is available, isolate expensive analysis: `systemd-run --user --scope --quiet -p MemoryMax=3G -p MemorySwapMax=512M ...`. Choose limits from available resources; hitting a limit is a failed analysis, not a target-program finding. The scope command was verified on this host.
- Validate a core with `readelf -h` and require capture completion. An incomplete file may have the wrong ELF magic. Do not try to interpret it.
- Capturing a deliberately aborted test is diagnostic, not an acceptance-test pass. Never lengthen test timeouts or serialize the acceptance suite just to call it green.

## Build without changing the application

Read the current upstream README and compiler support before proceeding:
<https://github.com/oxidecomputer/hansei/blob/main/README.adoc>

Download a pinned source archive outside the repository; do not use Git here. Example:

```sh
rev=088d7641d206869dcc257c2b5cff839ad991d743
mkdir -p /tmp/townframe-hansei
curl -fsSL "https://api.github.com/repos/oxidecomputer/hansei/tarball/$rev" \
  | tar -xz --strip-components=1 -C /tmp/townframe-hansei
```

Use the configured Cargo target directory, including the user-authorized external target when applicable. Serialize Cargo builds with the repository's validation lock. Build `--release --locked --bin hansei` against the downloaded manifest. Build the excluded `example/Cargo.toml` separately.

On Nix, `cargo +1.98.0` may not work because Cargo is not the rustup proxy. `rustup run 1.98.0 cargo` alone also produced a nightly target in this session. The verified supported-example build used **explicit Cargo and RUSTC paths**, from outside the Townframe directory:

```sh
rustup toolchain install 1.98.0 --profile minimal
cargo198=$(rustup which --toolchain 1.98.0 cargo)
rustc198=$(rustup which --toolchain 1.98.0 rustc)
cd /tmp/townframe-hansei
RUSTC="$rustc198" flock /tmp/townframe-cargo-validation.lock \
  "$cargo198" build --release --locked --manifest-path example/Cargo.toml
```

The example enables full release DWARF for dependencies. Hansei itself was successfully built with the available nightly compiler; the **target being inspected** determines extraction compatibility. Do not infer compatibility from Hansei's own compiler.

## Capture without changing global ptrace policy

Check `kernel.yama.ptrace_scope`. With value 1, a sibling `gcore` generally cannot attach unless the target explicitly permits it. The upstream example calls `PR_SET_PTRACER`; production tests need not. Launch a test as GDB's child instead. No root or global ptrace relaxation is required for the verified ancestor-owned workflow.

Use the accompanying `capture.gdb`, which stops the child after a configurable diagnostic interval and generates a core. It then kills only the test it launched. It has an outer command bound; if the target exits first, it reports that and produces no core. The interval is a capture trigger, not a product/test latency requirement.

```sh
umask 077
HANSEI_CAPTURE_AFTER=2 HANSEI_CORE=/tmp/hansei-example.core \
  timeout 30 "$GDB" --readnever -q -nx -batch \
  -x .agents/skills/hansei-debugging/capture.gdb --args "$EXAMPLE_BIN"
readelf -h /tmp/hansei-example.core
```

For an actual Rust test, resolve the executable using `cargo nextest list --lib -p daybook_core --message-format json`; read `rust-suites.daybook_core.binary-path`. Keep the repository working directory, then pass the complete test name and `--exact --nocapture` after the executable. Do not guess paths such as `debug/deps`: newer Cargo layouts differ. Allow build commands sufficient outer runtime and serialize them; a command killed while compiling is not test evidence.

### Kernel capture for a large native/Wasmtime process

When direct GDB generation is too expensive, use `HANSEI_CAPTURE_MODE=kernel` with the same helper. It prints `HANSEI_CORE_PID`, detaches, then sends SIGABRT to its own test child. Detachment allows execution to advance briefly before the abort; do not call this an exact snapshot of the preceding stop. This path terminates the target and uses the host's existing coredump policy, including systemd's storage and retention. Do not change global core or ptrace settings.

```sh
HANSEI_CAPTURE_MODE=kernel HANSEI_CAPTURE_AFTER=35 \
  timeout 120 "$GDB" --readnever -q -nx -batch \
  -x .agents/skills/hansei-debugging/capture.gdb --args "$BIN" "$TEST" --exact --nocapture
# Set PID to the helper's printed HANSEI_CORE_PID, not a remembered ID.
coredumpctl --no-pager info "$PID"
coredumpctl --no-pager dump "$PID" --output="$CORE"
readelf -h "$CORE"
```

Require a present, non-truncated core. The GDB exit code alone does not establish capture. Sending SIGABRT while still attached produced a GDB register-teardown error; the verified helper detaches first. The kernel path was exercised on both the native target and the supported example.

### Split DWARF

Townframe uses `split-debuginfo="unpacked"`. Hansei does not read the original binary's skeletons as complete DWARF; package the exact build's `.dwo` files without rebuilding:

```sh
llvm-dwp -e "$BIN" -o "$DWP"
"$HANSEI" --core "$CORE" --binary "$BIN" --debug-info "$DWP" --exec census
```

GNU `dwp -e "$BIN" -o "$DWP"` also completed. Use a bounded memory scope for packaging and extraction. Packaging was verified here; successful native extraction was not. Keep the binary, package, and original `.dwo` files together until analysis is finished.

The example's core had warnings about missing worker-thread stacks/TLS even though async tasks and waits decoded correctly. This limits native stack and runtime-thread attribution. Report warnings; do not claim complete core coverage.

## Inspect and reuse extracted layouts

```sh
"$HANSEI" --core "$CORE" --binary "$BIN" --debug-info "$BIN" \
  --exec 'census; tasks; tasks --with type dispatcher --exec task'
"$HANSEI" tokio-info extract "$BIN" --output "$TINFO"
"$HANSEI" --core "$CORE" --binary "$BIN" --tokio-info "$TINFO" \
  --exec 'tasks --with type dispatcher --exec trace'
```

These commands were exercised. `tasks --with type NAME` accepts a partial future type name. Discover task IDs from that snapshot; never reuse IDs from another run. `task ID`, `trace ID`, and `locals` inspect the selected task and frame. Avoid printing arbitrary locals from a real application; select only the fields needed for the diagnosis. A `.tinfo` must remain paired with the exact target build.

For scheduling hangs, locate the scheduling owner and test future. Distinguish a test awaiting a submit reply, an actor awaiting storage/authority/RPC, input classification waiting for materialization, and a listener waiting for notification. Identify the source await, wait primitive, and relevant identity/generation. Task presence or an idle task alone does not prove a deadlock.

## Report

State the Hansei revision, target compiler, exact capture and inspection commands, capture completeness warnings, blocked source await, and what is still unknown. Separate successful tool validation from a diagnosed application bug. Remove transient captures and launch scripts when no longer needed; retain the original binary/DWARF while diagnosis needs them. Never delete unrelated artifacts or credentials.
