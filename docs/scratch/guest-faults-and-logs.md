# Guest faults and logs: located traces, a log file, a fault that says why

Status: Accepted 2026-11-24 (operator; reviewed by claytxt and nodules)
Owner: intxt lane (proposal); the build splits by crate owner, see "Who builds what"
Updated: 2026-11-24
Scope: The low-level layer only: tracing, log files, panics, wasm traps and their stack traces, and the fault record a host keeps for a guest. The user-facing layer (the progress repo's descendant, a service cartridges consume) is deferred; this doc only fixes what that service will read faults from.

## Why

On 2026-11-24 the review micro died on "Show changes". The panel said "Component withdrawn." and the log said nothing at any `RUST_LOG` level. The reason existed only as a status string under Settings → Micro lifecycle: the shell had refused a presentation batch with a style property (`contain`) its vocabulary lacked. Finding it took more than an hour of clicking through the app over AT-SPI. claytxt-66 now names the refused property, and claytxt-67 logs `micro faulted` from the supervisor, which closes this one case. The general gaps remain:

1. **A trap has no source location.** Micros build with `debug = "line-tables-only"` (`[profile.wasm-release]`), but `clay_rt_wasmtime::engine()` never turns on `wasm_backtrace_details`, so a trap's frames are function indices and offsets, not `file:line`. The cartridge host (`big_nodule_wasm`, via wash-runtime) uses a default `Engine::builder()` with the same gap.
2. **A guest's panic message is unattributed.** A Rust guest prints its panic message to stderr and then traps. `clay_rt_wasmtime` calls `inherit_stderr()`, so the message lands in the process's stderr, interleaved with every other guest and the host, with nothing naming the micro, and it is not attached to the fault.
3. **A fault is a string in memory.** `clay_host` keeps one `String` per instance (`record_fault`), the supervisor turns it into `MicroState::Faulted(String)`, and nothing saves it. The panel that loses its micro shows "Component withdrawn." without the reason.
4. **There is no log file.** Every binary logs to stderr. `tracing-appender` is a workspace dependency that nothing uses. The desktop does not even go through `utils_rs::tracing`: under `dx serve` its output is dx's, and a release build has no subscriber of ours at all. A run that is not watched leaves nothing to read afterwards.

## What "done" looks like

- A micro or cartridge that panics or traps leaves a fault naming the guest, the kind of failure, the panic message, and the guest frames by function name, resolved to `file:line` wherever the build's debug side file is available. Shipped components carry no DWARF.
- The same fault is logged (at `error`, or `warn` for an expected refusal; section 5) with the guest's id, and it is on disk afterwards in a log file.
- The panel that lost its micro shows the fault's one-line reason, not "Component withdrawn."
- A test proves it: a fixture micro that panics on a known line yields a fault carrying that line and the panic text.

## The proposal, by the numbers

Nothing here is new technique; it is the standard setup, applied once and everywhere.

### 1. One subscriber per binary, with a rolling JSON-lines file

`utils_rs::tracing` gains a file layer, and every binary (`day`, `daytxt_desktop`, `daytxt_mobile`, the servers) calls it at start. The desktop and mobile shells stop relying on dx's logger.

- **Stderr**: unchanged (compact, human, ANSI only on a terminal).
- **File**: `tracing-appender`'s rolling writer, daily rotation, JSON lines (one event per line with level, target, span fields and timestamp), under `<app data dir>/logs/<binary>.<date>.jsonl`. Its non-blocking worker guard lives in `main` for the life of the process, so a crash flushes what was logged.
- **Retention**: keep 7 files per binary and delete older ones at start. 7 is a placeholder until we measure a day's volume; the INSTR probes stay off by default (`instr=off`), so the file carries the same events stderr does.
- **Filter**: the file takes the same filter as stderr (`RUST_LOG`, else the binary's default), so it holds what a watched run would have shown. `DAY_LOG_FILE=off` turns the file off for tests and CI. Which binaries keep a file: the serving node (`day node start`, `<node dir>/logs/day.*.jsonl`) and the desktop and Android apps (`<data dir>/logs/daytxt.*.jsonl`). Short `day` commands and `xtask` keep stderr only.
- **Android** keeps `tracing-android` (logcat) and adds the file under the app's files dir. **Web** is out of scope here.

### 2. Panics are events

The panic hook in `utils_rs::tracing` already builds an eyre report and brings the process down. It also emits the report as an `error` event before exiting, so the panic is in the file and not only on stdout.

### 3. Wasm traps carry symbolized frames, from debug info the guest does not ship

**What ships today** (measured 2026-11-24 on the desktop worktree's packages, `wasm-tools objdump` and `wasm-tools strip -a`):

| Package | Profile | Size | Without custom sections | DWARF |
|---|---|---|---|---|
| `review.component.wasm` (micro) | `wasm-release` | 69.6 MB | 3.9 MB | ~65 MB (`.debug_info` 25.1, `.debug_str` 20.1, `.debug_line` 12.8, `.debug_ranges` 6.8) |
| `counter.component.wasm` (micro) | `wasm-release` | 21.8 MB | 1.3 MB | ~20 MB |
| `intxt.guest.wasm` (cartridge) | `release` + `strip = "symbols"` | 1.28 MB | 1.27 MB | none, and no `name` section either |

Micros are release-optimized (`opt-level = 3`, thin LTO, one codegen unit), but `debug = "line-tables-only"` still links every crate's line tables, including std's, so 94% of a micro is DWARF. The APK bundles these packages, and the host reads the whole file to compile it. The cartridge was stripped because the blob service caps a put at 8 MiB (`src/wasm_utils_rs/Cargo.toml`), so its traps have no function names at all. Linear memory is unaffected either way: debug sections are never instantiated.

**The standard fix: split debug info.**

- **Ship stripped, keep names.** Every guest, micro and cartridge, ships with DWARF removed and the `name` section kept: a post-link `wasm-tools strip` (its default keeps `name` and `component-type`) in every path that packages a guest: `x/daytxt-packages.ts` for the dev loop, `src/nix/packages.nix` for `nix build .#daytxt-packages`, the APK and the CI "Nix package" job, and the nodule build for cartridges. Otherwise the dev loop ships 4 MB guests while the APK ships 70 MB ones. Function names cost little (0.8 MB for review, 0.3 MB for counter) and make a raw trap readable on their own. Shipped review: ~4.7 MB instead of 69.6 MB.
- **Cartridges, for now**: `intxt_nodule_guest` builds with `strip = "debuginfo"`, which keeps its `name` section (1.50 MB, under the 8 MiB blob cap). Its side file waits until every consumer (the `day_cli` and `daytxt_native` e2e tests, CI) reads a packaged guest instead of the raw cargo output.
- **Debug info is a side file.** The side file is the unstripped component as the linker wrote it. Stripping removes custom sections only, so it has the same core modules in the same order with the same code, plus their DWARF. It never goes to the phone or into the blob service.
- **One identity.** The side file is keyed by the digest that already identifies the shipped package or blob (the blob id where a guest is a blob), taken over the stripped component. Strip first, then hash: the stripped bytes are the shipped identity. No new hash.
- **Frames are per core module.** A component holds several core modules (the main one plus wasm-component-ld's adapter and shim modules). Wasmtime's frame offsets are relative to one core module's code, and the DWARF belongs to that module. So a frame is (component digest, core module index, function index, code offset). The side file resolves the module index to that module's DWARF, and `addr2line` resolves the offset.
- **Symbolize where the side file is**: the desktop's dev loop, `day`, and the agent tooling. On the phone the fault carries names and offsets, and resolving it is a lookup on a machine that has the build.
- **Engine**: `Config::wasm_backtrace(true)` (wasmtime's default) is all the host needs for raw frames. `wasm_backtrace_details` stays off, since the shipped component has no DWARF for it to read; it is only for a dev run that loads an unstripped component on purpose.
- **Code size is next, not now.** The 3.4 MB of code in review is `opt-level = 3`. `opt-level = "s"` and a `wasm-opt -Oz` pass are the usual next levers. Measure them separately; they trade speed for size, and they are not about traces.
- **Structured, not flattened**: `trap_reason` stops joining the error chain into one string. The host downcasts the error to `wasmtime::WasmBacktrace` and keeps the frames as data in the fault (section 5).

### 4. Guest stdio is captured per instance

`inherit_stderr()` is replaced by a per-instance pipe (stdout and stderr) on the host side:

- each line is forwarded as a `tracing` event at target `guest`, with `micro = <id>` (or `cartridge = <id>`) and `stream = stderr|stdout`;
- the last 16 KiB are kept in a ring buffer per instance and attached to the fault when the instance traps. That is how the panic message, which the guest prints just before its `unreachable` trap, travels with the fault.
- **Bounded, because the guest controls it.** The ring bounds memory; the forwarded events must be bounded too, or a guest looping on `eprintln!` fills the log file and the disk. A line is cut at 4 KiB, and each instance is rate-limited, with one "N lines dropped" event when the limit lifts.
- **It is data.** The stream is guest-written: the panel renders it as text, never as markup.

For cartridges this needs wash-runtime to accept our stdio for a workload, and at our pin it does not: `build_ctx_from_template` (`engine/linked_call.rs`) hard-codes `inherit_stdout().inherit_stderr()` for every workload. nodules patches the fork with a per-workload stdio provider keyed by workload id, and bumps the pin separately.

### 5. A fault is a record

`clay_host` keeps a `Fault` per closed instance instead of a `String`. `clay_host` is the engine-neutral foundation and also builds for wasm32 (the browser host runs micros through wcmp), so the record is plain data: no wasmtime type crosses into it, and `clay_rt_wasmtime` does the downcast to `WasmBacktrace`. In a browser, wcmp gives JS stack frames or none, so frames and stderr are optional.

| Field | Content |
|---|---|
| guest | micro or cartridge id, and the incarnation (required) |
| at | timestamp (required) |
| kind | `Trap`, `OutOfFuel`, `Refused` (the host turned down a batch or a call), `InitDeclined`, `Host` (the host failed the guest) (required) |
| message | one line: the trap cause, or the refusal's reason (required) |
| frames | optional; per frame: component digest, core module index, function index, function name, code offset (section 3) |
| stderr | optional; the stdio tail from section 4 |

**Levels**: `Trap`, `OutOfFuel` and `Host` are logged at `error`. `Refused` and `InitDeclined` are `warn`: they are expected outcomes of the guest contract, and logging them at `error` would drown the real ones.

- `MicroState::Faulted` carries it.
- The supervisor's `micro faulted` warning (claytxt-67) becomes an `error` event with these fields, so it lands in the file.
- `clay_dioxus` shows `message` where it now says "Component withdrawn.", with the rest one click away.
- The progress service, when it exists, publishes faults from here; this doc does not design that.

### 6. Tests

- A fixture micro that panics on a known line, run under `clay_rt_wasmtime`: the fault's kind is `Trap`, a frame names the fixture's file and the panic line, and `stderr` holds the panic message.
- The same for a fixture cartridge under `big_nodule_wasm`.
- A micro_review e2e that mounts "Show changes" with a real patch through the shell's vocabulary, so a refusal like 2026-11-24's fails a test, not a person (claytxt's ask; intxt owns `micro_review`).

## After this, toward a debugger

Each step needs the one before it. None is proposed for building now.

1. **Located traces**: this doc.
2. **Coredumps on trap**: `Config::coredump_on_trap(true)`, written beside the logs, so a fault can be inspected after the fact with the wasm coredump tools.
3. **Native debugger attach**: `Config::debug_info(true)` registers JIT code with gdb/lldb, so a desktop session can break inside a guest.
4. **Something of our own**: an in-app view of a guest's state at a fault, fed by 2 and 3. That is where the innovation the operator asked for lives, and it is worth designing once 1–3 have shown what is missing.

## Who builds what

- **Shared (`utils_rs::tracing`), sections 1–2**: whoever picks it up; it touches every binary's `main`. I propose intxt, since it is not in either lane's way.
- **claytxt (`clay_host`, `clay_rt_wasmtime`, `clay_dioxus`), sections 3–5 for micros**: claytxt asked to review before anything is built in `clay_host`.
- **nodules (`big_nodule_wasm`, the wash-runtime fork), sections 3–4 for cartridges.**
- **intxt, section 6**: the `micro_review` e2e, and the fixture micro if claytxt prefers.

## Settled 2026-11-24

*Built 2026-11-24 (intxt, local, not yet pushed): the strip and split as stack/intxt-40-guest-strip dc057a97 (claytxt approved it), and sections 1–2 as stack/intxt-41-log-file eea77792. Measured shipped sizes: review 4.68 MB (was 69.6), lab 3.28 (33.1), theme-lab 1.83 (22.4), layout 1.71 (22.2), counter 1.57 (21.8), the intxt cartridge 1.50 (with names). Still to build on intxt's side: the `micro_review` "Show changes" e2e.*

- **Retention**: 7 days per binary (claytxt agrees; revisit once a day's volume is measured).
- **Side files** live beside the build output for now. Publishing them, so a phone fault can be resolved later, is a follow-up.
- **Format**: JSON lines only; stderr covers `tail -f`.
- **Who builds**: claytxt builds sections 3–5 for micros plus the panicking fixture micro and its `clay_rt_wasmtime` test. nodules builds the cartridge side of 3–4 (executor fault and log, the wash-runtime stdio patch). intxt builds the strip and split in `x/daytxt-packages.ts`, `src/nix/packages.nix` and the nodule build, sections 1–2, and the `micro_review` "Show changes" e2e.

## Deferred: the progress repo's descendant

Recorded here so the shape is not lost; designed separately, as a service cartridges consume.

The old repo (`src/daybook_core/progress.rs`) in nouns and verbs:

- **Nouns**: task (id, title, tags, state, retention, viewed/dismissed times, latest update); update (status with severity, amount done of a total in a unit, completion with a final state); severity (info, warn, error); state (active, succeeded, failed, cancelled, dismissed); retention (user-dismissable, auto-dismiss after N seconds, dismiss after viewed, ephemeral); tag (a path, such as `/type/dispatch`, `/dispatch/<id>`, `/docs/<doc>`); event (task upserted, task removed, update added, list changed).
- **Verbs**: upsert a task, add an update, mark viewed, dismiss, set a retention override, clear completed, get, list, list by tag prefix, list updates.
- **Known cost**: every update allocates owned strings (task ids, tags, messages) end to end; the operator flagged this for the redesign.
