---
name: rr-debugging
description: Record and replay Rust tests with rr on Nix/Linux, including AMD Zen setup, direct test-binary invocation, and safe noninteractive GDB attachment.
---

# rr debugging for Rust tests

Use this skill only on Linux. First prove that rr can record and replay a tiny or focused test on the current host before adding project configuration or trusting a trace.

## Host prerequisites

`nixpkgs#rr` may lag the host's glibc syscall definitions. On AMD Zen, rr also requires the SpecLockMap workaround.

Check the host:

```bash
command -v rr || true
nix run nixpkgs#rr -- --version
uname -a
lscpu | rg 'Vendor ID|Model name'
```

For AMD Zen, a root operator must load MSR access and apply the rr workaround. The workaround is system-wide and normally must be repeated after reboot/suspend. If building rr from the source-tarball workflow above, acquire the exact script from that source tree:

```bash
cp /tmp/rr-master-src/source/scripts/zen_workaround.py /tmp/zen_workaround.py
sha256sum /tmp/zen_workaround.py
```

Apply and check it noninteractively (a password prompt is not safe in an agent command; ask the operator to run these if `sudo -n` fails):

```bash
sudo -n true || { echo 'root access required for the Zen workaround' >&2; exit 1; }
sudo modprobe msr
sudo python3 /tmp/zen_workaround.py
sudo python3 /tmp/zen_workaround.py --check
ls -l /dev/cpu/0/msr
```

The script reads and writes `/dev/cpu/*/msr`; running `--check` as an unprivileged user can report permission denied even after successful root setup. Verify the check as root. If `/dev/cpu/0/msr` is absent, `modprobe msr` must happen before running the script.

An unprivileged `--check` may fail merely because `/dev/cpu/*/msr` is root-only; that is not evidence that the workaround failed.

Do not use rr's `-F` force option as a substitute for the Zen workaround. Forced traces are not trustworthy for debugging.

## Build current rr when the Nix package is stale

If Nix rr aborts on a syscall such as `madvise(102)`, build current rr outside the repository. Do not use git mutation commands; a source tarball is sufficient:

```bash
rm -rf /tmp/rr-master-src /tmp/rr-master-build
mkdir -p /tmp/rr-master-src
curl -fsSL https://github.com/rr-debugger/rr/archive/refs/heads/master.tar.gz \
  | tar -xz -C /tmp/rr-master-src
mv /tmp/rr-master-src/rr-master /tmp/rr-master-src/source
capnp_store=$(nix eval --raw nixpkgs#capnproto.outPath)
PKG_CONFIG_LIBDIR="$capnp_store/lib/pkgconfig" \
  nix shell nixpkgs#cmake nixpkgs#capnproto nixpkgs#gdb nixpkgs#libpfm \
    nixpkgs#zlib nixpkgs#zstd nixpkgs#python3 nixpkgs#which nixpkgs#procps nixpkgs#gcc \
    --command bash -lc '
      cmake -S /tmp/rr-master-src/source -B /tmp/rr-master-build \
        -Ddisable32bit=TRUE -DBUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=RelWithDebInfo
      cmake --build /tmp/rr-master-build -j8
    '
```

Use `/tmp/rr-master-build/bin/rr`; it needs its adjacent `/tmp/rr-master-build/lib/rr` resources.

## Get a Rust test executable

Build without running tests and capture the executable path from Cargo's JSON output:

```bash
cargo test -p daybook_core --no-run --message-format=json \
  > /tmp/daybook-test-build.json 2>/tmp/daybook-test-build.err
python3 - <<'PY'
import json
from pathlib import Path
for line in Path('/tmp/daybook-test-build.json').read_text(errors='replace').splitlines():
    try:
        item = json.loads(line)
    except json.JSONDecodeError:
        continue
    if item.get('reason') == 'compiler-artifact' and item.get('executable'):
        print(item['executable'])
PY
```

Confirm the test name before recording:

```bash
TEST_BIN=/path/to/test-binary
"$TEST_BIN" --list | rg 'target_test_name'
```

## Record and replay

Record one test, not the whole suite:

```bash
RR=/tmp/rr-master-build/bin/rr
TRACE=/tmp/rr-focused-test
rm -rf "$TRACE"
"$RR" record -o "$TRACE" "$TEST_BIN" \
  module::tests::target_test --exact --nocapture \
  > /tmp/rr-record.log 2>&1
```

First validate deterministic replay without GDB:

```bash
"$RR" replay -a "$TRACE" > /tmp/rr-replay.log 2>&1
```

## GDB navigation without hanging the agent

Do not rely on `rr replay -x` for commands that require an attached target; those commands may run before rr has connected GDB. Start a debug server, parse its port, then attach GDB explicitly:

```bash
"$RR" replay -s 0 -k "$TRACE" > /tmp/rr-server.log 2>&1 &
rr_pid=$!
for _ in $(seq 1 100); do
  port=$(sed -n 's/.*127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p' \
    /tmp/rr-server.log | tail -1)
  test -n "$port" && break
  kill -0 "$rr_pid" 2>/dev/null || break
  sleep .1
done
```

The server must be started only after the Zen workaround is active; do not use `-F` to bypass a failed host check. Attach with a bounded GDB command file:

```gdb
set pagination off
set confirm off
target extended-remote 127.0.0.1:PORT
break fully::qualified::function
continue
bt 8
reverse-continue
bt 8
detach
quit
```

Run GDB with the same debug-symbol test binary and the GDB package used by rr:

```bash
PATH=/nix/store/...-gdb-.../bin:$PATH \
  gdb -q -x /tmp/rr-gdb.gdb "$TRACE"/mmap_hardlink_*
```

For agent-safe noninteractive use, make the GDB script after the port is known, run GDB in batch mode, and always clean up the replay server. Do not pipe commands into an interactive GDB process:

```bash
sed "s/PORT/$port/" /tmp/rr-gdb.template > /tmp/rr-gdb.gdb
set +e
trap 'kill "$rr_pid" 2>/dev/null; wait "$rr_pid" 2>/dev/null' EXIT INT TERM
(timeout 120s env PATH=/nix/store/...-gdb-.../bin:$PATH \
  gdb -q -batch -x /tmp/rr-gdb.gdb "$TRACE"/mmap_hardlink_*) \
  > /tmp/rr-gdb.log 2>&1
status=$?
trap - EXIT INT TERM
printf 'gdb_exit=%s\\n' "$status"
```

The replay server command is `rr replay -s 0 -k "$TRACE"`; `-k` takes no argument, and the trace directory is positional. If GDB or the test crashes, the trap must still terminate the server. Use `timeout` around both record and replay commands so a failed attach cannot leave an agent command or rr process waiting indefinitely.

Use reverse execution to find who last changed a state field, for example with a hardware watchpoint after stopping at the failing assertion:

```gdb
watch -l state.last_enabled_version
reverse-continue
```

## Safety and reporting

- Record only focused tests; traces can be very large.
- Redirect test/replay output to files and inspect bounded tails/slices.
- Do not trust traces recorded with `-F` on an unconfigured Zen host.
- A passing replay proves rr determinism, not that the source behavior is correct.
- Report the rr version, host workaround state, exact record/replay commands, trace result, and any source fix separately.
- Do not add rr to a flake or claim the workflow is supported until both recording and replay have succeeded without force overrides.
