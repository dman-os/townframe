---
name: dayframe-native-dev-loop
description: Run, screenshot, and semantically inspect the dayframe_dx Dioxus/Blitz native desktop app. Use when building or visually verifying src/dayframe_dx, when dx serve misbehaves, when taking app screenshots under niri, or when driving the native app with accessibility-cli over AT-SPI.
---

# Dayframe native dev loop

Use this for any build/run/screenshot/inspect cycle on `src/dayframe_dx` (the Dioxus 0.8-alpha
native app). Read the repository `AGENTS.md` first.

This skill exists because the loop is not obvious and every step below has cost a wasted cycle at
least once. Read the **False signals** section before concluding a build failed.

> **Update this skill when you use it.** If you re-derived a step by hand, or a step here was wrong
> or unnecessary, edit this file in the same turn. Keep a *Recently bitten by* list at the end.

## Prerequisites

- The dev shell must provide AT-SPI (`at-spi2-core`) and put its service directory on
  `XDG_DATA_DIRS`, or the native app will run but never register on the accessibility bus. The
  `feat/dayframe` flake does this; a bare `nix develop` on another branch may not.
- `accessibility-cli` is not published. It must be built from source (last known:
  `../accessibility-cli/target/release/accessibility-cli`).

## The one command

```sh
dx serve --desktop -p desktop --renderer native --hot-patch false
```

- **Native renderer always.** `--renderer native` is the default expectation for this work; wry
  exists only as a production fallback.
- **Do not pass `-p ui`.** `ui` is a lib crate, so `dx` cannot target it ("Failed to find target
  ui"). Target the binary crate `desktop`.
- Let **`dx serve` own compilation**. Do not run `cargo build` alongside it. `dx` reloads on file
  change; `cargo clippy -p <crate> --all-targets` is fine for diagnostics but see the contention
  trap below.

### `dx` is a footgun on the bare PATH

On this machine `dx` resolves to **deno's `deno x`** (`/nix/store/.../deno-2.9.4/bin/dx`), which is
`npx`-alike and will happily error with argument-parsing nonsense instead of building anything. The
real dioxus CLI is `~/.cargo/bin/dx`. Check with `dx --help | head -1` if anything looks strange, and
prefer the explicit path when scripting.

## Run it in tmux, not in the background

The operator's convention is tmux session **`townframe`**, window **4**:

```sh
tmux send-keys -t townframe:4 'dx serve --desktop -p desktop --renderer native --hot-patch false' Enter
```

Do not launch detached background processes. The tmux pane keeps the server's own keybindings
reachable — in particular `r` triggers a rebuild when the watcher misses an edit, which is the
documented recovery path after a failed hot-reload.

Verify the pane is alive before blaming a build: if the shell is back at a prompt, `dx serve` has
exited (it does this on its own after a few hours), and any keys you send go into the shell as literal
text. That has produced confusing "my edit didn't land" reports.

## Screenshots

Under niri, take **window-specific** captures rather than full-output `grim`:

```sh
.agents/skills/dayframe-native-dev-loop/scripts/window-shot.sh /tmp/dayframe.png desktop
```

The script resolves a window id from `niri msg -j windows` by `app_id`/`title` match (or `--pid`) and
then calls `niri msg action screenshot-window --id <id> --path <abs path>`. `--path` must be absolute.
`grim /tmp/full.png` remains the fallback for full-output captures when no window is resolvable.

The `dx`-launched native binary's name is `desktop-<hash>`, so matching on the exact name is a trap
when resolving PIDs — prefer matching the window by `app_id`/title, or use the PID that `dx` reports.
Note that several windows can share one PID (every alacritty window does), so `--pid` is not a unique
key either: when the script reports multiple matches it prints the candidate ids and title and asks for
`--id` rather than guessing.

A newly written screenshot may not exist yet when the capture command returns (the write is
asynchronous). Check the file exists before reading it rather than concluding the capture failed.

## Semantic inspection and interaction (AT-SPI)

`accessibility-cli` drives the app over the OS accessibility tree — no browser, no pixels:

```sh
A=../accessibility-cli/target/release/accessibility-cli
$A --platform linux --list-windows                      # find the app PID
$A --platform linux --pid "$PID" --llm-query            # compact tree + CSS-like selectors
$A --platform linux --pid "$PID" --click '[title="Daybook ▸"]'
$A --platform linux --pid "$PID" --listen               # watch a11y events
```

Actions and queries poll (`--timeout`, `--poll-interval`), so they can wait for UI that appears after
a state change. `--screenshot` / `--mouse-click x,y` exist but are position-dependent; prefer
semantic selectors.

This same tree is what makes the tiles worth giving real semantic roles. An app whose tiles are
`div`s dumps a useless tree; buttons with labels are both better a11y and a better agent interface.

## Hard limit: you cannot inject pointer input

On this Wayland setup there is **no way to synthesise a press-move-release**:

- `wtype` is keyboard-only.
- `xdotool` cannot reach a Wayland client.
- `ydotool` is not installed.
- `accessibility-cli` performs clicks, not drags.
- AT-SPI registration has also failed outright in some sessions, taking even clicks off the table.

So drag/swipe feel, hover behaviour and animation cannot be self-verified. Do not burn rebuild cycles
guessing at them. The protocol is:

1. Verify what *can* be verified without input: layout, computed geometry, state, the accessibility
   tree, screenshots of the resting state, and unit tests on the pure geometry/state functions.
2. Hand the operator **one named, bounded check** — "press `L`, the strip must not move until you
   drag" — rather than an open-ended "does it feel right".
3. Add an env-var seed (e.g. `DAYFRAME_OPEN="0:0,2;1:2"`) to render the state that needs a click to
   reach, so a screenshot can stand in for the interaction.

## False signals

These have each caused a wrong conclusion. Check them before reporting a build failure.

| Symptom | Real cause |
|---|---|
| `Build failed` in the `dx` pane while you were running clippy/tests | Target-dir contention between `dx serve` and cargo. The build succeeds on retry. Serialise heavy cargo commands with the serve process. |
| A CSS-only edit has no effect | The stylesheet is baked into the binary as an asset, so `dx serve` does **not** rebuild on a CSS-only change. Touch a `.rs` file, or press `r` in the pane. |
| `dx check` reports "No issues found" after a change | Stale cache; it does not reliably recheck. Trust `cargo check -p <crate>` for iteration. |
| Tailwind classes silently do nothing | The Tailwind watcher has died. Restart it; this can make an entire CSS change inert. |
| The app process "disappeared" | It can exit when a stylesheet hot-reloads. Re-check the pane before assuming a crash. |
| A glyph appears missing / a label misreads | Screenshot legibility, not layout. Verify contiguity with a test or an annotated capture before chasing it — a real session lost four rebuild cycles to a letter that was present but overlapping other text. |

## Context discipline

The operator has interrupted this loop to ask for less noise. When inspecting a running app:

- Never dump process/environment listings wholesale. `pgrep -f` also matches the wrapper shell's own
  command line, so it reports the wrong PID — filter by executable, not by full command line.
- Bound every log/grep: head/tail, one pattern, one file.
- Prefer `niri msg -j windows` filtered by a specific field over reading whole window lists.

## Related

- `.agents/skills/blitz-dioxus-native-limits` — the upstream Blitz/dioxus-native limits that shape
  what the app can do at all.
- `src/dayframe_dx/AGENTS.md` — Dioxus 0.8-alpha API notes (0.7 docs differ; `cx`/`Scope`/`use_state`
  are gone).
- `docs/fdrs/005-tiles-primitive.md`, `docs/adrs/010-dayframe-dx-architecture.md`.

## Recently bitten by

- (2026-09) `dx` resolving to deno's `dx`; `dx check` false-clean; CSS-only edits needing a Rust
  touch; tailwind watcher death; `dx serve` silently exiting after ~3.5h; misreading an overlapping
  glyph as a missing one.
