# Dayframe UI guidance

For any UI implementation or review under `src/dayframe_dx`, read `.agents/skills/dayframe-design-language/SKILL.md` before changing UI. It defines the canonical token source, Tailwind vocabulary, foundation-versus-feature boundary, and the process for surfacing missing design-language expressivity.

For native build, screenshot, accessibility, or renderer verification, also read `.agents/skills/dayframe-native-dev-loop/SKILL.md`.

---
name: dayframe-native-dev-loop
description: Run, screenshot, and semantically inspect the dayframe_dx Dioxus/Blitz native desktop app. Use when building or visually verifying src/dayframe_dx, when the wrapped Dioxus serve process misbehaves, when taking app screenshots under niri, or when driving the native app with accessibility-cli over AT-SPI.
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
- `accessibility-cli` is not published. Build it from source and use the resulting binary; on this
  machine the configured target is `/run/media/asdf/p3N/tmp/townframe-target/debug/accessibility-cli`.

## The one command

```sh
deno run --allow-all x/dx.ts serve --desktop -p desktop --renderer native --hot-patch false
```

- **Native renderer always.** `--renderer native` is the default expectation for this work; wry exists only as a production fallback.
- **Webview fallback for interaction checks.** When native/Blitz cannot dispatch accessibility actions, stop the native serve process and run the same command with `--renderer webview`. The webview build is slower on a cold target, but its AT-SPI tree exposes element titles and `accessibility-cli --click '[title="..."]'` can verify real callbacks. The native `with_window_attributes(...with_title(...))` configuration does not control the webview window title; expect `dioxus | ⛺` there.
- **Do not pass `-p ui`.** `ui` is a lib crate, so the Dioxus CLI cannot target it. Target the binary crate `desktop`.
- **Let the serve process own compilation.** Do not run `cargo build` alongside it. The wrapper forwards the arguments to the repository-local Dioxus CLI, and installs that CLI through `x/install-dx.ts` when needed.
- Use the same wrapper for other Dioxus commands, for example `deno run --allow-all x/dx.ts check ...`.

### Use the repository wrapper

Do not invoke a bare `dx` from the shell. On this machine it can resolve to deno's `dx` shim rather than the Dioxus CLI. Always use:

```sh
deno run --allow-all x/dx.ts <command> [args...]
```

The wrapper selects the repository-local CLI under `target/dioxus-cli`, installs it through `x/install-dx.ts` when absent, and forwards the command unchanged.

## Run it in tmux, not in the background

The operator's convention is tmux session **`townframe`**, window **4**:

```sh
tmux send-keys -t townframe:4 'deno run --allow-all x/dx.ts serve --desktop -p desktop --renderer native --hot-patch false' Enter
```

Do not launch detached background processes. The tmux pane keeps the server's own keybindings
reachable — in particular `r` triggers a rebuild when the watcher misses an edit, which is the
documented recovery path after a failed hot-reload.

Verify the pane is alive before blaming a build: if the shell is back at a prompt, the wrapped serve process has
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

The wrapped native binary's name is `desktop-<hash>`, so matching on the exact name is a trap
when resolving PIDs — prefer matching the window by `app_id`/title, or use the PID that the serve process reports.
Note that several windows can share one PID (every alacritty window does), so `--pid` is not a unique
key either: when the script reports multiple matches it prints the candidate ids and title and asks for
`--id` rather than guessing.

A newly written screenshot may not exist yet when the capture command returns (the write is
asynchronous). Check the file exists before reading it rather than concluding the capture failed.

## Semantic inspection and interaction (AT-SPI)

`accessibility-cli` drives the app over the OS accessibility tree — no browser, no pixels. The local helper resolves a niri window and finds the CLI binary:

```sh
.agents/skills/dayframe-native-dev-loop/scripts/a11y.sh desktop --llm-query
.agents/skills/dayframe-native-dev-loop/scripts/a11y.sh --pid "$PID" --click '[title="Refresh configuration"]'
.agents/skills/dayframe-native-dev-loop/scripts/a11y.sh --id "$NIRI_ID" --interactive
```

Use `ACCESSIBILITY_CLI=/path/to/accessibility-cli` when the binary is outside the usual locations. Direct CLI calls still work; actions and queries poll (`--timeout`, `--poll-interval`), so they can wait for UI that appears after
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
| `Build failed` in the wrapped Dioxus serve pane while you were running clippy/tests | Target-dir contention between the wrapped serve process and cargo. The build succeeds on retry. Serialise heavy cargo commands with the serve process. |
| A CSS-only edit has no effect | The stylesheet is baked into the binary as an asset, so the wrapped `serve` process does **not** rebuild on a CSS-only change. Touch a `.rs` file, or press `r` in the pane. |
| `deno run --allow-all x/dx.ts check` reports "No issues found" after a change | Stale cache; it does not reliably recheck. Trust `cargo check -p <crate>` for iteration. |
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

- Capture `a11y.sh --llm-query` to a file, then filter that file. Piping the CLI directly into `head` closes stdout early and makes its Rust printing path panic with `Broken pipe`; that is a CLI output failure, not an application crash.
- Semantic verification must target the actual lifecycle control, not a configuration switch with a similar label. Expand `Mock node and micro lifecycle controls` before targeting the demo lifecycle buttons. Selectors can include `[role-description="push button"]` to distinguish buttons from switches. The sample's configuration-flag toggle is now labelled separately from micro enablement.

- (2026-09) `blitz-shell 0.3.0-beta.2` enables accessibility but depends on `accesskit_xplat 0.1.1` with defaults disabled. Without an explicit Linux dependency enabling xplat defaults, its Unix adapter is absent and the app never registers on AT-SPI even with the service configured. Desktop now enables that backend. This is distinct from missing `XDG_DATA_DIRS`.
- The native window may have a CLI-generated title (`dioxus | ⛺`) and no app ID. Resolve it from `niri msg -j windows` rather than assuming `desktop` matches. Desktop now explicitly requests the title `Dayframe`.
- (2026-09) bare `dx` resolving to deno's shim; wrapped Dioxus checks being stale; CSS-only edits needing a Rust
  touch; tailwind watcher death; the wrapped serve process silently exiting after ~3.5h; misreading an overlapping
  glyph as a missing one.
