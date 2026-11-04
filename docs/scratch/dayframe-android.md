# Dayframe on Android: what builds, what Blitz allows

Status: Findings. The build results are measured; the platform constraints are read from source and every runtime claim is marked as needing a device.
Owner: Claude (S3 subagent); operator reviews.
Scope: `dayframe_mobile`, `dayframe_native`, `x/dayframe.ts android`, FDR 006 / ADR 032 §9 (what Dioxus on Android allows).
Updated: 2026-11-05

No emulator binary, system image or device existed in the session that wrote this, so
**nothing here has run on Android.** Everything below is a build result or a source reading.

## What was done

| Goal | Result |
|---|---|
| 1. `dayframe_mobile` builds for `aarch64-linux-android` with dioxus-native (Blitz) into an installable APK | Done. `x/dayframe.ts android` produced `app-debug.apk`. |
| 2. Same Clay composition as desktop, Wasm micros on Wasmtime from APK assets, SQLite under the app data dir | Built and linked; not run. Wasmtime, SQLite and the native micros are all in `libmain.so`. |
| 3. Guardrails `[builds]` for android | Not added: `check-matrix` supports only `native`, `wasm32-wasip3`, `wasm32-unknown-unknown` (`src/xtask/guardrails/matrix.rs:13`). See "CI". |
| 4. This document | Here. |
| 5. `x/dayframe.ts android` | Done (`--release`, `--install`). |

### Code layout

- New crate `src/dayframe_native`: the native-engine half of the composition root that desktop and mobile
  share. It owns the linked built-in micros (settings, toolbar, sample), the Wasmtime package factory
  (`Package::from_json` + `wasm_factory`), `open_store(path)` over `clay_store::SqliteStore`, the counter
  demo seed, and the `Shell` launch screen (loading, failure, mount `Application`). A shell supplies only
  where the store lives and where package bytes come from.
- `dayframe_desktop` keeps what is desktop-specific: `XDG_DATA_HOME` / `DAYFRAME_STORE_PATH` /
  `DAYFRAME_PACKAGE_DIR` and the directory scan. Its public `composition::desktop_runtime(store)` is
  unchanged, so its in-crate tests and e2e did not move.
- `dayframe_mobile/main.rs` (all Android-gated; a host build is a stub that panics):
  - `android_main` stores `AndroidApp::internal_data_path()` (app-private, no permission) and the store is
    `<that>/store.sqlite`.
  - Packages are `counter` and `layout`, read through `dioxus::asset_resolver::read_asset_bytes` from a folder
    asset (`asset!("/packages")`). The resolver reads the APK's `assets/` through `AAssetManager`
    (`packages/asset-resolver/src/native.rs`, `to_java_load_asset`). Android has no directory listing
    through it, so the two names are listed in code.
  - `dayframe_native` is an Android-target dependency only, so a host `cargo clippy -p dayframe_mobile`
    does not compile Wasmtime.
- The packages are copied into `src/dayframe_mobile/packages/` by `x/dayframe.ts android` before the build
  (gitignored). `dx` bundles only what `asset!` names; it has no "resources directory" for Android (its
  `public_dir` copy is web-only, `packages/cli/src/build/request.rs:1450`).

## Commands

Build (first run installs the Dioxus CLI, builds the guests, downloads Gradle 9.1.0 from
`services.gradle.org` and the Android Gradle plugin; needs network once):

```sh
deno run --allow-all x/dayframe.ts android              # debug APK
deno run --allow-all x/dayframe.ts android --release    # release profile (unsigned without a signing config)
```

It prints `apk: <path>`; the debug APK lands at
`$CARGO_TARGET_DIR/dx/dayframe_mobile/debug/android/app/app/build/outputs/apk/debug/app-debug.apk`.

Install and run when a device is connected (USB debugging on, `adb devices` lists it):

```sh
deno run --allow-all x/dayframe.ts android --install
adb shell am start -n com.example.mobile/dev.dioxus.main.MainActivity   # confirm the package id with: adb shell pm list packages | grep -i mobile
adb logcat -s RustStdoutStderr:V DioxusNative:V AndroidRuntime:E        # Rust panics appear under RustStdoutStderr
```

The `BuildConfig` alias in `MainActivity.kt` names `com.example.mobile`, which is where the package id above
comes from; verify it on the device before relying on the `am start` line.

`dx serve --platform android --device ...` (the old README command) also works for a live loop but was not
exercised, because nothing could run the result.

## Measurements (debug profile, aarch64, single ABI)

Build time on a warm target dir: 10m34s wall (29m47s user CPU) for `x/dayframe.ts android`, after the guest
packages were prepared (13m15s wall on a cold dir). A half-finished earlier attempt had already compiled most
of the dependency graph, so a fully cold APK build will take longer.

| Artifact | Size |
|---|---|
| `app-debug.apk` | 201,648,679 B (192 MiB) |
| `lib/arm64-v8a/libmain.so` (debuginfo, unoptimized) | 833,062,408 B |
| same, `llvm-strip --strip-all` | 400,251,928 B (89 MB gzip -9) |
| `counter.component.wasm` / `layout.component.wasm` (debug guests) | 15,086,008 B / 16,086,496 B |
| `classes*.dex` | ~12 MB |

These are all debug-profile numbers. The release profile was not built (the shared disk was at 17 GB free
after the debug build). Expect a large drop in `libmain.so`; the guests are built `debug` by
`x/dayframe-packages.ts` regardless of `--release` and would need a release variant to shrink.

## Verification run

- `cargo clippy -p dayframe_native -p dayframe_desktop -p dayframe_mobile --all-targets`: clean (only the
  repo-wide manifest warnings).
- `cargo clippy -p dayframe_native -p dayframe_mobile --target aarch64-linux-android`: clean.
- `cargo x guardrails lint`: the only failures are four `ERROR_DOC_METADATA` diagnostics for
  `docs/scratch/dayframe-shell-ledger.md` (written by the other agent on `feat/dayframe-shell`, not touched here).
- `cargo x guardrails api --all --check`: ok. `cargo x guardrails doctor`: fails with `ERROR_BUILD_FLAGS`
  because the session's `CARGO_TARGET_DIR` is shared between worktrees (environment, not this change).
- Desktop e2e / nextest were not run (instruction: no full gate). The desktop change is a move of existing
  code behind `dayframe_native`; `desktop_runtime(store)` keeps its signature.

## CI

`check-matrix` cannot express Android, so it should not be asked to. A CI job should instead:

1. `cargo clippy -p dayframe_native -p dayframe_mobile --target aarch64-linux-android` (compile-only; the
   NDK is not needed for `clippy`, only for linking).
2. On a schedule or per release: `deno run --allow-all x/dayframe.ts android` and upload the APK.

Caveat: `asset!("/packages")` names `src/dayframe_mobile/packages/`. The clippy run above happened with that
directory present but empty, and it passed; whether clippy passes with the directory entirely absent was not
tested, so a CI clippy job should `mkdir -p src/dayframe_mobile/packages` first. An APK needs the real files
(`x/dayframe.ts android` stages them).

Making `check-matrix` aware of `aarch64-linux-android` means adding it to `TARGETS` and giving it a
policy like `wasm_policy`; that is a guardrails change and was left alone.

## What Blitz-on-Android supports and lacks for FDR 006

Pinned versions (from `Cargo.lock`): `dioxus 0.8.0-alpha.3` (dman-os fork `a3e1e68`), `blitz-shell` and
`blitz-dom 0.3.0-beta.4`, `winit 0.31.0-beta.3` with `winit-android 0.31.0-beta.3`, `android-activity 0.6.1`
(native-activity). Paths below are inside those crates' sources.

| Need (FDR 006) | Verdict from source | Evidence | Needs a device to confirm |
|---|---|---|---|
| Touch input at all | Supported. Finger down/move/up/cancel become winit `PointerSource::Touch` and Blitz keeps multi-touch state. Mouse tool types are skipped on Android. | `winit-android/src/event_loop.rs:323-445`; `blitz-shell/src/window.rs:82-90,700-830` | Latency, multi-touch fidelity |
| Resizable sections / drawers by touch drag | Possible, with one trap: a finger drag past a 2 px threshold enters `DragMode::Panning` (scroll) for any target unless the CSS `touch-action` forbids panning on both axes. A drag handle must set `touch-action: none` or it scrolls instead of resizing. | `blitz-dom/src/events/pointer.rs:219-260` (threshold + `Finger` branch), `:158-190` (`touch_action_pan_axes`) | Whether `pointermove` still reaches the handle during a pan; the "no pointer capture" workaround from the Blitz-limits skill (full-window overlay) still applies |
| Edge-swipe gestures (open sidebar, arc strip) | App side: just pointer events from the screen edge, same as above. System side: Android 10+ gesture navigation owns the left and right edges as "back". Apps opt a region out with `View.setSystemGestureExclusionRects` (Java), which nothing in winit-android / android-activity / Blitz calls. | No `setSystemGestureExclusionRects` anywhere in `winit-android`, `android-activity`, `blitz-shell`; android-activity exposes JNI handles so an app could call it itself | Whether the edge drawer drag wins against the system back gesture; 3-button navigation is unaffected |
| System back | Delivered. winit-android forwards `KEYCODE_BACK` as `NamedKey::BrowserBack` and returns `Handled`, and the fork ships `dioxus_native::use_back_button(handler)` for exactly this. | `winit-android/src/keycodes.rs:165,250`; `event_loop.rs:454-509`; dioxus fork `packages/native/src/hooks.rs:39` | Important: with `targetSdk = 36` (`Dioxus.toml`) Android's predictive-back rules may stop dispatching back as a key event unless the manifest sets `android:enableOnBackInvokedCallback="false"` (my recollection of the Android 16 docs; not verified here). The generated manifest template does not set it (`packages/cli/assets/android/gen/app/src/main/AndroidManifest.xml.hbs`). Test back first on a device; `Dioxus.toml` `[android.raw.manifest]` is the hook if it needs setting. |
| Insets / edge-to-edge | **Not handled.** `Window::safe_area()` returns zero insets on Android, and `InsetsChanged` and `ContentRectChanged` are `warn!("TODO")`. Blitz does size its viewport by `safe_area`, so the plumbing exists but is fed zeros. With `targetSdk = 36` the system draws edge-to-edge, so content would sit under the status bar, the navigation bar and a display cutout. | `winit-android/src/event_loop.rs:196-198,253-256,925-927`; `blitz-shell/src/window.rs:35-41,180-190` | Whether the content really is under the bars; the cheapest fix is a local patch to `winit-android` reading `WindowInsets` over JNI into `safe_area()`, or lowering `target_sdk` (a stopgap that only works until the store requires 35+) |
| Soft keyboard | **Show/hide works; text entry through the IME is the problem.** Blitz asks winit for IME on focus of a text input (`blitz-dom/src/node/node.rs:731,754` -> `blitz-shell/src/lib.rs:116`), and winit-android maps that to `show_soft_input`/`hide_soft_input` (`event_loop.rs:995-1015`). But winit-android only consumes `InputEvent::KeyEvent`; composition text arrives as `InputEvent::TextEvent`, which android-activity produces only in its `game_activity` backend (`android-activity/src/game_activity/mod.rs:1089`), not the `native-activity` backend this crate uses. Expect hardware-style key events only: no composing text, no predictive text, problems with non-Latin input. `set_ime_cursor_area` is an empty function. | as cited | Typing in a text field; this is the most likely blocker for the editor/chat use cases. The remedy is switching to the GameActivity backend (`android-game-activity` winit feature plus its Java glue), a larger change than anything else on this list |
| Accessibility (TalkBack) | **Not compiled in.** `accesskit_xplat` selects its Android adapter only with its `accesskit_android` feature (`accesskit_xplat/src/platform_impl/mod.rs:30`), which `blitz-shell` does not enable; `accesskit_android` is absent from the resolved graph (`cargo tree --target aarch64-linux-android -i accesskit_android`: no match). The null adapter is used, so TalkBack sees one opaque surface. Adding `accesskit_xplat = { version = "0.2.1", default-features = false, features = ["accesskit_android"] }` for the Android target needs `accesskit_android 0.9.x`; crates.io has `0.9.0` and `0.9.1`, but this session's cached index only listed `0.7.x` and `cargo` would not refresh it (stale index; not re-tried with a network refresh). The Linux AT-SPI defects in the Blitz-limits skill (`has_changes` inverted, `ActionRequested` TODO) live in shared `blitz-shell` code and so apply on Android too. | as cited | All of it. Also whether `accesskit_android`'s `embedded-dex` feature works with the `NativeActivity` subclass `MainActivity.kt` |
| DPI / scale | Reported from `AConfiguration`. `ConfigChanged` computes the old and new scale from the same call (`event_loop.rs:210-212`), so a runtime density change (display size setting) will not be reported. | cited | Rare; font-scale and density change handling |
| Lifecycle / data | Pause and resume reach winit as `suspended`/`resumed` (`event_loop.rs:190-193`). `MainEvent::Destroy` is a `warn!("TODO")`, so there is no shutdown hook; `Application` closes the host in `use_drop`, which runs only if the Dioxus runtime is torn down. SQLite writes are durable per transaction so a kill is safe, and a micro that needs a graceful close cannot rely on one. | cited | Behaviour on swipe-away and on process death |

### Other Android facts the UI should assume

- The renderer stack (Blitz's Vello-based renderers via `anyrender`) compiled and linked for Android. Which
  GPU backend it picks, and its frame rate on a mid device, is unmeasured.
- Wasmtime is linked into `libmain.so` for `aarch64-linux-android` and the guests are bundled; whether the JIT can
  map executable memory under the app's SELinux domain is the first thing to confirm at launch. The operator
  states Wasmtime works on Android; this build only shows it compiles and links.
- `extractNativeLibs="true"` (from the template) means the 400 MB (stripped debug) library is extracted at
  install time on top of the APK. A release build and `strip` change this materially.
- App-private data: `internal_data_path()` is `/data/data/<package>/files`. No storage permission is declared
  or needed. Uninstalling removes the store.

## What needs a device (checklist for the first run)

1. Launch: does the app start, show "Loading Dayframe…", then the shell, with `installation_errors` empty
   (the `counter` and `layout` packages parsed)?
2. Wasmtime: open Settings, enable the counter micro, press its control; watch `adb logcat` for a JIT or
   mmap error.
3. Touch: scroll a list; drag a section handle with `touch-action: none` (see above).
4. Back: press the system back / use the back gesture; does a `use_back_button` handler fire?
5. Insets: is the header under the status bar and cutout?
6. Keyboard: focus a text input; does the keyboard appear; do letters arrive; do accents arrive?
7. TalkBack: swipe through the screen.
8. Persistence: change state, force-stop, relaunch.

## Recommendation for the FDR 006 UI

Safe to build on today: list/details stacking, a sidebar drawer, bottom/side sheets, touch-scrolled lists, and
a handle drag if it carries `touch-action: none`. Do not commit to before a device run: edge-swipe gestures that
compete with the system back gesture, anything that expects safe-area insets, rich text entry, and the arc strip's
press-and-hold gesture (no device evidence either way). Two upstream-shaped fixes will be needed regardless and
are small in isolation: safe-area insets in `winit-android`, and an Android accessibility adapter in
`blitz-shell`/`accesskit_xplat`. The IME path is the large one.

## Environment notes for the next person

- The session's worktree started from `main`, not from `feat/dayframe-shell`; it was reset onto the latter.
- A fresh worktree needs the gitignored `wit/deps` directories (copied from a checkout that had them) or
  `wasm_utils_rs` fails to compile in any clippy that reaches it.
- `x/install-dx.ts` builds the Dioxus CLI from the fork rev into `$CARGO_TARGET_DIR/dioxus-cli`; that took
  roughly 25 minutes cold.
- A first `dx build` run was killed with SIGTERM (exit 143) at exactly 6 minutes by something outside the
  build (the cause was not identified); it succeeded when relaunched detached with `setsid`.
