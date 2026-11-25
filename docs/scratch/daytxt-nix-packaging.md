# daytxt apps as Nix flake packages, with CI

Updated: 2026-11-18

Status: Investigation and plan. Read from `origin/feat/desktop-node` (cf3a90bf). Prototype outcome in the last section.
Owner: claytxt lane; operator decides the items under "Decisions".
Scope: `daytxt_desktop`, `daytxt_mobile`, `daytxt_web` (renamed), the Wasm micro packages, the intxt guest, `src/nix/`, `.github/workflows/`.

## Today

Artifacts exist only as developer-demo builds, all inside `nix develop` shells:

| App | Entry | What it really does |
|---|---|---|
| desktop | `x/daytxt.ts desktop` | `prepareDaytxtPackages` (cargo guests + manifests + intxt guest), then `x/dx.ts serve --desktop -p daytxt_desktop --renderer webview`. Runs, does not produce an artifact. |
| android | `x/daytxt.ts android` | `prepareDaytxtPackages`, copies counter/layout/review + `intxt.guest.wasm` into gitignored `src/daytxt_mobile/packages/`, then `dx build --platform android --target <triple> --renderer webview -p daytxt_mobile`; checks the APK manifest with `apkanalyzer`. |
| web | `x/daytxt-web.ts` | guests, `cargo build -p daytxt_web --target wasm32-unknown-unknown --profile wasm-release`, `wasm-bindgen --target web`, a hand-rolled index (`packages/index.json` with BLAKE3 digests), a byte-patch of the manganis `__ASSETS__` placeholder (no dx), copy `tailwind.css`, `boot.js`, `index.html`, a CSP, and a Deno static server. |

The only Nix package today is `packages.daybook-cli` (crane, `src/nix/packages.nix`), built by the `nix-packages` job in `checks.yml`. `builds.yml` ("build artifacts") does the real release path with plain cargo on `ubuntu-latest` + `nix develop`-free tooling, and uploads only `daybook-cli-linux-x64-*.tar.xz`. None of the three apps is built in CI as an artifact; `checks.yml` runs the web e2e (`daytxt-web.ts --build` under `nix develop .#ci-desktop`).

### Facts that shape the Nix design

- **dx is used for exactly one job: manganis assets.** `clay_dioxus_kit::foundation` has `asset!("/assets/tailwind.css")`; `daytxt_mobile` has `asset!("/packages")`. dx intercepts the link, reads the `__ASSETS__` section, copies the files and rewrites `bundled_path`. Without dx the path stays the `PLACEHOLDER_HASH` text (web script patches it by hand). Desktop and Android therefore need dx in the build; web does not (hence the byte-patch).
- **Desktop packages are runtime data, not compiled in.** `daytxt_desktop/composition.rs` reads `DAYTXT_PACKAGE_DIR` (else `<data dir>/packages`): `<name>.manifest.json` + `<name>.component.wasm` per micro, plus `intxt.guest.wasm` which the node installs as a cartridge at boot. A Nix package can therefore ship the Rust binary and a `packages/` directory separately and point a wrapper at it. No rebuild coupling.
- **Android packages are compile-time assets.** `asset!("/packages")` with a hardcoded name list (`counter`, `layout`, `review`; no `lab`) plus `intxt.guest.wasm`; the folder must exist beside the crate at build time.
- **Web packages are served files.** `site/packages/*.component.wasm` + `index.json` pinned by BLAKE3.
- **Guests build in two workspaces.** Micros (`micro_counter`, `micro_layout`, `micro_lab`, `micro_review`) live in the root workspace and build `--target wasm32-wasip3 --profile wasm-release` with `resolver.feature-unification="selected"`. `intxt_nodule_guest` is in `src/wasm_utils_rs` (own `Cargo.lock`, `--release`, stripped). rustc's wasm32-wasip3 target emits a component directly: no adapter step.
- **Manifests are emitted by native examples**: `cargo run -p micro_<name>_manifest --example emit_manifest -- out.json` (host build).
- **Toolchain is already pinned** (`toolchains.nix`: nightly 2026-09-17 with wasm32-unknown-unknown, wasm32-wasip3, the four Android triples in `rustAndroid`/`rustFull`; `rustRust` lacks Android targets). `.cargo/config.toml` carries the wasip3 link args (`--allow-multiple-definition`, custom getrandom backend) that every guest build needs; the existing source filter already keeps `.cargo`.
- **wit-deps** FOD (`src/nix/wit-deps.nix`) already covers every crate with a `wkg.lock`, including `wasm_utils_rs`.
- **Vendoring hashes drift.** `packages.nix` `outputHashes` pins the dioxus fork at rev `7f6251d8...`; `Cargo.lock` on feat/desktop-node pins `1696518a...`. The lock also has git deps absent from `outputHashes` (dman-os/wasmtime, blitz, subduction, wasm-component-model-polyfill, keyhive, wasmCloud). Whether each needs a hash depends on whether the rev is on a ref (fetchGit works) or orphaned (needs `outputHashes`); the existing comment says the orphan case is the reason for the table. The wasm_utils_rs lock has its own git set (dioxus, autosurgeon, polyfill). Every packaged app needs the table refreshed for both locks, and the root and guest-workspace vendor dirs are separate derivations.

## Build graph and Nix needs, per app

### Shared: micro packages and the intxt guest

Two derivations feed every app.

1. `daytxt-micros` (root workspace, crane): for each of `micro_counter micro_layout micro_lab micro_review`, build `wasm32-wasip3`, profile `wasm-release`, features isolated. Use `craneLib.buildPackage` with `CARGO_BUILD_TARGET=wasm32-wasip3`, `cargoExtraArgs = "--locked --config resolver.feature-unification=\"selected\" -p micro_counter -p micro_layout ..."`, `doCheck = false`, a custom `installPhase` that copies `target/wasm32-wasip3/wasm-release/micro_*.wasm`. crane `buildDepsOnly` for this target gives the cache layer. Needs: `rustRust` (has the target), the vendored deps for the root lock, wit-deps overlay, `HOST_CC`/protoc as the daybook-cli derivation (wasmtime and friends are host-only, guests are lighter but share the lock). Check at first build: whether wasm32-wasip3 links with only the rustc-bundled `wasm-component-ld` and `self-contained` libc (CI's `ci-rust` shell has no wasi-sdk, which suggests yes).
2. `daytxt-manifests` (host, crane): `cargo run -p micro_<n>_manifest --example emit_manifest` per micro. Build the four example binaries in one derivation and run them in `installPhase`/a second `runCommand`, writing `<n>.manifest.json`.
3. `intxt-guest` (crane against `src/wasm_utils_rs/Cargo.toml`): its own `cargoVendorDir` from `src/wasm_utils_rs/Cargo.lock` (with its own `outputHashes`), `--manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release`, copy `intxt_nodule_guest.wasm` to `$out/intxt.guest.wasm`. Source filter must include `src/wasm_utils_rs` and its path-member crates (`../intxt_nodule_guest`, plug_*, test_*), which `srcExtensions` already covers via `src/`.
4. `daytxt-packages` = `linkFarm`/`runCommand` joining 1-3 into one directory, the layout `DAYTXT_PACKAGE_DIR` expects: `<n>.component.wasm`, `<n>.manifest.json`, `intxt.guest.wasm`. This replaces `prepareDaytxtPackages` (the TS helper stays for the dev loop; it can later read the Nix output instead of rebuilding).

### Desktop (`daytxt_desktop`, Wry, Linux x86_64 first)

Graph: Rust host binary (wasmtime fork, sqlite, Dioxus desktop + `dayframe_native/wry`, no Blitz by default) -> assets via dx (`tailwind.css`) -> `packages/` directory at runtime.

Two ways to produce the binary:

- **A. crane + minimal asset handling.** `cargo build -p daytxt_desktop --release` leaves the manganis placeholder; styling breaks unless the asset resolver finds the file. Needs a post-link patch like the web script's. Rejected: the web patch is a stopgap that breaks when the blob format changes, and doing it per app adds hidden coupling.
- **B. dx inside the derivation (recommended).** `dx build --release -p daytxt_desktop --renderer webview --platform linux` inside a crane-style derivation with `cargoVendorDir` wired through a `CARGO_HOME/config.toml` (offline, `--locked`), `dioxus-cli` as a build input, then `install` the `target/dx/daytxt_desktop/release/linux/app/` tree (executable + `assets/`) into `$out/libexec/daytxt-desktop`. `dx bundle` formats (deb/AppImage via tauri-bundler) download tooling and are not sandbox-friendly; produce those later outside Nix, or via `appimageTools`/`makeDesktopItem` from the Nix output.

The dioxus-cli fork as a package: `x/install-dx.ts` already derives the install source from the root `[patch.crates-io] dioxus` entry (git dman-os/dioxus rev, `cargo install --features no-downloads`). In Nix: `packages.dioxus-cli = craneLib.buildPackage { src = fetchgit { url = "https://github.com/dman-os/dioxus"; rev = <patch rev>; hash }; cargoExtraArgs = "-p dioxus-cli --no-default-features --features no-downloads"; }` against the fork's own `Cargo.lock`, with the same `cargoLock.outputHashes` mechanics. The rev must be read from the root `Cargo.toml` (as the TS does) so the two never drift; a flake check should assert the Nix rev equals the patch rev. Mind: dx at link-time needs `wasm-bindgen-cli`/`binaryen` only for web targets, not desktop.

Runtime deps: `gtk3`, `webkitgtk_4_1`, `libsoup_3`, `xdotool`, `openssl`, `sqlite`, `glib-networking` (webkit TLS), `libayatana-appindicator` (already in `dioxusBuildInputs` of the dev shell; reuse that list). Wrap with `wrapGAppsHook3` so GIO modules and GSettings resolve; set `DAYTXT_PACKAGE_DIR=${daytxt-packages}` in the wrapper. The wrapper must not set the store/node paths, the app defaults to the user's data dir.

Secrets: unlike the CLI, the desktop does not embed mltools; keep `OLLAMA_*` empty as `daybook-cli` does if the dependency is reachable.

Outputs: `packages.<system>.daytxt-desktop` (wrapped binary + packages), `packages.<system>.daytxt-desktop-unwrapped` if useful, `apps.daytxt-desktop`.

Blitz: `--features blitz` pulls vello/wgpu and system GL; leave it out of the shipping package (`blitz` is additive, an opt-in dev renderer).

### Web (`daytxt_web`)

Graph: crate -> `cargo build --target wasm32-unknown-unknown --profile wasm-release` (isolated features) -> `wasm-bindgen --target web` (CLI must equal Cargo.lock's wasm-bindgen, currently `wasm-bindgen-cli_0_2_127`, already pinned in devshells) -> optional `wasm-opt` -> site assembly (packages + index with BLAKE3 digests, tailwind, boot.js, index.html, CSP).

Nix: crane with `CARGO_BUILD_TARGET=wasm32-unknown-unknown`, `doCheck = false`, `installPhase` runs `wasm-bindgen`. The site assembly and the manganis placeholder patch are currently TypeScript. Split it:

1. Move the pure site assembly out of `daytxt-web.ts` into a callable step (e.g. `x/daytxt-site.ts` taking `--wasm`, `--packages`, `--out`) so both the Nix derivation (`deno run` with `nativeBuildInputs = [ deno ]`, jsr deps vendored or avoided: it imports `jsr:@std/crypto`, a network fetch, so replace with `b3sum`/`nixpkgs.b3sum` or a FOD for the jsr cache) and the dev loop call the same code. Recommend the Nix derivation do the BLAKE3 digests with `b3sum` in shell, and keep only the manganis patch in code. Alternatively build the site assembly in Rust (`xtask`-adjacent), but xtask must stay fast.
2. Output `packages.<system>.daytxt-web` = the `site/` directory (static, relocatable).

Serving: see Decisions. A tiny `packages.daytxt-web-serve` (writeShellApplication around `python3 -m http.server` is wrong: the CSP header and `application/wasm` MIME need a real handler) should use the existing Deno handler (small, already tested by e2e) extracted into `x/serve-site.ts`, or a 40-line Rust `tiny_http`/`axum` bin. Deno via Nix is fine (`deno compile` is not hermetic; run from source with `writeShellApplication`).

### Mobile (`daytxt_mobile`, Android APK)

Graph: rustAndroid toolchain -> `libmain.so` per ABI (wasmtime for aarch64-linux-android via the NDK clang; `jni`, `dayframe_native` as target-only deps) -> dx generates a Gradle project (Kotlin `MainActivity.kt`, `AndroidManifest.xml` override from `Dioxus.toml`) -> Gradle assembles the APK -> manifest check -> signing.

Nix needs:

- The Android SDK/NDK is already composed in `devshells.nix` (`androidSdkOnly`, build-tools 37.0.0, API 31 NDK env vars). Extract it into a shared module (`src/nix/android.nix`) used by the shell and the package.
- `rustAndroid` toolchain (already in toolchains.nix).
- **Gradle is the blocker.** `dx build` downloads Gradle 9.1.0 and the Android Gradle Plugin dependencies from the network (documented in `dayframe-android.md`). In a Nix sandbox this needs either `gradle2nix`/nixpkgs `gradle.fetchDeps` (a mitm-cache lockfile of every Maven artifact the generated project resolves, regenerated when dx's template or AGP changes) or an impure derivation. The project is generated by dx at build time, so the lockfile must be produced against dx's template for the pinned fork rev. This is real engineering, not packaging.
- Signing: a release APK needs a keystore. Build the unsigned APK in Nix; sign in CI with `apksigner` from a secret keystore (never a derivation input). Debug-signed APK for PRs.
- The CI APK for sideloading the intxt phone viewer: unsigned-debug is enough to start.
- The two APK ABIs: ship `aarch64-linux-android` only first (the phone); `x86_64-linux-android` for emulators optionally.
- The `packages/` assets are staged by copying into the crate dir; in Nix the build copies `daytxt-packages` into `src/daytxt_mobile/packages/` in the source tree before dx runs (note: only counter/layout/review are named in code; `lab` is not shipped).
- Size: debug APK was 192 MiB; use release + `wasm-release` guests (already default) and `strip`.

Recommend deferring the Nix APK; interim, CI builds it the way `ci-android` already allows (`nix develop .#ci-android`) only on demand.

## Proposed flake outputs

All in `src/nix/packages.nix` (or split into `src/nix/daytxt.nix` imported from `flake.nix`; one file per app group avoids a 700-line packages.nix):

```
packages.<system>.daytxt-micros       # wasm32-wasip3 micro components
packages.<system>.daytxt-manifests    # manifest sidecars
packages.<system>.intxt-guest         # intxt_nodule_guest.wasm
packages.<system>.daytxt-packages     # the DAYTXT_PACKAGE_DIR layout
packages.<system>.dioxus-cli          # forked dx, pinned to the root patch rev
packages.<system>.daytxt-desktop      # wrapped binary + packages (x86_64-linux first)
packages.<system>.daytxt-web          # static site
packages.<system>.daytxt-web-serve    # site + the small server (decision 3)
packages.<system>.daytxt-android      # later: unsigned APK
apps.<system>.daytxt-desktop, apps.<system>.daytxt-web-serve
checks (via passthru.tests): daytxt-packages manifests parse; desktop `--help`-less smoke is not possible (GUI), so test = binary links + `DAYTXT_PACKAGE_DIR` set + `ldd` has webkit; web site contains index.json whose digests equal b3sum of the files.
```

`daytxt-*` attribute names follow the product name; crate names stay until the rename decision.

## CI

Existing: `checks.yml` job `nix-packages` (nix-installer + flakehub-cache, `ubuntu-latest`, 90 min), `builds.yml` (plain cargo, release matrix, uploads `daybook-cli` archive; the `linux` job `rm -rf`s toolcache to free disk).

Proposed:

- `nix-packages` gains a matrix over `daytxt-packages`, `daytxt-web`, `daytxt-desktop`, each `nix build -L --no-link .#<attr>` with `sandbox = true`. Draft PRs keep skipping. The three share the `daytxt-packages` and vendor derivations so the second and third are cache hits within a run.
- Caching: flakehub-cache (already used) caches store paths per run on the same commit; crane's `buildDepsOnly` split means source-only changes rebuild only the workspace crates. For a push-to-main cache that survives, add `cachix` (or the FlakeHub cache's main-branch publish) so PRs get the dep layer; without it every PR pays the cold wasmtime/webkit build. Runner: `ubuntu-latest` is 4 vCPU/16 GB/14 GB free SSD and the desktop dep layer (wasmtime, vello off, webkit headers) is large; expect the first desktop build to need the disk-freeing step from `builds.yml` and possibly a larger runner (decision). `mbx` is for dev shells and does not apply inside Nix derivations.
- Artifacts: on `release_kind != none`, `builds.yml` runs `nix build .#daytxt-web` and `.#daytxt-desktop`, repackages (`tar.xz` of `result/` for web; desktop per the format decision) and uploads via the existing `softprops/action-gh-release` step with the same tag (`dev` prerelease on main, `vX.Y.Z` on tags). PRs upload workflow artifacts (7-day retention for debug) as `daybook-cli` does now.
- Android: a separate job `android` on `ubuntu-latest`, `nix develop .#ci-android` (or the Nix package once the Gradle lock exists), `workflow_dispatch` + tags only at first, signing from `secrets.ANDROID_KEYSTORE`.
- Retire from CI: nothing in `x/dayframe*.ts` is removed yet; the e2e keeps using `daytxt-web.ts --build` until `daytxt-web` can feed Playwright (`nix build .#daytxt-web` + serve), then the `x/` build path becomes the dev loop only.

## Recommended order

1. **Shared plumbing** (days): refresh the `outputHashes` for both locks; add `daytxt-micros`, `daytxt-manifests`, `intxt-guest`, `daytxt-packages`. Everything else depends on these and they are plain crane.
2. **Web bundle** (small): `daytxt-web` + `daytxt-web-serve`; move the site assembly out of `daytxt-web.ts` into shared code; point the e2e at it. Ships today and needs no dx.
3. **dioxus-cli fork package**, then **desktop Linux x86_64** (`daytxt-desktop`), tarball artifact first.
4. **Release wiring** in `builds.yml`.
5. **Android APK** last: the Gradle offline lock and signing are the unknowns; keep `x/daytxt.ts android` as the way meanwhile.
6. macOS/aarch64 desktop and AppImage/deb after the Linux tarball is stable.

## Decisions for the operator

1. **Rename crates to daytxt_\* now or later?** Recommend later, as one mechanical commit by the rename lane: flake attrs are `daytxt-*` already, crate names are internal, and a rename mid-flake-work churns `guardrails.toml`, `Cargo.lock`, vendor hashes and every open lane. Nix attr names do not need to match crate names.
2. **Web served by the desktop/mobile binary or a separate server?** Recommend a separate small server (`daytxt-web-serve`) now: the bundle is static; coupling the desktop binary to an HTTP listener adds a network surface and an app lifecycle concern before webnodes decides the shape. The mobile app serving the web build is the webnodes story and should not block this. Keep the static `daytxt-web` output independent so any server, or webnodes, can host it.
3. **Server implementation**: Deno (existing handler, already CSP-correct) vs a Rust binary. Recommend extracting the Deno handler into `x/serve-site.ts` for now; revisit when a node serves it.
4. **Desktop artifact format**: recommend `tar.xz` of the Nix closure-free tree (wrapper script plus libexec) first; AppImage next (needs the WebKitGTK bundling story; `appimageTools` or a second tool), `.deb` only if someone asks. Nix users get the flake package. Flatpak/Snap not now.
5. **dx inside the derivation vs hand-patching manganis assets** (desktop): recommend dx, built from the pinned fork as a Nix package, one mechanism for desktop and Android; keep the web byte-patch until the web build can also use dx (web uses `wasm-bindgen` directly today).
6. **Where the Nix dioxus-cli rev comes from**: recommend deriving it from the root `Cargo.toml` patch entry (as `install-dx.ts` does) with a flake check that fails on drift, over a second hand-maintained pin.
7. **Runner size and a binary cache**: recommend adding a push-cache (cachix or FlakeHub cache publish on main) before enabling the desktop job on every PR; otherwise run the desktop/Android jobs on main and tags plus a `ready_for_review` label. A larger runner only if the disk-freeing step is not enough.
8. **Android signing and distribution**: recommend unsigned APK from Nix, sign in CI with a secret keystore, GitHub release asset for now (sideload), Play Store later. Needs an operator-provided keystore.
9. **Android Gradle offline lock**: recommend investing in `gradle.fetchDeps` after desktop ships, accepting a regenerated lock whenever dx or AGP moves; the alternative (impure network build) cannot be a flake package.
10. **Ship `lab`/`showcase`?** Recommend no: `daytxt-packages` carries counter, layout, lab (desktop and web already scan it) but mobile names only counter/layout/review; decide whether `lab` is a product package or a dev fixture. Showcase stays a dev tool.
11. **Release profile of guests**: recommend `wasm-release` everywhere (already default); the dev profile is only for the local loop.
12. **Blitz in the shipping desktop**: recommend off (Wry is the shipping renderer per the Android decision of 2026-11-18); the `blitz` feature stays a dev opt-in.

## Prototype outcome

Decision: not a small add, so no `daytxt-desktop` was written. Probe run instead, in worktree `.claude/worktrees/nix-daytxt` (branch `feat/nix-daytxt`, from `origin/feat/desktop-node` cf3a90bf): `nix build .#cargo-vendor-dir`, the existing step every package depends on, to see whether the `daybook-cli` vendoring still works on this tree.

Result: it did not finish. After 15 minutes (my `timeout 900`) it was still fetching git sources through Nix's `builtins.fetchGit` (full clones of dman-os/wasmCloud, 17k delta resolve, dman-os/wasmtime, ...) and was interrupted. Nix warned `No output hash provided for git+https://github.com/dman-os/wasmtime?rev=a24366c...`: the tree's git deps have no `outputHashes` entries beyond the three that predate this branch (autosurgeon, iroh-blobs, and dioxus at a stale rev `7f6251d...` vs the lock's `1696518a...`). Hard blockers, in order:

1. **Vendoring hash table is stale/incomplete** for feat/desktop-node's `Cargo.lock` (dioxus rev changed; wasmtime, blitz, subduction, wcmp, keyhive are new). Each needs the fakeHash loop; each loop re-fetches a large repo. wasmtime/dioxus/wasmCloud clones are multi-GB-scale and slow on this link.
2. Even with hashes, the desktop needs the dx fork package, the guest packages, and the wasm_utils_rs vendor set (its own lock, another hash table). That is at least four new derivations before `daytxt-desktop` exists.
3. Disk: `/` (the Nix store) is at 15 GB free, so a first desktop dep layer (wasmtime, Dioxus, webkit) would force a `nix store gc` mid-lane; not attempted.

Nothing else was built. The worktree has only this doc committed. The first hours of whoever implements step 1 of "Recommended order" are the hash refresh; do it on a machine with disk headroom (mothership).

## Implementation notes (steps 1 and 2, operator scope update 2026-11-18)

- Shipping package set: `daytxt-packages` is the lab micro plus `intxt.guest.wasm`. The shell micro is compiled into the host and has no package file. The hosted desktop refuses to boot without `intxt.guest.wasm` (`hosted_runtime` in `daytxt_desktop/composition.rs`), so the guest ships. `daytxt-packages-dev` holds counter, layout, lab and review; `daytxt-web` reads it.
- `daytxt-web` is a static dev and Playwright-testing artifact. It is not a release artifact until the web node lands, and is not wired into `builds.yml`.
- `x/daytxt-site.ts` is the one site-assembly implementation: `x/daytxt-web.ts` and the Nix package both run it (it has no imports, shells out to `wasm-bindgen` and `b3sum`).
- Caching is the existing Swatinem/rust-cache and flakehub-cache-action; no new cache.
- Android signing already exists as `x/build-a-dayb.ts` (`DAYBOOK_ANDROID_RELEASE_KEYSTORE_*` env, `apksigner`); the APK step reuses it. No APK is built now.
