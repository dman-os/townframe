# Packaged outputs. `packages.daybook-cli` is a sandboxed, network-free (apart from
# fixed-output fetches) build of the daybook CLI; see src/nix/wit-deps.nix for how
# gitignored WIT dependencies enter the sandbox.
{ inputs, ... }:
{
  perSystem =
    { pkgs, ... }:
    let
      inherit (pkgs) lib;
      tools = import ./toolchains.nix { inherit pkgs; };
      witDeps = import ./wit-deps.nix { inherit pkgs; };
      repoRoot = ../..;

      craneLib = (inputs.crane.mkLib pkgs).overrideToolchain (_: tools.rustRust);

      workspaceToml = builtins.fromTOML (builtins.readFile (repoRoot + "/Cargo.toml"));
      version = workspaceToml.workspace.package.version;

      # Sources the build actually needs: manifests, the workspace cargo config,
      # and compilable/embedded inputs under src/. Kotlin and other languages are
      # excluded by extension. wit/deps (gitignored fetch output) is subtracted so
      # a local fetch cannot change the derivation — the FOD overlay below is the
      # only source of those files. fileset.fileFilter predicates only see
      # name/type/hasExt, so the deps directories are removed with difference.
      srcDir = repoRoot + "/src";
      srcExtensions =
        extra: file:
        lib.any file.hasExt extra
        || file.hasExt "rs"
        || file.hasExt "wit"
        || file.hasExt "toml"
        || file.hasExt "lock"
        || file.hasExt "sql"
        || file.hasExt "md";
      witDepsDirs = lib.filter builtins.pathExists (
        map (name: srcDir + "/${name}/wit/deps") (
          lib.filter (name: (builtins.readDir srcDir).${name} == "directory") (
            builtins.attrNames (builtins.readDir srcDir)
          )
        )
      );
      # `extra` names further extensions the build reads with include_str!/asset!
      # (the wasm guests and the web shell embed .json fixtures and the .css bundle).
      mkSrcWithWit =
        extra:
        let
          srcWithoutWitDeps =
            let
              base = lib.fileset.fileFilter (srcExtensions extra) srcDir;
            in
            if witDepsDirs == [ ] then base else lib.fileset.difference base (lib.fileset.unions witDepsDirs);
          src = lib.fileset.toSource {
            root = repoRoot;
            fileset = lib.fileset.unions [
              (repoRoot + "/Cargo.toml")
              (repoRoot + "/Cargo.lock")
              (repoRoot + "/.cargo")
              srcWithoutWitDeps
            ];
          };
        in
        pkgs.runCommand "townframe-src-with-wit-deps" { } ''
          cp -r ${src} $out
          chmod -R u+w $out
          cp -r ${witDeps}/. $out/
        '';
      srcWithWit = mkSrcWithWit [ ];
      guestSrc = mkSrcWithWit [
        "json"
        "css"
      ];

      # Cargo.lock pins git dependencies by raw revision. Some of those
      # revisions are orphaned commits (reachable by SHA, contained in no ref),
      # which builtins.fetchGit refuses even with allRefs, so Nix 2.34 cannot
      # vendor them the default way. outputHashes makes crane use pkgs.fetchgit
      # (plain `git fetch <rev>`), which accepts them; the hashes were obtained
      # by building each fetchgit call with lib.fakeHash and recording `got`.
      # Refresh: fakeHash one of these, rebuild, copy the new value back.
      # FIXME: consider extracting this from the cargo.toml file
      git = repo: rev: "git+https://github.com/dman-os/${repo}?rev=${rev}#${rev}";
      outputHashes = {
        ${git "autosurgeon" "6a1df66f667667f1d14bc2272c4d426815de66b8"} =
          "sha256-dIhUdjaf/dlWufOu6X7694DqP7MHRpvOBdzXx6HFOi4=";
        ${git "dioxus" "1696518a2c449f44c0d2d4ac1fedd00127058d63"} =
          "sha256-PiiB2Ab21huCGtOg6EQOuFKIbk3Wp95JuOX69iFV+RA=";
        ${git "iroh-blobs" "b3d641036c807b30d9fc62fd22a83801a38386d3"} =
          "sha256-9S3fDD2eMHVECWvnX32I+RnPUNd2lRp3S6+TPZRBYGw=";
        ${git "wasmtime" "a24366c874177c2beecd96c7dfae6754c847d361"} =
          "sha256-T8q2cJyQXhaci4vadjjpC+IaoVkoPFO7W7WV3o8/h3k=";
        ${git "blitz" "ffaf2bd0518a2cf7571c43f08327bd580b1871ca"} =
          "sha256-RK6XcHOCVl4qmlMnWl7q1bB27ynukmPxwx0j+5+MZhc=";
        ${git "subduction" "5f418129c5188750a34847d2624ace583c192c59"} =
          "sha256-4cvvJQol5wkeVJmW8wzUy7S8Qsdl8PYyH1y/K/eIjRU=";
        ${git "wasm-component-model-polyfill" "da35a9b4e4cc71dacb57323b906093f9e18e78b6"} =
          "sha256-S+xSm+6zSdsYo6ReFJoT29rT2DT/VXYxCUVvnExajaw=";
        ${git "keyhive" "a84137d1f2bdd81cc6c883253a534cc3f0380b5e"} =
          "sha256-8Iok2MexAWnGFov2D+6jC+GsMhDLfOPlufYDRMLz9Ls=";
      };
      # wash-runtime comes from our fork (dman-os/wasmCloud, branch
      # townframe-changes); its pinned commit is on a branch, so it needs no
      # outputHashes entry. wash-runtime's build.rs finds the wasmCloud repository root by walking
      # up from its own crate directory to the first Cargo.lock, then compiles
      # the protobuf definitions in <root>/proto. crane's git vendoring keeps
      # only the individual crates, so both repo-root inputs are restored: first
      # into the git checkout derivation (its install phase still has the full
      # checkout unpacked), then into the vendor copy below.
      craneVendorDir = craneLib.vendorCargoDeps {
        src = srcWithWit;
        inherit outputHashes;
        overrideVendorGitCheckout =
          ps: drv:
          if lib.any (p: lib.hasPrefix "git+https://github.com/dman-os/wasmCloud" p.source) ps then
            drv.overrideAttrs (_: {
              postInstall = ''
                cp Cargo.lock $out/
                cp -r proto $out/proto
              '';
            })
          else
            drv;
      };
      # The vendored group directories are symlinks into read-only store paths,
      # so the crate cannot gain the repo-root files in place. Copy the vendor
      # dir, materialize the wasmCloud group as a real directory, and give the
      # crate itself the Cargo.lock/proto pair so the walk terminates there —
      # whether or not cargo resolves the manifest path through symlinks.
      # config.toml addresses every source directory by absolute path, so its
      # entries are retargeted at this copy before cargo reads it.
      cargoVendorDir = pkgs.runCommand "townframe-cargo-vendor-dir" { } ''
        cp -r ${craneVendorDir} $out
        chmod -R u+w $out
        sed -i "s|${craneVendorDir}/|$out/|g" $out/config.toml

        patched=
        for group in "$out"/*; do
          [ -L "$group" ] || continue
          locked=$(readlink "$group")
          crate=
          for entry in "$locked"/*; do
            case "''${entry##*/}" in
              wash-runtime-*)
                crate=$entry
                break
                ;;
            esac
          done
          [ -n "$crate" ] || continue
          name=$(basename "$crate")
          checkout=$(readlink "$crate")
          checkout=''${checkout%/*}
          materialized="$group.materialized"
          mkdir "$materialized"
          cp -r "$locked"/. "$materialized/"
          chmod -R u+w "$materialized"
          rm "$materialized/$name"
          mkdir "$materialized/$name"
          cp -r "$checkout/$name"/. "$materialized/$name"/
          cp "$checkout/Cargo.lock" "$materialized/$name/Cargo.lock"
          cp -r "$checkout/proto" "$materialized/$name/proto"
          rm "$group"
          mv "$materialized" "$group"
          patched=1
        done
        if [ -z "$patched" ]; then
          echo "wash-runtime missing from the vendored git checkouts; update cargoVendorDir in src/nix/packages.nix" >&2
          exit 1
        fi
      '';

      # The second guest workspace (src/wasm_utils_rs, builds intxt_nodule_guest)
      # has its own lock and so its own vendor set. It shares the dioxus,
      # autosurgeon and polyfill pins with the root lock but resolves to its own
      # vendor directory; it has no wash-runtime, so crane's plain vendoring is
      # enough.
      guestVendorDir = craneLib.vendorCargoDeps {
        src = srcWithWit;
        cargoLock = repoRoot + "/src/wasm_utils_rs/Cargo.lock";
        outputHashes = lib.getAttrs [
          (git "autosurgeon" "6a1df66f667667f1d14bc2272c4d426815de66b8")
          (git "dioxus" "1696518a2c449f44c0d2d4ac1fedd00127058d63")
          (git "wasm-component-model-polyfill" "da35a9b4e4cc71dacb57323b906093f9e18e78b6")
        ] outputHashes;
      };

      # Wasm guests. Guests resolve their features in isolation (CONTRIBUTING.md on
      # `resolver.feature-unification`), as x/dayframe-packages.ts builds them.
      isolated = "--config 'resolver.feature-unification=\"selected\"'";
      microNames = [
        "counter"
        "layout"
        "lab"
        "review"
      ];
      # Build scripts and proc macros still run on the host, so the guests keep
      # the host-side native tools the root workspace's build scripts expect.
      guestNativeBuildInputs = [
        pkgs.pkg-config
        pkgs.cmake
        pkgs.protobuf
        pkgs.llvmPackages.libclang
      ];
      wasmArgs = {
        version = version;
        src = guestSrc;
        inherit cargoVendorDir;
        doCheck = false;
        nativeBuildInputs = guestNativeBuildInputs;
        LIBCLANG_PATH = "${lib.getLib pkgs.llvmPackages.libclang}/lib";
      };

      # The micro components, `wasm-release`, for wasm32-wasip3. rustc's target
      # emits a component directly: no adapter step.
      microsArgs = wasmArgs // {
        pname = "daytxt-micros";
        CARGO_BUILD_TARGET = "wasm32-wasip3";
        CARGO_PROFILE = "wasm-release";
        cargoExtraArgs = "--locked ${isolated} ${
          lib.concatMapStringsSep " " (name: "-p micro_${name}") microNames
        }";
      };
      daytxt-micros = craneLib.buildPackage (
        microsArgs
        // {
          cargoArtifacts = craneLib.buildDepsOnly microsArgs;
          doInstallCargoArtifacts = false;
          installPhaseCommand = ''
            mkdir -p $out
            ${lib.concatMapStringsSep "\n" (
              name:
              "cp target/wasm32-wasip3/wasm-release/micro_${name}.wasm $out/${name}.component.wasm"
            ) microNames}
          '';
        }
      );

      # The manifest sidecars the desktop shell pairs with each component: each
      # micro's manifest crate emits its own through a native example (host build).
      manifestsArgs = wasmArgs // {
        pname = "daytxt-manifests";
        # Isolated features: workspace unification would build the desktop
        # renderer's GTK stack (glib-sys) for a manifest emitter.
        cargoExtraArgs = "--locked ${isolated} ${
          lib.concatMapStringsSep " " (name: "-p micro_${name}_manifest") microNames
        }";
      };
      daytxt-manifests = craneLib.buildPackage (
        manifestsArgs
        // {
          cargoArtifacts = craneLib.buildDepsOnly manifestsArgs;
          doInstallCargoArtifacts = false;
          buildPhaseCargoCommand = lib.concatMapStringsSep "\n" (name: ''
            cargo run --release --locked ${isolated} -q -p micro_${name}_manifest --example emit_manifest -- $PWD/${name}.manifest.json
          '') microNames;
          installPhaseCommand = ''
            mkdir -p $out
            cp *.manifest.json $out/
          '';
        }
      );

      # intxt's nodule handler lives in the second guest workspace
      # (src/wasm_utils_rs, own lock and vendor set), always `--release`; that
      # workspace's profile strips this one guest's final link.
      intxtGuestArgs = wasmArgs // {
        pname = "intxt-guest";
        cargoVendorDir = guestVendorDir;
        CARGO_BUILD_TARGET = "wasm32-wasip3";
        cargoExtraArgs = "--locked --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest";
      };
      intxt-guest = craneLib.buildPackage (
        intxtGuestArgs
        // {
          cargoArtifacts = null;
          doInstallCargoArtifacts = false;
          # crane's post-build binary scan runs cargo against the root manifest,
          # whose git sources are not in this workspace's vendor set.
          doNotPostBuildInstallCargoBinaries = true;
          installPhaseCommand = ''
            mkdir -p $out
            cp src/wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm $out/intxt.guest.wasm
          '';
        }
      );

      # The directories DAYFRAME_PACKAGE_DIR points at: <name>.manifest.json,
      # <name>.component.wasm and intxt.guest.wasm. The shell micro is compiled
      # into the host and ships no package file. The shipping set is the lab micro
      # plus the intxt guest (the hosted desktop refuses to boot without
      # intxt.guest.wasm); the dev set is every micro, which the web build and
      # tests that open counter/layout/review read.
      mkPackages =
        name: names:
        pkgs.runCommand name { } ''
          mkdir -p $out
          ${lib.concatMapStringsSep "\n" (n: ''
            cp ${daytxt-micros}/${n}.component.wasm ${daytxt-manifests}/${n}.manifest.json $out/
          '') names}
          cp ${intxt-guest}/intxt.guest.wasm $out/
        '';
      daytxt-packages = mkPackages "daytxt-packages" [ "lab" ];
      daytxt-packages-dev = mkPackages "daytxt-packages-dev" microNames;

      # dayframe_web for the browser: the raw cargo wasm module, `wasm-release`,
      # isolated features. wasm-bindgen and the site assembly are the next step.
      webArgs = wasmArgs // {
        pname = "daytxt-web-wasm";
        CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
        CARGO_PROFILE = "wasm-release";
        cargoExtraArgs = "--locked ${isolated} -p dayframe_web --bin dayframe_web";
      };
      daytxt-web-wasm = craneLib.buildPackage (
        webArgs
        // {
          cargoArtifacts = craneLib.buildDepsOnly webArgs;
          doInstallCargoArtifacts = false;
          installPhaseCommand = ''
            mkdir -p $out
            cp target/wasm32-unknown-unknown/wasm-release/dayframe_web.wasm $out/
          '';
        }
      );

      # The static site, assembled by x/dayframe-site.ts: the same script
      # x/dayframe-web.ts runs in the dev loop. Only static output; serving it
      # (and its Content-Security-Policy header) is not part of this package.
      daytxt-web =
        let
          siteSrc = lib.fileset.toSource {
            root = repoRoot;
            fileset = lib.fileset.unions [
              (repoRoot + "/Cargo.lock")
              (repoRoot + "/x/dayframe-site.ts")
              (repoRoot + "/src/ui/assets/tailwind.css")
            ];
          };
        in
        pkgs.runCommand "daytxt-web"
          {
            nativeBuildInputs = [
              pkgs.deno
              # Must match the single wasm-bindgen crate in Cargo.lock; the site
              # script asserts it. Same pin as the dev shells.
              pkgs.wasm-bindgen-cli_0_2_127
              pkgs.b3sum
              pkgs.gzip
            ];
          }
          ''
            export DENO_DIR=$TMPDIR/deno
            deno run --no-config --no-remote --allow-read --allow-write --allow-run \
              ${siteSrc}/x/dayframe-site.ts \
              --wasm ${daytxt-web-wasm}/dayframe_web.wasm \
              --packages ${daytxt-packages-dev} \
              --tailwind ${siteSrc}/src/ui/assets/tailwind.css \
              --cargo-lock ${siteSrc}/Cargo.lock \
              --out $out
          '';

      commonArgs = {
        pname = "daybook-cli";
        inherit version;
        src = srcWithWit;
        inherit cargoVendorDir;
        cargoExtraArgs = "--locked -p daybook_cli";
        doCheck = false;

        # mltools embeds these with env!() at compile time (src/mltools/models.rs).
        # Real credentials must never be derivation inputs — they would land in the
        # world-readable Nix store — so the baked defaults are empty and a packaged
        # binary has to take its cloud config from the runtime environment.
        OLLAMA_URL = "";
        OLLAMA_USERNAME = "";
        OLLAMA_PASSWORD = "";

        # ort-sys (via fastembed) probes ORT_LIB_LOCATION for a *static* archive
        # by default, which nixpkgs does not ship; ORT_PREFER_DYNAMIC_LINK makes
        # it link the provided shared library instead, keeping the build offline.
        ORT_LIB_LOCATION = "${pkgs.onnxruntime}/lib";
        ORT_PREFER_DYNAMIC_LINK = "1";
        # ort-sys' copy-dylibs step symlinks the dylibs into OUT_DIR's third
        # ancestor plus `examples/` and `deps/` subdirectories without creating
        # them first, which panics with NotFound on a fresh target dir.
        preBuild = "mkdir -p target/release/build/examples target/release/build/deps";

        nativeBuildInputs = [
          pkgs.pkg-config
          pkgs.cmake
          pkgs.protobuf
          pkgs.llvmPackages.libclang
        ];
        buildInputs = [
          pkgs.openssl
          pkgs.sqlite
          pkgs.onnxruntime
        ]
        ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [ pkgs.stdenv.cc.cc.lib ];

        LIBCLANG_PATH = "${lib.getLib pkgs.llvmPackages.libclang}/lib";
      }
      // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
        # Same headers bindgen sees inside the ci-rust shell.
        BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.linuxHeaders}/include -idirafter ${pkgs.stdenv.cc.libc.dev}/include";
      };

      cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      cliBuild = craneLib.buildPackage (
        commonArgs
        // {
          inherit cargoArtifacts;
          meta.mainProgram = "daybook_cli";
        }
      );
      # Exercises the installed binary end to end in an isolated HOME and
      # repo with an empty environment: init, two documents, persistence across
      # invocations and re-init, dump, and the uninitialized/missing-document
      # failures. Lives in passthru.tests rather than `checks` so the
      # `nix flake check` lint hook does not have to build the whole CLI; CI
      # builds it with `nix build .#daybook-cli.tests.round-trip`.
      roundTrip = pkgs.writeShellApplication {
        name = "daybook-cli-round-trip";
        runtimeInputs = [
          pkgs.coreutils
          pkgs.gnugrep
          cliBuild
        ];
        text = ''
          work=$(mktemp -d)
          trap 'rm -rf "$work"' EXIT
          export HOME="$work/home"
          export XDG_CONFIG_HOME="$HOME/config"
          export XDG_CACHE_HOME="$HOME/cache"
          export DAYB_REPO_PATH="$work/repo"
          export RUST_LOG=error,wflow::kvstore=off
          export NO_COLOR=1
          mkdir -p "$HOME"
          cd "$work"

          daybook_cli --help > help.txt
          grep -F 'Usage: daybook_cli' help.txt
          if daybook_cli ls > uninitialized.txt 2>&1; then
            echo 'ls unexpectedly succeeded before initialization' >&2
            exit 1
          fi
          daybook_cli init
          daybook_cli ls > empty.txt
          if grep -F 'Untitled' empty.txt; then
            echo 'fresh repository contains a document' >&2
            exit 1
          fi
          first=$(daybook_cli touch)
          second=$(daybook_cli touch)
          test -n "$first"
          test -n "$second"
          test "$first" != "$second"
          daybook_cli init
          daybook_cli ls > list.txt
          grep -F -- "$first" list.txt
          grep -F -- "$second" list.txt
          for id in "$first" "$second"; do
            daybook_cli cat "$id" > doc.txt
            grep -F -- "$id" doc.txt
            grep -F 'Untitled' doc.txt
          done
          daybook_cli dump > dump.txt
          test -s dump.txt
          if daybook_cli cat nonexistent-document > missing.txt 2>&1; then
            echo 'cat unexpectedly succeeded for a missing document' >&2
            exit 1
          fi
        '';
      };
      daybook-cli = cliBuild.overrideAttrs (prev: {
        passthru = (prev.passthru or { }) // {
          tests.round-trip = pkgs.runCommand "daybook-cli-round-trip" { } ''
            ${pkgs.coreutils}/bin/env -i ${lib.getExe roundTrip}
            touch $out
          '';
        };
      });
    in
    {
      packages = {
        inherit daybook-cli;
        guest-cargo-vendor-dir = guestVendorDir;
        inherit
          daytxt-micros
          daytxt-manifests
          intxt-guest
          daytxt-packages
          daytxt-packages-dev
          daytxt-web-wasm
          daytxt-web
          ;
        wit-deps = witDeps;
        cargo-vendor-dir = cargoVendorDir;
      };
      apps.daybook-cli = {
        type = "app";
        program = lib.getExe daybook-cli;
        meta.description = "Daybook local-first document CLI";
      };
    };
}
