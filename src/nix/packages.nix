# Packaged outputs: the pinned WIT dependency overlay and the vendored cargo
# dependencies (the latter patched for wash-runtime's build.rs); see
# src/nix/wit-deps.nix for how gitignored WIT dependencies enter the sandbox.
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
      # and compilable/embedded inputs under src/. Other languages are excluded by
      # extension. wit/deps (gitignored fetch output) is subtracted so
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
        ${git "dioxus" "49e05de24022388ee0e36802edf59897dbe41db3"} =
          "sha256-yiNdFaKGLzjZxvh9GqdQ9QlUy1Bb3bxvyDKDg2BfmvM=";
        ${git "iroh-blobs" "b3d641036c807b30d9fc62fd22a83801a38386d3"} =
          "sha256-9S3fDD2eMHVECWvnX32I+RnPUNd2lRp3S6+TPZRBYGw=";
        ${git "wasmtime" "38daa7ef0db706828c6fdfd87302fe00212db084"} =
          "sha256-XMnN2fcCym3pYd2ReATK/TjytsRGjQWVVsCJZWtB+9M=";
        ${git "blitz" "ffaf2bd0518a2cf7571c43f08327bd580b1871ca"} =
          "sha256-RK6XcHOCVl4qmlMnWl7q1bB27ynukmPxwx0j+5+MZhc=";
        ${git "subduction" "5f418129c5188750a34847d2624ace583c192c59"} =
          "sha256-4cvvJQol5wkeVJmW8wzUy7S8Qsdl8PYyH1y/K/eIjRU=";
        ${git "wasm-component-model-polyfill" "2a8d11a01d6630fabcca1730423e7103b4466d41"} =
          "sha256-Vqp3Je39JmnoRkBLmNTL3m6VmkcSsLBzV3UFKF1OVNI=";
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
          (git "dioxus" "49e05de24022388ee0e36802edf59897dbe41db3")
          (git "wasm-component-model-polyfill" "2a8d11a01d6630fabcca1730423e7103b4466d41")
        ] outputHashes;
      };

      # Wasm guests. Guests resolve their features in isolation (CONTRIBUTING.md on
      # `resolver.feature-unification`), as x/daytxt-packages.ts builds them.
      isolated = "--config 'resolver.feature-unification=\"selected\"'";
      microNames = [
        "counter"
        "layout"
        "lab"
        "review"
        "theme_lab"
      ];
      # The package file name of a micro crate: `micro_theme_lab` ships as `theme-lab`.
      microFile = lib.replaceStrings [ "_" ] [ "-" ];
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
              "cp target/wasm32-wasip3/wasm-release/micro_${name}.wasm $out/${microFile name}.component.wasm"
            ) microNames}
          '';
        }
      );

      # The manifest sidecars the desktop shell pairs with each component: each
      # micro's manifest crate emits its own through a native example (host build).
      manifestsArgs = wasmArgs // {
        pname = "daytxt-manifests";
        # Host binaries: clay_store links the system sqlite.
        buildInputs = [ pkgs.sqlite ];
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
          # The build phase runs the emitters, so there is no cargo build log.
          doNotPostBuildInstallCargoBinaries = true;
          buildPhaseCargoCommand = lib.concatMapStringsSep "\n" (name: ''
            cargo run --release --locked ${isolated} -q -p micro_${name}_manifest --example emit_manifest -- $PWD/${microFile name}.manifest.json
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

      # The directories DAYTXT_PACKAGE_DIR points at: <name>.manifest.json,
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
            cp ${daytxt-micros}/${microFile n}.component.wasm ${daytxt-manifests}/${microFile n}.manifest.json $out/
          '') names}
          cp ${intxt-guest}/intxt.guest.wasm $out/
        '';
      daytxt-packages = mkPackages "daytxt-packages" [ "lab" ];
      daytxt-packages-dev = mkPackages "daytxt-packages-dev" microNames;

      # daytxt_web for the browser: the raw cargo wasm module, `wasm-release`,
      # isolated features. wasm-bindgen and the site assembly are the next step.
      webArgs = wasmArgs // {
        pname = "daytxt-web-wasm";
        CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
        CARGO_PROFILE = "wasm-release";
        cargoExtraArgs = "--locked ${isolated} -p daytxt_web --bin daytxt_web";
      };
      daytxt-web-wasm = craneLib.buildPackage (
        webArgs
        // {
          cargoArtifacts = craneLib.buildDepsOnly webArgs;
          doInstallCargoArtifacts = false;
          installPhaseCommand = ''
            mkdir -p $out
            cp target/wasm32-unknown-unknown/wasm-release/daytxt_web.wasm $out/
          '';
        }
      );

      # The static site, assembled by x/daytxt-site.ts: the same script
      # x/daytxt-web.ts runs in the dev loop. Only static output; serving it
      # (and its Content-Security-Policy header) is not part of this package.
      daytxt-web =
        let
          siteSrc = lib.fileset.toSource {
            root = repoRoot;
            fileset = lib.fileset.unions [
              (repoRoot + "/Cargo.lock")
              (repoRoot + "/x/daytxt-site.ts")
              (repoRoot + "/src/clay_dioxus_kit/assets/tailwind.css")
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
              ${siteSrc}/x/daytxt-site.ts \
              --wasm ${daytxt-web-wasm}/daytxt_web.wasm \
              --packages ${daytxt-packages-dev} \
              --tailwind ${siteSrc}/src/clay_dioxus_kit/assets/tailwind.css \
              --cargo-lock ${siteSrc}/Cargo.lock \
              --out $out
          '';
    in
    {
      packages = {
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
    };
}
