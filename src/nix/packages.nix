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
      srcExtensions = file:
        file.hasExt "rs"
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
      srcWithoutWitDeps =
        let base = lib.fileset.fileFilter srcExtensions srcDir;
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
      srcWithWit = pkgs.runCommand "townframe-src-with-wit-deps" { } ''
        cp -r ${src} $out
        chmod -R u+w $out
        cp -r ${witDeps}/. $out/
      '';

      # Cargo.lock pins five git dependencies by raw revision. Some of those
      # revisions are orphaned commits (reachable by SHA, contained in no ref),
      # which builtins.fetchGit refuses even with allRefs, so Nix 2.34 cannot
      # vendor them the default way. outputHashes makes crane use pkgs.fetchgit
      # (plain `git fetch <rev>`), which accepts them; the hashes were obtained
      # by building each fetchgit call with lib.fakeHash and recording `got`.
      # Refresh: fakeHash one of these, rebuild, copy the new value back.
      outputHashes = {
        "git+https://github.com/dman-os/autosurgeon?rev=6a1df66f667667f1d14bc2272c4d426815de66b8#6a1df66f667667f1d14bc2272c4d426815de66b8" =
          "sha256-dIhUdjaf/dlWufOu6X7694DqP7MHRpvOBdzXx6HFOi4=";
        "git+https://github.com/dman-os/iroh-blobs?rev=b3d641036c807b30d9fc62fd22a83801a38386d3#b3d641036c807b30d9fc62fd22a83801a38386d3" =
          "sha256-9S3fDD2eMHVECWvnX32I+RnPUNd2lRp3S6+TPZRBYGw=";
        "git+https://github.com/dman-os/keyhive?rev=1063446711dcca3848430044f239830dc948290c#1063446711dcca3848430044f239830dc948290c" =
          "sha256-wCLRhV509AWZzI2IHlyChesSTSkV3exI57iuwqVdoHs=";
        "git+https://github.com/dman-os/subduction?rev=44cd14743b8ae2bf8b9eefb30e09618ef27d6f95#44cd14743b8ae2bf8b9eefb30e09618ef27d6f95" =
          "sha256-zogYtiZrpVSQF6D4t6bvDBshjvvODzImh9Cs3aQn9/I=";
        "git+https://github.com/wasmCloud/wasmCloud?rev=44fc4cf76a49a599b42dcb1466586b39f09ef658#44fc4cf76a49a599b42dcb1466586b39f09ef658" =
          "sha256-slqT6qZ9hQY0mUw+ihaaD4zTEZgM2ZznTju+IFnBPNo=";
      };
      # wash-runtime's build.rs finds the wasmCloud repository root by walking
      # up from its own crate directory to the first Cargo.lock, then compiles
      # the protobuf definitions in <root>/proto. crane's git vendoring keeps
      # only the individual crates, so both repo-root inputs are restored: first
      # into the git checkout derivation (its install phase still has the full
      # checkout unpacked), then into the vendor copy below.
      craneVendorDir = craneLib.vendorCargoDeps {
        src = srcWithWit;
        inherit outputHashes;
        overrideVendorGitCheckout = ps: drv:
          if lib.any (p: lib.hasPrefix "git+https://github.com/wasmCloud/wasmCloud" p.source) ps then
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
      daybook-cli = craneLib.buildPackage (
        commonArgs
        // {
          inherit cargoArtifacts;
          meta.mainProgram = "daybook_cli";
        }
      );
    in
    {
      packages.daybook-cli = daybook-cli;
      # Exposed so the refresh procedure in wit-deps.nix (`nix build .#wit-deps`)
      # works as documented; not a user-facing package.
      packages.wit-deps = witDeps;
      # Exposed so the vendored source tree (including the wash-runtime repo-root
      # restoration above) can be inspected without building the whole package.
      packages.cargo-vendor-dir = cargoVendorDir;
      apps.daybook-cli = {
        type = "app";
        program = lib.getExe daybook-cli;
      };
    };
}