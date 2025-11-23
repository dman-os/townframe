{
  description = "yep";

  inputs = {
    nixpkgs.url       = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url   = "github:hercules-ci/flake-parts";
    rust-overlay.url  = "github:oxalica/rust-overlay";
  };

  outputs = { flake-parts, nixpkgs, rust-overlay, ... } @ inputs: flake-parts.lib.mkFlake { inherit inputs; } {
    perSystem = { config, self', inputs', system, ... }:
      let
        # Import nixpkgs with rust-overlay applied
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
          config = {
            android_sdk.accept_license = true;
            allowUnfree = true;
          };
        };

        androidBuildToolsVersion = "35.0.0";
        androidApiLevel = "31";
        androidComposition = (pkgs.android-studio.withSdk (
          pkgs.androidenv.composeAndroidPackages { 
            cmdLineToolsVersion = "8.0";
            toolsVersion = "26.1.1";
            platformToolsVersion = "35.0.1";
            buildToolsVersions = [ androidBuildToolsVersion "34.0.0" ];
            platformVersions = [ "35" "33" ];
            includeNDK = true;
            includeExtras = [ "extras;google;gcm" ];
            includeSources = false;
            includeSystemImages = false;
            abiVersions = [ "armeabi-v7a" "arm64-v8a" ];
            systemImageTypes = [ "google_apis_playstore" ];
            includeEmulator = false;
            useGoogleAPIs = false;
            useGoogleTVAddOns = false;
          }
        ).androidsdk);

        androidSdkRoot = "${androidComposition.sdk}/libexec/android-sdk";

        rustVersion = "2025-09-01";
        rustChannel = pkgs.rust-bin.nightly.${rustVersion}.default.override {
          extensions = [ "rust-src" ];
          targets = [ 
            "wasm32-unknown-unknown" 
            "wasm32-wasip2" 
            "armv7-linux-androideabi" # For armeabi-v7a
            "aarch64-linux-android" # For arm64-v8a
            "i686-linux-android" # For x86"
            "x86_64-linux-android"  # For x86_64
          ];
        };

        # Map NDK host tag from nix platform (needed for correct toolchain path)
        ndkHostTag = if pkgs.stdenv.isDarwin then
          (if pkgs.stdenv.hostPlatform.parsed.cpu.name == "aarch64" then "darwin-aarch64" else "darwin-x86_64")
        else
          (if pkgs.stdenv.hostPlatform.parsed.cpu.name == "aarch64" then "linux-aarch64" else "linux-x86_64");

        ndkToolchainBinDir = "${androidSdkRoot}/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin";

        # Helper function to generate Rust cross-compilation env vars for a target
        rustTargetEnvVars = target: arch: lowercaseTarget: {
          "CARGO_TARGET_${target}_LINKER" = "${ndkToolchainBinDir}/${arch}-linux-android${androidApiLevel}-clang";
          "CARGO_TARGET_${target}_AR" = "${ndkToolchainBinDir}/llvm-ar";
          "CC_${lowercaseTarget}" = "${ndkToolchainBinDir}/${arch}-linux-android${androidApiLevel}-clang";
          "AR_${lowercaseTarget}" = "${ndkToolchainBinDir}/llvm-ar";
        };

        # All Rust cross-compilation environment variables
        rustCrossCompileEnv = pkgs.lib.foldl pkgs.lib.recursiveUpdate {} [
          (rustTargetEnvVars "ARMV7_LINUX_ANDROIDEABI" "armv7a" "armv7_linux_androideabi")
          (rustTargetEnvVars "AARCH64_LINUX_ANDROID" "aarch64" "aarch64_linux_android")
          (rustTargetEnvVars "I686_LINUX_ANDROID" "i686" "i686_linux_android")
          (rustTargetEnvVars "X86_64_LINUX_ANDROID" "x86_64" "x86_64_linux_android")
        ];

        # Android SDK environment variables
        androidSdkEnv = {
          ANDROID_SDK_ROOT = androidSdkRoot;
          ANDROID_HOME = androidSdkRoot;
          ANDROID_NDK_ROOT = "${androidSdkRoot}/ndk-bundle";
          ANDROID_NDK_TOOLCHAIN_BIN_DIR = ndkToolchainBinDir;
          GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${androidSdkRoot}/build-tools/${androidBuildToolsVersion}/aapt2";
        };

        # Helper to convert env vars to shell export statements
        envVarsToShellExports = envVars: pkgs.lib.concatStringsSep "\n" (
          pkgs.lib.mapAttrsToList (name: value: "export ${name}=\"${value}\"") envVars
        );

        # Dioxus-specific build inputs
        dioxusBuildInputs = with pkgs; [
          # needed to build stylo
          python314
          wayland
          wayland-protocols
          wayland-scanner
          # needed by dioxus
          openssl
          at-spi2-atk
          atkmm
          gdk-pixbuf
          glib
          gtk3
          harfbuzz
          librsvg
          libsoup_3
          pango
          webkitgtk_4_1
          alsa-lib
          xdotool
        ];

        # Build tools (formerly washBuildInputs)
        buildTools = with pkgs; [
          cmake
        ];

        # Development tools
        devTools = with pkgs; [
          rogcat
          opentofu
          terragrunt
          tokio-console
          infisical
        ];

        # FHS environment to run Android tools (provides writable SDK directory)
        fhsEnv = pkgs.buildFHSEnvBubblewrap {
          name = "dioxus-android-fhs";
          targetPkgs = pkgs: with pkgs; [
            rustChannel
            androidComposition
            # openjdk
            # gradle
            pkg-config
            protobuf
          ] ++ devTools ++ buildTools ++ dioxusBuildInputs;
          multiPkgs = pkgs: with pkgs; [
            # stdenv.cc.cc.lib
            zlib
          ];
          profile = ''
            ${envVarsToShellExports androidSdkEnv}
            export PATH="$PATH:${androidSdkRoot}/platform-tools"
            ${envVarsToShellExports rustCrossCompileEnv}
          '';
          runScript = "fish";
        };

        # Base shell with just the development environment setup
        baseShell = pkgs.mkShell (rec {
          name = "devshell-base";

          buildInputs = dioxusBuildInputs ++ buildTools ++ devTools ++ [
            rustChannel
            androidComposition
            pkgs.pkg-config
            pkgs.protobuf
          ];

          shellHook = with pkgs; ''
            export XDG_DATA_DIRS=${fontconfig.out}/share:$XDG_DATA_DIRS
            export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:./target/debug/:${lib.makeLibraryPath (lib.map (x: lib.getLib x) (buildInputs ++ [ 
              # needed by daybook_compose desktop
              sqlite.dev
              llvmPackages.libclang.dev
              xorg.libXrender.dev
              xorg.libXext.dev
              xorg.libXtst
              xorg.libX11.dev
              xorg.libXi.dev
              xorg.libXrandr.dev
              xorg.libxcb.dev
              libxkbcommon
              freetype.dev
              fontconfig.dev
              libglvnd.dev

              vulkan-loader
            ]))}"
            if [ "$(uname -s)" = "Darwin" ]; then
              export DYLD_LIBRARY_PATH="$LD_LIBRARY_PATH"
            fi
            export PATH=$PATH:${androidSdkRoot}/platform-tools
            echo "[!] Note: For Android builds, use 'nix develop .#fhs' to enter FHS environment with writable SDK"
            exec $(getent passwd $USER | cut -d: -f7)
          '';
        } // androidSdkEnv // rustCrossCompileEnv);

      in {
        devShells = {
          # Default shell that doesn't exec into interactive shell
          default = baseShell;
          # FHS environment for Android builds (provides writable SDK directory)
          fhs = fhsEnv.env;
        };
      };

    systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
  };
}
