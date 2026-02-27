{
  description = "yep";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    inputs@{
      self,
      flake-parts,
      nixpkgs,
      rust-overlay,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem =
        { system, ... }:
        let
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
          rustVersion = "2026-01-01";

          ghjkMainEnv = {
            CARGO_BUILD_JOBS = "8";
          };

          ghjkDevEnv = {
            GDK_SCALE = "2";
            KANIDM_URL = "https://localhost:8443";
            KANIDM_SKIP_HOSTNAME_VERIFICATION = "true";
            KANIDM_ACCEPT_INVALID_CERTS = "true";
            WASMCLOUD_OCI_ALLOWED_INSECURE = "localhost:5000";
          };

          # Android SDK/NDK without Studio (for CI)
          androidSdkOnly = pkgs.androidenv.composeAndroidPackages {
            includeNDK = true;
            platformToolsVersion = "36.0.0";
            buildToolsVersions = [ androidBuildToolsVersion ];
            platformVersions = [ "36" ];
          };

          # Android SDK/NDK with Studio (for dev)
          androidComposition = pkgs.android-studio.withSdk androidSdkOnly.androidsdk;

          # Rust toolchain for CI (wasm32 + native Linux targets)
          rustRust = pkgs.rust-bin.nightly.${rustVersion}.default.override {
            extensions = [ "rust-src" ];
            targets =
              [
                "wasm32-unknown-unknown"
                "wasm32-wasip2"
              ]
              ++ (
                if pkgs.stdenv.isLinux then
                  [
                    "x86_64-unknown-linux-gnu"
                    "aarch64-unknown-linux-gnu"
                  ]
                else
                  [ ]
              );
          };

          # Rust toolchain for Android CI (wasm32 + Android targets)
          rustAndroid = pkgs.rust-bin.nightly.${rustVersion}.default.override {
            extensions = [ "rust-src" ];
            targets = [
              "wasm32-unknown-unknown"
              "wasm32-wasip2"
              "armv7-linux-androideabi"
              "aarch64-linux-android"
              "i686-linux-android"
              "x86_64-linux-android"
            ];
          };

          # Rust toolchain for dev (all targets)
          rustFull = pkgs.rust-bin.nightly.${rustVersion}.default.override {
            extensions = [ "rust-src" ];
            targets = [
              "wasm32-unknown-unknown"
              "wasm32-wasip2"
              "armv7-linux-androideabi"
              "aarch64-linux-android"
              "i686-linux-android"
              "x86_64-linux-android"
            ];
          };

          ndkHostTag =
            if pkgs.stdenv.isDarwin then
              (
                if pkgs.stdenv.hostPlatform.parsed.cpu.name == "aarch64" then
                  "darwin-aarch64"
                else
                  "darwin-x86_64"
              )
            else
              (
                if pkgs.stdenv.hostPlatform.parsed.cpu.name == "aarch64" then
                  "linux-aarch64"
                else
                  "linux-x86_64"
              );

          dioxusBuildInputs = with pkgs; [
            python314
            wayland
            wayland-protocols
            wayland-scanner
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

          androidEnvVars =
            { androidSdk }:
            let
              sdkPath = if androidSdk ? sdk then androidSdk.sdk else androidSdk;
            in
            {
              ANDROID_SDK_ROOT = "${sdkPath}/libexec/android-sdk";
              ANDROID_HOME = "${sdkPath}/libexec/android-sdk";
              ANDROID_NDK_ROOT = "${sdkPath}/libexec/android-sdk/ndk-bundle";
              ANDROID_NDK_TOOLCHAIN_BIN_DIR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin";
              GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${sdkPath}/libexec/android-sdk/build-tools/${androidBuildToolsVersion}/aapt2";

              CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/armv7a-linux-androideabi${androidApiLevel}-clang";
              CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
              CC_armv7_linux_androideabi = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/armv7a-linux-androideabi${androidApiLevel}-clang";
              CXX_armv7_linux_androideabi = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/armv7a-linux-androideabi${androidApiLevel}-clang++";
              AR_armv7_linux_androideabi = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";

              CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/aarch64-linux-android${androidApiLevel}-clang";
              CARGO_TARGET_AARCH64_LINUX_ANDROID_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
              CC_aarch64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/aarch64-linux-android${androidApiLevel}-clang";
              CXX_aarch64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/aarch64-linux-android${androidApiLevel}-clang++";
              AR_aarch64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
              CMAKE_SYSTEM_PROCESSOR_aarch64_linux_android = "aarch64";
              CMAKE_ANDROID_ARCH_ABI_aarch64_linux_android = "arm64-v8a";

              CARGO_TARGET_I686_LINUX_ANDROID_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/i686-linux-android${androidApiLevel}-clang";
              CARGO_TARGET_I686_LINUX_ANDROID_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
              CC_i686_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/i686-linux-android${androidApiLevel}-clang";
              CXX_i686_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/i686-linux-android${androidApiLevel}-clang++";
              AR_i686_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";

              CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/x86_64-linux-android${androidApiLevel}-clang";
              CARGO_TARGET_X86_64_LINUX_ANDROID_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
              CC_x86_64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/x86_64-linux-android${androidApiLevel}-clang";
              CXX_x86_64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/x86_64-linux-android${androidApiLevel}-clang++";
              AR_x86_64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
            };

          baseBuildInputs = with pkgs; [
            pkg-config
            openssl
            protobuf
            mold
            deno
            libarchive
          ];

          rustLintInputs = with pkgs; [
            cargo-udeps
            prek
            cargo-nextest
          ];

          kotliLintTools = with pkgs; [
            ktlint
          ];

          androidBuildInputs = with pkgs; [
            androidSdkOnly.androidsdk
            openjdk21
            cmake
          ];

          desktopBuildInputs = with pkgs; [
            openjdk21
            # graalvmPackages.graalvm-ce
            # curl
            # file
            # patchelf
            # binutils
            appimage-run
            v4l-utils
            libv4l
          ];

          washBuildInputs = with pkgs; [ ];

          devTools = with pkgs; [
            rogcat
            opentofu
            terragrunt
            tokio-console
            infisical
            cargo-ndk
            wac-cli
            wasmtime
            wasm-tools
            cargo-leptos
            trunk
          ];

          devOnlyInputs = with pkgs; [
            # FIXME: why do we need golang for again?
            # did an llm strip comments?
            # go
            androidComposition
            v4l-utils
            libv4l
            gh
          ];

          desktopRuntimeLibPackages = with pkgs; [
            sqlite.dev
            llvmPackages.libclang
            libxrender
            libxext
            libxtst
            libx11
            libxi
            libxrandr
            libxcb
            libxkbcommon
            freetype
            fontconfig
            libglvnd
            vulkan-loader
          ];

          desktopRuntimeLibraryPath = pkgs.lib.makeLibraryPath (
            pkgs.lib.map (packageValue: pkgs.lib.getLib packageValue) desktopRuntimeLibPackages
          );

          devShellBuildInputs =
            baseBuildInputs
            ++ rustLintInputs
            ++ dioxusBuildInputs
            ++ androidBuildInputs
            ++ desktopBuildInputs
            ++ washBuildInputs
            ++ devTools
            ++ kotliLintTools
            ++ devOnlyInputs
            ++ [ rustFull ];

          ciRustShell = pkgs.mkShell ({
            name = "ci-rust";
            buildInputs =
              baseBuildInputs
              ++ rustLintInputs
              ++ [
                rustRust
                pkgs.llvmPackages.clang
                pkgs.llvmPackages.libclang
                pkgs.stdenv.cc.cc.lib
              ];
            shellHook = ''
              export LIBCLANG_PATH="${pkgs.lib.getLib pkgs.llvmPackages.libclang}/lib"
              export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${
                pkgs.lib.makeLibraryPath [
                  (pkgs.lib.getLib pkgs.llvmPackages.libclang)
                  pkgs.stdenv.cc.cc.lib
                ]
              }"
            '';
          } // ghjkMainEnv);

          ciAndroidShell =
            pkgs.mkShell ({
              name = "ci-android";
              buildInputs = baseBuildInputs ++ androidBuildInputs ++ [ rustAndroid ];
            } // ghjkMainEnv // androidEnvVars { androidSdk = androidSdkOnly.androidsdk; });

          ciDesktopShell = pkgs.mkShell ({
            name = "ci-desktop";
            buildInputs = baseBuildInputs ++ dioxusBuildInputs ++ desktopBuildInputs ++ [ rustRust ];
            shellHook = ''
              export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${desktopRuntimeLibraryPath}"
              if [ "$(uname -s)" = "Darwin" ]; then
                export DYLD_LIBRARY_PATH="$LD_LIBRARY_PATH"
              fi
            '';
          } // ghjkMainEnv);

          ciComposeShell = pkgs.mkShell ({
            name = "ci-compose";
            buildInputs = baseBuildInputs ++ [ pkgs.openjdk21 rustRust ];
          } // ghjkMainEnv);

          devShell =
            pkgs.mkShell ({
              name = "dev";

              buildInputs = devShellBuildInputs;

              shellHook = ''
                export XDG_DATA_DIRS=${pkgs.fontconfig.out}/share:$XDG_DATA_DIRS
                export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${self}/target/debug/:${
                  pkgs.lib.makeLibraryPath (
                    pkgs.lib.map (packageValue: pkgs.lib.getLib packageValue) (
                      devShellBuildInputs ++ desktopRuntimeLibPackages
                    )
                  )
                }"
                if [ "$(uname -s)" = "Darwin" ]; then
                  export DYLD_LIBRARY_PATH="$LD_LIBRARY_PATH"
                fi
                export PATH=$PATH:$PWD/x/
                if [ -e .env ]; then
                  source "$PWD/x/load-dotenv-safe.sh" .env
                fi
                if [[ -t 0 ]]; then
                  exec $(getent passwd $USER | cut -d: -f7)
                fi
              '';
            } // ghjkMainEnv // ghjkDevEnv // (androidEnvVars { androidSdk = androidComposition; }));

        in
        {
          devShells = {
            default = devShell;
            dev = devShell;
            ci-rust = ciRustShell;
            ci-android = ciAndroidShell;
            ci-desktop = ciDesktopShell;
            ci-compose = ciComposeShell;
          };
        };
    };
}
