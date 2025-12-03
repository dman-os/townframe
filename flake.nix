{
  description = "yep";

  inputs = {
    nixpkgs.url       = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url   = "github:numtide/flake-utils";
    rust-overlay.url  = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:

    flake-utils.lib.eachDefaultSystem (system:
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
        androidComposition = (pkgs.android-studio.withSdk (
          pkgs.androidenv.composeAndroidPackages { 
            includeNDK = true; 
            platformToolsVersion = "36.0.0";
            buildToolsVersions = [ androidBuildToolsVersion  ];
            platformVersions = [ "36" ];
          }
        ).androidsdk);

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

        # Base shell with just the development environment setup
        baseShell = pkgs.mkShell rec {
          name = "devshell-base";

          ANDROID_SDK_ROOT = "${androidComposition.sdk}/libexec/android-sdk";
          ANDROID_HOME = "${ANDROID_SDK_ROOT}";
          ANDROID_NDK_ROOT = "${ANDROID_SDK_ROOT}/ndk-bundle";
          # BINDGEN_EXTRA_CLANG_ARGS_aarch64_linux_android="--sysroot=${ANDROID_NDK_ROOT}/sysroot"
          ANDROID_NDK_TOOLCHAIN_BIN_DIR = "${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/${ndkHostTag}/bin";
          GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${ANDROID_SDK_ROOT}/build-tools/${androidBuildToolsVersion}/aapt2";
          
          # ARMv7 (armeabi-v7a)
          CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_LINKER = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/armv7a-linux-androideabi${androidApiLevel}-clang";
          CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_AR = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";
          # Ensure C toolchain is used by cc crate when cross-compiling
          CC_armv7_linux_androideabi = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/armv7a-linux-androideabi${androidApiLevel}-clang";
          AR_armv7_linux_androideabi = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";

          # ARM64 (arm64-v8a)
          CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/aarch64-linux-android${androidApiLevel}-clang";
          CARGO_TARGET_AARCH64_LINUX_ANDROID_AR = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";
          CC_aarch64_linux_android = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/aarch64-linux-android${androidApiLevel}-clang";
          AR_aarch64_linux_android = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";
          # CMake variables for aws-lc-sys to detect correct architecture
          CMAKE_SYSTEM_PROCESSOR_aarch64_linux_android = "aarch64";
          CMAKE_ANDROID_ARCH_ABI_aarch64_linux_android = "arm64-v8a";

          # x86
          CARGO_TARGET_I686_LINUX_ANDROID_LINKER = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/i686-linux-android${androidApiLevel}-clang";
          CARGO_TARGET_I686_LINUX_ANDROID_AR = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";
          CC_i686_linux_android = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/i686-linux-android${androidApiLevel}-clang";
          AR_i686_linux_android = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";

          # x86_64
          CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/x86_64-linux-android${androidApiLevel}-clang";
          CARGO_TARGET_X86_64_LINUX_ANDROID_AR = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";
          CC_x86_64_linux_android = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/x86_64-linux-android${androidApiLevel}-clang";
          AR_x86_64_linux_android = "${ANDROID_NDK_TOOLCHAIN_BIN_DIR}/llvm-ar";

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

          washBuildInputs = with pkgs; [

            # needed to build aws-something-something on android
            cmake

          ];

          tools = with pkgs; [
            rogcat
            opentofu
            terragrunt

            # checkov
            # terrascan
            # trivy
            
            tokio-console
            infisical
            rustChannel
          ];

          buildInputs = dioxusBuildInputs ++ washBuildInputs ++ tools ++ (with pkgs; [

            go
            openssl
            androidComposition

            # ollama

            # android-tools
            # (
            #   android-studio.withSdk (
            #     androidenv.composeAndroidPackages { 
            #       includeNDK = true; 
            #     }
            #   ).androidsdk
            # )
            # clang
            # llvmPackages.libclang
            pkg-config
            # libudev-sys

            # openjdk21
            # sqlite
            # deno

            # bashInteractive
            # zsh
            # fish
            # needed to build tonic for console-subscriber
            protobuf
          ]);

          shellHook = with pkgs; ''
            export XDG_DATA_DIRS=${fontconfig.out}/share:$XDG_DATA_DIRS
            export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${self}/target/debug/:${lib.makeLibraryPath (lib.map (x: lib.getLib x) (buildInputs ++ [ 
              # needed by daybook_compose desktop
              sqlite.dev
              llvmPackages.libclang.dev
              xorg.libXrender
              xorg.libXext
              xorg.libXtst
              xorg.libX11
              xorg.libXi
              xorg.libXrandr
              xorg.libxcb
              libxkbcommon
              freetype
              fontconfig
              libglvnd

              vulkan-loader
            ]))}"
            if [ "$(uname -s)" = "Darwin" ]; then
              export DYLD_LIBRARY_PATH="$LD_LIBRARY_PATH"
            fi
            exec $(getent passwd $USER | cut -d: -f7)
            # # If $SHELL is set, re-exec into it
            # if [ -n "$SHELL" ]; then
            #   exec "$SHELL"
            # fi
          '';
        };

      in {
        devShells = {
          # Default shell that doesn't exec into interactive shell
          default = baseShell;
        };
      }
    );
}

