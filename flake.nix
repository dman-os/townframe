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
        
        # Android SDK/NDK without Studio (for CI)
        androidSdkOnly = pkgs.androidenv.composeAndroidPackages { 
          includeNDK = true; 
          platformToolsVersion = "36.0.0";
          buildToolsVersions = [ androidBuildToolsVersion  ];
          platformVersions = [ "36" ];
        };
        
        # Android SDK/NDK with Studio (for dev)
        androidComposition = pkgs.android-studio.withSdk androidSdkOnly.androidsdk;

        rustVersion = "2026-01-01";
        
        # Rust toolchain for CI (wasm32 + native Linux targets)
        rustRust = pkgs.rust-bin.nightly.${rustVersion}.default.override {
          extensions = [ "rust-src" ];
          targets = [ 
            "wasm32-unknown-unknown" 
            "wasm32-wasip2" 
          ] ++ (if pkgs.stdenv.isLinux then [
            "x86_64-unknown-linux-gnu"
            "aarch64-unknown-linux-gnu"
          ] else []);
        };
        
        # Rust toolchain for Android CI (wasm32 + Android targets)
        rustAndroid = pkgs.rust-bin.nightly.${rustVersion}.default.override {
          extensions = [ "rust-src" ];
          targets = [ 
            "wasm32-unknown-unknown" 
            "wasm32-wasip2" 
            "armv7-linux-androideabi" # For armeabi-v7a
            "aarch64-linux-android" # For arm64-v8a
            "i686-linux-android" # For x86
            "x86_64-linux-android"  # For x86_64
          ];
        };
        
        # Rust toolchain for dev (all targets)
        rustFull = pkgs.rust-bin.nightly.${rustVersion}.default.override {
          extensions = [ "rust-src" ];
          targets = [ 
            "wasm32-unknown-unknown" 
            "wasm32-wasip2" 
            "armv7-linux-androideabi" # For armeabi-v7a
            "aarch64-linux-android" # For arm64-v8a
            "i686-linux-android" # For x86
            "x86_64-linux-android"  # For x86_64
          ];
        };

        # Map NDK host tag from nix platform (needed for correct toolchain path)
        ndkHostTag = if pkgs.stdenv.isDarwin then
          (if pkgs.stdenv.hostPlatform.parsed.cpu.name == "aarch64" then "darwin-aarch64" else "darwin-x86_64")
        else
          (if pkgs.stdenv.hostPlatform.parsed.cpu.name == "aarch64" then "linux-aarch64" else "linux-x86_64");

        # Desktop UI libraries (for dioxus/Compose desktop)
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

        # Function to generate Android environment variables
        # androidSdk can be either androidSdkOnly.androidsdk or androidComposition
        androidEnvVars = { androidSdk }: 
          let
            sdkPath = if androidSdk ? sdk then androidSdk.sdk else androidSdk;
          in {
            ANDROID_SDK_ROOT = "${sdkPath}/libexec/android-sdk";
            ANDROID_HOME = "${sdkPath}/libexec/android-sdk";
            ANDROID_NDK_ROOT = "${sdkPath}/libexec/android-sdk/ndk-bundle";
            ANDROID_NDK_TOOLCHAIN_BIN_DIR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin";
            GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${sdkPath}/libexec/android-sdk/build-tools/${androidBuildToolsVersion}/aapt2";
            
            # ARMv7 (armeabi-v7a)
            CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/armv7a-linux-androideabi${androidApiLevel}-clang";
            CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
            CC_armv7_linux_androideabi = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/armv7a-linux-androideabi${androidApiLevel}-clang";
            AR_armv7_linux_androideabi = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";

            # ARM64 (arm64-v8a)
            CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/aarch64-linux-android${androidApiLevel}-clang";
            CARGO_TARGET_AARCH64_LINUX_ANDROID_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
            CC_aarch64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/aarch64-linux-android${androidApiLevel}-clang";
            AR_aarch64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
            CMAKE_SYSTEM_PROCESSOR_aarch64_linux_android = "aarch64";
            CMAKE_ANDROID_ARCH_ABI_aarch64_linux_android = "arm64-v8a";

            # x86
            CARGO_TARGET_I686_LINUX_ANDROID_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/i686-linux-android${androidApiLevel}-clang";
            CARGO_TARGET_I686_LINUX_ANDROID_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
            CC_i686_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/i686-linux-android${androidApiLevel}-clang";
            AR_i686_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";

            # x86_64
            CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/x86_64-linux-android${androidApiLevel}-clang";
            CARGO_TARGET_X86_64_LINUX_ANDROID_AR = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
            CC_x86_64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/x86_64-linux-android${androidApiLevel}-clang";
            AR_x86_64_linux_android = "${sdkPath}/libexec/android-sdk/ndk-bundle/toolchains/llvm/prebuilt/${ndkHostTag}/bin/llvm-ar";
          };

        # Base build tools (used by all shells)
        baseBuildInputs = with pkgs; [
          pkg-config
          openssl
          protobuf
        ];

        # Rust linting tools
        rustLintInputs = with pkgs; [
          cargo-udeps
          prek
        ];

        # CI Rust shell (for Rust linting/checking and CLI builds)
        ciRustShell = pkgs.mkShell {
          name = "ci-rust";
          
          buildInputs = baseBuildInputs ++ rustLintInputs ++ (with pkgs; [
            rustRust
          ]);
        };

        # Android-specific build inputs
        androidBuildInputs = with pkgs; [
          androidSdkOnly.androidsdk
          openjdk21
          cmake
        ];

        # CI Android shell (for Android builds/lints)
        ciAndroidShell = pkgs.mkShell {
          name = "ci-android";
          
          buildInputs = baseBuildInputs ++ androidBuildInputs ++ (with pkgs; [
            rustAndroid
          ]);
          
          # Android environment variables
        } // (androidEnvVars { androidSdk = androidSdkOnly.androidsdk; });

        # Desktop-specific build inputs
        desktopBuildInputs = with pkgs; [
          openjdk21
          # appimagetools
        ];

        # CI Desktop shell (for Compose desktop builds/lints)
        ciDesktopShell = pkgs.mkShell {
          name = "ci-desktop";
          
          buildInputs = baseBuildInputs ++ dioxusBuildInputs ++ desktopBuildInputs ++ (with pkgs; [
            rustRust
          ]);
        };

        # Dev shell (full local development)
        # Includes everything from all other shells plus additional dev tools
        # Note: cmake is already in androidBuildInputs, so not duplicated here
        washBuildInputs = with pkgs; [
          # Additional wasmcloud build inputs if needed
        ];

        devTools = with pkgs; [
          rogcat
          opentofu
          terragrunt

          # checkov
          # terrascan
          # trivy
          
          tokio-console
          infisical
        ];

        devOnlyInputs = with pkgs; [
          go
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
          # libudev-sys

          # sqlite
          # deno

          # bashInteractive
          # zsh
          # fish
          # needed to build tonic for console-subscriber
          v4l-utils     # v4l2-ctl, device discovery
          libv4l        # V4L2 compatibility layer
        ];

        devShell = pkgs.mkShell rec {
          name = "dev";

          buildInputs = 
            baseBuildInputs ++
            rustLintInputs ++
            dioxusBuildInputs ++
            androidBuildInputs ++
            desktopBuildInputs ++
            washBuildInputs ++
            devTools ++
            devOnlyInputs ++
            (with pkgs; [
              rustFull
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
          
          # Android environment variables
        } // (androidEnvVars { androidSdk = androidComposition; });

      in {
        devShells = {
          default = devShell;  # backward compatibility
          dev = devShell;
          ci-rust = ciRustShell;
          ci-android = ciAndroidShell;
          ci-desktop = ciDesktopShell;
        };
      }
    );
}
