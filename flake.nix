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
        androidComposition = (pkgs.android-studio.withSdk (
          pkgs.androidenv.composeAndroidPackages { 
            includeNDK = true; 
            platformToolsVersion = "35.0.1";
            buildToolsVersions = [ androidBuildToolsVersion  ];
            platformVersions = [ "35" ];
          }
        ).androidsdk);

        rustVersion = "2025-09-01";
        rustChannel = pkgs.rust-bin.nightly.${rustVersion}.default.override {
          extensions = [ "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        # Base shell with just the development environment setup
        baseShell = pkgs.mkShell rec {
          name = "devshell-base";

          ANDROID_SDK_ROOT = "${androidComposition.sdk}/libexec/android-sdk";
          ANDROID_HOME = "${ANDROID_SDK_ROOT}";
          ANDROID_NDK_ROOT = "${ANDROID_SDK_ROOT}/ndk-bundle";
          GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${ANDROID_SDK_ROOT}/build-tools/${androidBuildToolsVersion}/aapt2";

          buildInputs = with pkgs; [
            rustChannel
            androidComposition
            android-tools
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
            openssl
            # kanidm CLI
            systemd # libudev-sys

            jdk21_headless
            # sqlite
            # deno

            # bashInteractive
            # zsh
            # fish
          ];

          shellHook = with pkgs; ''
            export XDG_DATA_DIRS=${fontconfig.out}/share:$XDG_DATA_DIRS
            export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${self}/target/debug/:${lib.makeLibraryPath [ 
              # needed by daybook_compose desktop
              (lib.getLib sqlite) 
              llvmPackages.libclang.lib 
              (lib.getLib xorg.libXrender) 
              (lib.getLib xorg.libXext)
              (lib.getLib xorg.libXtst)
              (lib.getLib xorg.libX11)
              (lib.getLib xorg.libXrender)
              (lib.getLib xorg.libXi)
              (lib.getLib freetype)
              (lib.getLib fontconfig)
              (lib.getLib libglvnd)
            ]}"
            if [ "$(uname -s)" = "Darwin" ]; then
              export DYLD_LIBRARY_PATH="$LD_LIBRARY_PATH"
            fi
            exec fish
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
