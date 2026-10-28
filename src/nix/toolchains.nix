# Shared Rust toolchain definitions used by the dev shells (src/nix/devshells.nix)
# and by packaged builds (src/nix/packages.nix). Keeping them in one place means a
# `nix build .#daybook-cli` compiles with the exact rustc the CI shells use.
{ pkgs }:
let
  rustVersion = "2026-09-17";
in
{
  inherit rustVersion;

  # Rust toolchain for CI and packaged builds (wasm32 + native Linux targets)
  rustRust = pkgs.rust-bin.nightly.${rustVersion}.default.override {
    extensions = [
      "rust-src"
      "llvm-tools-preview"
    ];
    targets = [
      "wasm32-unknown-unknown"
      "wasm32-wasip2"
    ]
    ++ (
      if pkgs.stdenv.hostPlatform.isLinux then
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
}
