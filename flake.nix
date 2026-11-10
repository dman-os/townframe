{
  description = "yep";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane.url = "github:ipetkov/crane";
    # mbx shares compiler outputs and one memory-aware build scheduler between every
    # checkout on the machine. Its shim and cache agent refuse to talk across mbx
    # versions, so every worktree must pin the same tag: bump it on main and rebase.
    # nixpkgs deliberately does not follow ours, so the store path is the one
    # `nix run github:jdx/mr-boxington/<tag>` produces.
    mr-boxington.url = "github:jdx/mr-boxington/v1.30.0";
  };

  outputs =
    inputs@{
      flake-parts,
      nixpkgs,
      rust-overlay,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        ./src/nix/checks.nix
        ./src/nix/devshells.nix
        ./src/nix/packages.nix
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        # x86_64-darwin removed: nixpkgs 26.11 dropped support, so evaluating it
        # throws (the old systems list had rotted silently).
        "aarch64-darwin"
      ];

      perSystem =
        { system, ... }:
        {
          _module.args.pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
            config = {
              android_sdk.accept_license = true;
              allowUnfree = true;
            };
          };
        };
    };
}
