{
  description = "yep";

  inputs = {
    nixpkgs.url       = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url   = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [  ];
        };


        # Base shell with just the development environment setup
        baseShell = pkgs.mkShell {
          name = "devshell-base";
          buildInputs = with pkgs; [
            # clang
            # llvmPackages.libclang
            pkg-config
            openssl
            # kanidm CLI
            systemd # libudev-sys

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
