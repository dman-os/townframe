# Fixed-output derivation that materialises `wit/deps/` for every crate that has a
# `wkg.toml`/`wkg.lock`, mirroring `x/wit-fetch.ts` (minus its post-fetch doc-fence
# patch, which only matters for doctests the package build does not run).
#
# `wit/deps` is gitignored, so a flake source never contains it; without this FOD a
# sandboxed build cannot resolve `wit_bindgen::generate!` imports (wasi:*, wasmcloud:*).
# The wash binary is pinned to the same version CI installs (v2.6.1) and downloaded
# from the wasmCloud release; the whole output is content-addressed by `outputHash`,
# so network access inside the builder is safe.
#
# Refresh procedure when a wkg.lock or local .wit changes: set `outputHash` to
# `lib.fakeHash`, `nix build .#wit-deps`, and copy the "got" hash back.
{ pkgs }:
let
  inherit (pkgs) lib;
  repoRoot = ../..;
  washRelease = "v2.6.1";
  washAsset = {
    x86_64-linux = {
      name = "wash-x86_64-unknown-linux-musl";
      hash = "sha256-CuNI0aSTQIXt9WTF0nIxABr+ffKdetdqlTtCgGCxmZU=";
    };
    aarch64-linux = {
      name = "wash-aarch64-unknown-linux-musl";
      hash = "sha256-2pWmbyRanSQIDg8t4KJgq87QkqLbu2JoNKTloeTv+5I=";
    };
    x86_64-darwin = {
      name = "wash-x86_64-apple-darwin";
      hash = "sha256-NCgH6VEpo0JspV/0PltyJVunI6upIC3EUkVRfPUBMnU=";
    };
    aarch64-darwin = {
      name = "wash-aarch64-apple-darwin";
      hash = "sha256-tyiP0W9P37MtG4Hhs4IRDG6l2jQkA5QmL3cSmUt0mS8=";
    };
  };
  system = pkgs.stdenv.hostPlatform.system;
  asset =
    washAsset.${system}
      or (throw "no pinned wash asset for system ${system}; add one to src/nix/wit-deps.nix");
  washFile = pkgs.fetchurl {
    url = "https://github.com/wasmCloud/wasmCloud/releases/download/${washRelease}/${asset.name}";
    inherit (asset) hash;
  };
  # Input tree: every wkg manifest/lock plus every .wit file. Local override
  # targets (../foo/wit) resolve because the whole src/ tree is mirrored. Any
  # locally present wit/deps is irrelevant: the fetch runs with --clean, so the
  # output is always a fresh resolution of the locks (matching the hash below).
  witFetchInputs = lib.fileset.toSource {
    root = repoRoot;
    fileset = lib.fileset.fileFilter (
      file: file.name == "wkg.toml" || file.name == "wkg.lock" || file.hasExt "wit"
    ) (repoRoot + "/src");
  };
in
pkgs.runCommand "townframe-wit-deps"
  {
    outputHashMode = "recursive";
    outputHashAlgo = "sha256";
    outputHash = "sha256-Mj1b+NetSYJimREROzcItC75Xpy752YLvNVinTwyD5k=";
  }
  ''
    export HOME="$TMPDIR"
    # The sandbox has no /etc/ssl; wash's reqwest client reads SSL_CERT_FILE
    # (verified: pointing it at an unloadable file reproduces the panic).
    export SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
    cp ${washFile} ./wash
    chmod +x ./wash
    WASH="$PWD/wash"

    cp -r ${witFetchInputs} tree
    chmod -R u+w tree
    cd tree
    for dir in $(find . \( -name wkg.toml -o -name wkg.lock \) -print | xargs -n1 dirname | sort -u); do
      (cd "$dir" && "$WASH" wit fetch --clean)
    done
    cd ..

    mkdir -p "$out"
    for deps in $(find tree -type d -path '*/wit/deps' | sort); do
      rel=''${deps#tree/}
      mkdir -p "$out/$(dirname "$rel")"
      cp -r "$deps" "$out/$rel"
    done
  ''
