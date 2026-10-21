# Source-only checks. Everything here is a pure function of the tracked tree:
# no ./target, no node_modules, no network (fixed-output fetches aside), no
# host environment. That means identical trees hit the Nix cache instead of
# re-running the tool, and CI, agents and local runs get the same verdict.
# Each check consumes the narrowest fileset that can change its verdict so an
# unrelated edit does not invalidate it.
#
# Pre-commit runs these through a single `nix flake check` hook, which makes
# them the CI lint gate too; `nix fmt` (the formatter below) applies the fixes
# the format checks ask for.
#
# Deliberately not here (they need build artifacts, a package manager's module
# tree, or facts about the host): `cargo x sql gen --check` and
# `cargo x guardrails lint|api` (building xtask in the store would discard the
# warm ./target), clippy/check-matrix/nextest/llvm-cov (need ./target plus the
# agent-build jobserver), tsc/detekt/gradle/playwright (node_modules or the
# Android SDK), and `cargo x guardrails doctor` (asserts facts about the
# invoking shell's environment, which a sandbox cannot represent). Those stay
# as pre-commit hooks run inside the devshells.
_: {
  perSystem =
    { pkgs, ... }:
    let
      inherit (pkgs) lib;
      repoRoot = ../..;
      tools = import ./toolchains.nix { inherit pkgs; };

      srcOf =
        fileset:
        lib.fileset.toSource {
          root = repoRoot;
          inherit fileset;
        };
      anyExt = exts: file: builtins.any file.hasExt exts;

      nixSrc = srcOf (
        lib.fileset.unions [
          (repoRoot + "/flake.nix")
          ./.
        ]
      );

      rustSrc = srcOf (
        lib.fileset.unions [
          (repoRoot + "/rustfmt.toml")
          (lib.fileset.fileFilter (file: file.hasExt "rs") (repoRoot + "/src"))
        ]
      );

      # Every .rs file is formatted individually, not just the module trees
      # `cargo fmt --all` walks, so orphan files outside any crate tree are
      # covered too (the old pre-commit rustfmt hook's behaviour). Generated
      # `sqlite/gen.rs` files are `include!`d and owned by `cargo x sql gen`.
      # rustfmt takes the edition and options from the root rustfmt.toml.
      rustFiles = "find src -name '*.rs' -not -path '*/sqlite/gen.rs' -not -path '*/target/*' -not -path '*/node_modules/*' -print0";

      # package.json is included because deno.lock records its devDependencies in
      # the workspace.packageJson section (Deno auto-discovers it), so --frozen
      # resolution fails without it. Both derivations copy the tree into a
      # writable workdir: Deno materialises node_modules/ next to it
      # (nodeModulesDir auto) and store paths are read-only.
      denoSrc = srcOf (
        lib.fileset.unions [
          (repoRoot + "/deno.json")
          (repoRoot + "/deno.lock")
          (repoRoot + "/package.json")
          (repoRoot + "/x")
        ]
      );

      # biome.jsonc enables `ignoreUnknown`, so files of types biome does not
      # parse are skipped either way; restricting the fileset to the types it
      # does parse keeps unrelated edits from invalidating the check without
      # changing the verdict. The x/ tree is excluded by the config itself.
      biomeSrc = srcOf (
        lib.fileset.unions [
          (repoRoot + "/biome.jsonc")
          (lib.fileset.fileFilter (anyExt [
            "js"
            "cjs"
            "mjs"
            "jsx"
            "ts"
            "cts"
            "mts"
            "tsx"
            "css"
            "json"
            "jsonc"
            "html"
            "vue"
            "svelte"
            "astro"
            "graphql"
            "gql"
          ]) repoRoot)
        ]
      );

      repoFilesSrc = srcOf (
        lib.fileset.fileFilter (anyExt [
          "json"
          "toml"
          "yaml"
          "yml"
        ]) repoRoot
      );

      # The x/ scripts import jsr: modules (dax, std). A sandboxed `deno
      # check` has no network, so the module cache is materialised here as a
      # fixed-output derivation keyed on deno.lock via --frozen.
      # Refresh: set outputHash to lib.fakeHash, build `checks.<system>.deno`,
      # and copy the "got" hash from the failing deno-cache build.
      denoCache =
        pkgs.runCommand "townframe-deno-cache"
          {
            outputHashMode = "recursive";
            outputHashAlgo = "sha256";
            outputHash = "sha256-cYwVeSvdTzRGZmJigrDe5DHj1mhwJ3Atfima69M8uFE=";
            nativeBuildInputs = [ pkgs.deno ];
            SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
          }
          ''
            export DENO_DIR=$out
            mkdir -p $out
            cp -r ${denoSrc} work
            chmod -R u+w work
            cd work
            for f in x/*.ts; do
              deno cache --frozen "$f"
            done
          '';

      repoFilesCheck =
        pkgs.writers.writePython3 "townframe-repo-files-check"
          {
            libraries = [ pkgs.python3Packages.pyyaml ];
          }
          ''
            import json
            import pathlib
            import sys
            import tomllib

            import yaml

            root = pathlib.Path(sys.argv[1])
            failures = []

            seen_lower = {}
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                rel = path.relative_to(root)
                lowered = str(rel).lower()
                if lowered in seen_lower:
                    failures.append(
                        f"case-insensitive path collision: {seen_lower[lowered]} vs {rel}"
                    )
                else:
                    seen_lower[lowered] = rel

                suffix = path.suffix.lower()
                try:
                    if suffix == ".json":
                        # .vscode allows comments/trailing commas; pre-commit's
                        # check-json excludes it for the same reason.
                        if rel.parts[0] == ".vscode":
                            continue
                        json.loads(path.read_text())
                    elif suffix == ".toml":
                        tomllib.loads(path.read_text())
                    elif suffix in (".yaml", ".yml"):
                        yaml.safe_load(path.read_text())
                except Exception as err:
                    failures.append(f"{rel}: {err}")

            for failure in failures:
                print(failure, file=sys.stderr)
            if failures:
                sys.exit(1)
          '';
    in
    {
      formatter = pkgs.writeShellApplication {
        name = "townframe-fmt";
        runtimeInputs = [
          pkgs.nixfmt
          tools.rustRust
          pkgs.deno
          pkgs.biome
          pkgs.findutils
        ];
        text = ''
          if [ ! -f flake.nix ] || [ ! -d src/nix ]; then
            echo "townframe-fmt formats the whole repository; run it from the repository root" >&2
            exit 1
          fi
          nixfmt flake.nix src/nix/*.nix
          ${rustFiles} | xargs -0 rustfmt
          deno fmt ./x/
          biome check --write .
        '';
      };

      checks = {
        nix-format =
          pkgs.runCommand "townframe-nix-format-check" { nativeBuildInputs = [ pkgs.nixfmt ]; }
            ''
              cd ${nixSrc}
              nixfmt --check flake.nix src/nix/*.nix
              touch $out
            '';

        nix-lint =
          pkgs.runCommand "townframe-nix-lint-check"
            {
              nativeBuildInputs = [
                pkgs.statix
                pkgs.deadnix
              ];
            }
            ''
              cd ${nixSrc}
              statix check flake.nix
              statix check src/nix
              deadnix --fail flake.nix src/nix
              touch $out
            '';

        # The pinned toolchain supplies rustfmt, so formatting cannot drift
        # between `nix fmt`, local runs and this check.
        rust-fmt = pkgs.runCommand "townframe-rust-fmt-check" { nativeBuildInputs = [ tools.rustRust ]; } ''
          cd ${rustSrc}
          ${rustFiles} | xargs -0 rustfmt --check
          touch $out
        '';

        deno = pkgs.runCommand "townframe-deno-check" { nativeBuildInputs = [ pkgs.deno ]; } ''
          export DENO_DIR=${denoCache}
          export HOME=$TMPDIR
          cp -r ${denoSrc} work
          chmod -R u+w work
          cd work
          deno fmt --check ./x/
          deno lint ./x/
          deno check --frozen ./x/
          touch $out
        '';

        # VCS mode is disabled: the sandbox has no .git, and the tracked-only
        # Nix source already excludes what .gitignore would (node_modules,
        # target dirs are never in the store input).
        biome = pkgs.runCommand "townframe-biome-check" { nativeBuildInputs = [ pkgs.biome ]; } ''
          cd ${biomeSrc}
          biome check --vcs-enabled=false --vcs-use-ignore-file=false .
          touch $out
        '';

        repo-files = pkgs.runCommand "townframe-repo-files-check" { } ''
          ${repoFilesCheck} ${repoFilesSrc}
          touch $out
        '';
      };
    };
}
