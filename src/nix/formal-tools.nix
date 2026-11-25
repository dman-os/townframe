{ pkgs }:
let
  quintVersion = "0.33.0";
  evaluatorVersion = "0.7.0";
  # 0.62.4, not the 0.62.1 that quint 0.33.0 was built against: 0.62.1 caps a
  # single JSON string at Jackson's 20 MB default, and quint sends the whole
  # flattened spec to the Apalache server as one string in the JSON-RPC config.
  # This repository's model flattens to ~20.05 MB, so every `quint verify` (and
  # `quint compile --target=tlaplus`) died in `<RPC configuration>: Invalid
  # strict JSON: String value length (20051112) exceeds the maximum allowed`
  # before a single property was checked. Jackson's limit cannot be raised from
  # outside the JVM in the version bundled there, so the fix has to come from
  # Apalache: 0.62.4 raises the JSON content limits from 20 MB to 1 GB
  # (apalache-mc/apalache#3494). 0.62.4 is a bug-fix release within 0.62.x.
  apalacheVersion = "0.62.4";

  source = pkgs.fetchFromGitHub {
    owner = "quint-co";
    repo = "quint";
    rev = "v${quintVersion}";
    hash = "sha256-ywcK3GahcA/GWoKTAYr9lSU7k2r1OfA5OALp4f04Vxs=";
  };

  evaluator = pkgs.rustPlatform.buildRustPackage {
    pname = "quint-evaluator";
    version = evaluatorVersion;
    src = source;
    sourceRoot = "source/evaluator";
    cargoHash = "sha256-aGVs/J+lAPHOsi01xShfZHBeUjd6eONpraNuMkaVfO8=";
    doCheck = false;
  };

  apalache = pkgs.fetchzip {
    url = "https://github.com/apalache-mc/apalache/releases/download/v${apalacheVersion}/apalache.tgz";
    hash = "sha256-N5+OWsL+x37q8QsJRSeHlZk+Hfs9wQNixy2PZOea6Hg=";
  };
in
assert pkgs.quint."quint-cli".version == quintVersion;
pkgs.stdenvNoCC.mkDerivation {
  pname = "quint";
  version = quintVersion;
  dontUnpack = true;
  nativeBuildInputs = [ pkgs.makeWrapper ];

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin
    makeWrapper ${pkgs.nodejs_24}/bin/node $out/bin/quint \
      --add-flags "${pkgs.quint."quint-cli"}/share/quint/dist/src/cli.js" \
      --set QUINT_HOME "$out/share/quint" \
      --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.openjdk21 ]}

    install -Dm755 ${evaluator}/bin/quint_evaluator -t $out/share/quint/rust-evaluator-v${evaluatorVersion}/

    mkdir -p $out/share/quint/apalache-dist-${apalacheVersion}
    cp -r ${apalache} $out/share/quint/apalache-dist-${apalacheVersion}/apalache
    chmod +x $out/share/quint/apalache-dist-${apalacheVersion}/apalache/bin/apalache-mc
    # quint 0.33.0's compiled-in default Apalache version is 0.62.1 (see
    # DEFAULT_APALACHE_VERSION_TAG in dist/src/apalache.js), and it looks the
    # distribution up at $QUINT_HOME/apalache-dist-<version>. Without this alias
    # a bare `quint verify` would miss the directory we ship and try to download
    # 0.62.1 into $out, which is read-only. Aliasing is safe here: 0.62.4 is a
    # bug-fix-only release on the same 0.62 line, and callers that want to pin a
    # version still get this one, which is the only one installed.
    ln -s apalache-dist-${apalacheVersion} $out/share/quint/apalache-dist-0.62.1

    runHook postInstall
  '';

  passthru = {
    inherit apalache evaluator;
  };

  meta = {
    description = "Quint with pre-provisioned Apalache and Rust evaluator";
    mainProgram = "quint";
  };
}
