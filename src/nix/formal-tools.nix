{ pkgs }:
let
  quintVersion = "0.33.0";
  evaluatorVersion = "0.7.0";
  apalacheVersion = "0.62.1";

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
    hash = "sha256-/BLs3nfUuYeGhBmiA/Fa0mzOyKibMHdnG9rU/6ZU6ZM=";
  };
in
assert pkgs.quint-cli.version == quintVersion;
pkgs.stdenvNoCC.mkDerivation {
  pname = "quint";
  version = quintVersion;
  dontUnpack = true;
  nativeBuildInputs = [ pkgs.makeWrapper ];

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin
    makeWrapper ${pkgs.nodejs_24}/bin/node $out/bin/quint \
      --add-flags "${pkgs.quint-cli}/share/quint/dist/src/cli.js" \
      --set QUINT_HOME "$out/share/quint" \
      --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.openjdk21 ]}

    install -Dm755 ${evaluator}/bin/quint_evaluator -t $out/share/quint/rust-evaluator-v${evaluatorVersion}/

    mkdir -p $out/share/quint/apalache-dist-${apalacheVersion}
    cp -r ${apalache} $out/share/quint/apalache-dist-${apalacheVersion}/apalache
    chmod +x $out/share/quint/apalache-dist-${apalacheVersion}/apalache/bin/apalache-mc

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
