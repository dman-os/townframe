//! Builds the micros nodule's wasm component (ADR 052) and leaves it in `OUT_DIR`, where
//! `core_cartridge.rs` embeds it, so a node carries its core cartridge and the GUI works offline.
//!
//! The component is built by a nested cargo over the `src/wasm_utils_rs` workspace, the one that
//! holds every guest. The nested build:
//!
//! - uses that workspace's own target directory (`src/wasm_utils_rs/target`, which the mbx shim
//!   manages like the root's), never this build's: the outer build's `CARGO_TARGET_DIR`,
//!   `RUSTFLAGS` and wrapper are removed from its environment, because they describe a native
//!   build of this crate and would otherwise decide the guest's;
//! - passes `--locked`, so the guest's lockfile is what builds it and a stale one fails the build
//!   rather than being rewritten in a build script;
//! - inherits this build's jobserver (`CARGO_MAKEFLAGS` is left in place), so the two cargos
//!   share one job budget.
//!
//! Cargo reruns this script when the guest crates, the workspace manifest or its lockfile change,
//! and, once a build has left its dependency file, when any source the guest was built from
//! changes.

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

const GUEST: &str = "micros_nodule_guest";

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets it"));
    let src = manifest_dir
        .parent()
        .expect("big_node lives in the `src` directory");
    let workspace = src.join("wasm_utils_rs");
    let manifest = workspace.join("Cargo.toml");
    for path in [
        src.join("micros_nodule"),
        src.join(GUEST),
        manifest.clone(),
        workspace.join("Cargo.lock"),
    ] {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let cargo = std::env::var_os("CARGO").expect("cargo sets it for a build script");
    let status = Command::new(cargo)
        .args(["build", "--locked", "--manifest-path"])
        .arg(&manifest)
        .args(["-p", GUEST, "--target", "wasm32-wasip3", "--release"])
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTFLAGS")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_BUILD_TARGET")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .env_remove("CARGO_PRIMARY_PACKAGE")
        .env_remove("CARGO_MANIFEST_DIR")
        .status()
        .expect("running the nested cargo");
    assert!(
        status.success(),
        "building {GUEST} for wasm32-wasip3 failed: {status}"
    );

    let target = workspace.join("target/wasm32-wasip3/release");
    // The dependency file lists every source the guest was built from.
    if let Ok(deps) = std::fs::read_to_string(target.join(format!("{GUEST}.d"))) {
        for source in deps
            .lines()
            .filter_map(|line| line.split_once(": "))
            .flat_map(|(_, sources)| sources.split_whitespace())
        {
            if Path::new(source).is_file() {
                println!("cargo:rerun-if-changed={source}");
            }
        }
    }
    let out = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets it"));
    std::fs::copy(
        target.join(format!("{GUEST}.wasm")),
        out.join(format!("{GUEST}.wasm")),
    )
    .expect("copying the guest component");
}
