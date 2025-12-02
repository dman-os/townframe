//! Runtime installation and cluster initialization

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use tar::Archive;
use tracing::{debug, info};
use wasmtime::{Engine, Module, Store, TypedFunc};
use xz2::read::XzDecoder;

use crate::wire::WasiState;
use crate::Config;

/// URL to download pglite runtime
const PGLITE_DOWNLOAD_URL: &str = "https://electric-sql.github.io/pglite-build/pglite-wasi.tar.xz";

/// Install the pglite runtime to the configured paths
///
/// This will:
/// - Download the pglite-wasi.tar.xz if not present
/// - Unpack to pgroot
/// - Seed /dev/urandom
/// - Install any configured extensions
pub async fn install_runtime(config: &Config) -> Result<()> {
    fs::create_dir_all(&config.pgroot).context("create pgroot")?;
    fs::create_dir_all(&config.pgdata).context("create pgdata")?;

    // Download and unpack runtime if needed
    if !config.wasm_path().exists() {
        download_and_unpack_runtime(config).await?;
    }

    // Seed /dev/urandom for PostgreSQL's randomness needs
    seed_urandom(&config.dev_path())?;

    // Install extensions
    for ext_path in &config.extensions {
        install_extension(config, ext_path)?;
    }

    info!("runtime installation complete");
    Ok(())
}

/// Download pglite runtime and unpack to pgroot
async fn download_and_unpack_runtime(config: &Config) -> Result<()> {
    info!("downloading pglite runtime from {}", PGLITE_DOWNLOAD_URL);

    // Download using reqwest or curl - for simplicity, use blocking curl
    let tar_xz_path = config.pgroot.join("pglite-wasi.tar.xz");

    let output = std::process::Command::new("curl")
        .arg("-L")
        .arg("-o")
        .arg(&tar_xz_path)
        .arg(PGLITE_DOWNLOAD_URL)
        .output()
        .context("failed to run curl")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("curl failed: {}", stderr);
    }

    info!("unpacking runtime to {:?}", config.pgroot);

    // Unpack the tar.xz
    let file = fs::File::open(&tar_xz_path).context("open tar.xz")?;
    let decoder = XzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    archive.unpack(&config.pgroot).context("unpack tar.xz")?;

    // Normalize layout - move nested pglite dir if present
    normalize_runtime_layout(config)?;

    // Cleanup downloaded archive
    let _ = fs::remove_file(&tar_xz_path);

    Ok(())
}

/// Normalize the runtime layout after unpacking
///
/// Some archives have nested directories like tmp/pglite/...
/// We need to flatten to pgroot/pglite/...
fn normalize_runtime_layout(config: &Config) -> Result<()> {
    // Check for nested structure
    let nested = config.pgroot.join("tmp").join("pglite");
    if nested.join("bin").join("pglite.wasi").exists() {
        debug!("normalizing nested runtime layout");

        let target = config.pgroot.join("pglite");
        if target.exists() {
            fs::remove_dir_all(&target)?;
        }

        // Move nested pglite to pgroot
        if fs::rename(&nested, &target).is_err() {
            // If rename fails (cross-device), copy recursively
            copy_dir_recursive(&nested, &target)?;
            fs::remove_dir_all(config.pgroot.join("tmp"))?;
        }
    }

    Ok(())
}

/// Recursively copy a directory
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Seed /dev/urandom with random bytes
fn seed_urandom(dev_path: &Path) -> Result<()> {
    fs::create_dir_all(dev_path)?;
    let urandom_path = dev_path.join("urandom");
    if !urandom_path.exists() {
        let mut buf = [0u8; 128];
        getrandom::getrandom(&mut buf)?;
        fs::write(&urandom_path, buf)?;
        debug!("seeded urandom");
    }
    Ok(())
}

/// Install an extension from a tar.gz archive
fn install_extension(config: &Config, archive_path: &Path) -> Result<()> {
    info!("installing extension from {:?}", archive_path);

    let file = fs::File::open(archive_path)
        .with_context(|| format!("open extension archive {:?}", archive_path))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);

    let target = config.pgroot.join("pglite");
    fs::create_dir_all(&target)?;
    archive
        .unpack(&target)
        .with_context(|| format!("unpack extension {:?}", archive_path))?;

    Ok(())
}

/// Initialize the database cluster by running initdb via WASM
pub fn init_cluster(config: &Config, engine: &Engine, module: &Module) -> Result<()> {
    info!("running initdb to create cluster");

    fs::create_dir_all(&config.pgdata)?;

    // Create password file
    let pw_path = config.pgroot.join("pglite").join("password");
    if !pw_path.exists() {
        if let Some(parent) = pw_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&pw_path, "localdevpassword\n")?;
    }

    // Build WASI context
    let wasi = crate::wire::build_wasi_ctx(config)?;
    let mut store = Store::new(engine, WasiState { wasi });

    // Link and instantiate
    let mut linker = wasmtime::Linker::<WasiState>::new(engine);
    wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state| &mut state.wasi)?;

    let instance = linker.instantiate(&mut store, module)?;

    // Call _start for embed setup
    if let Ok(start) = instance.get_typed_func::<(), ()>(&mut store, "_start") {
        let _ = start.call(&mut store, ());
        debug!("_start completed");
    }

    // Call pgl_initdb
    let initdb: TypedFunc<(), i32> = instance
        .get_typed_func(&mut store, "pgl_initdb")
        .context("get pgl_initdb export")?;

    let rc = initdb.call(&mut store, ())?;
    info!("initdb returned: {}", rc);

    // Verify cluster was created
    if !config.cluster_exists() {
        anyhow::bail!("initdb did not create cluster (rc={})", rc);
    }

    // Best-effort shutdown
    if let Ok(shutdown) = instance.get_typed_func::<(), ()>(&mut store, "pgl_shutdown") {
        let _ = shutdown.call(&mut store, ());
    }

    info!("cluster initialization complete");
    Ok(())
}
