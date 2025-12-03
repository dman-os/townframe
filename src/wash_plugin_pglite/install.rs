//! Runtime installation and cluster initialization

use crate::interlude::*;

use std::path::Path;

use flate2::read::GzDecoder;
use tar::Archive;
use tokio::fs;
use wasmtime::{Engine, Module, Store};

use crate::wire::WasiState;
use crate::Config;

/// Embedded pglite runtime archive (compressed with zstd)
/// Generated at build time by build.rs
const EMBEDDED_PGLITE_ZST: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/pglite_embedded.tar.zst"));

/// Install the pglite runtime to the configured paths
///
/// This will:
/// - Download the pglite-wasi.tar.xz if not present
/// - Unpack to pgroot
/// - Seed /dev/urandom
/// - Install any configured extensions
pub async fn install_runtime(config: &Config) -> Res<()> {
    fs::create_dir_all(&config.pgroot)
        .await
        .wrap_err("create pgroot")?;
    fs::create_dir_all(&config.pgdata)
        .await
        .wrap_err("create pgdata")?;

    // Download and unpack runtime if needed
    if !config.wasm_path().exists() {
        unpack_runtime(config).await?;
    }

    // Seed /dev/urandom for PostgreSQL's randomness needs
    seed_urandom(&config.dev_path()).await?;

    // Install extensions
    for ext_path in &config.extensions {
        install_extension(config, ext_path).await?;
    }

    info!("runtime installation complete");
    Ok(())
}

/// Extract embedded pglite runtime and unpack to pgroot
async fn unpack_runtime(config: &Config) -> Res<()> {
    info!("extracting embedded pglite runtime to {:?}", config.pgroot);

    // Decompress and unpack embedded archive (spawn blocking)
    let pgroot = config.pgroot.clone();
    let embedded_zst = EMBEDDED_PGLITE_ZST.to_vec();

    tokio::task::spawn_blocking(move || {
        // Decompress zstd archive
        let mut tar_bytes = Vec::new();
        zstd::stream::copy_decode(embedded_zst.as_slice(), &mut tar_bytes)
            .wrap_err("decompress embedded zstd archive")?;

        // Unpack tar archive
        let mut archive = Archive::new(tar_bytes.as_slice());
        archive
            .unpack(&pgroot)
            .wrap_err("unpack embedded archive")?;

        Ok::<(), eyre::Report>(())
    })
    .await
    .wrap_err("unpack task panicked")??;

    // Normalize layout - move nested pglite dir if present
    normalize_runtime_layout(config).await?;

    // Move cwasm to expected location if it's at the root
    let root_cwasm = config.pgroot.join("pglite.cwasm");
    let expected_cwasm = config.cwasm_path();
    if root_cwasm.exists() && !expected_cwasm.exists() {
        if let Some(parent) = expected_cwasm.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&root_cwasm, &expected_cwasm).await?;
    }

    Ok(())
}

/// Normalize the runtime layout after unpacking
///
/// Some archives have nested directories like tmp/pglite/...
/// We need to flatten to pgroot/pglite/...
async fn normalize_runtime_layout(config: &Config) -> Res<()> {
    // Check for nested structure
    let nested = config.pgroot.join("tmp").join("pglite");
    if nested.join("bin").join("pglite.wasi").exists() {
        debug!("normalizing nested runtime layout");

        let target = config.pgroot.join("pglite");
        if target.exists() {
            fs::remove_dir_all(&target).await?;
        }

        // Move nested pglite to pgroot
        if fs::rename(&nested, &target).await.is_err() {
            // If rename fails (cross-device), copy recursively
            tokio::task::spawn_blocking({
                let nested = nested.clone();
                let target = target.clone();
                move || copy_dir_recursive(&nested, &target)
            })
            .await
            .wrap_err("copy_dir_recursive task panicked")??;
            fs::remove_dir_all(config.pgroot.join("tmp")).await?;
        }
    }

    Ok(())
}

/// Recursively copy a directory
fn copy_dir_recursive(src: &Path, dst: &Path) -> Res<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Seed /dev/urandom with random bytes
async fn seed_urandom(dev_path: &Path) -> Res<()> {
    fs::create_dir_all(dev_path).await?;
    let urandom_path = dev_path.join("urandom");
    if !urandom_path.exists() {
        let mut buf = [0u8; 128];
        getrandom::getrandom(&mut buf)?;
        fs::write(&urandom_path, buf).await?;
        debug!("seeded urandom");
    }
    Ok(())
}

/// Install an extension from a tar.gz archive
async fn install_extension(config: &Config, archive_path: &Path) -> Res<()> {
    info!("installing extension from {:?}", archive_path);

    let archive_path = archive_path.to_path_buf();
    let target = config.pgroot.join("pglite");
    fs::create_dir_all(&target).await?;

    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&archive_path)
            .wrap_err_with(|| format!("open extension archive {:?}", archive_path))?;
        let decoder = GzDecoder::new(file);
        let mut archive = Archive::new(decoder);
        archive
            .unpack(&target)
            .wrap_err_with(|| format!("unpack extension {:?}", archive_path))?;
        Ok::<(), eyre::Report>(())
    })
    .await
    .wrap_err("install extension task panicked")??;

    Ok(())
}

/// Initialize the database cluster by running initdb via WASM (async)
pub async fn init_cluster(config: &Config, engine: &Engine, module: &Module) -> Res<()> {
    info!("running initdb to create cluster");

    fs::create_dir_all(&config.pgdata).await?;

    // Create password file
    let pw_path = config.pgroot.join("pglite").join("password");
    if let Some(parent) = pw_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&pw_path, "localdevpassword\n").await?;

    // Build WASI context
    let wasi = crate::wire::build_wasi_ctx(config).await?;
    let mut store = Store::new(engine, WasiState { wasi });
    store.set_epoch_deadline(u64::MAX);

    // Link and instantiate
    let mut linker = wasmtime::Linker::<WasiState>::new(engine);
    wasmtime_wasi::p1::add_to_linker_async(&mut linker, |state| &mut state.wasi).to_eyre()?;

    let instance = linker
        .instantiate_async(&mut store, module)
        .await
        .to_eyre()?;

    // Call _start for embed setup
    if let Ok(start) = instance.get_typed_func::<(), ()>(&mut store, "_start") {
        let _ = start.call_async(&mut store, ()).await;
        debug!("_start completed");
    }

    // Call pgl_initdb
    let initdb = instance
        .get_typed_func::<(), i32>(&mut store, "pgl_initdb")
        .to_eyre()
        .wrap_err("get pgl_initdb export")?;

    let rc = initdb.call_async(&mut store, ()).await.to_eyre()?;
    info!("initdb returned: {}", rc);

    // Verify cluster was created
    if !config.cluster_exists() {
        eyre::bail!("initdb did not create cluster (rc={})", rc);
    }

    // Best-effort shutdown
    if let Ok(shutdown) = instance.get_typed_func::<(), ()>(&mut store, "pgl_shutdown") {
        let _ = shutdown.call_async(&mut store, ()).await;
    }

    info!("cluster initialization complete");
    Ok(())
}
