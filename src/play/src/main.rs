use anyhow::Context;
use anyhow::Result;
use std::path::PathBuf;
use std::process::Command;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting pglite wasmtime experiment");

    // Ensure we have the precompiled module
    let pglite_cwasm = ensure_precompiled_module().await?;
    info!("Using precompiled module: {:?}", pglite_cwasm);

    info!("Experiment completed successfully");

    let wasmtime_conf = wasmtime::Config::new();
    let engine = wasmtime::Engine::new(&wasmtime_conf)?;

    let mut linker = wasmtime::Linker::<CompState>::new(&engine);
    wasmtime_wasi::p1::add_to_linker_async(&mut linker, |state| &mut state.wasi)?;
    let state = CompState { wasi };
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PglitePaths {
    pub pgroot: PathBuf,
    pub pgdata: PathBuf,
}

pub(crate) fn standard_wasi_builder(paths: &PglitePaths) -> Result<WasiCtxBuilder> {
    let pgroot_dir = Dir::open_ambient_dir(&paths.pgroot, ambient_authority())?;
    let pgdata_dir = Dir::open_ambient_dir(&paths.pgdata, ambient_authority())?;
    let dev_dir_path = paths.pgroot.join("dev");
    let dev_dir = Dir::open_ambient_dir(&dev_dir_path, ambient_authority())?;

    let mut builder = WasiCtxBuilder::new();
    builder
        .inherit_stdin()
        .inherit_stdout()
        .inherit_stderr()
        .preopened_dir(pgroot_dir, DirPerms::all(), FilePerms::all(), "/tmp")
        .preopened_dir(
            pgdata_dir,
            DirPerms::all(),
            FilePerms::all(),
            "/tmp/pglite/base",
        )
        .preopened_dir(dev_dir, DirPerms::all(), FilePerms::all(), "/dev")
        .env("ENVIRONMENT", "wasm32_wasi_preview1")
        .env("PREFIX", "/tmp/pglite")
        .env("PGDATA", "/tmp/pglite/base")
        .env("PGSYSCONFDIR", "/tmp/pglite")
        .env("PGUSER", "postgres")
        .env("PGDATABASE", "template1")
        .env("MODE", "REACT")
        .env("REPL", "N")
        .env("TZ", "UTC")
        .env("PGTZ", "UTC")
        .env("PATH", "/tmp/pglite/bin");
    Ok(builder)
}

struct CompState {
    wasi: wasmtime_wasi::p1::WasiP1Ctx,
}

async fn ensure_precompiled_module() -> Result<PathBuf> {
    let target_dir = PathBuf::from("target");
    let pglite_wasi = target_dir.join("pglite.wasi");
    let pglite_cwasm = target_dir.join("pglite.wasi.cwasm");

    // Step 1: Ensure pglite.wasi exists
    if !pglite_wasi.exists() {
        info!("Downloading pglite.wasi...");
        let output = Command::new("curl")
            .arg("-o")
            .arg(&pglite_wasi)
            .arg("https://electric-sql.github.io/pglite-build/pglite.wasi")
            .output()
            .context("Failed to run curl. Is curl installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("curl failed: {}", stderr);
        }
        info!("Downloaded pglite.wasi to {:?}", pglite_wasi);
    }

    // Step 2: Precompile to .cwasm if needed
    if !pglite_cwasm.exists() {
        info!("Precompiling pglite.wasi to .cwasm (this may take a while, but only once)...");
        let output = Command::new("wasmtime")
            .arg("compile")
            .arg("-o")
            .arg(&pglite_cwasm)
            .arg(&pglite_wasi)
            .output()
            .context("Failed to run wasmtime compile. Is wasmtime installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("wasmtime compile failed: {}", stderr);
        }
        info!("Precompiled module saved to {:?}", pglite_cwasm);
    } else {
        info!("Using existing precompiled module: {:?}", pglite_cwasm);
    }

    Ok(pglite_cwasm)
}
