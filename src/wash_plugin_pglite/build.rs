use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use reqwest::blocking::Client;
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PGLITE_DOWNLOAD_URL: &str =
        "https://electric-sql.github.io/pglite-build/pglite-wasi.tar.xz";
    const EXPECTED_SHA256: &str =
        "c725235f22a4fd50fed363f4065edb151a716fa769cba66f2383b8b854e6bdb5";

    let cwd = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let target = std::env::var("TARGET")?;
    let profile = std::env::var("PROFILE")?;

    let target_dir = if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        PathBuf::from(dir)
    } else {
        cwd.parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("target"))
            .unwrap_or_else(|| PathBuf::from("target"))
    };

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=PGLITE_DOWNLOAD_URL");
    println!("cargo:rerun-if-env-changed=EXPECTED_SHA256");

    let temp_dir = out_dir.join("pglite_build");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
    }
    fs::create_dir_all(&temp_dir)?;

    let tar_xz_path = target_dir.join("pglite-wasi.tar.xz");
    if !tar_xz_path.exists() {
        println!(
            "cargo:info=Downloading pglite runtime from {}",
            PGLITE_DOWNLOAD_URL
        );

        if let Some(parent) = tar_xz_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let client = Client::builder().build()?;
        let mut response = client.get(PGLITE_DOWNLOAD_URL).send()?.error_for_status()?;

        let mut file = fs::File::create(&tar_xz_path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];

        loop {
            let read = response.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            file.write_all(&buffer[..read])?;
        }

        let hash = format!("{:x}", hasher.finalize());
        if hash != EXPECTED_SHA256 {
            return Err(format!(
                "downloaded pglite runtime checksum mismatch (expected {EXPECTED_SHA256}, got {hash})"
            )
            .into());
        }

        println!("cargo:info=Downloaded pglite runtime to {:?}", tar_xz_path);
    } else {
        println!(
            "cargo:info=Using cached pglite runtime from {:?}",
            tar_xz_path
        );
    }

    println!("cargo:info=Unpacking pglite runtime");
    let file = fs::File::open(&tar_xz_path)?;
    let decoder = xz2::read::XzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(&temp_dir)?;

    let normalized_pglite = temp_dir.join("pglite");
    if !normalized_pglite.exists() {
        let nested = temp_dir.join("tmp").join("pglite");
        if nested.exists() {
            println!("cargo:info=Normalizing nested pglite structure");
            if normalized_pglite.exists() {
                fs::remove_dir_all(&normalized_pglite)?;
            }
            fs::rename(&nested, &normalized_pglite)?;
        }
    }

    let wasm_path = normalized_pglite.join("bin").join("pglite.wasi");
    if !wasm_path.exists() {
        return Err(format!("pglite.wasi not found at {:?}", wasm_path).into());
    }
    println!("cargo:info=Found pglite.wasi at {:?}", wasm_path);

    let mut config = wasmtime::Config::new();
    config
        .wasm_backtrace(true)
        .epoch_interruption(true)
        .wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable)
        .target(&target)
        .map_err(|err| format!("error configuring wasmtime for target {target}: {err}"))?;

    let engine = wasmtime::Engine::new(&config)
        .map_err(|err| format!("error making wasmtime engine: {err}"))?;

    println!("cargo:info=Compiling pglite.wasi to cwasm (this may take a while)...");
    let module = wasmtime::Module::from_file(&engine, &wasm_path)
        .map_err(|err| format!("error loading module from file: {err}"))?;

    let cwasm = module
        .serialize()
        .map_err(|err| format!("error serializing component: {err}"))?;

    let cwasm_path = temp_dir.join("pglite.cwasm");
    fs::write(&cwasm_path, &cwasm)?;
    println!("cargo:info=Compiled cwasm ({} bytes)", cwasm.len());

    let archive_path = out_dir.join("pglite_embedded.tar");
    {
        let file = fs::File::create(&archive_path)?;
        let mut tar = tar::Builder::new(file);
        if normalized_pglite.exists() {
            add_dir_to_tar(&mut tar, &normalized_pglite, "pglite")?;
        } else {
            add_dir_to_tar(&mut tar, &temp_dir, "")?;
        }
        tar.append_file("pglite.cwasm", &mut fs::File::open(&cwasm_path)?)?;
        tar.finish()?;
    }

    println!("cargo:info=Compressing embedded archive...");
    let compression_level = if profile == "release" { 19 } else { 1 };

    let tar_data = fs::read(&archive_path)?;
    let compressed_path = out_dir.join("pglite_embedded.tar.zst");
    {
        let mut encoder =
            zstd::Encoder::new(fs::File::create(&compressed_path)?, compression_level)?;
        encoder.write_all(&tar_data)?;
        encoder.finish()?;
    }

    let compressed_size = fs::metadata(&compressed_path)?.len();
    println!(
        "cargo:info=Compressed archive size: {} bytes",
        compressed_size
    );

    fs::remove_dir_all(&temp_dir)?;
    let _ = fs::remove_file(&archive_path);

    println!(
        "cargo:rustc-env=PGLITE_EMBEDDED_ZST={}",
        compressed_path.display()
    );
    Ok(())
}

fn add_dir_to_tar(
    tar: &mut tar::Builder<fs::File>,
    dir: &PathBuf,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_name = entry.file_name();
            let archive_path = if prefix.is_empty() {
                file_name.to_string_lossy().to_string()
            } else {
                format!("{}/{}", prefix, file_name.to_string_lossy())
            };

            if path.is_dir() {
                add_dir_to_tar(tar, &path, &archive_path)?;
            } else {
                let mut file = fs::File::open(&path)?;
                tar.append_file(&archive_path, &mut file)?;
            }
        }
    }
    Ok(())
}
