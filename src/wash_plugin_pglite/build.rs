use std::path::PathBuf;
use std::fs;
use std::io::Write;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Download URL for pglite runtime
    const PGLITE_DOWNLOAD_URL: &str = "https://electric-sql.github.io/pglite-build/pglite-wasi.tar.xz";

    let cwd = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let target = std::env::var("TARGET")?;
    let profile = std::env::var("PROFILE")?;

    // Get target directory (where cargo stores build artifacts)
    let target_dir = if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        PathBuf::from(dir)
    } else {
        // If CARGO_TARGET_DIR not set, assume ./target relative to workspace root
        // CARGO_MANIFEST_DIR is the crate root, so go up to workspace root
        cwd.parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("target"))
            .unwrap_or_else(|| PathBuf::from("target"))
    };

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=PGLITE_DOWNLOAD_URL");

    // Create temp directory for unpacking
    let temp_dir = out_dir.join("pglite_build");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
    }
    fs::create_dir_all(&temp_dir)?;

    // Download pglite-wasi.tar.xz to target directory if not cached
    let tar_xz_path = target_dir.join("pglite-wasi.tar.xz");
    if !tar_xz_path.exists() {
        println!("cargo:warning=Downloading pglite runtime from {}", PGLITE_DOWNLOAD_URL);
        
        // Ensure target directory exists
        if let Some(parent) = tar_xz_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let mut response = ureq::get(PGLITE_DOWNLOAD_URL)
            .call()
            .map_err(|e| format!("failed to download pglite: {}", e))?;
        
        let mut file = fs::File::create(&tar_xz_path)?;
        std::io::copy(&mut response.into_reader(), &mut file)?;
        println!("cargo:warning=Downloaded pglite runtime to {:?}", tar_xz_path);
    } else {
        println!("cargo:warning=Using cached pglite runtime from {:?}", tar_xz_path);
    }

    // Unpack the tar.xz
    println!("cargo:warning=Unpacking pglite runtime");
    let file = fs::File::open(&tar_xz_path)?;
    let decoder = xz2::read::XzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(&temp_dir)?;

    // Normalize structure: ensure we have pglite/ directory at temp_dir root
    let normalized_pglite = temp_dir.join("pglite");
    if !normalized_pglite.exists() {
        // Check for nested structure (tmp/pglite)
        let nested = temp_dir.join("tmp").join("pglite");
        if nested.exists() {
            println!("cargo:warning=Normalizing nested pglite structure");
            if normalized_pglite.exists() {
                fs::remove_dir_all(&normalized_pglite)?;
            }
            // Move nested to root
            if fs::rename(&nested, &normalized_pglite).is_err() {
                // Cross-device rename failed, copy instead
                copy_dir_recursive(&nested, &normalized_pglite)?;
                fs::remove_dir_all(temp_dir.join("tmp"))?;
            }
        }
    }

    // Find pglite.wasi after normalization
    let wasm_path = normalized_pglite.join("bin").join("pglite.wasi");
    if !wasm_path.exists() {
        // Fallback: search for it
        let found = find_pglite_wasm(&temp_dir)?;
        println!("cargo:warning=Found pglite.wasi at {:?} (not in expected location)", found);
        return Err(format!("pglite.wasi not found at expected location {:?}", wasm_path).into());
    }
    println!("cargo:warning=Found pglite.wasi at {:?}", wasm_path);

    // Create wasmtime engine for compilation
    let mut config = wasmtime::Config::new();
    config
        .wasm_backtrace(true)
        .epoch_interruption(true)
        .wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable)
        .target(&target)
        .map_err(|err| format!("error configuring wasmtime for target {target}: {err}"))?;
    
    let engine = wasmtime::Engine::new(&config)
        .map_err(|err| format!("error making wasmtime engine: {err}"))?;

    // Compile wasm to cwasm
    println!("cargo:warning=Compiling pglite.wasi to cwasm (this may take a while)...");
    let module = wasmtime::Module::from_file(&engine, &wasm_path)
        .map_err(|err| format!("error loading module from file: {err}"))?;
    
    let cwasm = module
        .serialize()
        .map_err(|err| format!("error serializing component: {err}"))?;

    // Write cwasm to temp directory
    let cwasm_path = temp_dir.join("pglite.cwasm");
    fs::write(&cwasm_path, &cwasm)?;
    println!("cargo:warning=Compiled cwasm ({} bytes)", cwasm.len());

    // Create a tar archive containing everything (original contents + cwasm)
    let archive_path = out_dir.join("pglite_embedded.tar");
    {
        let file = fs::File::create(&archive_path)?;
        let mut tar = tar::Builder::new(file);
        
        // Add normalized pglite directory structure
        // The structure should be: pglite/bin/pglite.wasi, pglite/share/, etc.
        if normalized_pglite.exists() {
            add_dir_to_tar(&mut tar, &normalized_pglite, "pglite")?;
        } else {
            // Fallback: add everything from temp_dir
            add_dir_to_tar(&mut tar, &temp_dir, "")?;
        }
        
        // Add cwasm at the root (alongside pglite directory)
        tar.append_file("pglite.cwasm", &mut fs::File::open(&cwasm_path)?)?;
        
        tar.finish()?;
    }

    // Compress the tar archive with zstd
    println!("cargo:warning=Compressing embedded archive...");
    let compression_level = if profile == "release" { 19 } else { 1 };
    
    let tar_data = fs::read(&archive_path)?;
    let compressed_path = out_dir.join("pglite_embedded.tar.zst");
    {
        let mut encoder = zstd::Encoder::new(
            fs::File::create(&compressed_path)?,
            compression_level,
        )?;
        encoder.write_all(&tar_data)?;
        encoder.finish()?;
    }

    let compressed_size = fs::metadata(&compressed_path)?.len();
    println!("cargo:warning=Compressed archive size: {} bytes", compressed_size);

    // Cleanup temp files (but keep tar_xz_path in target/ for reuse)
    fs::remove_dir_all(&temp_dir)?;
    let _ = fs::remove_file(&archive_path);

    // Tell cargo about the output file so it can be included
    println!("cargo:rustc-env=PGLITE_EMBEDDED_ZST={}", compressed_path.display());

    Ok(())
}

/// Find pglite.wasi in the unpacked directory
fn find_pglite_wasm(dir: &PathBuf) -> Result<PathBuf, Box<dyn std::error::Error>> {
    // Check common locations
    let candidates = [
        dir.join("pglite").join("bin").join("pglite.wasi"),
        dir.join("tmp").join("pglite").join("bin").join("pglite.wasi"),
        dir.join("bin").join("pglite.wasi"),
    ];

    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }

    // Recursive search
    fn search_recursive(dir: &PathBuf, target: &str) -> Option<PathBuf> {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(found) = search_recursive(&path, target) {
                        return Some(found);
                    }
                } else if path.file_name().and_then(|n| n.to_str()) == Some(target) {
                    return Some(path);
                }
            }
        }
        None
    }

    search_recursive(dir, "pglite.wasi")
        .ok_or_else(|| format!("pglite.wasi not found in {:?}", dir).into())
}

/// Recursively add directory contents to tar archive
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

/// Recursively copy directory
fn copy_dir_recursive(src: &PathBuf, dst: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
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

