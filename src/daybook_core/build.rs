use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cwd = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let profile = std::env::var("PROFILE")?;
    let target_dir = if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        PathBuf::from(dir)
    } else {
        cwd.join("../../target/").canonicalize().unwrap()
    };
    let wflows_target_dir = target_dir.join("wasm");
    // let target = std::env::var("TARGET")?;
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../daybook_wflows/")
            .canonicalize()
            .unwrap()
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../wflow_sdk/").canonicalize().unwrap().display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../wash_plugin_wflow/wit/")
            .canonicalize()
            .unwrap()
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../daybook_core/wit/")
            .canonicalize()
            .unwrap()
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../api_utils_rs/wit/")
            .canonicalize()
            .unwrap()
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../am_utils_rs/")
            .canonicalize()
            .unwrap()
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../daybook_types/")
            .canonicalize()
            .unwrap()
            .display()
    );
    build_wasm_crate(&cwd, &wflows_target_dir, "daybook_wflows");

    // let cwasm_path = out_dir.join("daybook_wflows.cwasm");
    // assert!(
    //     std::process::Command::new("wasmtime")
    //         .args([
    //             "compile",
    //             "-o",
    //             &cwasm_path.as_os_str().to_string(),
    //             "--target",
    //             &target,
    //             &wasm_path.as_os_str().to_string()
    //         ])
    //         .current_dir(cwd.join("../../"))
    //         .spawn()
    //         .expect("error spawning cargo")
    //         .wait()
    //         .expect("error building wasm")
    //         .success(),
    //     "error building daybook_wflows wasm"
    // );

    // let engine = wasmtime::Engine::new(
    //     wasmtime::Config::new()
    //         .wasm_backtrace(true)
    //         // embedded wasm images have backtrace enabled
    //         .wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable)
    //         .cache_config_load_default()
    //         .map_err(|err| format!("error reading system's wasmtime cache config: {err}"))?
    //         .target(&target)
    //         .map_err(|err| format!("error configuring wasmtime for target {target}: {err}"))?,
    // )
    // .map_err(|err| format!("error making wasmtiem engine: {err}"))?;
    // // note: compilation here is extra-slow if building under the debug profile
    // // since wasmtime will also be in the debug profile
    // // consider upgrading the cranelift crates to opt3 if this proves
    // // to be an issue.
    // // At first, I was just using the wasmtime CLI for precomiplation.
    // // The  cli is distrubuted in release mode and did the deed in 3 secs max.
    // // The engine kept rejecting the checksum from the CLI even on the same
    // // version (19.0.0).
    // let comp = wasmtime::component::Component::from_file(&engine, wasm_path)
    //     .map_err(|err| format!("error making component from file: {err}"))?;
    // let cwasm = comp
    //     .serialize()
    //     .map_err(|err| format!("error serializing component: {err}"))?;

    compress_wasm(
        &wflows_target_dir,
        &out_dir,
        "daybook_wflows",
        if profile == "release" { 19 } else { 1 },
    )?;

    Ok(())
}

fn build_wasm_crate(cwd: &Path, wflows_target_dir: &Path, crate_name: &str) {
    let mut build_wflows = std::process::Command::new("cargo");
    build_wflows
        .args([
            "build",
            "--lib",
            "-p",
            crate_name,
            "--release",
            "--target",
            "wasm32-wasip2",
        ])
        .current_dir(cwd.join("../../"))
        .env("CARGO_TARGET_DIR", wflows_target_dir);
    configure_wasm_rustflags(&mut build_wflows);
    assert!(
        build_wflows
            .spawn()
            .expect("error spawning cargo")
            .wait()
            .expect("error building wasm")
            .success(),
        "error building {crate_name} wasm"
    );
}

fn compress_wasm(
    wflows_target_dir: &Path,
    out_dir: &Path,
    crate_name: &str,
    level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let wasm_path = wflows_target_dir
        .join("wasm32-wasip2")
        .join("release")
        .join(format!("{crate_name}.wasm"));
    let wasm_path = wasm_path
        .canonicalize()
        .map_err(|err| format!("error resolving {}: {err}", wasm_path.display()))?;
    let wasm_bytes = std::fs::read(wasm_path)?;
    zstd::stream::copy_encode(
        &wasm_bytes[..],
        std::fs::File::create(out_dir.join(format!("{crate_name}.wasm.zst")))
            .map_err(|err| format!("error creating {crate_name}.wasm.zst: {err}"))?,
        level,
    )
    .map_err(|err| format!("error compress writing {crate_name}.wasm.zst: {err}"))?;
    Ok(())
}

fn configure_wasm_rustflags(cmd: &mut std::process::Command) {
    const TOKIO_UNSTABLE_FLAG: &str = "--cfg";
    const TOKIO_UNSTABLE_VALUE: &str = "tokio_unstable";

    fn sanitize_flags(flags: &[String]) -> Vec<String> {
        let mut result = Vec::new();
        let mut index = 0;
        while index < flags.len() {
            let flag = &flags[index];
            if flag == "-C"
                && index + 1 < flags.len()
                && flags[index + 1].starts_with("instrument-coverage")
            {
                index += 2;
                continue;
            }
            if flag == "--cfg"
                && index + 1 < flags.len()
                && (flags[index + 1] == "coverage" || flags[index + 1] == "coverage_nightly")
            {
                index += 2;
                continue;
            }
            if flag.starts_with("-Cinstrument-coverage")
                || flag.starts_with("-C instrument-coverage")
                || flag.starts_with("--cfg=coverage")
                || flag == "coverage"
                || flag == "coverage_nightly"
            {
                index += 1;
                continue;
            }
            result.push(flag.clone());
            index += 1;
        }
        result
    }

    let encoded_raw = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let encoded_parts: Vec<String> = encoded_raw
        .split('\x1f')
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_string())
        .collect();
    let mut sanitized_encoded = sanitize_flags(&encoded_parts);
    if !sanitized_encoded
        .iter()
        .any(|part| part == TOKIO_UNSTABLE_VALUE)
    {
        sanitized_encoded.push(TOKIO_UNSTABLE_FLAG.to_string());
        sanitized_encoded.push(TOKIO_UNSTABLE_VALUE.to_string());
    }
    cmd.env("CARGO_ENCODED_RUSTFLAGS", sanitized_encoded.join("\x1f"));

    let rustflags_raw = std::env::var("RUSTFLAGS").unwrap_or_default();
    let rustflags_words: Vec<String> = rustflags_raw
        .split_whitespace()
        .map(|segment| segment.to_string())
        .collect();
    let mut sanitized_rustflags = sanitize_flags(&rustflags_words);
    if !sanitized_rustflags
        .iter()
        .any(|part| part == TOKIO_UNSTABLE_VALUE)
    {
        sanitized_rustflags.push(TOKIO_UNSTABLE_FLAG.to_string());
        sanitized_rustflags.push(TOKIO_UNSTABLE_VALUE.to_string());
    }
    cmd.env("RUSTFLAGS", sanitized_rustflags.join(" "));
}
