use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cwd = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let profile = std::env::var("PROFILE")?;
    let target_dir = if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        PathBuf::from(dir)
    } else {
        cwd.join("../../target/").canonicalize().unwrap()
    };
    // let target = std::env::var("TARGET")?;
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../daybook_wflows/")
            .canonicalize()
            .unwrap()
            .to_string_lossy()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../wflow_sdk/")
            .canonicalize()
            .unwrap()
            .to_string_lossy()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../wash_plugin_wflow/wit/")
            .canonicalize()
            .unwrap()
            .to_string_lossy()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../daybook_core/wit/")
            .canonicalize()
            .unwrap()
            .to_string_lossy()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cwd.join("../api_utils_rs/wit/")
            .canonicalize()
            .unwrap()
            .to_string_lossy()
    );
    // TODO: use wasmtools on resulting wasm
    let mut build_wflows = std::process::Command::new("cargo");
    build_wflows
        .args([
            "build",
            "-p",
            "daybook_wflows",
            "--release",
            "--target",
            "wasm32-wasip2",
        ])
        .current_dir(cwd.join("../../"));
    append_tokio_unstable_rustflags(&mut build_wflows);
    assert!(
        build_wflows
            .spawn()
            .expect("error spawning cargo")
            .wait()
            .expect("error building wasm")
            .success(),
        "error building daybook_wflows wasm"
    );
    // let cwasm_path = out_dir.join("daybook_wflows.cwasm");
    // assert!(
    //     std::process::Command::new("wasmtime")
    //         .args([
    //             "compile",
    //             "-o",
    //             &cwasm_path.as_os_str().to_string_lossy(),
    //             "--target",
    //             &target,
    //             &wasm_path.as_os_str().to_string_lossy()
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

    let wflows_build_profile = "release";
    let wasm_path = target_dir
        .join("wasm32-wasip2")
        .join(wflows_build_profile)
        .join("daybook_wflows.wasm");
    let wasm_path = wasm_path.canonicalize().map_err(|err| {
        format!(
            "error resolving daybook_wflows.wasm at {}: {err}",
            wasm_path.display()
        )
    })?;
    let wasm_bytes = std::fs::read(wasm_path)?;
    zstd::stream::copy_encode(
        &wasm_bytes[..],
        std::fs::File::create(out_dir.join("daybook_wflows.wasm.zst"))
            .map_err(|err| format!("error creating daybook_wflows.wasm.zst: {err}"))?,
        if profile == "release" { 19 } else { 1 },
    )
    .map_err(|err| format!("error compress writing daybook_wflows.wasm.zst: {err}"))?;
    Ok(())
}

fn append_tokio_unstable_rustflags(cmd: &mut std::process::Command) {
    const TOKIO_UNSTABLE_FLAG: &str = "--cfg";
    const TOKIO_UNSTABLE_VALUE: &str = "tokio_unstable";

    let encoded_flag = format!("{TOKIO_UNSTABLE_FLAG}\x1f{TOKIO_UNSTABLE_VALUE}");
    let encoded = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    if !encoded
        .split('\x1f')
        .any(|part| part == TOKIO_UNSTABLE_VALUE)
    {
        let new_encoded = if encoded.is_empty() {
            encoded_flag
        } else {
            format!("{encoded}\x1f{encoded_flag}")
        };
        cmd.env("CARGO_ENCODED_RUSTFLAGS", new_encoded);
    }

    let rustflags = std::env::var("RUSTFLAGS").unwrap_or_default();
    if !rustflags.contains("tokio_unstable") {
        let new_rustflags = if rustflags.is_empty() {
            format!("{TOKIO_UNSTABLE_FLAG} {TOKIO_UNSTABLE_VALUE}")
        } else {
            format!("{rustflags} {TOKIO_UNSTABLE_FLAG} {TOKIO_UNSTABLE_VALUE}")
        };
        cmd.env("RUSTFLAGS", new_rustflags);
    }
}
