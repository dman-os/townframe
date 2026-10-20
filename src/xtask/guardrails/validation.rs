use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;

use crate::guardrails::repository::files;
use crate::guardrails::repository::output;
use crate::guardrails::repository::relative;
use crate::interlude::*;

#[derive(Deserialize, Serialize)]
pub struct Receipt {
    pub schema: u32,
    pub before: String,
    pub after: String,
    pub rustc: String,
    pub cargo: String,
    pub environment: BTreeMap<String, String>,
    pub command: Vec<String>,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u128,
}

pub fn fingerprint(root: &Path) -> Res<String> {
    let mut hash = blake3::Hasher::new();
    for path in files(root)? {
        let name = relative(root, &path)?;
        let first = name.split('/').next().unwrap_or_default();
        if !matches!(
            first,
            "src"
                | "tools"
                | "x"
                | "docs"
                | ".cargo"
                | ".config"
                | ".github"
                | ".agents"
                | "Cargo.toml"
                | "Cargo.lock"
                | "flake.nix"
                | "flake.lock"
                | "rustfmt.toml"
                | "clippy.toml"
                | ".gitattributes"
                | "AGENTS.md"
                | "CONTRIBUTING.md"
                | "README.md"
                | "deno.json"
                | "deno.lock"
                | "package.json"
                | "pnpm-lock.yaml"
                | ".pre-commit-config.yaml"
        ) {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        hash.update(&(name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update(&(bytes.len() as u64).to_le_bytes());
        hash.update(&bytes);
    }
    Ok(hash.finalize().to_hex().to_string())
}

fn identity(program: &str) -> Res<String> {
    Ok(
        String::from_utf8(output(Command::new(program).arg("--version"))?)?
            .trim()
            .into(),
    )
}

fn environment() -> BTreeMap<String, String> {
    [
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTDOCFLAGS",
        "RUSTC",
        "RUSTC_WRAPPER",
        "RUSTUP_TOOLCHAIN",
        "CARGO_BUILD_TARGET",
        "CARGO_INCREMENTAL",
        "TEST_SEED",
        "RUST_LOG_TEST",
        "NEXTEST_PROFILE",
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS",
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER",
    ]
    .into_iter()
    .filter_map(|key| std::env::var(key).ok().map(|value| (key.into(), value)))
    .collect()
}

pub fn record(root: &Path, destination: &Path, command: &[String]) -> Res<bool> {
    eyre::ensure!(
        !destination.is_absolute()
            && destination.starts_with("target")
            && !destination
                .components()
                .any(|part| part == std::path::Component::ParentDir),
        "receipts must live under workspace-relative target/ so they do not hash themselves"
    );
    let before = fingerprint(root)?;
    let rustc = identity("rustc")?;
    let cargo = identity("cargo")?;
    let env_map = environment();
    let started = Instant::now();
    let status = Command::new(&command[0])
        .args(&command[1..])
        .current_dir(root)
        .status()?;
    let receipt = Receipt {
        schema: 1,
        before,
        after: fingerprint(root)?,
        rustc,
        cargo,
        environment: env_map,
        command: command.into(),
        success: status.success(),
        exit_code: status.code(),
        elapsed_ms: started.elapsed().as_millis(),
    };
    let destination = root.join(destination);
    std::fs::create_dir_all(destination.parent().ok_or_eyre("receipt parent")?)?;
    std::fs::write(destination, serde_json::to_string_pretty(&receipt)?)?;
    Ok(receipt.success && receipt.before == receipt.after)
}

pub fn verify(root: &Path, path: &Path) -> Res<bool> {
    let full_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let receipt: Receipt = serde_json::from_slice(&std::fs::read(&full_path)?)?;
    eyre::ensure!(receipt.schema == 1, "unsupported receipt schema");
    eyre::ensure!(
        receipt.success && receipt.exit_code == Some(0),
        "receipt records a failed command"
    );
    eyre::ensure!(
        receipt.before == receipt.after,
        "source changed during validation; rerun the check"
    );
    eyre::ensure!(
        receipt.after == fingerprint(root)?,
        "source changed since validation; receipt is stale"
    );
    eyre::ensure!(
        receipt.rustc == identity("rustc")? && receipt.cargo == identity("cargo")?,
        "toolchain changed"
    );
    eyre::ensure!(
        receipt.environment == environment(),
        "validation environment changed"
    );
    Ok(true)
}

pub fn doctor(root: &Path, json: bool) -> Res<bool> {
    let rustc = String::from_utf8(output(Command::new("rustc").arg("-vV"))?)?;
    let sysroot = String::from_utf8(output(Command::new("rustc").args(["--print", "sysroot"]))?)?;
    let config = String::from_utf8(output(Command::new("cargo").current_dir(root).args([
        "-Zunstable-options",
        "config",
        "get",
        "--format",
        "json",
    ]))?)?;
    let mut issues = Vec::new();
    for key in [
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTFLAGS",
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS",
    ] {
        if let Ok(value) = std::env::var(key)
            && !value.contains("tokio_unstable")
        {
            issues.push(format!(
                "{key} overrides repository rustflags without tokio_unstable: `{value}`"
            ));
        }
    }
    if let Ok(value) = std::env::var("CARGO_TARGET_DIR") {
        let target_dir = if Path::new(&value).is_absolute() {
            PathBuf::from(&value)
        } else {
            root.join(&value)
        };
        if target_dir.starts_with(root) {
            issues.push(format!(
                "CARGO_TARGET_DIR {value} is inside the worktree; point it outside the checkout so nix copies and ntfs small-file creation stay off the build path"
            ));
        } else if target_dir
            .file_name()
            .is_none_or(|name| name != root.file_name().unwrap_or_default())
        {
            issues.push(format!(
                "CARGO_TARGET_DIR {value} is shared with sibling worktrees; single-binary crates such as xtask collide at <target>/debug/xtask, so run builds through ./x/agent-build.ts which appends the worktree name"
            ));
        }
    }
    let gitignore_path = root.join(".gitignore");
    let gitignore = std::fs::read_to_string(&gitignore_path)?;
    if root.join("src/wasm_utils_rs/target").is_dir()
        && !gitignore.lines().any(|line| line == "/src/*/target")
    {
        issues.push(
            "src/wasm_utils_rs/target exists but .gitignore lacks `/src/*/target`; nix would copy build output into store paths"
                .into(),
        );
    }
    let cargo_cfg_path = root.join(".cargo/config.toml");
    if cargo_cfg_path.is_file() {
        let cfg_text = std::fs::read_to_string(&cargo_cfg_path)?;
        if !cfg_text.contains("line-tables-only") {
            issues.push(
                ".cargo/config.toml is missing `[profile.dev.package.\"*\"] debug = \"line-tables-only\"`"
                    .into(),
            );
        }
        if !cfg_text.contains("[unstable]") || !cfg_text.contains("feature-unification = true") {
            issues.push(
                ".cargo/config.toml is missing `[unstable] feature-unification = true`; the resolver setting below it is ignored without it"
                    .into(),
            );
        }
        if !cfg_text.contains("feature-unification = \"workspace\"") {
            issues.push(
                ".cargo/config.toml is missing `[resolver] feature-unification = \"workspace\"`; alternating `-p` selections would rebuild shared dependencies under different feature sets"
                    .into(),
            );
        }
    }
    let rustfmt_path = root.join("rustfmt.toml");
    if !rustfmt_path.is_file() {
        issues.push("rustfmt.toml is missing `imports_granularity = \"Item\"`".into());
    } else {
        let fmt_text = std::fs::read_to_string(&rustfmt_path)?;
        if !fmt_text.contains("imports_granularity") {
            issues.push("rustfmt.toml is missing `imports_granularity = \"Item\"`".into());
        }
    }
    let flake_path = root.join("flake.nix");
    if flake_path.is_file() {
        let flake_text = std::fs::read_to_string(&flake_path)?;
        for line in flake_text.lines() {
            if line.contains("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS")
                && !line.contains("tokio_unstable")
            {
                issues.push(
                    "flake.nix sets CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS without --cfg tokio_unstable"
                        .into(),
                );
            }
        }
    }

    let makeflags = std::env::var("MAKEFLAGS").ok();
    let rustc_wrapper = std::env::var("RUSTC_WRAPPER").ok();
    let result = serde_json::json!({
        "rustc": rustc.trim(),
        "sysroot": sysroot.trim(),
        "cargo": identity("cargo")?,
        "makeflags": makeflags,
        "rustc_wrapper": rustc_wrapper,
        "configuration": serde_json::from_str::<serde_json::Value>(&config)?,
        "issues": issues,
    });
    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("{rustc}sysroot: {}", sysroot.trim());
        println!(
            "makeflags: {:?}, rustc_wrapper: {:?}",
            makeflags, rustc_wrapper
        );
        for issue in &issues {
            println!("ERROR_BUILD_FLAGS: {issue}");
        }
    }
    Ok(issues.is_empty())
}
