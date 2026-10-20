pub mod analysis;
pub mod contracts;
pub mod docs;
pub mod matrix;
pub mod policy;
pub mod repository;
pub mod validation;

use serde::Serialize;

use crate::interlude::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, clap::Subcommand)]
pub enum GuardrailsCmd {
    /// Run all AST, architecture, migration-identity, and documentation guardrails.
    Check {
        #[arg(long)]
        package: Option<String>,
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
        #[arg(long)]
        json: bool,
    },
    /// Emit full workspace symbol/module/dependency/migration inventory as JSON.
    Inventory {
        #[arg(long)]
        package: Option<String>,
    },
    /// Bootstrap crate-local `guardrails.toml` policies once with reasoned legacy exemptions.
    Adopt {
        #[arg(long, required = true)]
        reason: String,
        #[arg(long, required = true)]
        reference: String,
    },
    /// Ratchet down existing exemption ceilings that have improved; never adds or raises ceilings.
    Bless {
        #[arg(long)]
        package: Option<String>,
    },
    /// Compute affected crates and validation commands for workspace-relative changed paths.
    Plan {
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Check or bless public-API snapshots and print change-impact reports.
    Api {
        #[arg(long)]
        package: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long, alias = "accept")]
        bless: bool,
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
    /// Record a source-bound validation receipt around a command execution.
    Record {
        #[arg(long)]
        output: PathBuf,
        #[arg(required = true, last = true)]
        command: Vec<String>,
    },
    /// Verify that a validation receipt matches the current source tree and toolchain.
    Verify { receipt: PathBuf },
    /// Check effective rustflags, sysroot identity, rustfmt/profile config, and build environment.
    Doctor {
        #[arg(long)]
        json: bool,
    },
    /// Detect symbol-level, migration-stream, and API-contract overlap across sibling worktrees.
    Overlap {
        /// Trunk checkout every worktree is diffed against (default: $TOWNFRAME_TRUNK_DIR).
        #[arg(long)]
        base: Option<PathBuf>,
        #[arg(long)]
        worktrees_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Finding {
    pub code: String,
    pub path: String,
    pub symbol: String,
    pub line: usize,
    pub actual: usize,
    pub limit: usize,
    pub message: String,
}

impl Finding {
    pub fn new(
        code: &str,
        path: &str,
        symbol: &str,
        line: usize,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path: path.into(),
            symbol: symbol.into(),
            line,
            actual: 1,
            limit: 0,
            message: message.into(),
        }
    }
}

pub fn dispatch(root: &Path, cmd: GuardrailsCmd) -> Res<()> {
    let root = root.canonicalize()?;
    let ok = match cmd {
        GuardrailsCmd::Check {
            package,
            format,
            json,
        } => run_check(
            &root,
            package.as_deref(),
            json || format == OutputFormat::Json,
        )?,
        GuardrailsCmd::Inventory { package } => {
            let repo = repository::Repository::load(&root)?;
            let report = repo.scan(package.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            true
        }
        GuardrailsCmd::Adopt { reason, reference } => {
            let repo = repository::Repository::load(&root)?;
            let report = repo.scan(None)?;
            repo.adopt(&report, &reason, &reference)?;
            println!(
                "adopted guardrails policies for {} targets",
                report.findings.len()
            );
            true
        }
        GuardrailsCmd::Bless { package } => {
            let repo = repository::Repository::load(&root)?;
            let report = repo.scan(package.as_deref())?;
            let lowered = repo.bless(&report)?;
            println!("ratcheted down {lowered} exemption(s)");
            true
        }
        GuardrailsCmd::Plan { files } => {
            let repo = repository::Repository::load(&root)?;
            println!("{}", serde_json::to_string_pretty(&repo.plan(&files)?)?);
            true
        }
        GuardrailsCmd::Api {
            package,
            all,
            bless,
            check: _,
            json,
        } => contracts::api(&root, package.as_deref(), all, bless, json)?,
        GuardrailsCmd::Record { output, command } => validation::record(&root, &output, &command)?,
        GuardrailsCmd::Verify { receipt } => validation::verify(&root, &receipt)?,
        GuardrailsCmd::Doctor { json } => validation::doctor(&root, json)?,
        GuardrailsCmd::Overlap {
            base,
            worktrees_dir,
            json,
        } => contracts::overlap(&root, base.as_deref(), worktrees_dir.as_deref(), json)?,
    };
    eyre::ensure!(ok, "guardrails check failed");
    Ok(())
}

pub fn run_check(root: &Path, package: Option<&str>, json: bool) -> Res<bool> {
    let repo = repository::Repository::load(root)?;
    let report = repo.scan(package)?;
    let mut findings = repo.check(&report)?;
    findings.extend(matrix::validate(&repo)?);
    if json {
        println!("{}", serde_json::to_string_pretty(&findings)?);
    } else {
        for finding in &findings {
            println!(
                "{}:{}: {} [{}]: {} ({} > {})",
                finding.path,
                finding.line,
                finding.code,
                finding.symbol,
                finding.message,
                finding.actual,
                finding.limit
            );
        }
        println!(
            "{} diagnostics; {} source modules inventoried",
            findings.len(),
            report.metrics.len()
        );
    }
    Ok(findings.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guardrails::policy::Exemption;
    use crate::guardrails::policy::LayerRole;
    use crate::guardrails::policy::Policy;
    use crate::guardrails::policy::Reason;

    #[test]
    fn generated_files_are_exempt_from_authored_metrics() {
        let src = "// @generated by `cargo x sql-gen`. Do not edit by hand.\npub fn a() {}\npub fn b() {}\n";
        let (metrics, findings) = analysis::scan("src/foo/sqlite/gen.rs", src);
        assert_eq!(metrics.len(), 1);
        assert!(metrics[0].generated);
        assert_eq!(metrics[0].definitions, 0);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_forbidden_patterns_and_missing_expect_reasons() {
        let src = r#"
            #[allow(dead_code)]
            #[expect(unused_variables)]
            pub use crate::inner::*;
            use tokio::sync::Mutex;

            pub fn bad(tx: std::sync::mpsc::Sender<u8>) {
                let _p = "/tmp/shared-test-dir";
                tx.send(1).ok();
            }

            #[test]
            #[ignore]
            fn skipped_test() {
                let _addr = "127.0.0.1:8080";
            }
        "#;
        let (_, findings) = analysis::scan("src/foo/lib.rs", src);
        let codes: std::collections::BTreeSet<&str> = findings
            .iter()
            .map(|finding| finding.code.as_str())
            .collect();
        assert!(codes.contains("ERROR_ALLOW_ATTRIBUTE"));
        assert!(codes.contains("ERROR_LINT_REASON"));
        assert!(codes.contains("ERROR_GLOB_EXPORT"));
        assert!(codes.contains("ERROR_SHARED_ASYNC_STATE"));
        assert!(codes.contains("ERROR_FIXED_TEMP_PATH"));
        assert!(codes.contains("ERROR_SEND_DISCARDED"));
        assert!(codes.contains("ERROR_IGNORED_TEST"));
        assert!(codes.contains("ERROR_FIXED_TEST_PORT"));
    }

    #[test]
    fn aggregates_methods_across_multiple_impl_blocks() {
        let mut src = String::from("pub struct GodObject;\n");
        for block in 0..5 {
            src.push_str("impl GodObject {\n");
            for method_idx in 0..9 {
                src.push_str(&format!("    pub fn m_{block}_{method_idx}(&self) {{}}\n"));
            }
            src.push_str("}\n");
        }
        let (metrics, findings) = analysis::scan("src/foo/lib.rs", &src);
        assert_eq!(metrics[0].methods.get("GodObject"), Some(&45));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == "ERROR_TYPE_ITEMS" && finding.actual == 45)
        );
    }

    #[test]
    fn policy_enforces_ceilings_and_detects_stale_exemptions() {
        let finding = Finding {
            code: "ERROR_MODULE_ITEMS".into(),
            path: "src/foo/lib.rs".into(),
            symbol: "module".into(),
            line: 1,
            actual: 45,
            limit: 40,
            message: "split responsibilities".into(),
        };
        let mut policy = Policy {
            version: 1,
            role: LayerRole::Domain,
            dependency_reason: "baseline".into(),
            allowed_dependencies: std::collections::BTreeSet::new(),
            allow_external_api_types: false,
            api: None,
            exemptions: vec![Exemption {
                code: "ERROR_MODULE_ITEMS".into(),
                path: "src/foo/lib.rs".into(),
                symbol: "module".into(),
                ceiling: 45,
                class: Reason::LegacyDebt,
                reason: "legacy module size".into(),
                reference: "agent-guardrails-open-items.md".into(),
            }],
            migrations: std::collections::BTreeMap::new(),
            builds: std::collections::BTreeMap::new(),
        };

        let active = policy
            .apply("src/foo/guardrails.toml", std::slice::from_ref(&finding))
            .unwrap();
        assert!(active.is_empty());

        let mut improved = finding.clone();
        improved.actual = 42;
        let stale = policy
            .apply("src/foo/guardrails.toml", std::slice::from_ref(&improved))
            .unwrap();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].code, "ERROR_STALE_EXEMPTION");

        let lowered = policy.ratchet(std::slice::from_ref(&improved));
        assert_eq!(lowered, 1);
        assert_eq!(policy.exemptions[0].ceiling, 42);
    }

    #[test]
    fn detects_changed_symbols_between_branches() {
        let base = "pub fn a() -> u32 { 1 }\npub fn b() -> u32 { 2 }\n";
        let branch = "pub fn a() -> u32 { 1 }\npub fn b() -> u32 { 99 }\n";
        let changed = analysis::changed_symbols(base, branch);
        assert!(changed.contains("module::fn b"));
        assert!(!changed.contains("module::fn a"));
    }
}
