use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;
use serde::Serialize;
use walkdir::WalkDir;

use crate::guardrails::Finding;
use crate::guardrails::analysis;
use crate::guardrails::policy::Api;
use crate::guardrails::policy::Exemption;
use crate::guardrails::policy::LayerRole;
use crate::guardrails::policy::Policy;
use crate::guardrails::policy::Reason;
use crate::interlude::*;

pub struct Repository {
    pub root: PathBuf,
    pub packages: BTreeMap<String, Package>,
    /// Workspace-root manifests under `src/` (relative to `root`), excluding the root manifest.
    pub workspace_manifests: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Package {
    pub name: String,
    pub manifest_path: PathBuf,
    pub dependencies: Vec<Dependency>,
    pub features: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Dependency {
    pub name: String,
    pub path: Option<PathBuf>,
    pub kind: Option<String>,
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Serialize)]
pub struct Report {
    pub metrics: Vec<analysis::Metric>,
    pub findings: BTreeMap<String, Vec<Finding>>,
    pub dependencies: BTreeMap<String, BTreeSet<String>>,
    pub reverse_dependencies: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Serialize)]
pub struct Plan {
    pub directly_changed: BTreeSet<String>,
    pub affected: BTreeSet<String>,
    pub global_input_changed: bool,
    pub commands: Vec<Vec<String>>,
}

const PILOT_API_PACKAGES: &[&str] = &[
    "big_sync_core",
    "daybook_ffi",
    "daybook_pdk",
    "daybook_types",
    "wflow_sdk",
];

fn infer_role(name: &str) -> LayerRole {
    match name {
        "utils_rs" | "sqlite_rs" | "am_utils_rs" | "wasm_utils_rs" | "secrets_rs" => {
            LayerRole::Foundation
        }
        "daybook_types" | "big_sync_core" | "wflow_core" => LayerRole::Domain,
        "daybook_pdk" | "wflow_sdk" | "daybook_ffi" => LayerRole::Interface,
        "plug_test" | "plug_plabels" | "plug_dayledger" | "test_wflows" => LayerRole::Plugin,
        "daybook_cli" | "btress_sysadmin" => LayerRole::Application,
        "xtask" => LayerRole::Tooling,
        _ => LayerRole::Runtime,
    }
}

impl Repository {
    fn metadata(root: &Path, manifest: Option<&Path>) -> Res<Metadata> {
        let mut command = Command::new("cargo");
        command.current_dir(root).args([
            "metadata",
            "--no-deps",
            "--locked",
            "--offline",
            "--format-version",
            "1",
        ]);
        if let Some(manifest) = manifest {
            command.arg("--manifest-path").arg(manifest);
        }
        let bytes = output(&mut command)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn load(root: &Path) -> Res<Self> {
        let mut metadata = Self::metadata(root, None)?;
        let mut names: BTreeSet<String> = metadata
            .packages
            .iter()
            .map(|package| package.name.clone())
            .collect();
        let mut workspace_manifests = Vec::new();
        for entry in std::fs::read_dir(root.join("src"))? {
            let manifest = entry?.path().join("Cargo.toml");
            if !manifest.is_file() {
                continue;
            }
            let value: toml::Value = toml::from_str(&std::fs::read_to_string(&manifest)?)?;
            if value.get("workspace").is_none() {
                continue;
            }
            let extra = Self::metadata(root, Some(&manifest))?;
            for package in &extra.packages {
                eyre::ensure!(
                    names.insert(package.name.clone()),
                    "duplicate package {} across workspaces",
                    package.name
                );
            }
            metadata.packages.extend(extra.packages);
            workspace_manifests.push(manifest.strip_prefix(root)?.to_path_buf());
        }
        workspace_manifests.sort();
        Ok(Self {
            root: root.into(),
            packages: metadata
                .packages
                .into_iter()
                .map(|package| (package.name.clone(), package))
                .collect(),
            workspace_manifests,
        })
    }

    pub fn edges(&self, package: &Package) -> BTreeSet<String> {
        package
            .dependencies
            .iter()
            .filter(|dependency| {
                dependency.path.is_some() && self.packages.contains_key(&dependency.name)
            })
            .map(|dependency| dependency.name.clone())
            .collect()
    }

    pub fn reverse_dependencies(&self, target: &str) -> BTreeSet<String> {
        let mut affected = BTreeSet::new();
        affected.insert(target.to_string());
        loop {
            let before = affected.len();
            for package in self.packages.values() {
                if self.edges(package).iter().any(|dep| affected.contains(dep)) {
                    affected.insert(package.name.clone());
                }
            }
            if affected.len() == before {
                break;
            }
        }
        affected.remove(target);
        affected
    }

    pub fn scan(&self, filter_package: Option<&str>) -> Res<Report> {
        let mut report = Report {
            metrics: Vec::new(),
            findings: BTreeMap::new(),
            dependencies: BTreeMap::new(),
            reverse_dependencies: BTreeMap::new(),
        };
        for package in self.packages.values() {
            if let Some(target) = filter_package
                && package.name != target
            {
                continue;
            }
            let dir = package
                .manifest_path
                .parent()
                .ok_or_eyre("manifest without parent")?;
            let mut findings = Vec::new();
            for file in files(dir)? {
                if file.extension().is_some_and(|extension| extension == "rs") {
                    let text = std::fs::read_to_string(&file)?;
                    let (metrics, mut diagnostics) =
                        analysis::scan(&relative(&self.root, &file)?, &text);
                    report.metrics.extend(metrics);
                    findings.append(&mut diagnostics);
                }
            }
            findings.sort();
            report.findings.insert(package.name.clone(), findings);
            report
                .dependencies
                .insert(package.name.clone(), self.edges(package));
            report.reverse_dependencies.insert(
                package.name.clone(),
                self.reverse_dependencies(&package.name),
            );
        }
        if filter_package.is_none() {
            report
                .findings
                .insert("@docs".into(), crate::guardrails::docs::scan(&self.root)?);
        }
        Ok(report)
    }

    pub fn policy_path(&self, name: &str) -> Res<PathBuf> {
        if name == "@docs" {
            return Ok(self.root.join("docs/guardrails.toml"));
        }
        Ok(self
            .packages
            .get(name)
            .ok_or_else(|| eyre::eyre!("unknown package {name}"))?
            .manifest_path
            .parent()
            .ok_or_eyre("manifest parent")?
            .join("guardrails.toml"))
    }

    pub fn check(&self, report: &Report) -> Res<Vec<Finding>> {
        let mut result = Vec::new();
        for (name, findings) in &report.findings {
            let path = self.policy_path(name)?;
            if !path.is_file() {
                result.push(Finding::new(
                    "ERROR_POLICY_MISSING",
                    &relative(&self.root, &path)?,
                    name,
                    1,
                    "review inventory and add a crate-local policy; automatic acceptance is not part of check",
                ));
                result.extend(findings.iter().cloned());
                continue;
            }
            let policy: Policy = toml::from_str(&std::fs::read_to_string(&path)?)
                .wrap_err_with(|| format!("invalid policy {}", path.display()))?;
            result.extend(policy.apply(&relative(&self.root, &path)?, findings)?);
            if let Some(dependencies) = report.dependencies.get(name) {
                for added in dependencies.difference(&policy.allowed_dependencies) {
                    result.push(Finding::new(
                        "ERROR_DEPENDENCY_BOUNDARY",
                        &relative(&self.root, &path)?,
                        name,
                        1,
                        format!(
                            "new dependency on {added}; review architecture and amend policy explicitly"
                        ),
                    ));
                }
                for removed in policy.allowed_dependencies.difference(dependencies) {
                    result.push(Finding::new(
                        "ERROR_STALE_DEPENDENCY",
                        &relative(&self.root, &path)?,
                        name,
                        1,
                        format!("remove obsolete dependency allowance {removed}"),
                    ));
                }
                let actual = self.migrations(name)?;
                for (migration, checksum) in &policy.migrations {
                    if actual.get(migration) != Some(checksum) {
                        result.push(Finding::new(
                            "ERROR_MIGRATION_HISTORY",
                            &relative(&self.root, &path)?,
                            migration,
                            1,
                            "applied migration changed or moved; preserve SQL bytes and persisted path-derived identity",
                        ));
                    }
                }
                for migration in actual
                    .keys()
                    .filter(|key| !policy.migrations.contains_key(*key))
                {
                    result.push(Finding::new(
                        "ERROR_MIGRATION_REVIEW",
                        &relative(&self.root, &path)?,
                        migration,
                        1,
                        "new migration: review stream ordering and record its hash in the local policy",
                    ));
                }
            }
        }
        result.sort();
        Ok(result)
    }

    pub fn migrations(&self, name: &str) -> Res<BTreeMap<String, String>> {
        let mut result = BTreeMap::new();
        let package = &self.packages[name];
        for file in files(
            package
                .manifest_path
                .parent()
                .ok_or_eyre("manifest parent")?,
        )? {
            if file.extension().is_some_and(|extension| extension == "sql")
                && file
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "migrations")
            {
                result.insert(
                    relative(&self.root, &file)?,
                    blake3::hash(&std::fs::read(&file)?).to_hex().to_string(),
                );
            }
        }
        Ok(result)
    }

    pub fn adopt(&self, report: &Report, reason: &str, reference: &str) -> Res<()> {
        eyre::ensure!(
            !reason.trim().is_empty() && !reference.trim().is_empty(),
            "adoption needs a reason and reference"
        );
        for name in report.findings.keys() {
            eyre::ensure!(
                !self.policy_path(name)?.exists(),
                "policy for {name} already exists; edit specific exemptions, never rebaseline"
            );
        }
        for (name, findings) in &report.findings {
            let api = if PILOT_API_PACKAGES.contains(&name.as_str()) {
                Some(Api {
                    target: "x86_64-unknown-linux-gnu".into(),
                    default_features: true,
                    features: Vec::new(),
                })
            } else {
                None
            };
            let role = if name == "@docs" {
                LayerRole::Tooling
            } else {
                infer_role(name)
            };
            let policy = Policy {
                version: 1,
                role,
                dependency_reason: reason.into(),
                allowed_dependencies: report.dependencies.get(name).cloned().unwrap_or_default(),
                allow_external_api_types: matches!(name.as_str(), "utils_rs" | "daybook_ffi"),
                api,
                builds: BTreeMap::new(),
                exemptions: findings
                    .iter()
                    .map(|finding| Exemption {
                        code: finding.code.clone(),
                        path: finding.path.clone(),
                        symbol: finding.symbol.clone(),
                        ceiling: finding.actual,
                        class: Reason::LegacyDebt,
                        reason: format!("{reason}: {}", finding.message),
                        reference: reference.into(),
                    })
                    .collect(),
                migrations: if name == "@docs" {
                    BTreeMap::new()
                } else {
                    self.migrations(name)?
                },
            };
            policy.validate()?;
            std::fs::write(self.policy_path(name)?, toml::to_string_pretty(&policy)?)?;
        }
        Ok(())
    }

    pub fn bless(&self, report: &Report) -> Res<usize> {
        let mut total_lowered = 0;
        for (name, findings) in &report.findings {
            let path = self.policy_path(name)?;
            if !path.is_file() {
                continue;
            }
            let mut policy: Policy = toml::from_str(&std::fs::read_to_string(&path)?)?;
            let lowered = policy.ratchet(findings);
            if lowered > 0 {
                policy.validate()?;
                std::fs::write(&path, toml::to_string_pretty(&policy)?)?;
                total_lowered += lowered;
            }
        }
        Ok(total_lowered)
    }

    pub fn plan(&self, paths: &[PathBuf]) -> Res<Plan> {
        let mut directly_changed = BTreeSet::new();
        let mut global = false;
        for path in paths {
            eyre::ensure!(
                !path.is_absolute()
                    && !path
                        .components()
                        .any(|part| part == std::path::Component::ParentDir),
                "plan paths must be workspace-relative, including deleted paths"
            );
            if self
                .workspace_manifests
                .iter()
                .any(|manifest| path == manifest || path == &manifest.with_file_name("Cargo.lock"))
            {
                global = true;
            }
            let absolute = self.root.join(path);
            let mut owner = false;
            for package in self.packages.values() {
                if absolute.starts_with(
                    package
                        .manifest_path
                        .parent()
                        .ok_or_eyre("manifest parent")?,
                ) {
                    directly_changed.insert(package.name.clone());
                    owner = true;
                }
            }
            if !owner {
                global = true;
            }
        }
        let mut affected = directly_changed.clone();
        if global {
            affected.extend(self.packages.keys().cloned());
        }
        loop {
            let before = affected.len();
            for package in self.packages.values() {
                if self
                    .edges(package)
                    .iter()
                    .any(|dependency| affected.contains(dependency))
                {
                    affected.insert(package.name.clone());
                }
            }
            if before == affected.len() {
                break;
            }
        }
        let mut commands = Vec::new();
        for package in &affected {
            let policy: Policy =
                toml::from_str(&std::fs::read_to_string(self.policy_path(package)?)?)?;
            if !policy.builds.is_empty() {
                commands.push(vec![
                    "cargo".into(),
                    "x".into(),
                    "guardrails".into(),
                    "check-matrix".into(),
                    "--package".into(),
                    package.clone(),
                ]);
                if !policy.builds.contains_key("native") {
                    continue;
                }
            }
            for command in [
                vec![
                    "cargo",
                    "clippy",
                    "--locked",
                    "-p",
                    package,
                    "--all-targets",
                    "--all-features",
                ],
                vec!["cargo", "nextest", "run", "--locked", "-p", package],
            ] {
                commands.push(command.into_iter().map(String::from).collect());
            }
        }
        Ok(Plan {
            directly_changed,
            affected,
            global_input_changed: global,
            commands,
        })
    }
}

pub fn files(root: &Path) -> Res<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            !matches!(
                entry.file_name().to_str(),
                Some(
                    "target"
                        | "node_modules"
                        | ".git"
                        | ".jj"
                        | ".gradle"
                        | "build"
                        | ".agent-duel"
                        | "daybook_compose"
                )
            )
        })
    {
        let entry = entry?;
        if entry.file_type().is_file() {
            files.push(entry.into_path());
        }
    }
    files.sort();
    Ok(files)
}

pub fn relative(root: &Path, path: &Path) -> Res<String> {
    Ok(path
        .strip_prefix(root)?
        .to_str()
        .ok_or_eyre("non-UTF8 input path")?
        .replace('\\', "/"))
}

pub fn output(command: &mut Command) -> Res<Vec<u8>> {
    let result = command
        .output()
        .wrap_err_with(|| format!("execute {command:?}"))?;
    eyre::ensure!(
        result.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(result.stdout)
}
