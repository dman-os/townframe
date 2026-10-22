use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::process::Command;

use serde::Deserialize;
use serde::Serialize;

use crate::guardrails::Finding;
use crate::guardrails::policy::Policy;
use crate::guardrails::repository::Repository;
use crate::interlude::*;

const TARGETS: [&str; 3] = ["native", "wasm32-wasip3", "wasm32-unknown-unknown"];

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Build {
    #[serde(default)]
    pub features: BTreeSet<String>,
    #[serde(default)]
    pub required_features: BTreeSet<String>,
    #[serde(default = "enabled")]
    pub default_features: bool,
    #[serde(default)]
    pub powerset: bool,
}

fn enabled() -> bool {
    true
}

pub fn validate(repo: &Repository) -> Res<Vec<Finding>> {
    let policies = policies(repo)?;
    let mut findings = Vec::new();
    for (name, policy) in &policies {
        let package = &repo.packages[name];
        let policy_path = format!("src/{name}/guardrails.toml");
        let manifest: toml::Value =
            toml::from_str(&std::fs::read_to_string(&package.manifest_path)?)?;
        if manifest
            .get("package")
            .and_then(|value| value.get("workspace"))
            .and_then(toml::Value::as_str)
            == Some("../wasm_utils_rs")
        {
            let declares_wasm = policy
                .builds
                .keys()
                .any(|target| target.starts_with("wasm32-"));
            if !declares_wasm {
                findings.push(Finding::new(
                    "ERROR_TARGET_CONTRACT",
                    &policy_path,
                    name,
                    1,
                    "member of the wasm workspace without a wasm32 build contract",
                ));
            }
        }
        let defaults: BTreeSet<String> = package
            .features
            .get("default")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        if !policy.builds.is_empty() {
            let declared: BTreeSet<&String> = policy
                .builds
                .values()
                .flat_map(|build| build.features.union(&build.required_features))
                .collect();
            for feature in package
                .features
                .keys()
                .filter(|feature| *feature != "default")
            {
                if !declared.contains(feature) {
                    findings.push(Finding::new(
                        "ERROR_FEATURE_CONTRACT",
                        &policy_path,
                        feature,
                        1,
                        format!("feature {name}/{feature} is not covered by any build contract"),
                    ));
                }
            }
        }
        for feature in package.features.keys() {
            if matches!(feature.as_str(), "native" | "wasm" | "wasm32") {
                findings.push(Finding::new(
                    "ERROR_PLATFORM_FEATURE",
                    &policy_path,
                    feature,
                    1,
                    format!(
                        "{name}/{feature} encodes a platform as an additive feature; use a target contract or a separate crate"
                    ),
                ));
            }
        }
        for (target, build) in &policy.builds {
            if !TARGETS.contains(&target.as_str()) {
                findings.push(Finding::new(
                    "ERROR_TARGET_CONTRACT",
                    &policy_path,
                    target,
                    1,
                    format!("unsupported build target {target}; expected one of {TARGETS:?}"),
                ));
                continue;
            }
            for feature in build.features.union(&build.required_features) {
                if !package.features.contains_key(feature) {
                    findings.push(Finding::new(
                        "ERROR_FEATURE_CONTRACT",
                        &policy_path,
                        feature,
                        1,
                        format!("{name} declares unknown feature {feature}"),
                    ));
                }
            }
            if !build.powerset && build.features.len() > 8 {
                findings.push(Finding::new(
                    "ERROR_FEATURE_CONTRACT",
                    &policy_path,
                    name,
                    1,
                    format!(
                        "{} varies {} features per target; split the contract or narrow `features`",
                        name,
                        build.features.len()
                    ),
                ));
            }
            for effective in configurations(build, &defaults) {
                let resolved = effective.iter().all(|feature| {
                    package
                        .features
                        .get(feature)
                        .is_some_and(|implied| implied.iter().all(|dep| dep != feature))
                        || defaults.contains(feature)
                });
                if !resolved {
                    findings.push(Finding::new(
                        "ERROR_FEATURE_CONTRACT",
                        &policy_path,
                        name,
                        1,
                        "feature contract expands to an unknown feature",
                    ));
                }
            }
            for dependency in package
                .dependencies
                .iter()
                .filter(|dependency| dependency.kind.is_none())
            {
                let Some(dependency_policy) = policies.get(&dependency.name) else {
                    continue;
                };
                let covered = target == "native" && dependency_policy.builds.is_empty()
                    || dependency_policy.builds.contains_key(target);
                if !covered {
                    findings.push(Finding::new(
                        "ERROR_TARGET_BOUNDARY",
                        &policy_path,
                        name,
                        1,
                        format!(
                            "{target} build depends on {}, which has no contract for that target",
                            dependency.name
                        ),
                    ));
                }
            }
        }
    }
    Ok(findings)
}

fn policies(repo: &Repository) -> Res<BTreeMap<String, Policy>> {
    repo.packages
        .keys()
        .map(|name| {
            let path = repo.policy_path(name)?;
            let policy = toml::from_str(&std::fs::read_to_string(&path)?)?;
            Ok((name.clone(), policy))
        })
        .collect()
}

pub fn run(
    repo: &Repository,
    package: Option<&str>,
    target: Option<&str>,
    dry_run: bool,
) -> Res<()> {
    if let Some(name) = package {
        eyre::ensure!(repo.packages.contains_key(name), "unknown package {name}");
    }
    if let Some(name) = target {
        eyre::ensure!(
            TARGETS.contains(&name),
            "unsupported target {name}; expected one of {TARGETS:?}"
        );
    }
    let mut commands = 0;
    for (name, policy) in policies(repo)? {
        if package.is_some_and(|filter| filter != name) {
            continue;
        }
        let defaults: BTreeSet<String> = repo.packages[&name]
            .features
            .get("default")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for (triple, build) in policy.builds {
            if target.is_some_and(|filter| filter != triple) {
                continue;
            }
            for features in configurations(&build, &defaults) {
                let mut args = vec![
                    "clippy".to_string(),
                    "--config".into(),
                    "resolver.feature-unification=\"selected\"".into(),
                    "--locked".into(),
                    "--manifest-path".into(),
                    repo.packages[&name].manifest_path.display().to_string(),
                    "-p".into(),
                    name.clone(),
                    "--lib".into(),
                    "--no-default-features".into(),
                ];
                if triple != "native" {
                    args.extend([
                        "--target".into(),
                        triple.clone(),
                        "--profile".into(),
                        "wasm-release".into(),
                    ]);
                }
                if !features.is_empty() {
                    args.extend([
                        "--features".into(),
                        features.into_iter().collect::<Vec<_>>().join(","),
                    ]);
                }
                args.extend(["--".into(), "-D".into(), "warnings".into()]);
                println!("cargo {}", args.join(" "));
                commands += 1;
                if dry_run {
                    continue;
                }
                let status = Command::new("cargo")
                    .args(&args)
                    .current_dir(&repo.root)
                    .status()?;
                eyre::ensure!(
                    status.success(),
                    "matrix check failed for {name} ({triple})"
                );
            }
        }
    }
    eyre::ensure!(
        commands > 0,
        "no declared build matched the requested matrix selection"
    );
    Ok(())
}

fn configurations(build: &Build, defaults: &BTreeSet<String>) -> BTreeSet<BTreeSet<String>> {
    let optional: Vec<&String> = build.features.iter().collect();
    let mut subsets: Vec<BTreeSet<String>> = Vec::new();
    if build.powerset {
        for mask in 0..(1usize << optional.len()) {
            subsets.push(
                optional
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| mask & (1 << index) != 0)
                    .map(|(_, feature)| (*feature).clone())
                    .collect(),
            );
        }
    } else {
        subsets.push(BTreeSet::new());
        for feature in &optional {
            subsets.push([(*feature).clone()].into());
        }
        subsets.push(optional.into_iter().cloned().collect());
    }
    let mut configurations = BTreeSet::new();
    for subset in subsets {
        let mut features = build.required_features.clone();
        features.extend(subset);
        configurations.insert(features.clone());
        if build.default_features {
            features.extend(defaults.iter().cloned());
            configurations.insert(features);
        }
    }
    configurations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guardrails::policy::Policy;
    use crate::guardrails::repository::Dependency;
    use crate::guardrails::repository::Package;

    fn build(
        features: &[&str],
        required: &[&str],
        default_features: bool,
        powerset: bool,
    ) -> Build {
        Build {
            features: features.iter().map(|feature| feature.to_string()).collect(),
            required_features: required.iter().map(|feature| feature.to_string()).collect(),
            default_features,
            powerset,
        }
    }

    fn repo(packages: &[(&str, &str, Policy, Vec<Dependency>)]) -> (tempfile::TempDir, Repository) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let mut map = BTreeMap::new();
        for (name, workspace, policy, dependencies) in packages {
            let dir = temp.path().join(name);
            std::fs::create_dir(&dir).unwrap();
            std::fs::write(
                dir.join("guardrails.toml"),
                toml::to_string(policy).unwrap(),
            )
            .unwrap();
            std::fs::write(
                dir.join("Cargo.toml"),
                format!("[package]\nname = \"{name}\"\nworkspace = \"{workspace}\"\n"),
            )
            .unwrap();
            map.insert(
                name.to_string(),
                Package {
                    name: name.to_string(),
                    manifest_path: dir.join("Cargo.toml"),
                    dependencies: dependencies.clone(),
                    features: BTreeMap::new(),
                },
            );
        }
        (
            temp,
            Repository {
                root,
                packages: map,
                workspace_manifests: Vec::new(),
            },
        )
    }

    fn wasm_policy(target: &str, required: &[&str]) -> Policy {
        let mut policy = Policy {
            version: 1,
            dependency_reason: "test".into(),
            ..Policy::default()
        };
        policy
            .builds
            .insert(target.into(), build(&[], required, false, false));
        policy
    }

    #[test]
    fn varies_each_feature_once_plus_the_default_set() {
        let defaults: BTreeSet<String> = ["hash".into()].into();
        let configs = configurations(&build(&["hash", "schema"], &[], true, false), &defaults);
        assert_eq!(
            configs,
            [
                BTreeSet::new(),
                ["hash".into()].into(),
                ["schema".into()].into(),
                ["hash".into(), "schema".into()].into()
            ]
            .into()
        );
    }

    #[test]
    fn powerset_covers_every_subset_without_defaults() {
        let configs = configurations(&build(&["a", "b"], &["rt"], false, true), &BTreeSet::new());
        assert_eq!(
            configs,
            [
                ["rt".into()].into(),
                ["rt".into(), "a".into()].into(),
                ["rt".into(), "b".into()].into(),
                ["rt".into(), "a".into(), "b".into()].into()
            ]
            .into()
        );
    }

    #[test]
    fn rejects_native_dependencies_but_not_native_test_harnesses() {
        let guest_policy = wasm_policy("wasm32-wasip3", &[]);
        let host_dependency = Dependency {
            name: "host".into(),
            path: Some("host".into()),
            kind: None,
        };
        let (_temp, mut repository) = repo(&[
            (
                "guest",
                "../wasm_utils_rs",
                guest_policy,
                vec![host_dependency],
            ),
            ("host", "", Policy::default(), vec![]),
        ]);
        let findings = validate(&repository).unwrap();
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["ERROR_TARGET_BOUNDARY"]
        );
        repository.packages.get_mut("guest").unwrap().dependencies[0].kind = Some("dev".into());
        assert!(validate(&repository).unwrap().is_empty());
    }

    #[test]
    fn rejects_platform_named_features_and_uncovered_features() {
        let (_temp, mut repository) = repo(&[(
            "guest",
            "../wasm_utils_rs",
            wasm_policy("wasm32-wasip3", &[]),
            vec![],
        )]);
        repository.packages.get_mut("guest").unwrap().features =
            [("native".into(), vec![]), ("extra".into(), vec![])]
                .into_iter()
                .collect();
        let findings = validate(&repository).unwrap();
        let codes: BTreeSet<&str> = findings
            .iter()
            .map(|finding| finding.code.as_str())
            .collect();
        assert!(codes.contains("ERROR_PLATFORM_FEATURE"));
        assert!(codes.contains("ERROR_FEATURE_CONTRACT"));
    }

    #[test]
    fn requires_a_wasm_contract_from_wasm_workspace_members() {
        let (_temp, repository) = repo(&[("guest", "../wasm_utils_rs", Policy::default(), vec![])]);
        let findings = validate(&repository).unwrap();
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, "ERROR_TARGET_CONTRACT");
    }
}
