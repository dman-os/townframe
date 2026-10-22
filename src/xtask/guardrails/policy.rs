use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::guardrails::Finding;
use crate::interlude::*;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub version: u32,
    #[serde(default)]
    pub role: LayerRole,
    pub dependency_reason: String,
    pub allowed_dependencies: BTreeSet<String>,
    #[serde(default)]
    pub allow_external_api_types: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<Api>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exemptions: Vec<Exemption>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub migrations: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub builds: BTreeMap<String, super::matrix::Build>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerRole {
    Foundation,
    #[default]
    Domain,
    Runtime,
    Interface,
    Plugin,
    Application,
    Tooling,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Api {
    pub target: String,
    pub default_features: bool,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Exemption {
    pub code: String,
    pub path: String,
    pub symbol: String,
    pub ceiling: usize,
    pub class: Reason,
    pub reason: String,
    pub reference: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Reason {
    LegacyDebt,
    Generated,
    ContractTest,
    SingleWriterActor,
    ArchitecturalException,
}

impl Policy {
    pub fn validate(&self) -> Res<()> {
        eyre::ensure!(
            self.version == 1,
            "unsupported guardrail policy version {}",
            self.version
        );
        eyre::ensure!(
            !self.dependency_reason.trim().is_empty(),
            "dependency policy needs a reason"
        );
        let mut selectors = BTreeSet::new();
        for exemption in &self.exemptions {
            eyre::ensure!(
                !exemption.reason.trim().is_empty() && !exemption.reference.trim().is_empty(),
                "exemption {}:{} needs a reason and reference",
                exemption.path,
                exemption.symbol
            );
            eyre::ensure!(
                selectors.insert((&exemption.code, &exemption.path, &exemption.symbol)),
                "duplicate exemption {}:{} {}",
                exemption.path,
                exemption.symbol,
                exemption.code
            );
            eyre::ensure!(exemption.ceiling > 0, "exemption ceiling must be positive");
            eyre::ensure!(
                !exemption.path.contains('*') && !exemption.symbol.contains('*'),
                "wildcard exemptions are not permitted"
            );
        }
        Ok(())
    }

    pub fn apply(&self, path: &str, findings: &[Finding]) -> Res<Vec<Finding>> {
        self.validate()?;
        let mut remaining = findings.to_vec();
        let mut stale = Vec::new();
        for exemption in &self.exemptions {
            let matching = remaining.iter().position(|finding| {
                finding.code == exemption.code
                    && finding.path == exemption.path
                    && finding.symbol == exemption.symbol
            });
            if let Some(index) = matching {
                if remaining[index].actual == exemption.ceiling {
                    remaining.remove(index);
                } else if remaining[index].actual < exemption.ceiling {
                    stale.push(Finding::new(
                        "ERROR_STALE_EXEMPTION",
                        path,
                        &exemption.symbol,
                        1,
                        format!(
                            "{}:{} improved; lower the ceiling to {} or remove the exemption",
                            exemption.path, exemption.code, remaining[index].actual
                        ),
                    ));
                    remaining.remove(index);
                } else {
                    remaining[index].limit = exemption.ceiling;
                    remaining[index].message = format!(
                        "{}; legacy ceiling exceeded, do not broaden the exemption automatically",
                        remaining[index].message
                    );
                }
            } else {
                stale.push(Finding::new(
                    "ERROR_STALE_EXEMPTION",
                    path,
                    &exemption.symbol,
                    1,
                    format!(
                        "remove unused exemption {}:{}",
                        exemption.path, exemption.code
                    ),
                ));
            }
        }
        remaining.extend(stale);
        Ok(remaining)
    }

    pub fn ratchet(&mut self, findings: &[Finding]) -> usize {
        let mut lowered = 0;
        self.exemptions.retain_mut(|exemption| {
            if let Some(finding) = findings.iter().find(|finding| {
                finding.code == exemption.code
                    && finding.path == exemption.path
                    && finding.symbol == exemption.symbol
            }) {
                if finding.actual < exemption.ceiling {
                    exemption.ceiling = finding.actual;
                    lowered += 1;
                }
                true
            } else {
                lowered += 1;
                false
            }
        });
        lowered
    }
}
