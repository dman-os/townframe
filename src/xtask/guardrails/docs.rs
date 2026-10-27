use std::collections::BTreeMap;
use std::path::Path;

use pulldown_cmark::Event;
use pulldown_cmark::LinkType;
use pulldown_cmark::Parser;
use pulldown_cmark::Tag;

use crate::guardrails::Finding;
use crate::guardrails::repository::files;
use crate::guardrails::repository::relative;
use crate::interlude::*;

pub fn scan(root: &Path) -> Res<Vec<Finding>> {
    let mut findings = Vec::new();
    let mut adr_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in files(&root.join("docs"))? {
        if path.extension().is_none_or(|extension| extension != "md") {
            continue;
        }
        if path == root.join("docs/DEVDOC/log.md") {
            continue;
        }
        let name = relative(root, &path)?;
        let text = std::fs::read_to_string(&path)?;
        findings.extend(links(root, &path, &text)?);
        if name.starts_with("docs/adrs/") {
            let stem = path.file_stem().expect("markdown stem").to_string_lossy();
            if let Some((id, _)) = stem.split_once('-') {
                adr_ids.entry(id.into()).or_default().push(name.clone());
            }
            if !text
                .lines()
                .any(|line| line.starts_with("Status:") || line.starts_with("**Status:**"))
            {
                findings.push(Finding::new(
                    "ERROR_ADR_STATUS",
                    &name,
                    "document",
                    1,
                    "state Proposed, Accepted, Superseded or Rejected; proposals must not look authoritative",
                ));
            }
        }
        if name.starts_with("docs/scratch/") {
            for field in ["Status:", "Owner:", "Updated:", "Scope:"] {
                if !text.lines().take(25).any(|line| line.starts_with(field)) {
                    findings.push(Finding::new(
                        "ERROR_DOC_METADATA",
                        &name,
                        field,
                        1,
                        format!(
                            "scratch notes need {field} in the first 25 lines; distinguish decisions from proposals"
                        ),
                    ));
                }
            }
        }
    }
    for (id, paths) in adr_ids {
        if paths.len() > 1 {
            let mut finding = Finding::new(
                "ERROR_ADR_ID",
                "docs/adrs",
                &id,
                1,
                format!(
                    "duplicate ADR identity: {}; do not rename accepted identities silently",
                    paths.join(", ")
                ),
            );
            finding.actual = paths.len();
            finding.limit = 1;
            findings.push(finding);
        }
    }
    let agents_md = root.join("AGENTS.md");
    if agents_md.is_file() {
        let agents_text = std::fs::read_to_string(&agents_md)?;
        let skills_dir = root.join(".agents/skills");
        if skills_dir.is_dir() {
            for entry in std::fs::read_dir(&skills_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    let skill_name = entry.file_name().to_string_lossy().to_string();
                    if !path.join("SKILL.md").is_file() {
                        findings.push(Finding::new(
                            "ERROR_SKILL_DOC",
                            "AGENTS.md",
                            &skill_name,
                            1,
                            format!(
                                "skill directory .agents/skills/{skill_name} is missing SKILL.md"
                            ),
                        ));
                    } else if !agents_text.contains(&skill_name) {
                        findings.push(Finding::new(
                            "ERROR_SKILL_DOC",
                            "AGENTS.md",
                            &skill_name,
                            1,
                            format!("skill `{skill_name}` is not referenced in AGENTS.md"),
                        ));
                    }
                }
            }
        }
    }
    findings.sort();
    Ok(findings)
}

pub fn links(root: &Path, path: &Path, text: &str) -> Res<Vec<Finding>> {
    let mut findings: BTreeMap<String, Finding> = BTreeMap::new();
    for (event, span) in Parser::new(text).into_offset_iter() {
        if let Event::Start(
            Tag::Link {
                dest_url,
                link_type,
                ..
            }
            | Tag::Image {
                dest_url,
                link_type,
                ..
            },
        ) = event
        {
            if matches!(link_type, LinkType::Autolink | LinkType::Email) {
                continue;
            }
            let target = dest_url.split(['#', '?']).next().unwrap_or_default();
            if target.is_empty() || target.starts_with('/') || target.contains(':') {
                continue;
            }
            let target_path = path.parent().expect("document parent").join(target);
            if !target_path.exists() {
                let line = text[..span.start]
                    .bytes()
                    .filter(|byte| *byte == b'\n')
                    .count()
                    + 1;
                findings.entry(target.to_string()).or_insert(Finding::new(
                    "ERROR_DOC_LINK",
                    &relative(root, path)?,
                    target,
                    line,
                    "relative link target does not exist; fix it or explicitly classify historical debt",
                ));
            }
        }
    }
    Ok(findings.into_values().collect())
}
