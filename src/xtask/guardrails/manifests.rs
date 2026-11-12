//! Manifest-shape lints over every `Cargo.toml` in the repository.
//!
//! These checks read the manifest text rather than `cargo metadata`: metadata resolves
//! `foo.workspace = true` to the same `path` a literal `path = "../foo"` produces, so only the
//! source can tell the two spellings apart.
//!
//! `ERROR_PATH_DEPENDENCY`: a member crate never writes `path = ".."` in a dependency table;
//! the path is declared once in `[workspace.dependencies]` and members inherit it with
//! `workspace = true`, so moving a crate or changing its default features is a one-line edit.
//! The rule is per workspace, not root-only: `src/wasm_utils_rs` and `src/btress_sysadmin` are
//! separate workspaces (own lockfile and feature unification), each with its own
//! `[workspace.dependencies]` that points at the shared crates by path. Those tables are the
//! allowed place for a path, so they are exempt; their members are not.
//!
//! `ERROR_DEPENDENCY_GROUP`: manifests split a dependency table into groups, each opened by a
//! bare `#` line followed by a `# <label>` line (`# encoding`, `# async`, `# log`, ..). Group
//! names and their order are free-form except one: every dependency on a crate of this
//! repository sits in the group labelled `# internal`, so the internal edges of a crate are
//! readable in one block. A dependency before the first group header is ungrouped and fails.
//! The `[dependencies.name]` subtable spelling has no place for a group and is not accepted for
//! internal crates.

use std::collections::BTreeSet;
use std::path::Path;

use crate::guardrails::Finding;
use crate::guardrails::repository::Repository;
use crate::guardrails::repository::relative;
use crate::interlude::*;

const DEPENDENCY_TABLES: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];

/// One dependency declaration found in a manifest.
struct Entry<'a> {
    /// 1-based line of the declaration (its key line, or its `[dependencies.name]` header).
    line: usize,
    /// The dependency key as written; `spec.package` takes over when it renames a crate.
    key: String,
    spec: &'a toml::Value,
    /// Declared in `[workspace.dependencies]`, the one place a `path` is allowed.
    workspace_level: bool,
    /// Label of the `#` / `# label` comment group the declaration sits under, if any.
    group: Option<String>,
}

/// Lints the root `Cargo.toml`, which no crate policy owns, so its findings cannot be exempted.
pub fn scan_root(repo: &Repository) -> Res<Vec<Finding>> {
    scan(repo, &repo.root.join("Cargo.toml"))
}

/// Lints one manifest. `ERROR_PATH_DEPENDENCY` is reported per declaration. Internal crates
/// outside the `internal` group are one `ERROR_DEPENDENCY_GROUP` finding per manifest whose
/// `actual` is the offender count, so a crate's `guardrails.toml` can exempt a legacy manifest
/// at its current count and `bless` ratchets the ceiling down as it is migrated.
pub fn scan(repo: &Repository, manifest: &Path) -> Res<Vec<Finding>> {
    let relative_path = relative(&repo.root, manifest)?;
    let text = std::fs::read_to_string(manifest)?;
    let value: toml::Value =
        toml::from_str(&text).wrap_err_with(|| format!("parse {relative_path}"))?;
    let mut findings = Vec::new();
    let mut ungrouped = Vec::new();
    for entry in entries(&text, &value)? {
        let package = entry
            .spec
            .get("package")
            .and_then(toml::Value::as_str)
            .unwrap_or(&entry.key);
        if repo.packages.contains_key(package) && entry.group.as_deref() != Some("internal") {
            ungrouped.push((entry.line, package.to_string()));
        }
        if !entry.workspace_level && entry.spec.get("path").is_some() {
            findings.push(Finding::new(
                "ERROR_PATH_DEPENDENCY",
                &relative_path,
                &entry.key,
                entry.line,
                format!(
                    "`{}` is a path dependency; declare it once in `[workspace.dependencies]` of this manifest's workspace and write `{} = {{ workspace = true }}` here",
                    entry.key, entry.key
                ),
            ));
        }
    }
    if let Some((first_line, _)) = ungrouped.first() {
        let mut finding = Finding::new(
            "ERROR_DEPENDENCY_GROUP",
            &relative_path,
            "dependency-groups",
            *first_line,
            format!(
                "internal crates must sit under a `#` / `# internal` comment header; outside it: {}",
                ungrouped
                    .iter()
                    .map(|(line, name)| format!("{name} (line {line})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
        finding.actual = ungrouped.len();
        findings.push(finding);
    }
    findings.sort();
    Ok(findings)
}
/// Walks the manifest text for dependency declarations, pairing each with its parsed spec.
///
/// A line-based pass is needed for line numbers; the table a header names is recovered by
/// parsing the header itself as TOML, so quoting in `[target.'cfg(unix)'.dependencies]` needs no
/// hand-rolled handling. Declarations are keys at column 0 of a dependency table (continuation
/// lines of multi-line values are indented or start with a closing bracket), and a dotted
/// `foo.workspace = true` / `foo.features = [..]` pair counts once.
fn entries<'a>(text: &str, value: &'a toml::Value) -> Res<Vec<Entry<'a>>> {
    let mut result = Vec::new();
    // The dependency table the cursor is inside: its parsed contents and whether it is workspace-level.
    let mut table: Option<(&'a toml::Table, bool)> = None;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut group: Option<String> = None;
    let lines: Vec<&str> = text.lines().collect();
    for (index, line) in lines.iter().copied().enumerate() {
        let line_number = index + 1;
        if line.starts_with('[') {
            table = None;
            seen.clear();
            group = None;
            let header: toml::Table = toml::from_str(&format!("{line}\n"))
                .wrap_err_with(|| format!("table header at line {line_number}"))?;
            let mut segments = Vec::new();
            let mut cursor = &header;
            while let Some((key, next)) = cursor.iter().next() {
                segments.push(key.as_str());
                match next.as_table() {
                    Some(inner) => cursor = inner,
                    None => break,
                }
            }
            let Some(at) = segments
                .iter()
                .position(|segment| DEPENDENCY_TABLES.contains(segment))
            else {
                continue;
            };
            let workspace_level = segments[0] == "workspace";
            let mut resolved = Some(value);
            for segment in &segments[..=at] {
                resolved = resolved.and_then(|value| value.get(segment));
            }
            let resolved = resolved
                .and_then(toml::Value::as_table)
                .ok_or_else(|| eyre::eyre!("no table for header at line {line_number}"))?;
            match segments.get(at + 1) {
                // `[dependencies.name]` is itself the declaration.
                Some(name) => result.push(Entry {
                    line: line_number,
                    key: (*name).to_string(),
                    spec: &resolved[*name],
                    workspace_level,
                    group: None,
                }),
                None => table = Some((resolved, workspace_level)),
            }
            continue;
        }
        let Some((specs, workspace_level)) = table else {
            continue;
        };
        if line.trim_end() == "#"
            && let Some(label) = lines
                .get(index + 1)
                .and_then(|next| next.strip_prefix("# "))
        {
            group = Some(label.trim().to_string());
            continue;
        }
        let key_len = line
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
            .unwrap_or(line.len());
        let (key, rest) = line.split_at(key_len);
        if key.is_empty() || !(rest.trim_start().starts_with('=') || rest.starts_with('.')) {
            continue;
        }
        if !seen.insert(key.to_string()) {
            continue;
        }
        let spec = specs
            .get(key)
            .ok_or_else(|| eyre::eyre!("key `{key}` at line {line_number} is not in its table"))?;
        result.push(Entry {
            line: line_number,
            key: key.to_string(),
            spec,
            workspace_level,
            group: group.clone(),
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lint(text: &str) -> Vec<(usize, String)> {
        let value: toml::Value = toml::from_str(text).unwrap();
        entries(text, &value)
            .unwrap()
            .into_iter()
            .filter(|entry| !entry.workspace_level && entry.spec.get("path").is_some())
            .map(|entry| (entry.line, entry.key))
            .collect()
    }

    fn groups(text: &str) -> Vec<(String, Option<String>)> {
        let value: toml::Value = toml::from_str(text).unwrap();
        entries(text, &value)
            .unwrap()
            .into_iter()
            .map(|entry| (entry.key, entry.group))
            .collect()
    }

    #[test]
    fn tracks_comment_groups_per_table() {
        let text = r#"[dependencies]
a.workspace = true
#
# internal
b.workspace = true
c = { workspace = true, features = [
    "f",
] }
# not a header, same group
#
# encoding
d.workspace = true

[dev-dependencies]
e.workspace = true
"#;
        let internal = Some("internal".to_string());
        assert_eq!(
            groups(text),
            [
                ("a".into(), None),
                ("b".into(), internal.clone()),
                ("c".into(), internal),
                ("d".into(), Some("encoding".into())),
                ("e".into(), None),
            ]
        );
    }

    #[test]
    fn flags_path_dependencies_in_every_member_table_form() {
        let text = r#"[package]
name = "x"

[dependencies]
a = { path = "../a" }
b.workspace = true
c = { workspace = true, features = [
    "f",
] }
d.path = "../d"
d.features = ["x"]

[dev-dependencies]
e = { path = "../e", features = ["f"] }

[target.'cfg(unix)'.dependencies]
f = { path = "../f" }

[build-dependencies.g]
path = "../g"

[workspace.dependencies]
h = { path = "./h" }
"#;
        assert_eq!(
            lint(text),
            [
                (5, "a".into()),
                (10, "d".into()),
                (14, "e".into()),
                (17, "f".into()),
                (19, "g".into()),
            ]
        );
    }
}
