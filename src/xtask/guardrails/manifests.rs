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

use std::collections::BTreeSet;
use std::path::PathBuf;

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
}

pub fn validate(repo: &Repository) -> Res<Vec<Finding>> {
    let mut manifests: BTreeSet<PathBuf> = repo
        .packages
        .values()
        .map(|package| package.manifest_path.clone())
        .collect();
    manifests.insert(repo.root.join("Cargo.toml"));
    manifests.extend(
        repo.workspace_manifests
            .iter()
            .map(|manifest| repo.root.join(manifest)),
    );
    let mut findings = Vec::new();
    for manifest in &manifests {
        let relative_path = relative(&repo.root, manifest)?;
        let text = std::fs::read_to_string(manifest)?;
        let value: toml::Value =
            toml::from_str(&text).wrap_err_with(|| format!("parse {relative_path}"))?;
        for entry in entries(&text, &value)? {
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
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        if line.starts_with('[') {
            table = None;
            seen.clear();
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
                }),
                None => table = Some((resolved, workspace_level)),
            }
            continue;
        }
        let Some((specs, workspace_level)) = table else {
            continue;
        };
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
