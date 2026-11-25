//! Renders the Lexicons the node publishes as checked-in files, and checks or rewrites them.
//!
//! Layout under the snapshot directory: `index.json` (NSID, owning crate, kind, revision, whether
//! a sidecar exists), `lexicons/<nsid>.json`, and `sidecars/<nsid>.schema.json`: a datum's
//! sidecar JSON Schema document, which carries the storage forms its Lexicon cannot state.

use crate::evolution;
use big_cabinet::SchemaRepo;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use utils_rs::prelude::*;

/// One published Lexicon with the crate that owns it.
struct Entry {
    nsid: String,
    owner: &'static str,
    lexicon: Value,
    sidecar: Option<Value>,
}

pub struct Snapshot {
    entries: Vec<Entry>,
    /// Every file the snapshot consists of, by path relative to the snapshot directory.
    files: BTreeMap<String, String>,
}

/// What stands between the checked-in directory and the generated snapshot.
#[derive(Debug, Default)]
pub struct Problems {
    /// Files that are missing, extra or different, by relative path.
    pub stale: Vec<String>,
    /// Lexicons whose change breaks the evolution rules.
    pub evolution: Vec<String>,
}

impl Problems {
    pub fn is_empty(&self) -> bool {
        self.stale.is_empty() && self.evolution.is_empty()
    }

    pub fn render(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .stale
            .iter()
            .map(|path| format!("stale: {path}"))
            .collect();
        if !self.stale.is_empty() {
            lines.push(
                "the snapshot is stale: run `cargo x guardrails wire --bless` and review the diff"
                    .to_owned(),
            );
        }
        lines.extend(
            self.evolution
                .iter()
                .map(|line| format!("evolution: {line}")),
        );
        lines
    }
}

/// Pretty JSON with every object's keys sorted, so a rewrite never reorders a file.
fn render(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let ordered: BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, value)| (key, sorted(value)))
                    .collect();
                Value::Object(
                    ordered
                        .into_iter()
                        .map(|(key, value)| (key.clone(), value))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    let mut text = serde_json::to_string_pretty(&sorted(value)).expect("a JSON value renders");
    text.push('\n');
    text
}

impl Snapshot {
    /// Everything the node publishes: the datums its schema repo knows (the core datums, then
    /// each group a node registers), the type library they refer to, and every built-in method.
    pub fn generate() -> Res<Self> {
        let mut entries = Vec::new();
        let mut repo = SchemaRepo::open([])?;
        let mut seen = BTreeSet::new();
        let mut take = |repo: &SchemaRepo, owner: &'static str, entries: &mut Vec<Entry>| {
            for handle in repo.schemas() {
                if seen.insert(handle.datum_tag().to_owned()) {
                    entries.push(Entry {
                        nsid: handle.datum_tag().to_owned(),
                        owner,
                        lexicon: handle.lexicon().clone(),
                        sidecar: handle.sidecar_document(),
                    });
                }
            }
        };
        take(&repo, "big_cabinet", &mut entries);
        for (owner, group) in big_node::schema_groups() {
            repo = repo.with_groups([group])?;
            take(&repo, owner, &mut entries);
        }
        for library in big_cabinet_core::reference::node_type_libraries() {
            entries.push(Entry {
                nsid: library["id"]
                    .as_str()
                    .expect("a Lexicon has an id")
                    .to_owned(),
                owner: "big_cabinet_core",
                lexicon: library.clone(),
                sidecar: None,
            });
        }
        for (nsid, owner, assemble) in method_lexicon::builtin_methods() {
            entries.push(Entry {
                nsid: nsid.to_owned(),
                owner,
                lexicon: assemble()?,
                sidecar: None,
            });
        }
        entries.sort_by(|left, right| left.nsid.cmp(&right.nsid));
        for pair in entries.windows(2) {
            eyre::ensure!(
                pair[0].nsid != pair[1].nsid,
                "`{}` is published twice",
                pair[0].nsid
            );
        }
        let mut files = BTreeMap::new();
        let mut index = Vec::new();
        for entry in &entries {
            eyre::ensure!(
                !entry.nsid.is_empty() && !entry.nsid.contains(['/', '\\']),
                "`{}` cannot be a file name",
                entry.nsid
            );
            files.insert(
                format!("lexicons/{}.json", entry.nsid),
                render(&entry.lexicon),
            );
            if let Some(sidecar) = &entry.sidecar {
                files.insert(
                    format!("sidecars/{}.schema.json", entry.nsid),
                    render(sidecar),
                );
            }
            index.push(json!({
                "nsid": entry.nsid,
                "crate": entry.owner,
                "kind": entry.lexicon["defs"]["main"]["type"].as_str().unwrap_or("defs"),
                "revision": entry.lexicon.get("revision"),
                "sidecar": entry.sidecar.is_some(),
            }));
        }
        files.insert("index.json".to_owned(), render(&Value::Array(index)));
        Ok(Self { entries, files })
    }

    pub fn summary(&self) -> String {
        let methods = self
            .entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.lexicon["defs"]["main"]["type"].as_str(),
                    Some("query" | "procedure" | "subscription")
                )
            })
            .count();
        let sidecars = self
            .entries
            .iter()
            .filter(|entry| entry.sidecar.is_some())
            .count();
        format!(
            "{} Lexicons, {} methods, {} sidecars",
            self.entries.len(),
            methods,
            sidecars
        )
    }

    /// Compares the snapshot to what `dir` holds: stale files, and for every Lexicon that
    /// changed, whether the change is allowed to keep its NSID.
    pub fn problems(&self, dir: &Path) -> Res<Problems> {
        let on_disk = read_dir(dir)?;
        let mut problems = Problems::default();
        for (path, text) in &self.files {
            if on_disk.get(path) != Some(text) {
                problems.stale.push(path.clone());
            }
        }
        problems.stale.extend(
            on_disk
                .keys()
                .filter(|path| !self.files.contains_key(*path))
                .cloned(),
        );
        for entry in &self.entries {
            let Some(old) = on_disk.get(&format!("lexicons/{}.json", entry.nsid)) else {
                continue;
            };
            let old: Value = serde_json::from_str(old)?;
            problems
                .evolution
                .extend(evolution::violations(&entry.nsid, &old, &entry.lexicon));
        }
        let published: BTreeSet<_> = self
            .entries
            .iter()
            .map(|entry| entry.nsid.as_str())
            .collect();
        for path in on_disk.keys() {
            if let Some(nsid) = path
                .strip_prefix("lexicons/")
                .and_then(|name| name.strip_suffix(".json"))
                && !published.contains(nsid)
            {
                problems.evolution.push(format!(
                    "{nsid}: breaking change: the Lexicon is no longer published"
                ));
            }
        }
        Ok(problems)
    }

    /// Replaces the contents of `dir` with the snapshot.
    pub fn bless(&self, dir: &Path) -> Res<()> {
        for path in read_dir(dir)?.keys() {
            std::fs::remove_file(dir.join(path))?;
        }
        for (path, text) in &self.files {
            let target = dir.join(path);
            std::fs::create_dir_all(target.parent().expect("a file has a parent"))?;
            std::fs::write(target, text)?;
        }
        Ok(())
    }
}

/// Every file under `dir`, by path relative to it, `/`-separated; empty when `dir` is absent.
fn read_dir(dir: &Path) -> Res<BTreeMap<String, String>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        let listing = match std::fs::read_dir(&next) {
            Ok(listing) => listing,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && next == dir => continue,
            Err(error) => return Err(error.into()),
        };
        for item in listing {
            let path = item?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path.strip_prefix(dir)?.to_string_lossy().replace('\\', "/");
                files.insert(relative, std::fs::read_to_string(&path)?);
            }
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bless_then_check_passes_and_an_edit_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = Snapshot::generate().unwrap();
        assert!(!snapshot.entries.is_empty());
        let before = snapshot.problems(dir.path()).unwrap();
        assert!(!before.stale.is_empty() && before.evolution.is_empty());

        snapshot.bless(dir.path()).unwrap();
        assert!(snapshot.problems(dir.path()).unwrap().is_empty());

        let lexicon = dir
            .path()
            .join(format!("lexicons/{}.json", snapshot.entries[0].nsid));
        let mut text = std::fs::read_to_string(&lexicon).unwrap();
        text.push('\n');
        std::fs::write(&lexicon, text).unwrap();
        let after = snapshot.problems(dir.path()).unwrap();
        assert_eq!(after.stale.len(), 1);
    }

    #[test]
    fn a_dropped_property_in_the_checked_in_copy_is_reported_as_evolution() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = Snapshot::generate().unwrap();
        snapshot.bless(dir.path()).unwrap();
        // Pretend the checked-in copy had one more property than the generated one, i.e. that
        // the generated copy removed it.
        let (entry, pointer) = snapshot
            .entries
            .iter()
            .find_map(|entry| {
                entry.lexicon["defs"]
                    .as_object()?
                    .iter()
                    .find_map(|(name, def)| {
                        def["properties"]
                            .is_object()
                            .then(|| (entry, format!("/defs/{name}/properties")))
                    })
            })
            .expect("some Lexicon states an object with properties");
        let mut old = entry.lexicon.clone();
        old.pointer_mut(&pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("zzGhost".to_owned(), json!({"type": "string"}));
        std::fs::write(
            dir.path().join(format!("lexicons/{}.json", entry.nsid)),
            render(&old),
        )
        .unwrap();
        let problems = snapshot.problems(dir.path()).unwrap();
        assert!(
            problems
                .evolution
                .iter()
                .any(|line| line.contains("removed or renamed") && line.contains(&entry.nsid)),
            "{problems:?}"
        );
    }
}
