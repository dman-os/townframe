//! Renders the Lexicons the node publishes as checked-in files, and checks or rewrites them.
//!
//! Layout under the snapshot directory: `index.json` (NSID, owning crate, kind, revision, whether
//! a sidecar exists), `lexicons/<nsid>.json`, and `sidecars/<nsid>.schema.json`: a datum's
//! sidecar JSON Schema document, which carries the storage forms its Lexicon cannot state.

use crate::evolution;
use crate::evolution::Surface;
use crate::items;
use big_cabinet::SchemaRepo;
use big_cabinet_core::datum::lexicon::stability::Declared;
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

impl Entry {
    fn surface(&self) -> Surface<'_> {
        Surface {
            lexicon: &self.lexicon,
            sidecar: self.sidecar.as_ref(),
        }
    }
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
            // Unstable is the default, so the index lists the file's own stability and then only
            // the items that are under the evolution rules: those are what a reviewer signs off.
            let mut declared = items::items(&entry.surface());
            let file = declared.remove("").expect("the file is an item");
            let stable: BTreeMap<_, _> = declared
                .iter()
                .filter(|(_, item)| item.is_stable())
                .map(|(path, item)| (path.clone(), items::describe(item)))
                .collect();
            index.push(json!({
                "nsid": entry.nsid,
                "crate": entry.owner,
                "kind": entry.lexicon["defs"]["main"]["type"].as_str().unwrap_or("defs"),
                "revision": entry.lexicon.get("revision"),
                "sidecar": entry.sidecar.is_some(),
                "stability": items::describe(&file),
                "stableItems": stable,
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
            let old_sidecar: Option<Value> = on_disk
                .get(&format!("sidecars/{}.schema.json", entry.nsid))
                .map(|text| serde_json::from_str(text))
                .transpose()?;
            problems.evolution.extend(evolution::violations(
                &entry.nsid,
                Surface {
                    lexicon: &old,
                    sidecar: old_sidecar.as_ref(),
                },
                Surface {
                    lexicon: &entry.lexicon,
                    sidecar: entry.sidecar.as_ref(),
                },
            ));
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
                let old: Value = serde_json::from_str(&on_disk[path])?;
                let old_sidecar: Option<Value> = on_disk
                    .get(&format!("sidecars/{nsid}.schema.json"))
                    .map(|text| serde_json::from_str(text))
                    .transpose()?;
                let file = items::items(&Surface {
                    lexicon: &old,
                    sidecar: old_sidecar.as_ref(),
                })
                .remove("")
                .expect("the file is an item");
                // Only an unstable Lexicon (the default) may be dropped: a stable one has data in drawers and
                // may be named by other cartridges, so it is deprecated and keeps shipping.
                if file.is_stable() {
                    problems.evolution.push(format!(
                        "{nsid}: breaking change: the stable Lexicon is no longer published; there \
                         is no retirement path, deprecate it and keep shipping it"
                    ));
                }
            }
        }
        Ok(problems)
    }

    /// One line per item that is stable in the snapshot and was not in `dir`: stabilizing is the
    /// operator's decision, so a bless reports each one on its own line. A file `dir` does not
    /// hold counts as not stable, so a new Lexicon's stable items are reported too.
    pub fn stabilized(&self, dir: &Path) -> Res<Vec<String>> {
        let on_disk = read_dir(dir)?;
        let mut lines = Vec::new();
        for entry in &self.entries {
            let old = match on_disk.get(&format!("lexicons/{}.json", entry.nsid)) {
                Some(text) => {
                    let lexicon: Value = serde_json::from_str(text)?;
                    let sidecar: Option<Value> = on_disk
                        .get(&format!("sidecars/{}.schema.json", entry.nsid))
                        .map(|text| serde_json::from_str(text))
                        .transpose()?;
                    items::items(&Surface {
                        lexicon: &lexicon,
                        sidecar: sidecar.as_ref(),
                    })
                }
                None => BTreeMap::new(),
            };
            for (pointer, item) in items::items(&entry.surface()) {
                if item.is_stable() && !old.get(&pointer).is_some_and(Declared::is_stable) {
                    lines.push(format!(
                        "STABILIZED {}{pointer} {}",
                        entry.nsid,
                        items::describe(&item)
                    ));
                }
            }
        }
        Ok(lines)
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
    fn a_dropped_property_of_an_unstable_lexicon_is_stale_and_not_evolution() {
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
        // Every Lexicon of the corpus is unstable until an item states `since`, so the change
        // makes the snapshot stale and is outside the evolution rules.
        let problems = snapshot.problems(dir.path()).unwrap();
        assert!(problems.evolution.is_empty(), "{problems:?}");
        assert!(
            problems
                .stale
                .contains(&format!("lexicons/{}.json", entry.nsid)),
            "{problems:?}"
        );
    }

    #[test]
    fn only_an_unstable_lexicon_may_stop_being_published() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = Snapshot::generate().unwrap();
        snapshot.bless(dir.path()).unwrap();
        let stale = |name: &str, lexicon: Value| {
            std::fs::write(
                dir.path().join(format!("lexicons/{name}.json")),
                render(&lexicon),
            )
            .unwrap();
            snapshot.problems(dir.path()).unwrap().evolution
        };
        let defs = json!({"main": {"type": "object", "properties": {}}});
        let unstable = stale(
            "x.dropped",
            json!({"lexicon": 1, "id": "x.dropped", "defs": defs}),
        );
        assert!(unstable.is_empty(), "{unstable:?}");
        let stable = stale(
            "x.kept",
            json!({"lexicon": 1, "id": "x.kept", "revision": 1, "defs": defs}),
        );
        assert!(
            stable
                .iter()
                .any(|line| line.contains("x.kept") && line.contains("no retirement path")),
            "{stable:?}"
        );
    }

    #[test]
    fn a_corpus_of_unstable_items_stabilizes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = Snapshot::generate().unwrap();
        assert!(snapshot.stabilized(dir.path()).unwrap().is_empty());
    }
}
