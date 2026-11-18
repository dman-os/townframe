//! `day x intxt edit`: every change to an intxt through one document (ledger D64,
//! `docs/scratch/intxt-terminal.md`, "Mutations go through `$EDITOR`"). The model and the plan
//! that turns a saved document into changes are `intxt_core::edit`; this is the command around
//! them: building the document from the intxt, applying a plan with `rm`'s and `add`'s own
//! internals, and the `$EDITOR` loop.

use super::package::Packaging;
use super::package::Resolved;
use super::package::View;
use super::package::sha_of;
use crate::interlude::*;

use big_cabinet_core::DatumKey;
use big_cabinet_core::drawer::protocol::DrawerId;
use intxt_core::edit::Applied;
use intxt_core::edit::Change;
use intxt_core::edit::EditDocument;
use intxt_core::edit::EditEntry;
use intxt_core::edit::EditPlan;
use intxt_core::edit::EntryChange;
use intxt_core::edit::ExistingEntry;
use intxt_core::edit::Failed;
use intxt_core::edit::Named;
use intxt_core::edit::PlanError;
use intxt_core::edit::Refusal;
use intxt_core::edit::Summary;
use intxt_core::generic;
use intxt_core::work;
use serde_json::Value;
use short_ids::ShortIds;
use std::collections::BTreeSet;
use std::io::Write;

/// The exit code of an apply refused because the intxt changed since the document was printed:
/// distinct from 1 (the document is wrong) and 2 (the command line is), so a caller knows to
/// print again and not to fix its edit.
pub(super) const EXIT_STALE: u8 = 3;

/// What applying a document came to.
pub(super) enum Outcome {
    Applied(Applied),
    Refused(Failed),
}

/// The editor as git finds it: `$VISUAL`, `$EDITOR`, then `vi`. Its value is a shell command, so
/// it may carry arguments (`code --wait`).
pub(super) fn editor_command() -> String {
    ["VISUAL", "EDITOR"]
        .into_iter()
        .filter_map(|name| std::env::var(name).ok())
        .find(|editor| !editor.trim().is_empty())
        .unwrap_or_else(|| "vi".to_owned())
}

/// Opening an editor needs a terminal. Without one the command refuses at once, naming the forms
/// that do not: it never waits on an editor nobody can see.
pub(super) fn ensure_interactive(is_terminal: bool) -> Res<()> {
    eyre::ensure!(
        is_terminal,
        "`edit` opens $EDITOR and needs a terminal; without one use `day x intxt edit --print` \
         to get the document, change it, and `day x intxt edit --apply <file|->` to apply it \
         (`--schema` prints its JSON Schema)"
    );
    Ok(())
}

/// A document's reference text without its `via` hint, which is not part of the intxt's identity.
fn bare(reference: &str) -> &str {
    reference.split('?').next().unwrap_or(reference)
}

/// The heads the document is based on: the descriptor's, sorted, as base58 multibase text.
fn base_of(view: &View) -> Vec<String> {
    let mut base: Vec<String> = view
        .heads
        .iter()
        .map(|head| utils_rs::hash::encode_base58_multibase(head.0))
        .collect();
    base.sort();
    base
}

/// `{ "$type": .. }` with `errors` set: the document the editor reopens on, written from the
/// JSON the user saved so that nothing else of it changes.
fn with_errors(mut saved: Value, errors: &[PlanError]) -> String {
    saved["errors"] = errors.iter().map(|error| error.to_string()).collect();
    serde_json::to_string_pretty(&saved).expect("JSON serializes") + "\n"
}

impl Packaging<'_> {
    /// The document for the intxt as `view` read it, and the datum key of each entry by the full
    /// id the document names it with.
    fn current_document(
        &self,
        drawer: &DrawerId,
        view: &View,
        summaries: Vec<Summary>,
    ) -> Res<(EditDocument, Vec<(String, DatumKey)>)> {
        let ids = ShortIds::new(
            view.entries
                .iter()
                .map(|(key, _)| key.to_string())
                .collect::<Vec<_>>(),
        );
        let mut keys = Vec::new();
        let mut entries = Vec::new();
        for ((key, _), summary) in view.entries.iter().zip(summaries) {
            let rendered = ids.render(&key.to_string());
            let id = rendered.unique + &rendered.rest;
            keys.push((id.clone(), key.clone()));
            entries.push(ExistingEntry { id, summary });
        }
        Ok((
            EditDocument::new(
                generic::Intxt::reference(drawer, []).to_string(),
                base_of(view),
                view.title.title.clone(),
                view.title.description.clone(),
                entries,
            ),
            keys,
        ))
    }

    async fn summaries(&self, view: &View) -> Res<Vec<Summary>> {
        let mut summaries = Vec::with_capacity(view.entries.len());
        for (key, value) in &view.entries {
            summaries.push(self.describe(key, value).await?);
        }
        Ok(summaries)
    }

    /// The current document of `intxt`, with where its schema is, for `--print` and the editor.
    async fn printed(&self, intxt: Option<&str>) -> Res<(EditDocument, DrawerId)> {
        let (drawer, _) = self.resolve(intxt).await?;
        let view = self.load(&drawer).await?;
        let (mut document, _) =
            self.current_document(&drawer, &view, self.summaries(&view).await?)?;
        document.schema = Some(self.schema_location()?);
        Ok((document, drawer))
    }

    /// Where the document's JSON Schema is, as a `file://` URL: written beside the node's other
    /// files when it is missing or out of date, so an editor that follows `$schema` finds it.
    fn schema_location(&self) -> Res<String> {
        let schema = self.node_dir.join("intxt-edit.schema.json");
        let text = EditDocument::json_schema();
        if std::fs::read_to_string(&schema).ok().as_deref() != Some(text.as_str()) {
            std::fs::write(&schema, text)
                .wrap_err_with(|| format!("writing {}", schema.display()))?;
        }
        Ok(format!("file://{}", schema.display()))
    }

    /// `day x intxt edit --print`.
    pub(crate) async fn edit_print(&self, out: &mut impl Write, intxt: Option<&str>) -> Res<()> {
        let (document, _) = self.printed(intxt).await?;
        out.write_all(document.to_text().as_bytes())?;
        Ok(())
    }

    /// `day x intxt edit --apply`: reads the document from `source` (a file, or `-` for stdin)
    /// and applies it. The result goes to `out` as JSON with `json`, and as text otherwise, where
    /// a refusal says why on `err` and a stale base prints the current document on `out`. The
    /// exit code is 0, 1 for a document that is wrong, or [`EXIT_STALE`].
    pub(crate) async fn edit_apply(
        &self,
        out: &mut impl Write,
        err: &mut impl Write,
        intxt: Option<&str>,
        source: &str,
        json: bool,
        dry_run: bool,
    ) -> Res<ExitCode> {
        let text = if source == "-" {
            std::io::read_to_string(std::io::stdin())?
        } else {
            std::fs::read_to_string(source).wrap_err_with(|| format!("reading {source}"))?
        };
        let outcome = match serde_json::from_str::<Value>(&text) {
            Ok(saved) => match EditDocument::parse(saved) {
                Ok(document) => self.apply_document(document, intxt, dry_run).await?,
                Err(errors) => Outcome::Refused(Failed {
                    errors,
                    current: None,
                }),
            },
            Err(error) => Outcome::Refused(Failed {
                errors: vec![PlanError::new("", format!("not JSON: {error}"))],
                current: None,
            }),
        };
        match outcome {
            Outcome::Applied(applied) if json => {
                writeln!(out, "{}", serde_json::to_string_pretty(&applied)?)?;
                Ok(ExitCode::SUCCESS)
            }
            Outcome::Applied(applied) => {
                Self::report(out, &applied)?;
                Ok(ExitCode::SUCCESS)
            }
            Outcome::Refused(failed) => {
                let code = if failed.current.is_some() {
                    ExitCode::from(EXIT_STALE)
                } else {
                    ExitCode::FAILURE
                };
                if json {
                    writeln!(out, "{}", serde_json::to_string_pretty(&failed)?)?;
                } else {
                    for error in &failed.errors {
                        writeln!(err, "{error}")?;
                    }
                    if let Some(current) = &failed.current {
                        out.write_all(current.to_text().as_bytes())?;
                    }
                }
                Ok(code)
            }
        }
    }

    /// The text of a successful apply.
    fn report(out: &mut impl Write, applied: &Applied) -> Res<()> {
        if applied.changes.is_empty() {
            writeln!(out, "nothing to change")?;
            return Ok(());
        }
        writeln!(
            out,
            "{}",
            if applied.dry_run {
                "dry run, nothing written; would change:"
            } else {
                "changed:"
            }
        )?;
        for change in &applied.changes {
            writeln!(out, "  {:<11}{}", change.op, change.text)?;
        }
        for undo in &applied.undo {
            writeln!(out, "  undo: {undo}")?;
        }
        Ok(())
    }

    /// Applies a saved `document` to the intxt it names (`intxt` is the one named on the command
    /// line, else the document's, else the worktree's), unless it is stale or wrong. Everything
    /// that can be wrong about the document is found before anything is written, adds included:
    /// they are resolved first, so a refusal leaves the intxt as it was. What fails after that
    /// is not the document's fault and is an error.
    pub(super) async fn apply_document(
        &self,
        mut document: EditDocument,
        intxt: Option<&str>,
        dry_run: bool,
    ) -> Res<Outcome> {
        let named = intxt.or_else(|| {
            document
                .intxt
                .as_deref()
                .map(bare)
                .and_then(|reference| reference.strip_prefix("dtxt:doc:"))
        });
        let (drawer, intxts) = self.resolve(named).await?;
        let view = self.load(&drawer).await?;
        let (current, keys) =
            self.current_document(&drawer, &view, self.summaries(&view).await?)?;

        let stale = |mut current: EditDocument| -> Res<Outcome> {
            current.schema = Some(self.schema_location()?);
            Ok(Outcome::Refused(Failed {
                errors: vec![PlanError::new(
                    "/base",
                    "the intxt changed since the document was printed; `current` is its document now",
                )],
                current: Some(current),
            }))
        };
        let heads = |base: &[String]| base.iter().cloned().collect::<BTreeSet<_>>();
        let is_stale = heads(&document.base) != heads(&current.base);

        // Ids are read at the apply: any prefix that is unambiguous among the entries now.
        let by_key = ShortIds::new(
            keys.iter()
                .map(|(_, key)| key.to_string())
                .collect::<Vec<_>>(),
        );
        let mut errors = Vec::new();
        let mut normalize = |id: &mut String, path: String| match by_key.resolve(id) {
            Ok(key) => {
                let rendered = by_key.render(key);
                *id = rendered.unique + &rendered.rest;
            }
            // An entry that is gone since the base: a retry's removal of it already happened,
            // which `holds_in` below sees.
            Err(short_ids::ResolveError::NotFound(_)) if is_stale => {}
            Err(error) => errors.push(PlanError::new(path, error.to_string())),
        };
        for (index, entry) in document.entries.iter_mut().flatten().enumerate() {
            if let EditEntry::Existing(existing) = entry {
                normalize(&mut existing.id, format!("/entries/{index}/id"));
            }
        }
        for (index, change) in document.entry_changes.iter_mut().flatten().enumerate() {
            if let EntryChange::Remove { remove } = change {
                normalize(remove, format!("/entryChanges/{index}/remove"));
            }
        }
        if is_stale {
            // The intxt changed since the document was printed. A retry of a document that
            // applied has nothing left to do and is not a conflict; anything else is.
            return if errors.is_empty() && document.holds_in(&current) {
                Ok(Outcome::Applied(Applied {
                    dry_run,
                    ..Applied::default()
                }))
            } else {
                stale(current)
            };
        }
        if !errors.is_empty() {
            return Ok(Outcome::Refused(Failed {
                errors,
                current: None,
            }));
        }

        let plan = match document.plan(&current) {
            Ok(plan) => plan,
            Err(Refusal::Stale) => return stale(current),
            Err(Refusal::Invalid(errors)) => {
                return Ok(Outcome::Refused(Failed {
                    errors,
                    current: None,
                }));
            }
        };
        let identity = self.identity();
        let mut resolved = Vec::with_capacity(plan.add.len());
        let mut errors = Vec::new();
        for (path, named) in &plan.add {
            match identity
                .as_ref()
                .map_err(|error| eyre::eyre!("{error:#}"))
                .and_then(|identity| Resolved::new(named, identity))
            {
                Ok(found) => resolved.push((named, found)),
                Err(error) => errors.push(PlanError::new(path.clone(), format!("{error:#}"))),
            }
        }
        if !errors.is_empty() {
            return Ok(Outcome::Refused(Failed {
                errors,
                current: None,
            }));
        }

        Ok(Outcome::Applied(
            self.apply_plan((&drawer, &intxts), &view, &keys, &plan, &resolved, dry_run)
                .await?,
        ))
    }

    /// Writes `plan`, or only describes it in a dry run, with the title and description first,
    /// then the removals, then the adds, as `applied`'s changes and the commands that undo them,
    /// in the order to run them: the one that sets the title and description back (a document
    /// whose base is where the intxt stands after this apply) comes first, because the others
    /// change the intxt's heads and would make it stale.
    async fn apply_plan(
        &self,
        (drawer, intxts): (&DrawerId, &ShortIds<String>),
        view: &View,
        keys: &[(String, DatumKey)],
        plan: &EditPlan,
        resolved: &[(&Named, Resolved)],
        dry_run: bool,
    ) -> Res<Applied> {
        let mut applied = Applied {
            dry_run,
            ..Applied::default()
        };
        let short = shown_plain(intxts, &drawer.to_string());
        let mut undo = Vec::new();
        let mut set_back = EditDocument {
            schema: None,
            kind: None,
            intxt: None,
            base: Vec::new(),
            title: None,
            description: None,
            entries: None,
            entry_changes: None,
            errors: Vec::new(),
        };
        if plan.title.is_some() || plan.description.is_some() {
            let mut title = view.title.clone();
            if let Some(new) = &plan.title {
                applied.changes.push(Change {
                    op: "title".to_owned(),
                    text: format!("{:?} -> {new:?}", title.title),
                });
                set_back.title = Some(view.title.title.clone());
                title.title = new.clone();
            }
            if let Some(new) = &plan.description {
                applied.changes.push(Change {
                    op: "description".to_owned(),
                    text: format!("{:?} -> {new:?}", title.description),
                });
                set_back.description = Some(view.title.description.clone());
                title.description = new.clone();
            }
            if !applied.dry_run {
                self.caller.write_title(drawer, &title).await?;
            }
        }

        let removed: BTreeSet<DatumKey> = plan
            .remove
            .iter()
            .map(|id| {
                keys.iter()
                    .find(|(held, _)| held == id)
                    .map(|(_, key)| key.clone())
                    .expect("the plan names entries the intxt holds")
            })
            .collect();
        let mut removals = Vec::new();
        for (key, value) in view.entries.iter().filter(|(key, _)| removed.contains(key)) {
            let summary = self.describe(key, value).await?;
            let id = keys
                .iter()
                .find(|(_, held)| held == key)
                .map(|(id, _)| id.as_str())
                .expect("the entry has an id");
            applied.changes.push(Change {
                op: "remove".to_owned(),
                text: format!(
                    "{} {}  {}",
                    &id[..id.len().min(6)],
                    summary.kind(),
                    summary.text()
                ),
            });
            removals.push(removal_undo(&short, &summary, key, value)?);
        }
        if !removed.is_empty() && !applied.dry_run {
            self.remove_held(drawer, view, &removed).await?;
        }

        let mut added_undo = Vec::new();
        for (named, found) in resolved {
            if applied.dry_run {
                applied.changes.push(Change {
                    op: "add".to_owned(),
                    text: named.flag(),
                });
                continue;
            }
            let added = self.add_resolved(drawer, intxts, found, false).await?;
            applied.changes.push(Change {
                op: "add".to_owned(),
                text: if added.changed {
                    format!(
                        "{}: {}",
                        named.flag(),
                        added
                            .lines
                            .iter()
                            .map(|line| line.trim())
                            .collect::<Vec<_>>()
                            .join("; ")
                    )
                } else {
                    format!("{}: already in", named.flag())
                },
            });
            added_undo.extend(added.undo);
        }

        if (set_back.title.is_some() || set_back.description.is_some()) && !applied.dry_run {
            // The base the intxt stands at after the writes above.
            set_back.base = base_of(&self.load(drawer).await?);
            let line = serde_json::to_string(&set_back)?.replace('\'', "'\\''");
            undo.push(format!(
                "echo '{line}' | day x intxt edit {short} --apply -"
            ));
        }
        undo.extend(added_undo);
        undo.extend(removals);
        applied.undo = if applied.dry_run { Vec::new() } else { undo };
        Ok(applied)
    }

    /// `day x intxt edit` with an editor: prints the document into a temporary file, runs
    /// `editor` on it, and applies what was saved. A document that cannot be applied is written
    /// back with an `errors` member and the editor runs again, until it applies or the file is
    /// left as it was written or emptied. The editor runs once per pass, so the loop never
    /// spins, and the file goes with the function whichever way it returns.
    pub(crate) async fn edit_interactive(
        &self,
        out: &mut impl Write,
        intxt: Option<&str>,
        editor: &str,
        dry_run: bool,
    ) -> Res<ExitCode> {
        let (document, _) = self.printed(intxt).await?;
        let file = tempfile::Builder::new()
            .prefix("intxt-edit-")
            .suffix(".json")
            .tempfile()?;
        let mut written = document.to_text();
        let mut retrying = false;
        loop {
            std::fs::write(file.path(), &written)?;
            let status = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(format!("{editor} \"$@\""))
                .arg(editor)
                .arg(file.path())
                .status()
                .await
                .wrap_err_with(|| format!("running the editor {editor:?}"))?;
            eyre::ensure!(
                status.success(),
                "the editor {editor:?} exited with {status}"
            );
            let saved = std::fs::read_to_string(file.path())?;
            if saved.trim().is_empty() || saved == written {
                writeln!(out, "edit aborted, nothing changed")?;
                return Ok(if retrying {
                    ExitCode::FAILURE
                } else {
                    ExitCode::SUCCESS
                });
            }
            let value = match serde_json::from_str::<Value>(&saved) {
                Ok(value) => value,
                Err(error) => {
                    // Not JSON: what the editor last saw, with the syntax error on top.
                    written = with_errors(
                        serde_json::from_str(&written).expect("the file was written as JSON"),
                        &[PlanError::new("", format!("not JSON: {error}"))],
                    );
                    retrying = true;
                    continue;
                }
            };
            // The document as saved, for reopening it with the errors, in the fixed layout.
            let (outcome, saved) = match EditDocument::parse(value.clone()) {
                Ok(parsed) => (
                    self.apply_document(parsed.clone(), intxt, dry_run).await?,
                    Some(parsed),
                ),
                Err(errors) => (
                    Outcome::Refused(Failed {
                        errors,
                        current: None,
                    }),
                    None,
                ),
            };
            match outcome {
                Outcome::Applied(applied) => {
                    Self::report(out, &applied)?;
                    return Ok(ExitCode::SUCCESS);
                }
                Outcome::Refused(Failed {
                    errors,
                    current: Some(mut current),
                }) => {
                    // The intxt moved while the editor was open. The edit is not carried over:
                    // which of its removals still mean what they did is not known.
                    current.errors = errors.iter().map(ToString::to_string).collect();
                    written = current.to_text();
                }
                Outcome::Refused(Failed {
                    errors,
                    current: None,
                }) => {
                    written = match saved {
                        Some(mut saved) => {
                            saved.errors = errors.iter().map(ToString::to_string).collect();
                            saved.to_text()
                        }
                        // Not a document: the JSON as saved, with the errors added.
                        None => with_errors(value, &errors),
                    }
                }
            }
            retrying = true;
        }
    }
}

/// The short id of an intxt as text, without the dimming `shown` adds on a terminal.
fn shown_plain(ids: &ShortIds<String>, id: &str) -> String {
    ids.render(&id.to_owned()).unique
}

/// The command that puts a removed entry back, or a comment saying that none does.
fn removal_undo(short: &str, summary: &Summary, key: &DatumKey, value: &Value) -> Res<String> {
    let slot = String::from_utf8_lossy(key.slot_bytes()).into_owned();
    let command = match summary {
        Summary::Commits(_) => {
            let commits: work::GitCommitEntry = serde_json::from_value(value.clone())?;
            commits.base.as_deref().map(|base| {
                format!(
                    "day x intxt add {short} --commits {}..{}",
                    sha_of(base),
                    sha_of(&commits.tip)
                )
            })
        }
        Summary::Change(_) => Some(format!("day x intxt add {short} --change {slot}")),
        Summary::Ref(_) => slot
            .split_once('/')
            .and_then(|(_, reference)| reference.strip_prefix("refs/heads/"))
            .map(|branch| format!("day x intxt add {short} --branch {branch}")),
        Summary::Session(_) | Summary::GithubPull(_) | Summary::Other(_) => None,
    };
    Ok(command.unwrap_or_else(|| {
        format!(
            "# no command adds {} back ({}); run in the worktree that holds the work for the others",
            summary.kind(),
            key.tag().as_str()
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_a_terminal_edit_names_the_commands_that_work() {
        let error = ensure_interactive(false).unwrap_err().to_string();
        for form in ["--print", "--apply", "--schema"] {
            assert!(error.contains(form), "{error}");
        }
        ensure_interactive(true).unwrap();
    }

    #[test]
    fn a_reference_is_compared_without_its_via_hint() {
        assert_eq!(bare("dtxt:doc:z4k?via=zA"), "dtxt:doc:z4k");
        assert_eq!(bare("dtxt:doc:z4k"), "dtxt:doc:z4k");
    }
}
