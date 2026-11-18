//! The document `day x intxt edit` prints, edits and applies (ledger D64, `docs/scratch/intxt-terminal.md`,
//! "Mutations go through `$EDITOR`"): one JSON document per intxt that holds its title, its
//! description and its entries, and the plan that turns the difference between the document as
//! printed and as saved into changes.
//!
//! The model is shared by the terminal command, `--json` and the review micro, so it carries no
//! I/O: [`EditDocument::plan`] compares a saved document with the one the intxt prints now and
//! answers what to change, or why it must not.
//!
//! # Identity of an entry
//!
//! An existing entry is named by its full `short_ids` rendering (32 characters), of which every
//! short id `show` prints is a prefix. The full form does not grow when entries come and go, so it
//! is the same when the document is printed and when it is applied; the caller resolves any
//! prefix that is unambiguous among the intxt's entries at apply time to the full form before
//! [`EditDocument::plan`]. The heads in [`EditDocument::base`] (of the intxt's descriptor
//! document, where the entries are written) pin the rest: a changed base is refused before any
//! entry is read. The entry's summary is carried for the reader and checked on apply: an entry
//! is added or removed, never rewritten.
//!
//! # Partial documents
//!
//! A document to apply may hold only `base` and the members it changes. An omitted `title`,
//! `description`, `entries` or `entryChanges` is unchanged (`"description": null` clears it).
//! `entries` is the whole list, as printed: the existing entries missing from it are removed, and
//! `{ "add": .. }` entries in it are added. `entryChanges` is a delta that leaves every entry it
//! does not name alone: `{ "remove": "<id>" }` removes and `{ "add": .. }` adds. A document
//! with both is an error. Which one a document uses is never inferred from what is in it.
//!
//! # Retrying
//!
//! A document whose base is stale is still a no-op when what it asks for already holds in the
//! intxt as it is now ([`EditDocument::holds_in`]): an agent that retries after a timeout is not
//! in conflict with itself.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

/// The `$type` of the document.
pub const EDIT_TYPE: &str = "org.example.intxt.edit";

/// What an `add` names: exactly one per call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Named {
    /// A range (`main..feat/x`) or a single commit.
    Commits(String),
    /// A local branch: its commits since the merge base with its upstream (or `main`), and the
    /// branch itself, tracked as this clone sees it.
    Branch(String),
    /// A jj change, followed through its rewrites.
    Change(String),
}

impl Named {
    /// The one thing `add`'s `--commits`, `--branch` and `--change` flags name; clap requires
    /// exactly one of them, so any other combination is a bug in the command's definition.
    pub fn from_flags(
        commits: Option<String>,
        branch: Option<String>,
        change: Option<String>,
    ) -> Self {
        match (commits, branch, change) {
            (Some(range), None, None) => Self::Commits(range),
            (None, Some(branch), None) => Self::Branch(branch),
            (None, None, Some(change)) => Self::Change(change),
            _ => unreachable!("clap requires exactly one of --commits, --branch and --change"),
        }
    }

    /// How the name reads in a message: `--commits main..feat/x`.
    pub fn flag(&self) -> String {
        match self {
            Self::Commits(spec) => format!("--commits {spec}"),
            Self::Branch(name) => format!("--branch {name}"),
            Self::Change(change) => format!("--change {change}"),
        }
    }
}

/// An existing entry's kind, and its human summary as `show` prints it after the id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Summary {
    /// A range of commits: `base..tip`, in short shas.
    Commits(String),
    /// A jj change, by its change id.
    Change(String),
    /// A ref tracked on a clone.
    Ref(String),
    /// An agent session.
    Session(String),
    /// A GitHub pull request.
    GithubPull(String),
    /// An entry of a kind this version does not know, by its datum key.
    Other(String),
}

impl Summary {
    /// The member name the summary has in the document.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Commits(_) => "commits",
            Self::Change(_) => "change",
            Self::Ref(_) => "ref",
            Self::Session(_) => "session",
            Self::GithubPull(_) => "githubPull",
            Self::Other(_) => "other",
        }
    }

    pub fn text(&self) -> &str {
        match self {
            Self::Commits(text)
            | Self::Change(text)
            | Self::Ref(text)
            | Self::Session(text)
            | Self::GithubPull(text)
            | Self::Other(text) => text,
        }
    }
}

/// An entry the intxt holds: `{ "id": "<32 characters>", "commits": "1e26cf9..c91468a" }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ExistingEntry {
    /// The entry's full short id, as printed. Any unambiguous prefix is read when applying.
    pub id: String,
    #[serde(flatten)]
    pub summary: Summary,
}

/// An entry of the document's whole list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum EditEntry {
    /// One the intxt holds.
    Existing(ExistingEntry),
    /// `{ "add": { "branch": "feat/x" } }`: resolved in the worktree the command runs in, as
    /// `add` does.
    Add { add: Named },
}

/// One change of a delta (`entryChanges`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum EntryChange {
    /// `{ "remove": "<id>" }`: takes an existing entry out.
    Remove { remove: String },
    /// `{ "add": { "branch": "feat/x" } }`.
    Add { add: Named },
}

/// The member of an entry that is alone in it (`add`, `remove`), and its value.
fn lone<E: serde::de::Error>(
    members: &mut Map<String, Value>,
    names: &[&str],
) -> Result<Option<(String, Value)>, E> {
    for name in names {
        let Some(value) = members.remove(*name) else {
            continue;
        };
        if let Some(other) = members.keys().next() {
            return Err(E::custom(format!(
                "unknown member `{other}` beside `{name}`"
            )));
        }
        return Ok(Some(((*name).to_owned(), value)));
    }
    Ok(None)
}

impl<'de> Deserialize<'de> for EntryChange {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let mut members = Map::deserialize(deserializer)?;
        match lone(&mut members, &["add", "remove"])? {
            Some((name, value)) if name == "add" => Ok(Self::Add {
                add: serde_json::from_value(value)
                    .map_err(|error| D::Error::custom(format!("`add`: {error}")))?,
            }),
            Some((_, value)) => Ok(Self::Remove {
                remove: serde_json::from_value(value)
                    .map_err(|error| D::Error::custom(format!("`remove`: {error}")))?,
            }),
            None => Err(D::Error::custom(match members.keys().next() {
                Some(other) => {
                    format!("unknown member `{other}`; a change is an `add` or a `remove`")
                }
                None => "a change is an `add` or a `remove`".to_owned(),
            })),
        }
    }
}

impl<'de> Deserialize<'de> for EditEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let mut members = Map::deserialize(deserializer)?;
        if members.len() == 1 && members.contains_key("remove") {
            return Err(D::Error::custom(
                "`remove` belongs in `entryChanges`; in `entries`, leave the entry out",
            ));
        }
        if let Some((_, value)) = lone(&mut members, &["add"])? {
            let add = serde_json::from_value(value)
                .map_err(|error| D::Error::custom(format!("`add`: {error}")))?;
            return Ok(Self::Add { add });
        }
        let id = match members.remove("id") {
            Some(Value::String(id)) => id,
            Some(other) => return Err(D::Error::custom(format!("`id` is {other}, not a string"))),
            None => return Err(D::Error::custom("an entry has an `id`, or is an `add`")),
        };
        let mut rest = members.into_iter();
        let (Some((kind, text)), None) = (rest.next(), rest.next()) else {
            return Err(D::Error::custom(format!(
                "entry `{id}` has exactly one member beside `id`: its kind and summary"
            )));
        };
        let summary = serde_json::from_value(Value::Object(Map::from_iter([(kind.clone(), text)])))
            .map_err(|error| D::Error::custom(format!("unknown member `{kind}`: {error}")))?;
        Ok(Self::Existing(ExistingEntry { id, summary }))
    }
}

/// `Some(None)` for a member that is present and null, none for one that is absent.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

/// The document `day x intxt edit` prints, and reads back whole or in part. Unknown members are
/// an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EditDocument {
    /// Where the JSON Schema of this document is, for editors. Not part of the comparison.
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Always [`EDIT_TYPE`] when printed; may be left out of a partial document.
    #[serde(rename = "$type", default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("const" = "org.example.intxt.edit"))]
    pub kind: Option<String>,
    /// The intxt: its `dtxt:doc:` reference. Applied to the intxt it names when none is named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intxt: Option<String>,
    /// The heads of the intxt's descriptor document, where its title, description and entries
    /// are written, at the time the document was printed. An intxt whose heads differ now has
    /// changed since, and the document is refused. Each is a base58 multibase hash. Required.
    pub base: Vec<String>,
    /// Unchanged when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Markdown. Unchanged when absent, cleared by null.
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<String>")]
    pub description: Option<Option<String>>,
    /// The whole list of entries: the existing ones it leaves out are removed. Unchanged when
    /// absent. Not with `entryChanges`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<EditEntry>>,
    /// Changes to some entries, the others left as they are. Unchanged when absent. Not with
    /// `entries`.
    #[serde(
        rename = "entryChanges",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_changes: Option<Vec<EntryChange>>,
    /// Why the last attempt to apply the document failed, set by the editor loop. Ignored when
    /// applying.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

/// What is wrong, and where: `path` is a JSON pointer into the document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanError {
    pub path: String,
    pub message: String,
}

impl PlanError {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.path, self.message)
    }
}

/// What applying a document changes, with the entries named by full id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EditPlan {
    /// The existing entries to remove.
    pub remove: Vec<String>,
    /// What to add, with the JSON pointer of the `add` in the document.
    pub add: Vec<(String, Named)>,
    pub title: Option<String>,
    /// The new description, where none is `Some(None)`.
    pub description: Option<Option<String>>,
}

impl EditPlan {
    pub fn is_empty(&self) -> bool {
        self.remove.is_empty()
            && self.add.is_empty()
            && self.title.is_none()
            && self.description.is_none()
    }
}

/// Why a document is not applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The intxt changed since the document was printed. The caller prints the current document.
    Stale,
    /// The document cannot be applied as it is.
    Invalid(Vec<PlanError>),
}

/// One thing a successful apply did (or, in a dry run, would do).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    /// `remove`, `add`, `title` or `description`.
    pub op: String,
    pub text: String,
}

/// `edit --apply --json` on success.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applied {
    pub changes: Vec<Change>,
    /// One command per change that takes it back.
    pub undo: Vec<String>,
    /// Set when nothing was written.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
}

/// `edit --apply --json` on failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failed {
    pub errors: Vec<PlanError>,
    /// Set when the base is stale, with the current document to edit again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<EditDocument>,
}

impl EditDocument {
    /// The full document for an intxt whose entries are already described.
    pub fn new(
        intxt: String,
        base: Vec<String>,
        title: String,
        description: Option<String>,
        entries: Vec<ExistingEntry>,
    ) -> Self {
        Self {
            schema: None,
            kind: Some(EDIT_TYPE.to_owned()),
            intxt: Some(intxt),
            base,
            title: Some(title),
            description: Some(description),
            entries: Some(entries.into_iter().map(EditEntry::Existing).collect()),
            entry_changes: None,
            errors: Vec::new(),
        }
    }

    /// Reads a document from JSON, saying which entry is wrong when one is: serde's error for
    /// the whole document would name a member but not the entry it was in.
    pub fn parse(mut value: Value) -> Result<Self, Vec<PlanError>> {
        fn list<T: serde::de::DeserializeOwned>(
            value: &mut Value,
            name: &str,
            errors: &mut Vec<PlanError>,
        ) -> Option<Vec<T>> {
            let Value::Array(listed) = value.get_mut(name)?.take() else {
                return None;
            };
            value[name] = Value::Array(Vec::new());
            let mut parsed = Vec::new();
            for (index, item) in listed.into_iter().enumerate() {
                match serde_json::from_value::<T>(item) {
                    Ok(item) => parsed.push(item),
                    Err(error) => errors.push(PlanError::new(
                        format!("/{name}/{index}"),
                        error.to_string(),
                    )),
                }
            }
            Some(parsed)
        }
        let mut errors = Vec::new();
        let entries = list::<EditEntry>(&mut value, "entries", &mut errors);
        let entry_changes = list::<EntryChange>(&mut value, "entryChanges", &mut errors);
        if entries.is_some() && entry_changes.is_some() {
            errors.push(PlanError::new(
                "/entryChanges",
                "`entries` is the whole list and `entryChanges` a delta; a document has one of them",
            ));
        }
        let mut document = match serde_json::from_value::<Self>(value) {
            Ok(document) => document,
            Err(error) => {
                errors.push(PlanError::new("", error.to_string()));
                return Err(errors);
            }
        };
        if let Some(kind) = document.kind.as_deref().filter(|kind| *kind != EDIT_TYPE) {
            errors.push(PlanError::new(
                "/$type",
                format!("is {kind:?}, expected {EDIT_TYPE:?}"),
            ));
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        document.entries = entries;
        document.entry_changes = entry_changes;
        Ok(document)
    }

    /// The document as text, in a fixed layout so a text-edit tool can match a unique line:
    /// members in the order of the struct, each on its own line, one entry (or error) per line.
    pub fn to_text(&self) -> String {
        fn json(value: &impl Serialize) -> String {
            serde_json::to_string(value).expect("a document member serializes")
        }
        fn list(name: &str, items: Vec<String>) -> String {
            if items.is_empty() {
                format!("  \"{name}\": []")
            } else {
                format!("  \"{name}\": [\n    {}\n  ]", items.join(",\n    "))
            }
        }
        let mut members = Vec::new();
        if let Some(schema) = &self.schema {
            members.push(format!("  \"$schema\": {}", json(schema)));
        }
        if let Some(kind) = &self.kind {
            members.push(format!("  \"$type\": {}", json(kind)));
        }
        if let Some(intxt) = &self.intxt {
            members.push(format!("  \"intxt\": {}", json(intxt)));
        }
        members.push(format!("  \"base\": {}", json(&self.base)));
        if let Some(title) = &self.title {
            members.push(format!("  \"title\": {}", json(title)));
        }
        if let Some(description) = &self.description {
            members.push(format!("  \"description\": {}", json(description)));
        }
        if let Some(entries) = &self.entries {
            members.push(list("entries", entries.iter().map(json).collect()));
        }
        if let Some(changes) = &self.entry_changes {
            members.push(list("entryChanges", changes.iter().map(json).collect()));
        }
        if !self.errors.is_empty() {
            members.push(list("errors", self.errors.iter().map(json).collect()));
        }
        format!("{{\n{}\n}}\n", members.join(",\n"))
    }

    /// The changes that turn `current`, the full document the intxt prints now, into `self`.
    /// Ids are compared as written: the caller has resolved prefixes to full ids.
    ///
    /// Refused when the intxt changed since `self` was printed ([`Refusal::Stale`], before any
    /// entry is read: the ids are only meaningful at the base they were printed at), and when an
    /// entry is unknown, repeated or of another kind than the one it names, or the title is
    /// empty. The summary of an existing entry is display only and is not compared. Reordering
    /// the existing entries changes nothing.
    pub fn plan(&self, current: &Self) -> Result<EditPlan, Refusal> {
        let heads = |document: &Self| {
            let mut heads = document.base.clone();
            heads.sort();
            heads
        };
        if heads(self) != heads(current) {
            return Err(Refusal::Stale);
        }
        self.plan_against(current, false)
    }

    /// Whether `self` asks for nothing `current` does not already have, whatever its base: the
    /// entries it removes are gone, its title and description are the intxt's, and it adds
    /// nothing. That is what a retried document looks like after the first try applied.
    pub fn holds_in(&self, current: &Self) -> bool {
        self.plan_against(current, true)
            .is_ok_and(|plan| plan.is_empty())
    }

    /// [`Self::plan`] without the base check. With `retry`, an entry to remove that is not there
    /// is not an error: it was removed already.
    fn plan_against(&self, current: &Self, retry: bool) -> Result<EditPlan, Refusal> {
        let mut errors = Vec::new();
        if let (Some(named), Some(here)) = (&self.intxt, &current.intxt)
            && named.split('?').next() != here.split('?').next()
        {
            errors.push(PlanError::new(
                "/intxt",
                format!("the document is for {named}, this is {here}"),
            ));
        }
        if self.entries.is_some() && self.entry_changes.is_some() {
            errors.push(PlanError::new(
                "/entryChanges",
                "a document has `entries` or `entryChanges`, not both",
            ));
        }
        let held: Vec<&ExistingEntry> = current
            .entries
            .iter()
            .flatten()
            .filter_map(|entry| match entry {
                EditEntry::Existing(existing) => Some(existing),
                EditEntry::Add { .. } => None,
            })
            .collect();
        let mut listed: Vec<&str> = Vec::new();
        let mut removed: Vec<&str> = Vec::new();
        let mut add = Vec::new();
        for (index, entry) in self.entries.iter().flatten().enumerate() {
            match entry {
                EditEntry::Existing(entry) => {
                    let path = format!("/entries/{index}");
                    let Some(held) = held.iter().find(|held| held.id == entry.id) else {
                        errors.push(PlanError::new(
                            format!("{path}/id"),
                            format!(
                                "the intxt holds no entry `{}` (to add one, write an `add` entry)",
                                entry.id
                            ),
                        ));
                        continue;
                    };
                    if listed.contains(&entry.id.as_str()) {
                        errors.push(PlanError::new(format!("{path}/id"), "listed twice"));
                    } else if held.summary.kind() != entry.summary.kind() {
                        errors.push(PlanError::new(
                            format!("{path}/{}", entry.summary.kind()),
                            format!(
                                "the entry is a {}, not a {}; entries are added or removed, not rewritten",
                                held.summary.kind(),
                                entry.summary.kind()
                            ),
                        ));
                    }
                    listed.push(&entry.id);
                }
                EditEntry::Add { add: named } => {
                    add.push((format!("/entries/{index}/add"), named.clone()));
                }
            }
        }
        for (index, change) in self.entry_changes.iter().flatten().enumerate() {
            match change {
                EntryChange::Remove { remove } => {
                    let path = format!("/entryChanges/{index}/remove");
                    if removed.contains(&remove.as_str()) {
                        errors.push(PlanError::new(path, "removed twice"));
                    } else if !held.iter().any(|held| &held.id == remove) && !retry {
                        errors.push(PlanError::new(
                            path,
                            format!("the intxt holds no entry `{remove}`"),
                        ));
                    }
                    removed.push(remove);
                }
                EntryChange::Add { add: named } => {
                    add.push((format!("/entryChanges/{index}/add"), named.clone()));
                }
            }
        }
        if self
            .title
            .as_deref()
            .is_some_and(|title| title.trim().is_empty())
        {
            errors.push(PlanError::new("/title", "must not be empty"));
        }
        if !errors.is_empty() {
            return Err(Refusal::Invalid(errors));
        }
        Ok(EditPlan {
            remove: held
                .iter()
                .map(|held| held.id.as_str())
                .filter(|id| {
                    if self.entries.is_some() {
                        !listed.contains(id)
                    } else {
                        removed.contains(id)
                    }
                })
                .map(str::to_owned)
                .collect(),
            add,
            title: self
                .title
                .clone()
                .filter(|title| Some(title) != current.title.as_ref()),
            description: self
                .description
                .clone()
                .filter(|description| Some(description) != current.description.as_ref()),
        })
    }

    /// The JSON Schema of the document, to ship beside it.
    pub fn json_schema() -> String {
        serde_json::to_string_pretty(&schemars::schema_for!(Self)).expect("a schema serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const S: &str = "zyxwvutsrqponmlkzyxwvutsrqponmlk";
    const O: &str = "kmlnmpoqrstuvwxykmlnmpoqrstuvwxy";

    fn full(entries: Value) -> Value {
        json!({
            "$type": EDIT_TYPE,
            "intxt": "dtxt:doc:z4k",
            "base": ["zB", "zA"],
            "title": "Install",
            "description": "At boot.",
            "entries": entries,
        })
    }

    fn current() -> EditDocument {
        EditDocument::parse(full(json!([
            { "id": S, "commits": "1e26cf9..c91468a" },
            { "id": O, "githubPull": "dman-os/townframe#179" },
        ])))
        .unwrap()
    }

    fn partial(members: Value) -> Value {
        let mut document = json!({ "base": ["zA", "zB"] });
        document.as_object_mut().unwrap().extend(
            members
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        document
    }

    fn plan(edited: Value) -> Result<EditPlan, Refusal> {
        EditDocument::parse(edited).unwrap().plan(&current())
    }

    fn invalid(edited: Value) -> Vec<PlanError> {
        match plan(edited) {
            Err(Refusal::Invalid(errors)) => errors,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_document_round_trips_through_text_in_a_fixed_layout() {
        let printed = current();
        let text = printed.to_text();
        assert_eq!(
            text,
            format!(
                "{{\n  \"$type\": \"{EDIT_TYPE}\",\n  \"intxt\": \"dtxt:doc:z4k\",\n  \"base\": [\"zB\",\"zA\"],\n  \"title\": \"Install\",\n  \"description\": \"At boot.\",\n  \"entries\": [\n    {{\"id\":\"{S}\",\"commits\":\"1e26cf9..c91468a\"}},\n    {{\"id\":\"{O}\",\"githubPull\":\"dman-os/townframe#179\"}}\n  ]\n}}\n"
            )
        );
        let read = EditDocument::parse(serde_json::from_str(&text).unwrap()).unwrap();
        assert_eq!(read, printed);
        assert_eq!(read.to_text(), text);
        let added = EditDocument::parse(partial(json!({ "entryChanges": [
            { "add": { "branch": "feat/x" } },
            { "add": { "commits": "main..feat/y" } },
            { "add": { "change": "kxqoltvm" } },
            { "remove": S },
        ]})))
        .unwrap();
        let text = added.to_text();
        assert!(
            text.contains("\n    {\"add\":{\"branch\":\"feat/x\"}},\n"),
            "{text}"
        );
        assert_eq!(
            EditDocument::parse(serde_json::from_str(&text).unwrap()).unwrap(),
            added
        );
    }

    #[test]
    fn the_schema_describes_the_document() {
        let schema: Value = serde_json::from_str(&EditDocument::json_schema()).unwrap();
        assert_eq!(schema["properties"]["$type"]["const"], EDIT_TYPE);
        assert_eq!(schema["additionalProperties"], false);
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("base"))
        );
    }

    #[test]
    fn unknown_members_are_errors_that_name_them() {
        let mut top = full(json!([]));
        top["surprise"] = json!(1);
        let errors = EditDocument::parse(top).unwrap_err();
        assert!(errors[0].message.contains("surprise"), "{errors:?}");

        let errors = EditDocument::parse(full(json!([
            { "id": S, "commits": "a..b" },
            { "id": O, "colour": "red" },
            { "add": { "branch": "x" }, "also": 1 },
            { "add": { "tag": "v1" } },
            { "commits": "a..b" },
        ])))
        .unwrap_err();
        assert_eq!(errors.len(), 4, "{errors:?}");
        assert_eq!(errors[0].path, "/entries/1");
        assert!(errors[0].message.contains("colour"));
        assert!(errors[1].message.contains("also"));
        assert!(errors[2].message.contains("tag"));
        assert_eq!(errors[3].path, "/entries/4");
        let mut other = full(json!([]));
        other["$type"] = json!("org.example.intxt.prLinks");
        assert_eq!(EditDocument::parse(other).unwrap_err()[0].path, "/$type");
    }

    #[test]
    fn a_partial_document_changes_only_what_it_holds() {
        assert!(plan(partial(json!({}))).unwrap().is_empty());
        let planned = plan(partial(json!({
            "title": "Renamed",
            "entryChanges": [{ "remove": O }, { "add": { "branch": "feat/x" } }],
        })))
        .unwrap();
        assert_eq!(
            planned,
            EditPlan {
                remove: vec![O.to_owned()],
                add: vec![(
                    "/entryChanges/1/add".to_owned(),
                    Named::Branch("feat/x".to_owned())
                )],
                title: Some("Renamed".to_owned()),
                description: None,
            }
        );
        // An omitted description is unchanged, a null one is cleared.
        assert_eq!(
            plan(partial(json!({ "description": null })))
                .unwrap()
                .description,
            Some(None)
        );
        // Adds alone never remove what they do not mention.
        let planned = plan(partial(
            json!({ "entryChanges": [{ "add": { "change": "k" } }] }),
        ))
        .unwrap();
        assert!(planned.remove.is_empty() && planned.add.len() == 1);
        let errors = invalid(partial(
            json!({ "entryChanges": [{ "remove": "nope" }, { "remove": S }, { "remove": S }] }),
        ));
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert_eq!(errors[0].path, "/entryChanges/0/remove");
        assert_eq!(errors[1].path, "/entryChanges/2/remove");
    }

    #[test]
    fn a_full_document_removes_what_it_no_longer_lists() {
        let mut edited = full(json!([
            { "id": S, "commits": "1e26cf9..c91468a" },
            { "add": { "branch": "feat/x" } },
        ]));
        edited["title"] = json!("Install, again");
        edited
            .as_object_mut()
            .unwrap()
            .insert("description".to_owned(), Value::Null);
        assert_eq!(
            plan(edited).unwrap(),
            EditPlan {
                remove: vec![O.to_owned()],
                add: vec![(
                    "/entries/1/add".to_owned(),
                    Named::Branch("feat/x".to_owned())
                )],
                title: Some("Install, again".to_owned()),
                description: Some(None),
            }
        );
        let mut reordered = current();
        reordered.entries.as_mut().unwrap().reverse();
        reordered.schema = Some("file:///elsewhere".to_owned());
        reordered.errors = vec!["left over".to_owned()];
        assert!(reordered.plan(&current()).unwrap().is_empty());
    }

    #[test]
    fn an_entry_of_another_kind_is_refused_but_its_summary_is_display_only() {
        // The summary text drifts (a session's title changes); only the id and kind are checked.
        assert!(
            plan(full(json!([
                { "id": S, "commits": "1e26cf9..ffffff" },
                { "id": O, "githubPull": "retitled" },
            ])))
            .unwrap()
            .is_empty()
        );
        let errors = invalid(full(json!([{ "id": S, "change": "1e26cf9..c91468a" }])));
        assert_eq!(errors[0].path, "/entries/0/change");
        assert!(errors[0].message.contains("not rewritten"), "{errors:?}");
    }

    #[test]
    fn entries_and_entry_changes_are_one_or_the_other() {
        let mut both = full(json!([]));
        both["entryChanges"] = json!([{ "remove": O }]);
        assert_eq!(
            EditDocument::parse(both).unwrap_err()[0].path,
            "/entryChanges"
        );
        // A remove in the whole list is explained, not read as a deletion.
        let errors = EditDocument::parse(full(json!([{ "remove": O }]))).unwrap_err();
        assert!(errors[0].message.contains("entryChanges"), "{errors:?}");
        // Adds alone in `entries` are the whole list: everything else is removed.
        let planned = plan(full(json!([{ "add": { "change": "k" } }]))).unwrap();
        assert_eq!(planned.remove, [S, O]);
        // The same adds as changes remove nothing.
        let planned = plan(partial(
            json!({ "entryChanges": [{ "add": { "change": "k" } }] }),
        ))
        .unwrap();
        assert!(planned.remove.is_empty());
        let errors =
            EditDocument::parse(partial(json!({ "entryChanges": [{ "id": S }] }))).unwrap_err();
        assert_eq!(errors[0].path, "/entryChanges/0");
    }

    #[test]
    fn unknown_and_repeated_ids_and_an_empty_title_are_refused() {
        let mut edited = full(json!([
            { "id": S, "commits": "1e26cf9..c91468a" },
            { "id": S, "commits": "1e26cf9..c91468a" },
            { "id": "zz", "session": "k7" },
        ]));
        edited["title"] = json!(" ");
        let errors = invalid(edited);
        assert_eq!(errors.len(), 3, "{errors:?}");
        assert_eq!(errors[0].path, "/entries/1/id");
        assert_eq!(errors[1].path, "/entries/2/id");
        assert_eq!(errors[2].path, "/title");
    }

    #[test]
    fn a_stale_base_is_refused_before_the_entries_are_read() {
        let mut edited = full(json!([{ "id": "gone", "session": "k7" }]));
        edited["base"] = json!(["zA", "zC"]);
        assert_eq!(plan(edited), Err(Refusal::Stale));
        // The order of the heads is not part of the base.
        assert!(plan(partial(json!({}))).unwrap().is_empty());
    }

    #[test]
    fn a_retry_after_the_first_apply_holds_and_a_conflicting_one_does_not() {
        let edited = EditDocument::parse(partial(json!({
            "title": "Install",
            "entryChanges": [{ "remove": O }],
        })))
        .unwrap();
        assert_eq!(edited.plan(&current()).unwrap().remove, [O]);
        assert!(!edited.holds_in(&current()));
        // After the first apply the intxt is at other heads and no longer holds the entry.
        let mut after =
            EditDocument::parse(full(json!([{ "id": S, "commits": "1e26cf9..c91468a" }]))).unwrap();
        after.base = vec!["zC".to_owned()];
        assert_eq!(edited.plan(&after), Err(Refusal::Stale));
        assert!(edited.holds_in(&after));
        // A document that still has something to do, or an entry that is not there, is not a retry.
        let mut renamed = edited.clone();
        renamed.title = Some("Other".to_owned());
        assert!(!renamed.holds_in(&after));
        let adding = EditDocument::parse(partial(json!({
            "entryChanges": [{ "add": { "branch": "b" } }],
        })))
        .unwrap();
        assert!(!adding.holds_in(&after));
        let whole = EditDocument::parse(full(json!([{ "id": O, "githubPull": "x" }]))).unwrap();
        assert!(!whole.holds_in(&after));
        // The document printed again is the intxt: nothing to change.
        let mut reprinted = after.clone();
        reprinted.title = Some("Install".to_owned());
        assert!(reprinted.plan(&after).unwrap().is_empty());
    }
}
