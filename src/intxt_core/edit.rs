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
//! `description` or `entries` is unchanged (`"description": null` clears it). An `entries` list
//! with no existing entry in it is a delta: `{ "remove": "<id>" }` removes and `{ "add": .. }`
//! adds, and entries it does not mention stay. A list that does hold existing entries is the
//! whole list, as printed: the ones missing from it are removed. A list that deleted every
//! existing entry and kept an `add` therefore removes nothing: the unsafe reading of the
//! ambiguity is never the one taken.

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

/// An entry of the document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum EditEntry {
    /// One the intxt holds.
    Existing(ExistingEntry),
    /// `{ "add": { "branch": "feat/x" } }`: resolved in the worktree the command runs in, as
    /// `add` does.
    Add { add: Named },
    /// `{ "remove": "<id>" }`: takes an existing entry out, in a partial document.
    Remove { remove: String },
}

impl<'de> Deserialize<'de> for EditEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let mut members = Map::deserialize(deserializer)?;
        for lone in ["add", "remove"] {
            let Some(value) = members.remove(lone) else {
                continue;
            };
            if let Some(other) = members.keys().next() {
                return Err(D::Error::custom(format!(
                    "unknown member `{other}` beside `{lone}`"
                )));
            }
            return Ok(if lone == "add" {
                Self::Add {
                    add: serde_json::from_value(value)
                        .map_err(|error| D::Error::custom(format!("`add`: {error}")))?,
                }
            } else {
                Self::Remove {
                    remove: serde_json::from_value(value)
                        .map_err(|error| D::Error::custom(format!("`remove`: {error}")))?,
                }
            });
        }
        let id = match members.remove("id") {
            Some(Value::String(id)) => id,
            Some(other) => return Err(D::Error::custom(format!("`id` is {other}, not a string"))),
            None => {
                return Err(D::Error::custom(
                    "an entry has an `id`, or is an `add` or a `remove`",
                ));
            }
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
    /// Unchanged when absent. See the module's "Partial documents" for what a list means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<EditEntry>>,
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
    /// What to add, with its index in the document's entries.
    pub add: Vec<(usize, Named)>,
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
            errors: Vec::new(),
        }
    }

    /// Reads a document from JSON, saying which entry is wrong when one is: serde's error for
    /// the whole document would name a member but not the entry it was in.
    pub fn parse(mut value: Value) -> Result<Self, Vec<PlanError>> {
        let mut errors = Vec::new();
        let mut entries = None;
        if let Some(Value::Array(listed)) = value.get_mut("entries").map(Value::take) {
            let mut parsed = Vec::new();
            for (index, entry) in listed.into_iter().enumerate() {
                match serde_json::from_value::<EditEntry>(entry) {
                    Ok(entry) => parsed.push(entry),
                    Err(error) => errors.push(PlanError::new(
                        format!("/entries/{index}"),
                        error.to_string(),
                    )),
                }
            }
            entries = Some(parsed);
            value["entries"] = Value::Array(Vec::new());
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
        if entries.is_some() {
            document.entries = entries;
        }
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
    /// entry is unknown, repeated or edited in place, or the title is empty. Reordering the
    /// existing entries changes nothing. Applying the same document twice changes nothing: the
    /// second time its base is stale.
    pub fn plan(&self, current: &Self) -> Result<EditPlan, Refusal> {
        let mut errors = Vec::new();
        if let (Some(named), Some(here)) = (&self.intxt, &current.intxt)
            && named.split('?').next() != here.split('?').next()
        {
            errors.push(PlanError::new(
                "/intxt",
                format!("the document is for {named}, this is {here}"),
            ));
        }
        let heads = |document: &Self| {
            let mut heads = document.base.clone();
            heads.sort();
            heads
        };
        if heads(self) != heads(current) {
            return Err(Refusal::Stale);
        }
        let held: Vec<&ExistingEntry> = current
            .entries
            .iter()
            .flatten()
            .filter_map(|entry| match entry {
                EditEntry::Existing(existing) => Some(existing),
                _ => None,
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
                    } else if held.summary != entry.summary {
                        errors.push(PlanError::new(
                            format!("{path}/{}", entry.summary.kind()),
                            format!(
                                "was changed to {:?} (it is {:?}); entries are added or removed, not rewritten",
                                entry.summary.text(),
                                held.summary.text()
                            ),
                        ));
                    }
                    listed.push(&entry.id);
                }
                EditEntry::Remove { remove } => {
                    if !held.iter().any(|held| &held.id == remove) {
                        errors.push(PlanError::new(
                            format!("/entries/{index}/remove"),
                            format!("the intxt holds no entry `{remove}`"),
                        ));
                    } else if removed.contains(&remove.as_str()) {
                        errors.push(PlanError::new(
                            format!("/entries/{index}/remove"),
                            "removed twice",
                        ));
                    }
                    removed.push(remove);
                }
                EditEntry::Add { add: named } => add.push((index, named.clone())),
            }
        }
        for id in &removed {
            if listed.contains(id) {
                errors.push(PlanError::new(
                    "/entries",
                    format!("entry `{id}` is both listed and removed"),
                ));
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
        // A list holding existing entries is the whole list; one holding none is a delta.
        let whole = !listed.is_empty();
        Ok(EditPlan {
            remove: held
                .iter()
                .map(|held| held.id.as_str())
                // In a whole list the removed entries are among those it does not list.
                .filter(|id| {
                    if whole {
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
        let added = EditDocument::parse(partial(json!({ "entries": [
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
            "entries": [{ "remove": O }, { "add": { "branch": "feat/x" } }],
        })))
        .unwrap();
        assert_eq!(
            planned,
            EditPlan {
                remove: vec![O.to_owned()],
                add: vec![(1, Named::Branch("feat/x".to_owned()))],
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
            json!({ "entries": [{ "add": { "change": "k" } }] }),
        ))
        .unwrap();
        assert!(planned.remove.is_empty() && planned.add.len() == 1);
        let errors = invalid(partial(
            json!({ "entries": [{ "remove": "nope" }, { "remove": S }, { "remove": S }] }),
        ));
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert_eq!(errors[0].path, "/entries/0/remove");
        assert_eq!(errors[1].path, "/entries/2/remove");
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
                add: vec![(1, Named::Branch("feat/x".to_owned()))],
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
    fn an_entry_edited_in_place_is_refused_naming_it() {
        let errors = invalid(full(json!([
            { "id": S, "commits": "1e26cf9..ffffff" },
            { "id": O, "githubPull": "dman-os/townframe#179" },
        ])));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].path, "/entries/0/commits");
        assert!(errors[0].message.contains("not rewritten"), "{errors:?}");
        let errors = invalid(full(json!([{ "id": S, "change": "1e26cf9..c91468a" }])));
        assert_eq!(errors[0].path, "/entries/0/change");
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
    fn applying_twice_changes_nothing_the_second_time() {
        let edited = EditDocument::parse(partial(json!({ "entries": [{ "remove": O }] }))).unwrap();
        assert_eq!(edited.plan(&current()).unwrap().remove, [O]);
        // After the first apply the intxt is at other heads and no longer holds the entry.
        let mut after =
            EditDocument::parse(full(json!([{ "id": S, "commits": "1e26cf9..c91468a" }]))).unwrap();
        after.base = vec!["zC".to_owned()];
        assert_eq!(edited.plan(&after), Err(Refusal::Stale));
        // The document printed again is the intxt: nothing to change.
        let mut reprinted = after.clone();
        reprinted.title = Some("Install".to_owned());
        assert!(reprinted.plan(&after).unwrap().is_empty());
    }
}
