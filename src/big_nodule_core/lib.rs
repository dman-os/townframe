//! The nodule description: a plug's one node-facing facet (ADR 027 §2–3, §5–7).
//!
//! A [`Nodule`] carries the Lexicons a package declares, both its datums and its operation
//! contracts (`query` | `procedure` | `subscription`), and what the runtime understands about
//! serving them:
//! - **services:** one XRPC [`Handler`] component and the operations it `implements`, plus the
//!   foreign operations the nodule `requires`;
//! - **facilities:** private state, revisioned stores and a config schema;
//! - **lifecycle:** initializers.
//!
//! Everything else (commands, views, processors, future sections) lives in
//! [`Nodule::bindings`]: sections the runtime carries and round-trips but never interprets.
//!
//! The datum type is the wire contract and stays open: a member, mode or binding section this
//! build does not know is carried untouched so the description survives a catalog that is older
//! than the package. [`Nodule::admit`] is the gate a runtime applies before it will host the
//! nodule: it resolves every reference under the nodule's own authority, checks that what it
//! serves is declared, and refuses what this build could not serve. It reads no binding.

use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::GrammarError;
use big_cabinet_core::datum::is_nsid;
use big_cabinet_core::datum::operation;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::datum::parse_ref;
use big_cabinet_core::datum::validate_object_schema;
use big_cabinet_core::datum::validate_object_value;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub mod config;
pub mod inventory;
pub mod protocol;

pub const AUTHORITY: &str = "org.example.daybook";

/// `org.example.daybook.nodule/main`: what a package contributes to a node. The nodule's `id` is
/// an NSID authority prefix: every Lexicon the nodule declares is `id` itself or lives under
/// `id.`, which is what lets a reference be classified as local or foreign without a registry.
#[macros::datum(authority = AUTHORITY)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nodule {
    pub id: String,
    /// Lexicon files as published: datum Lexicons for the datum compiler, operation Lexicons for
    /// [`big_cabinet_core::datum::operation`]. Carried opaque because a Lexicon is already its own
    /// schema language.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lexicons: Vec<IndexMap<String, Value>>,
    /// Operation NSIDs this nodule invokes but does not declare. The runtime binds a provider for
    /// each; an initializer may only target a foreign operation listed here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<String>,
    /// The one HTTP-shaped XRPC handler component serving `/xrpc/<nsid>` for every operation in
    /// [`Self::implements`]. It is handed all of [`Self::private_state`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<Handler>,
    /// Operation NSIDs the handler serves. One under this nodule's authority must be a query,
    /// procedure or subscription declared in its own Lexicons. A foreign one (a provider for a
    /// contract another authority declares) is only checked to be an NSID; a non-empty list
    /// needs a [`Self::handler`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub implements: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revisioned_stores: Vec<RevisionedStore>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initialization: Vec<Initializer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub private_state: Vec<PrivateState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Config>,
    /// Binding sections (commands, views, processors, ...) keyed by section tag, for example
    /// `org.example.daybook.nodule.commands`. The runtime carries them verbatim and never
    /// validates them: an unknown section or unknown content inside a known one does not refuse
    /// admission.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub bindings: IndexMap<String, Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The XRPC handler component: a WASI HTTP-shaped component from an envelope artifact that
/// serves `/xrpc/<nsid>` for the nodule's implemented operations (ADR 027 §3).
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Handler {
    /// Key of an artifact in the envelope that carries this description.
    pub artifact: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An ordered, replayable revision stream (ADR 027 §7). `selector` and `entry` are Lexicon
/// references; `source`, when stated, is the implemented operation whose handler writes it, else
/// the host does.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevisionedStore {
    pub key: String,
    pub selector: String,
    pub entry: String,
    pub mode: StoreMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What a revisioned store retains: the latest entry per key, or every entry in order.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StoreMode {
    Frontier,
    Log,
    #[datum(other)]
    Unknown(String),
}

/// An operation the runtime invokes on the nodule's behalf when it is enabled. Registration is
/// intent; whether it ran is the task layer's record, not this description's.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Initializer {
    pub target: String,
    pub mode: RunMode,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RunMode {
    Once,
    EveryStart,
    #[datum(other)]
    Unknown(String),
}

/// Named storage private to the nodule, handed to its handler.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivateState {
    pub key: String,
    pub kind: StateKind,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StateKind {
    Sqlite,
    Kv,
    #[datum(other)]
    Unknown(String),
}

/// The schema of the nodule's mutable runtime settings and their defaults. Settings live in the
/// runtime's configuration, validated against this; the description never carries a setting.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<IndexMap<String, Value>>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What admission established about a description: the operations it serves and the artifacts the
/// envelope carrying it must ship. An implemented operation's kind is `Some` when its contract is
/// declared in this nodule's own Lexicons and `None` when it is foreign: a single description
/// cannot know another authority's contract, so the kind (and the contract itself) is resolved at
/// bind time. Keeping one map, rather than a second set of foreign NSIDs, lets every consumer ask
/// "is this implemented?" in one lookup and treat an unknown kind as unchecked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    pub operations: BTreeMap<String, Option<OperationKind>>,
    pub artifacts: BTreeSet<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum NoduleError {
    #[error("nodule description invalid at `{path}`: {reason}")]
    Invalid { path: String, reason: String },
}

impl NoduleError {
    fn invalid(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

/// A schema position the shared Lexicon grammar refuses while an operation contract is admitted:
/// the grammar owns the position, this nodule owns the file it was reached from.
impl From<GrammarError> for NoduleError {
    fn from(error: GrammarError) -> Self {
        Self::Invalid {
            path: error.path,
            reason: error.reason,
        }
    }
}

type Result<T> = std::result::Result<T, NoduleError>;

/// Implemented operations by NSID; the kind is `None` for a foreign operation.
type Implemented = BTreeMap<String, Option<OperationKind>>;

impl Nodule {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            lexicons: Vec::new(),
            requires: Vec::new(),
            handler: None,
            implements: Vec::new(),
            revisioned_stores: Vec::new(),
            initialization: Vec::new(),
            private_state: Vec::new(),
            config: None,
            bindings: IndexMap::new(),
            extra: Default::default(),
        }
    }

    /// Whether `nsid` is under this nodule's authority: declared here or nowhere.
    pub fn owns(&self, nsid: &str) -> bool {
        nsid == self.id
            || nsid
                .strip_prefix(self.id.as_str())
                .is_some_and(|rest| rest.starts_with('.'))
    }

    /// The Lexicon files that declare datums rather than operations, for the datum compiler.
    pub fn datum_lexicons(&self) -> impl Iterator<Item = &IndexMap<String, Value>> {
        self.lexicons
            .iter()
            .filter(|file| operation_kind(file).is_none())
    }

    /// The gate before hosting. Every reference under the nodule's own authority resolves; the
    /// handler serves only operations declared here; foreign operations an initializer invokes
    /// are in `requires`; nothing this build cannot serve is accepted. Bindings are not read.
    pub fn admit(&self) -> Result<Admitted> {
        if !is_nsid(&self.id) {
            return Err(NoduleError::invalid(
                "id",
                format!("`{}` is not an NSID", self.id),
            ));
        }
        let admission = Admission {
            nodule: self,
            lexicons: self.lexicons()?,
            requires: self.requires()?,
        };
        admission.lexicon_refs()?;
        admission.private_state()?;
        let (operations, artifacts) = admission.services()?;
        admission.stores(&operations)?;
        admission.initializers(&operations)?;
        admission.config()?;
        Ok(Admitted {
            operations,
            artifacts,
        })
    }

    pub fn validate_config(&self, settings: &IndexMap<String, Value>, path: &str) -> Result<()> {
        let Some(config) = &self.config else {
            return Err(NoduleError::invalid(
                path,
                "nodule declares no config schema",
            ));
        };
        let lexicons = self.lexicons()?;
        let defs: BTreeMap<&str, &serde_json::Map<String, Value>> = lexicons
            .iter()
            .filter(|(_, lexicon)| lexicon.operation.is_none())
            .map(|(&id, lexicon)| (id, lexicon.defs))
            .collect();
        let value = Value::Object(
            settings
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        validate_object_value(&defs, &config.schema, &value, path)?;
        Ok(())
    }

    /// Reads every Lexicon file's frame and admits the operation ones, indexed by id.
    fn lexicons(&self) -> Result<BTreeMap<&str, LexiconFile<'_>>> {
        let mut lexicons = BTreeMap::new();
        for (index, file) in self.lexicons.iter().enumerate() {
            let path = format!("lexicons[{index}]");
            if file.get("lexicon") != Some(&Value::from(1)) {
                return Err(NoduleError::invalid(
                    format!("{path}.lexicon"),
                    "the language version must be 1",
                ));
            }
            let id = file
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| is_nsid(id))
                .ok_or_else(|| {
                    NoduleError::invalid(format!("{path}.id"), "a Lexicon file states its NSID")
                })?;
            if !self.owns(id) {
                return Err(NoduleError::invalid(
                    format!("{path}.id"),
                    format!("`{id}` is not under this nodule's authority `{}`", self.id),
                ));
            }
            let defs = file
                .get("defs")
                .and_then(Value::as_object)
                .filter(|defs| !defs.is_empty())
                .ok_or_else(|| {
                    NoduleError::invalid(
                        format!("{path}.defs"),
                        "non-empty definitions are required",
                    )
                })?;
            let operation = operation_kind(file);
            let refs = match operation {
                Some(kind) => operation::validate(kind, defs, &format!("{path}.defs"))?,
                None => Vec::new(),
            };
            let lexicon = LexiconFile {
                id,
                defs,
                operation,
                refs,
            };
            if lexicons.insert(id, lexicon).is_some() {
                return Err(NoduleError::invalid(
                    format!("{path}.id"),
                    format!("Lexicon `{id}` is declared twice"),
                ));
            }
        }
        Ok(lexicons)
    }

    /// The foreign operations this nodule invokes, each an NSID outside its authority.
    fn requires(&self) -> Result<BTreeSet<&str>> {
        let mut requires = BTreeSet::new();
        for (index, nsid) in self.requires.iter().enumerate() {
            let path = format!("requires[{index}]");
            if !is_nsid(nsid) {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is not an NSID"),
                ));
            }
            if self.owns(nsid) {
                return Err(NoduleError::invalid(
                    path,
                    format!(
                        "`{nsid}` is under this nodule's authority; `requires` lists foreign operations"
                    ),
                ));
            }
            if !requires.insert(nsid.as_str()) {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is required twice"),
                ));
            }
        }
        Ok(requires)
    }
}

/// One admission in progress: the description plus what its Lexicons and `requires` established,
/// which every contribution check reads.
struct Admission<'a> {
    nodule: &'a Nodule,
    lexicons: BTreeMap<&'a str, LexiconFile<'a>>,
    requires: BTreeSet<&'a str>,
}

impl<'a> Admission<'a> {
    /// Every `ref` an operation's schemas make, resolved against the file that makes it.
    fn lexicon_refs(&self) -> Result<()> {
        for (index, lexicon) in self.lexicons.values().enumerate() {
            for reference in &lexicon.refs {
                self.resolve_ref(
                    Some(lexicon.id),
                    reference.target,
                    &format!("lexicons[{index}].{}", reference.path),
                )?;
            }
        }
        Ok(())
    }

    /// Private state: unique keys and a kind this build serves.
    fn private_state(&self) -> Result<()> {
        let state = keyed(&self.nodule.private_state, "privateState", |state| {
            &state.key
        })?;
        for (key, state) in &state {
            if let StateKind::Unknown(kind) = &state.kind {
                return Err(NoduleError::invalid(
                    format!("privateState[{key}].kind"),
                    format!("this runtime has no `{kind}` state"),
                ));
            }
        }
        Ok(())
    }

    /// Services: the handler's artifact and the operations it implements, each declared in this
    /// nodule's own Lexicons, once. Returns the implemented operations by kind and the artifact
    /// keys the envelope must ship.
    fn services(&self) -> Result<(Implemented, BTreeSet<String>)> {
        let nodule = self.nodule;
        let mut artifacts = BTreeSet::new();
        if let Some(handler) = &nodule.handler {
            if handler.artifact.is_empty() {
                return Err(NoduleError::invalid(
                    "handler.artifact",
                    "the handler names the artifact it runs from",
                ));
            }
            artifacts.insert(handler.artifact.clone());
        } else if !nodule.implements.is_empty() {
            return Err(NoduleError::invalid(
                "implements",
                "implemented operations need a `handler` to serve them",
            ));
        }
        let mut operations = BTreeMap::new();
        for (index, nsid) in nodule.implements.iter().enumerate() {
            let path = format!("implements[{index}]");
            if !is_nsid(nsid) {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is not an NSID"),
                ));
            }
            // `requires` names operations provided by others; one this nodule also implements
            // would be a self-edge in the runtime dependency graph.
            if self.requires.contains(nsid.as_str()) {
                return Err(NoduleError::invalid(
                    path,
                    format!(
                        "`{nsid}` is both required and implemented; `requires` lists operations other nodules provide"
                    ),
                ));
            }
            // A local operation's contract is in this description, so it is checked here. A
            // foreign one is admitted on syntax alone: its contract lives in another nodule's
            // Lexicons, which admission cannot see. Resolving it is a bind-time concern, and
            // dispatch reports this service unavailable until the contract is known (per-service
            // readiness, ledger D4).
            let kind = if nodule.owns(nsid) {
                let Some(kind) = self
                    .lexicons
                    .get(nsid.as_str())
                    .and_then(|lexicon| lexicon.operation)
                else {
                    return Err(NoduleError::invalid(
                        path,
                        format!(
                            "`{nsid}` is under this nodule's authority but is not a declared operation"
                        ),
                    ));
                };
                Some(kind)
            } else {
                None
            };
            if operations.insert(nsid.clone(), kind).is_some() {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is implemented twice"),
                ));
            }
        }
        Ok((operations, artifacts))
    }

    /// Revisioned stores: schemas that resolve, a mode this build serves, and a source that is
    /// an implemented operation.
    fn stores(&self, implemented: &Implemented) -> Result<()> {
        for (index, store) in self.nodule.revisioned_stores.iter().enumerate() {
            let path = format!("revisionedStores[{index}]");
            self.resolve_ref(None, &store.selector, &format!("{path}.selector"))?;
            self.resolve_ref(None, &store.entry, &format!("{path}.entry"))?;
            if let StoreMode::Unknown(mode) = &store.mode {
                return Err(NoduleError::invalid(
                    format!("{path}.mode"),
                    format!("this runtime has no `{mode}` store"),
                ));
            }
            if let Some(source) = &store.source
                && !implemented.contains_key(source.as_str())
            {
                return Err(NoduleError::invalid(
                    format!("{path}.source"),
                    format!("`{source}` is not an implemented operation"),
                ));
            }
        }
        keyed(
            &self.nodule.revisioned_stores,
            "revisionedStores",
            |store| &store.key,
        )?;
        Ok(())
    }

    /// Initializers: each invokes an implemented or required query or procedure, in a run mode
    /// this build has.
    fn initializers(&self, implemented: &Implemented) -> Result<()> {
        for (index, initializer) in self.nodule.initialization.iter().enumerate() {
            let path = format!("initialization[{index}]");
            self.invocation_target(&initializer.target, implemented, &format!("{path}.target"))?;
            if let RunMode::Unknown(mode) = &initializer.mode {
                return Err(NoduleError::invalid(
                    format!("{path}.mode"),
                    format!("this runtime has no `{mode}` run mode"),
                ));
            }
        }
        Ok(())
    }

    /// Config schema and defaults: the schema must live under this nodule's own authority as a
    /// valid datum `object` schema, and `defaults` (when present) must conform to it.
    fn config(&self) -> Result<()> {
        let Some(config) = &self.nodule.config else {
            return Ok(());
        };
        self.resolve_ref(None, &config.schema, "config.schema")?;
        let Some((Some(file), _)) = parse_ref(&config.schema) else {
            unreachable!("resolve_ref verified a non-local Lexicon reference");
        };
        if !self.nodule.owns(file) {
            return Err(NoduleError::invalid(
                "config.schema",
                format!(
                    "`{file}` is not under this nodule's authority `{}`",
                    self.nodule.id
                ),
            ));
        }
        let defs = self.schema_defs();
        validate_object_schema(&defs, &config.schema, "config.schema")?;
        if let Some(defaults) = &config.defaults {
            let value = Value::Object(
                defaults
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            validate_object_value(&defs, &config.schema, &value, "config.defaults")?;
        }
        Ok(())
    }

    fn schema_defs(&self) -> BTreeMap<&'a str, &'a serde_json::Map<String, Value>> {
        self.lexicons
            .iter()
            .filter(|(_, lexicon)| lexicon.operation.is_none())
            .map(|(&id, lexicon)| (id, lexicon.defs))
            .collect()
    }

    /// A Lexicon reference from a contribution or from an operation's schema. Under this nodule's
    /// authority it must name a declared definition that is data, not an operation; foreign, it is
    /// left to the bundle that binds the provider.
    fn resolve_ref(&self, local_file: Option<&str>, target: &str, path: &str) -> Result<()> {
        let Some((file, def)) = parse_ref(target) else {
            return Err(NoduleError::invalid(
                path,
                format!("`{target}` is not a Lexicon reference: `nsid`, `nsid#def` or `#def`"),
            ));
        };
        let file = match (file, local_file) {
            (Some(file), _) => file,
            (None, Some(local)) => local,
            (None, None) => {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{target}` is a file-local reference; a contribution names its file"),
                ));
            }
        };
        if !self.nodule.owns(file) {
            return Ok(());
        }
        let Some(lexicon) = self.lexicons.get(file) else {
            return Err(NoduleError::invalid(
                path,
                format!("`{file}` is under this nodule's authority but not declared"),
            ));
        };
        let def = def.unwrap_or("main");
        if !lexicon.defs.contains_key(def) {
            return Err(NoduleError::invalid(
                path,
                format!("`{file}` declares no definition `{def}`"),
            ));
        }
        if def == "main" && lexicon.operation.is_some() {
            return Err(NoduleError::invalid(
                path,
                format!("`{file}` is an operation, not a schema"),
            ));
        }
        Ok(())
    }

    /// An operation an initializer invokes: one this nodule implements (local, or foreign with
    /// an unchecked kind) or a required foreign one, never a known subscription: it is observed,
    /// not invoked.
    fn invocation_target(&self, nsid: &str, implemented: &Implemented, path: &str) -> Result<()> {
        if !is_nsid(nsid) {
            return Err(NoduleError::invalid(
                path,
                format!("`{nsid}` is not an NSID"),
            ));
        }
        match implemented.get(nsid) {
            Some(Some(OperationKind::Subscription)) => Err(NoduleError::invalid(
                path,
                format!("`{nsid}` is a subscription; an invocation targets a query or procedure"),
            )),
            Some(_) => Ok(()),
            None if self.requires.contains(nsid) => Ok(()),
            None if self.nodule.owns(nsid) => Err(NoduleError::invalid(
                path,
                format!("`{nsid}` is under this nodule's authority but is not implemented"),
            )),
            None => Err(NoduleError::invalid(
                path,
                format!("`{nsid}` is foreign and not in `requires`"),
            )),
        }
    }
}

struct LexiconFile<'a> {
    id: &'a str,
    defs: &'a serde_json::Map<String, Value>,
    operation: Option<OperationKind>,
    refs: Vec<operation::SchemaRef<'a>>,
}

fn operation_kind(file: &IndexMap<String, Value>) -> Option<OperationKind> {
    file.get("defs")?
        .get("main")?
        .get("type")?
        .as_str()
        .and_then(OperationKind::parse)
}

/// Indexes a keyed contribution list, refusing an empty or repeated key.
fn keyed<'a, T>(
    items: &'a [T],
    field: &str,
    key: impl Fn(&T) -> &String,
) -> Result<BTreeMap<&'a str, &'a T>> {
    let mut index = BTreeMap::new();
    for (position, item) in items.iter().enumerate() {
        let key = key(item);
        if key.is_empty() {
            return Err(NoduleError::invalid(
                format!("{field}[{position}].key"),
                "a key is not empty",
            ));
        }
        if index.insert(key.as_str(), item).is_some() {
            return Err(NoduleError::invalid(
                format!("{field}[{position}].key"),
                format!("`{key}` is declared twice"),
            ));
        }
    }
    Ok(index)
}

#[cfg(test)]
mod tests;
