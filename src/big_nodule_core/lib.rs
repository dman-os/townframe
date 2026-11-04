//! The nodule description: a plug's one node-facing facet (ADR 027 §2–3, §5–7).
//!
//! A [`Nodule`] carries the Lexicons a package declares, both its datums and its operation
//! contracts (`query` | `procedure` | `subscription`), and the contributions built around those
//! contracts: routines that **implement** operations, the external operations it **requires**,
//! revisioned stores, commands, views, initializers, processors, private state and a config
//! schema. Commands, processors and initializers target operation NSIDs, never another nodule's
//! private routine keys, so a different provider can implement an operation without the consumer
//! changing.
//!
//! The datum type is the wire contract and stays open: a member, routine kind or mode this build
//! does not know is carried untouched so the description survives a catalog that is older than
//! the package. [`Nodule::admit`] is the gate a runtime applies before it will host the nodule:
//! it resolves every reference under the nodule's own authority, checks each contribution against
//! the kind of operation it names, and refuses what this build could not serve.

use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::GrammarError;
use big_cabinet_core::datum::UnknownMember;
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
    /// each; a command or processor may only target a foreign operation listed here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routines: Vec<Routine>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revisioned_stores: Vec<RevisionedStore>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<Command>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub views: Vec<View>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initialization: Vec<Initializer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub processors: Vec<Processor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub private_state: Vec<PrivateState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Config>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An executable implementation of one operation. `implements` is the operation's NSID, local or
/// foreign; the routine's own key is private to the nodule and nothing outside it refers to it.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Routine {
    pub key: String,
    pub implements: String,
    pub execution: Execution,
    /// Keys of [`Nodule::private_state`] the routine is given.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub state: Vec<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// How a routine runs. Wasm is the environment; `call` and `workflow` are the semantics (ADR 027
/// §3). Open: a kind this build does not know is carried, and refused only at admission.
#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum Execution {
    Call(Call),
    Workflow(Workflow),
    #[datum(other)]
    Unknown(UnknownMember),
}

/// `org.example.daybook.nodule.call`: request/response execution of an export in an artifact.
/// Cursored long-polling is an ordinary call, so this also serves subscriptions.
#[macros::datum(authority = Nodule::ID)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Call {
    /// Key of an artifact in the envelope that carries this description.
    pub artifact: String,
    pub export: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.nodule.workflow`: durable execution; only a `procedure` is one.
#[macros::datum(authority = Nodule::ID)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Workflow {
    pub artifact: String,
    pub export: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An ordered, replayable revision stream (ADR 027 §7). `selector` and `entry` are Lexicon
/// references; `source`, when stated, is the routine that writes it, else the host does.
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

/// An explicit invocation of an operation, optionally presented through a view.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub key: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A stateless presentation: typed input, a portable view out, produced by a `call` routine.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct View {
    pub key: String,
    pub input: String,
    pub presenter: String,
    #[serde(flatten)]
    pub extra: Extra,
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

/// A reactive invocation. The trigger policy is carried for the task layer that owns dispatch;
/// nothing here reads it.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Processor {
    pub key: String,
    pub target: String,
    pub trigger: IndexMap<String, Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Named storage private to the nodule, handed to the routines that list it.
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

/// What admission established about a description: the contracts it declares and the artifacts
/// its routines need the carrying envelope to ship.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    pub operations: BTreeMap<String, OperationKind>,
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

impl Nodule {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            lexicons: Vec::new(),
            requires: Vec::new(),
            routines: Vec::new(),
            revisioned_stores: Vec::new(),
            commands: Vec::new(),
            views: Vec::new(),
            initialization: Vec::new(),
            processors: Vec::new(),
            private_state: Vec::new(),
            config: None,
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

    /// The gate before hosting. Every reference under the nodule's own authority resolves;
    /// foreign operations a contribution invokes are in `requires`; each contribution fits the
    /// kind of operation it names; nothing this build cannot serve is accepted.
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
        let routines = keyed(&self.routines, "routines", |routine| &routine.key)?;
        let artifacts = admission.routines()?;
        admission.stores(&routines)?;
        let views = admission.views(&routines)?;
        admission.invocations(&views)?;
        admission.config()?;
        Ok(Admitted {
            operations: admission.operations(),
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
    fn operations(&self) -> BTreeMap<String, OperationKind> {
        self.lexicons
            .iter()
            .filter_map(|(id, lexicon)| lexicon.operation.map(|kind| ((*id).to_owned(), kind)))
            .collect()
    }

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

    /// Routines: one per operation, an execution this build has, an artifact to run from, and
    /// only declared private state. Returns the artifact keys the envelope must ship.
    fn routines(&self) -> Result<BTreeSet<String>> {
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
        let mut artifacts = BTreeSet::new();
        let mut implemented = BTreeMap::new();
        for (index, routine) in self.nodule.routines.iter().enumerate() {
            let path = format!("routines[{index}]");
            let kind = self.operation_for(&routine.implements, &format!("{path}.implements"))?;
            if let Some(previous) = implemented.insert(routine.implements.as_str(), &routine.key) {
                return Err(NoduleError::invalid(
                    format!("{path}.implements"),
                    format!(
                        "`{}` is already implemented by routine `{previous}`",
                        routine.implements
                    ),
                ));
            }
            let (artifact, workflow) = match &routine.execution {
                Execution::Call(call) => (&call.artifact, false),
                Execution::Workflow(workflow) => (&workflow.artifact, true),
                Execution::Unknown(member) => {
                    return Err(NoduleError::invalid(
                        format!("{path}.execution"),
                        format!(
                            "this runtime has no `{}` execution",
                            member.id("$type").unwrap_or("?")
                        ),
                    ));
                }
            };
            if artifact.is_empty() {
                return Err(NoduleError::invalid(
                    format!("{path}.execution.artifact"),
                    "a routine names the artifact it runs from",
                ));
            }
            artifacts.insert(artifact.clone());
            if workflow && kind.is_some_and(|kind| kind != OperationKind::Procedure) {
                return Err(NoduleError::invalid(
                    format!("{path}.execution"),
                    "a workflow is durable execution with effects; only a `procedure` is one",
                ));
            }
            for (position, key) in routine.state.iter().enumerate() {
                if !state.contains_key(key.as_str()) {
                    return Err(NoduleError::invalid(
                        format!("{path}.state[{position}]"),
                        format!("`{key}` is not a declared private state"),
                    ));
                }
            }
        }
        Ok(artifacts)
    }

    /// Revisioned stores: schemas that resolve, a mode this build serves, a declared source.
    fn stores(&self, routines: &BTreeMap<&str, &Routine>) -> Result<()> {
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
                && !routines.contains_key(source.as_str())
            {
                return Err(NoduleError::invalid(
                    format!("{path}.source"),
                    format!("`{source}` is not a declared routine"),
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

    /// Views: an input schema that resolves and a `call` routine as presenter.
    fn views(&self, routines: &BTreeMap<&str, &Routine>) -> Result<BTreeMap<&'a str, &'a View>> {
        let views = keyed(&self.nodule.views, "views", |view| &view.key)?;
        for (index, view) in self.nodule.views.iter().enumerate() {
            let path = format!("views[{index}]");
            self.resolve_ref(None, &view.input, &format!("{path}.input"))?;
            let Some(presenter) = routines.get(view.presenter.as_str()) else {
                return Err(NoduleError::invalid(
                    format!("{path}.presenter"),
                    format!("`{}` is not a declared routine", view.presenter),
                ));
            };
            if !matches!(presenter.execution, Execution::Call(_)) {
                return Err(NoduleError::invalid(
                    format!("{path}.presenter"),
                    "a presenter is stateless; it is a `call` routine",
                ));
            }
        }
        Ok(views)
    }

    /// Commands, initializers and processors: each invokes a query or procedure that is declared
    /// or required.
    fn invocations(&self, views: &BTreeMap<&str, &View>) -> Result<()> {
        let nodule = self.nodule;
        for (index, command) in nodule.commands.iter().enumerate() {
            let path = format!("commands[{index}]");
            self.invocation_target(&command.target, &format!("{path}.target"))?;
            if let Some(view) = &command.view
                && !views.contains_key(view.as_str())
            {
                return Err(NoduleError::invalid(
                    format!("{path}.view"),
                    format!("`{view}` is not a declared view"),
                ));
            }
        }
        keyed(&nodule.commands, "commands", |command| &command.key)?;
        for (index, initializer) in nodule.initialization.iter().enumerate() {
            let path = format!("initialization[{index}]");
            self.invocation_target(&initializer.target, &format!("{path}.target"))?;
            if let RunMode::Unknown(mode) = &initializer.mode {
                return Err(NoduleError::invalid(
                    format!("{path}.mode"),
                    format!("this runtime has no `{mode}` run mode"),
                ));
            }
        }
        for (index, processor) in nodule.processors.iter().enumerate() {
            let path = format!("processors[{index}]");
            self.invocation_target(&processor.target, &format!("{path}.target"))?;
        }
        keyed(&nodule.processors, "processors", |processor| &processor.key)?;
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

    /// The kind of an operation a routine implements: known when declared here, unknown when
    /// foreign (its contract belongs to another nodule's Lexicons).
    fn operation_for(&self, nsid: &str, path: &str) -> Result<Option<OperationKind>> {
        if !is_nsid(nsid) {
            return Err(NoduleError::invalid(
                path,
                format!("`{nsid}` is not an NSID"),
            ));
        }
        match self
            .lexicons
            .get(nsid)
            .and_then(|lexicon| lexicon.operation)
        {
            Some(kind) => Ok(Some(kind)),
            None if self.nodule.owns(nsid) => Err(NoduleError::invalid(
                path,
                format!(
                    "`{nsid}` is under this nodule's authority but is not a declared operation"
                ),
            )),
            None => Ok(None),
        }
    }

    /// An operation a command, initializer or processor invokes: a declared one or a required
    /// one, and a `query` or `procedure`; a subscription is observed, not invoked.
    fn invocation_target(&self, nsid: &str, path: &str) -> Result<()> {
        match self.operation_for(nsid, path)? {
            Some(OperationKind::Subscription) => Err(NoduleError::invalid(
                path,
                format!("`{nsid}` is a subscription; an invocation targets a query or procedure"),
            )),
            Some(_) => Ok(()),
            None if self.requires.contains(nsid) => Ok(()),
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
