//! The nodule description: a plug's one node-facing facet (ADR 027 §2–3, §5–7).
//!
//! A [`Nodule`] carries the Lexicons a package declares, both its datums and its operation
//! contracts (`query` | `procedure` | `subscription`), and what the runtime understands about
//! serving them:
//! - **services:** one XRPC [`Handler`] component and the operations it `implements`, plus the
//!   foreign contracts the nodule `requires`, in the shared [`Request`] shape (ADR 028 §4);
//! - **facilities:** private state, revisioned stores and a config schema;
//! - **lifecycle:** initializers.
//!
//! Everything else (commands, views, processors, future sections) lives in
//! [`Nodule::bindings`]: sections the runtime carries and round-trips but never interprets.
//!
//! The datum type is the wire contract and stays open: a member, mode or binding section this
//! build does not know is carried untouched so the description survives a catalog that is older
//! than the package. [`Nodule::admit`] is the gate a runtime applies before it will host the
//! nodule: it resolves every reference the nodule makes (to what it declares, implements or
//! requests), checks that what it serves is declared, and refuses what this build could not
//! serve. It reads no binding.
//!
//! The nodule has no identity or authority of its own: the envelope's `PlugId` names the package
//! and the envelope's `authority` is the NSID prefix it holds. [`Nodule::admit`] takes that
//! prefix: a Lexicon file under it is the nodule's own, and one outside it is a vendored copy that
//! must be the contract of a request or an operation the nodule implements.

use crate::dispatch::DispatchScope;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::GrammarError;
use big_cabinet_core::datum::is_nsid;
use big_cabinet_core::datum::operation;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::datum::parse_ref;
use big_cabinet_core::datum::validate_object_schema;
use big_cabinet_core::datum::validate_object_value;
use big_plugs_core::PlugError;
use big_plugs_core::request::Request;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub mod config;
pub mod dispatch;
pub mod inventory;
pub mod protocol;
pub mod providers;

pub const AUTHORITY: &str = "org.example.daybook";

/// `org.example.daybook.nodule/main`: what a package contributes to a node. It carries no
/// identity or authority: the envelope's `PlugId` names the package and its `authority` is the NSID
/// prefix the package holds. An NSID in the description is *held* when it is under that prefix and
/// declared in `lexicons`; every other NSID it uses must be implemented or requested.
#[macros::datum(authority = AUTHORITY)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nodule {
    /// Lexicon files as published: datum Lexicons for the datum compiler, operation Lexicons for
    /// [`big_cabinet_core::datum::operation`]. Carried opaque because a Lexicon is already its own
    /// schema language.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lexicons: Vec<IndexMap<String, Value>>,
    /// What the nodule uses that it does not hold, in the shared request shape: foreign
    /// operations it calls (a request without `access`), datum contracts it reads or writes.
    /// Strictness: any NSID the description references outside the envelope's authority that it
    /// does not implement must be a `contract` here, and an initializer may only target a foreign operation listed
    /// here. The runtime binds a provider for each.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<Request>,
    /// The one HTTP-shaped XRPC handler component serving `/xrpc/<nsid>` for every operation in
    /// [`Self::implements`]. It is handed all of [`Self::private_state`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<Handler>,
    /// Operation NSIDs the handler serves. One whose Lexicon this description declares must be a
    /// query, procedure or subscription. One with no Lexicon here (a provider for a contract
    /// another nodule declares) is only checked to be an NSID; a non-empty list needs a
    /// [`Self::handler`].
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

/// An operation the runtime invokes on the nodule's behalf each time it starts. Registration is
/// intent; whether it ran is the node's record (`NoduleRecord::init`), not this description's.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Initializer {
    /// Names this initializer within the nodule: unique and not empty. A node records that a
    /// [`RunMode::Once`] initializer ran under this key and the plug revision, and derives the
    /// idempotency key of each invocation from it, so renaming one makes it a new initializer.
    pub key: String,
    pub target: String,
    pub mode: RunMode,
    #[serde(flatten)]
    pub extra: Extra,
}

/// When an initializer runs on a node. Initializers must be idempotent either way: the node
/// has no synced record of what ran, so a mirrored node or a new revision runs them again.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RunMode {
    /// Once per node per plug revision: not again on restart or re-enable, again on a new revision.
    Once,
    /// Each time the nodule starts: node boot, and enabling it.
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

/// What admission established about a description: the operations it serves, the authority it
/// held and the artifacts the envelope carrying it must ship. An implemented operation's kind is `Some`
/// when its contract is declared in this nodule's own Lexicons and `None` when it is foreign: a
/// single description cannot know another nodule's contract, so the kind (and the contract itself) is resolved at
/// bind time. Keeping one map, rather than a second set of foreign NSIDs, lets every consumer ask
/// "is this implemented?" in one lookup and treat an unknown kind as unchecked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    pub operations: BTreeMap<String, Option<OperationKind>>,
    /// The envelope's NSID authority this admission ran under. Two nodules on one node must not
    /// hold the same or nested prefixes.
    pub authority: Option<String>,
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
    /// Whether `nsid` is `authority` itself or lives under `authority.`: the one NSID-prefix
    /// rule for authorities, whole segments only (`org.example.photos` owns
    /// `org.example.photos.list`, not `org.example.photosynthesis`).
    pub fn authority_owns(authority: &str, nsid: &str) -> bool {
        nsid.strip_prefix(authority)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    }

    /// The requests that call an operation (no `access`): the ones a dispatch grant can satisfy.
    pub fn operation_requests(&self) -> impl Iterator<Item = &Request> {
        self.requires
            .iter()
            .filter(|request| request.is_operation_call())
    }

    /// Splits operation `grants` into what may take effect and what may not. The manifest bounds
    /// what configuration can confer: a grant is applied only as the exact requested operations
    /// it covers, so an authority-prefix grant never widens beyond what the nodule asked for, and
    /// "can X ever reach Y" is answerable from the manifest alone. The second scope is the
    /// entries that cover no requested operation, which the record reports instead of applying.
    pub fn bind_operations(&self, grants: &DispatchScope) -> (DispatchScope, DispatchScope) {
        let requested: BTreeSet<&str> = self
            .operation_requests()
            .map(|request| request.contract.as_str())
            .collect();
        let applied = DispatchScope {
            operations: requested
                .iter()
                .filter(|nsid| grants.permits(nsid))
                .map(|nsid| (*nsid).to_owned())
                .collect(),
            authorities: BTreeSet::new(),
        };
        let unrequested = DispatchScope {
            operations: grants
                .operations
                .iter()
                .filter(|nsid| !requested.contains(nsid.as_str()))
                .cloned()
                .collect(),
            authorities: grants
                .authorities
                .iter()
                .filter(|authority| {
                    !requested
                        .iter()
                        .any(|nsid| Self::authority_owns(authority, nsid))
                })
                .cloned()
                .collect(),
        };
        (applied, unrequested)
    }

    /// The required operation requests that `granted` does not permit: while any exists the
    /// nodule needs configuration. An optional request never appears.
    pub fn unmet_requests<'a>(
        &'a self,
        granted: &'a DispatchScope,
    ) -> impl Iterator<Item = &'a Request> {
        self.operation_requests().filter(|request| {
            request.necessity.is_required() && !granted.permits(&request.contract)
        })
    }

    /// The required operation requests whose `statuses` (by binding, see
    /// [`providers::RequestStatus::evaluate`]) say no provider satisfies them: while any exists
    /// the nodule is held at [`inventory::Readiness::ProviderMissing`]. An optional request
    /// never appears.
    pub fn missing_providers<'a>(
        &'a self,
        statuses: &'a BTreeMap<String, providers::RequestStatus>,
    ) -> impl Iterator<Item = &'a Request> {
        self.operation_requests().filter(|request| {
            // An enabled nodule has a status for every operation request, which is the only
            // case this is asked in.
            request.necessity.is_required() && statuses[&request.binding].unresolved.is_some()
        })
    }

    /// The `revision` the Lexicon file defining `nsid` declares, if this nodule carries that
    /// file and it declares one. Admission checked every declared revision, so a malformed one
    /// cannot reach here.
    pub fn lexicon_revision(&self, nsid: &str) -> Option<u32> {
        let file = self
            .lexicons
            .iter()
            .find(|file| file.get("id").and_then(Value::as_str) == Some(nsid))?;
        let revision = file.get("revision")?;
        Some(
            revision
                .as_u64()
                .and_then(|revision| u32::try_from(revision).ok())
                .expect("admission checked the Lexicon revision"),
        )
    }

    /// The Lexicon files that declare datums rather than operations, for the datum compiler.
    pub fn datum_lexicons(&self) -> impl Iterator<Item = &IndexMap<String, Value>> {
        self.lexicons
            .iter()
            .filter(|file| operation_kind(file).is_none())
    }

    /// The gate before hosting, under the NSID `authority` of the envelope that carries the
    /// description (`None`: it holds no prefix, so everything it declares is a vendored copy).
    /// Requests are well formed and name contracts other plugs hold; every Lexicon file and
    /// reference outside the authority is implemented or requested; the handler serves only
    /// operations declared here; nothing this build cannot serve is accepted. Bindings are not
    /// read.
    pub fn admit(&self, authority: Option<&str>) -> Result<Admitted> {
        Request::validate_all(&self.requires, "requires").map_err(|error| match error {
            PlugError::Invalid { path, reason } => NoduleError::invalid(path, reason),
            other => unreachable!("request validation only reports invalid fields: {other}"),
        })?;
        let admission = Admission {
            nodule: self,
            authority,
            lexicons: self.lexicons()?,
            implemented: self.implements.iter().map(String::as_str).collect(),
        };
        admission.requests()?;
        admission.lexicon_files()?;
        admission.lexicon_refs()?;
        admission.private_state()?;
        let (operations, artifacts) = admission.services()?;
        admission.stores()?;
        admission.initializers(&operations)?;
        admission.config()?;
        Ok(Admitted {
            operations,
            authority: authority.map(str::to_owned),
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
            if let Some(revision) = file.get("revision")
                && !revision
                    .as_u64()
                    .is_some_and(|revision| (1..=u64::from(u32::MAX)).contains(&revision))
            {
                return Err(NoduleError::invalid(
                    format!("{path}.revision"),
                    "a Lexicon revision is an integer starting at 1",
                ));
            }
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
}

/// One admission in progress: the description plus what its Lexicons established, which every
/// contribution check reads. `implemented` is the NSIDs of [`Nodule::implements`], known before
/// their own check so a reference may name one.
struct Admission<'a> {
    nodule: &'a Nodule,
    /// The envelope's NSID authority.
    authority: Option<&'a str>,
    lexicons: BTreeMap<&'a str, LexiconFile<'a>>,
    implemented: BTreeSet<&'a str>,
}

impl<'a> Admission<'a> {
    /// Whether `nsid` is under the envelope's authority (whole segments, as dispatch matches
    /// prefixes).
    fn owns(&self, nsid: &str) -> bool {
        self.authority
            .is_some_and(|authority| Nodule::authority_owns(authority, nsid))
    }

    /// Whether the nodule holds `nsid`: under its authority and declared in its Lexicons.
    fn holds(&self, nsid: &str) -> bool {
        self.owns(nsid) && self.lexicons.contains_key(nsid)
    }

    /// A request names a contract another plug holds: one under this nodule's own authority
    /// would be a self-edge.
    fn requests(&self) -> Result<()> {
        for (index, request) in self.nodule.requires.iter().enumerate() {
            if self.owns(&request.contract) {
                return Err(NoduleError::invalid(
                    format!("requires[{index}].contract"),
                    format!(
                        "`{}` is under this plug's authority; requests name contracts other plugs hold",
                        request.contract
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Strictness for Lexicon files: one outside the authority is a vendored copy of a
    /// contract, so it must be requested or an operation this nodule implements. (A file under
    /// the authority is the nodule's own.)
    fn lexicon_files(&self) -> Result<()> {
        for (index, file) in self.nodule.lexicons.iter().enumerate() {
            let id = file
                .get("id")
                .and_then(Value::as_str)
                .expect("lexicons() verified every file states its NSID");
            if !self.owns(id) && !self.is_requested(id) && !self.implemented.contains(id) {
                return Err(NoduleError::invalid(
                    format!("lexicons[{index}].id"),
                    format!(
                        "`{id}` is outside this plug's authority{} and is neither requested nor implemented",
                        self.authority
                            .map(|authority| format!(" `{authority}`"))
                            .unwrap_or_default()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Every `ref` an operation's schemas make, resolved against the file that makes it.
    fn lexicon_refs(&self) -> Result<()> {
        for lexicon in self.lexicons.values() {
            // The reference's path already starts at its file (`lexicons[<index>].defs...`).
            for reference in &lexicon.refs {
                self.resolve_ref(Some(lexicon.id), reference.target, &reference.path)?;
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
            // A request names operations provided by others; one this nodule also implements
            // would be a self-edge in the runtime dependency graph.
            if nodule
                .requires
                .iter()
                .any(|request| &request.contract == nsid)
            {
                return Err(NoduleError::invalid(
                    path,
                    format!(
                        "`{nsid}` is both requested and implemented; `requires` lists contracts other nodules provide"
                    ),
                ));
            }
            // An operation under this plug's authority must be declared here as an operation. A
            // foreign one is checked against its vendored Lexicon when this description carries
            // one, and admitted on syntax alone otherwise: its contract lives in another
            // nodule's Lexicons, which admission cannot see. Resolving it is a bind-time
            // concern, and dispatch reports this service unavailable until the contract is known
            // (per-service readiness, ledger D4).
            let kind = match self.lexicons.get(nsid.as_str()) {
                Some(lexicon) => {
                    let Some(kind) = lexicon.operation else {
                        return Err(NoduleError::invalid(
                            path,
                            format!("`{nsid}` is declared here but is not an operation"),
                        ));
                    };
                    Some(kind)
                }
                None if self.owns(nsid) => {
                    return Err(NoduleError::invalid(
                        path,
                        format!(
                            "`{nsid}` is under this plug's authority but is not a declared operation"
                        ),
                    ));
                }
                None => None,
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
    fn stores(&self) -> Result<()> {
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
                && !self.implemented.contains(source.as_str())
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

    /// Initializers: each has a unique key and invokes an implemented or required query or
    /// procedure, in a run mode this build has.
    fn initializers(&self, implemented: &Implemented) -> Result<()> {
        keyed(&self.nodule.initialization, "initialization", |init| {
            &init.key
        })?;
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

    /// Config schema and defaults: the schema must be declared by this nodule (not a vendored
    /// copy) as a valid datum `object` schema, and `defaults` (when present) must conform to it.
    fn config(&self) -> Result<()> {
        let Some(config) = &self.nodule.config else {
            return Ok(());
        };
        self.resolve_ref(None, &config.schema, "config.schema")?;
        let Some((Some(file), _)) = parse_ref(&config.schema) else {
            unreachable!("resolve_ref verified a non-local Lexicon reference");
        };
        if !self.holds(file) {
            return Err(NoduleError::invalid(
                "config.schema",
                format!("`{file}` is not a Lexicon under this plug's authority"),
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

    /// A Lexicon reference from a contribution or from an operation's schema. A file this nodule
    /// holds must name a declared definition that is data, not an operation. Any other file must
    /// be requested or implemented (strictness, ADR 028 §4), and is then left to the bundle that
    /// binds the provider.
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
        if !self.owns(file) {
            return if self.is_requested(file) || self.implemented.contains(file) {
                Ok(())
            } else {
                Err(NoduleError::invalid(
                    path,
                    format!(
                        "`{file}` is outside this plug's authority and is neither implemented nor requested"
                    ),
                ))
            };
        }
        let Some(lexicon) = self.lexicons.get(file) else {
            return Err(NoduleError::invalid(
                path,
                format!("`{file}` is under this plug's authority but not declared"),
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

    fn is_requested(&self, nsid: &str) -> bool {
        self.nodule
            .requires
            .iter()
            .any(|request| request.contract == nsid)
    }

    /// An operation an initializer invokes: one this nodule implements (local, or foreign with
    /// an unchecked kind) or a requested operation call, never a known subscription: it is
    /// observed, not invoked.
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
            None => match self
                .nodule
                .requires
                .iter()
                .find(|request| request.contract == nsid)
            {
                Some(request) if request.is_operation_call() => Ok(()),
                Some(request) => Err(NoduleError::invalid(
                    path,
                    format!(
                        "`{nsid}` is requested with `{}` access; an initializer calls an operation",
                        request.access.as_deref().unwrap_or_default()
                    ),
                )),
                None if self.lexicons.contains_key(nsid) => Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is declared here but is not implemented"),
                )),
                None => Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is neither implemented nor requested"),
                )),
            },
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
