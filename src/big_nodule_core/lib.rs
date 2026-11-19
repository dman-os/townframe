//! The nodule description: a cartridge's one node-facing facet (ADR 027 §2–3, §5–7).
//!
//! A [`Nodule`] carries the Lexicons a package declares, both its datums and its operation
//! contracts (`query` | `procedure` | `subscription`), and what the runtime understands about
//! serving them:
//! - **services:** one XRPC [`Handler`] component and the operations it `implements`, plus the
//!   foreign contracts the nodule `requires`, in the shared [`Request`] shape (ADR 028 §4);
//! - **facilities:** private state, revisioned stores and the NSIDs of its settings datums;
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
//! The nodule has no identity or authority of its own: the envelope's `CartridgeId` names the package
//! and the envelope's `authority` is the NSID prefix it holds. [`Nodule::admit`] takes that
//! prefix: a Lexicon file under it is the nodule's own, and one outside it is a vendored copy that
//! must be the contract of a request or an operation the nodule implements.

use big_cabinet_core::DatumTag;
use big_cabinet_core::datum::GrammarError;
use big_cabinet_core::datum::Nsid;
use big_cabinet_core::datum::is_nsid;
use big_cabinet_core::datum::operation;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::datum::parse_ref;
use big_cabinet_core::doc::DatumKinds;

use big_cartridges_core::Nodule;
use big_cartridges_core::Permbind;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::RunMode;
use big_cartridges_core::StateKind;
use big_cartridges_core::StoreMode;
use big_dispatch_core::DispatchScope;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub mod config;
pub mod inventory;
pub mod protocol;
pub mod providers;

pub const AUTHORITY: &str = "org.example.daybook";

/// A method permbind projected into the provider-resolution surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub binding: String,
    pub contract: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    pub necessity: big_cartridges_core::Necessity,
    #[serde(default, skip_serializing_if = "crate::resolution_is_default")]
    pub resolution: big_cartridges_core::Resolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_revision: Option<u32>,
}

fn resolution_is_default(value: &big_cartridges_core::Resolution) -> bool {
    *value == big_cartridges_core::Resolution::Configured
}

#[cfg(test)]
impl Request {
    fn new(binding: impl Into<String>, contract: impl Into<String>) -> Self {
        Self {
            binding: binding.into(),
            contract: contract.into(),
            access: None,
            necessity: big_cartridges_core::Necessity::Required,
            resolution: big_cartridges_core::Resolution::Configured,
            min_revision: None,
        }
    }
}

impl Request {
    fn is_operation_call(&self) -> bool {
        self.access.is_none()
    }
}

#[derive(Debug, Default)]
pub struct RegistrableDatums<'a> {
    #[expect(
        clippy::type_complexity,
        reason = "public split preserves the registry's borrowed datum tuple shape"
    )]
    pub held: Vec<(
        &'a str,
        &'a IndexMap<String, Value>,
        Option<&'a IndexMap<String, Value>>,
    )>,
    pub carried: Vec<&'a IndexMap<String, Value>>,
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

pub trait NoduleAdmission {
    fn operation_requests(&self) -> Vec<Request>;
    fn bind_operations(&self, grants: &DispatchScope) -> (DispatchScope, DispatchScope);
    fn unmet_requests(&self, granted: &DispatchScope) -> Vec<Request>;
    fn missing_providers(
        &self,
        statuses: &BTreeMap<String, providers::RequestStatus>,
    ) -> Vec<Request>;
    fn lexicon_revision(&self, nsid: &str) -> Option<u32>;

    fn datum_lexicons(&self) -> Box<dyn Iterator<Item = &IndexMap<String, Value>> + '_>;
    fn registrable_datums(&self, authority: Option<&str>) -> RegistrableDatums<'_>;
    fn admit(&self, authority: Option<&str>) -> Result<Admitted>;
    /// The datum tags the cartridge declares as settings: what a nodule's settings capability may
    /// write in its config document (ADR 044 §6).
    fn settings_kinds(&self) -> DatumKinds;
}
impl NoduleAdmission for ResolvedCartridge {
    fn settings_kinds(&self) -> DatumKinds {
        DatumKinds::Only(
            self.cartridge
                .settings
                .iter()
                .map(|nsid| {
                    DatumTag::new(nsid.to_string()).expect("a settings NSID is a valid datum tag")
                })
                .collect(),
        )
    }

    /// The requests that call an operation (no `access`): the ones a dispatch grant can satisfy.
    fn operation_requests(&self) -> Vec<Request> {
        self.cartridge
            .permbinds
            .iter()
            .filter_map(|(binding, permbind)| match permbind {
                Permbind::Method(method) => Some(Request {
                    binding: binding.clone(),
                    access: None,
                    resolution: big_cartridges_core::Resolution::Configured,
                    contract: method.nsid.to_string(),
                    necessity: method.necessity.clone(),
                    min_revision: method.min_revision,
                }),
                _ => None,
            })
            .collect()
    }

    /// Splits operation `grants` into what may take effect and what may not. The manifest bounds
    /// what configuration can confer: a grant is applied only as the exact requested operations
    /// it covers, so an authority-prefix grant never widens beyond what the nodule asked for, and
    /// "can X ever reach Y" is answerable from the manifest alone. The second scope is the
    /// entries that cover no requested operation, which the record reports instead of applying.
    fn bind_operations(&self, grants: &DispatchScope) -> (DispatchScope, DispatchScope) {
        let requested: BTreeSet<String> = self
            .operation_requests()
            .into_iter()
            .map(|request| request.contract)
            .collect();
        let applied = DispatchScope {
            operations: requested
                .iter()
                .filter(|nsid| grants.permits(nsid))
                .cloned()
                .collect(),
            authorities: BTreeSet::new(),
            // Privilege is not narrowed by the manifest (a request names a method, not its
            // declaration), so a grant that asks for it carries it through; the mint then
            // refuses it unless the nodule service itself holds it.
            privileged: grants.privileged,
        };
        let unrequested = DispatchScope {
            operations: grants
                .operations
                .iter()
                .filter(|nsid| !requested.contains(*nsid))
                .cloned()
                .collect(),
            authorities: grants
                .authorities
                .iter()
                .filter(|authority| {
                    !requested
                        .iter()
                        .any(|nsid| DispatchScope::authority_owns(authority, nsid.as_str()))
                })
                .cloned()
                .collect(),
            privileged: false,
        };
        (applied, unrequested)
    }

    /// The required operation requests that `granted` does not permit: while any exists the
    /// nodule needs configuration. An optional request never appears.
    fn unmet_requests(&self, granted: &DispatchScope) -> Vec<Request> {
        self.operation_requests()
            .into_iter()
            .filter(|request| {
                request.necessity.is_required() && !granted.permits(&request.contract)
            })
            .collect()
    }

    /// The required requests, operation or data, whose `statuses` (by binding, see
    /// [`providers::RequestStatus::evaluate`]) say no provider satisfies them: while any exists
    /// the nodule is held at [`big_dispatch_core::Readiness::ProviderMissing`]. An optional request
    /// never appears.
    fn missing_providers(
        &self,
        statuses: &BTreeMap<String, providers::RequestStatus>,
    ) -> Vec<Request> {
        self.operation_requests()
            .into_iter()
            .filter(|request| {
                request.necessity.is_required() && statuses[&request.binding].unresolved.is_some()
            })
            .collect()
    }

    /// The `revision` the Lexicon file defining `nsid` declares, if this nodule carries that
    /// file and it declares one. Admission checked every declared revision, so a malformed one
    /// cannot reach here.
    fn lexicon_revision(&self, nsid: &str) -> Option<u32> {
        let id = nsid.parse::<Nsid>().ok()?;
        let file = self.lexicons.get(&id)?;
        let revision = file.get("revision")?;
        revision
            .as_u64()
            .and_then(|revision| u32::try_from(revision).ok())
    }

    fn datum_lexicons(&self) -> Box<dyn Iterator<Item = &IndexMap<String, Value>> + '_> {
        Box::new(
            self.lexicons
                .values()
                .filter(|file| operation_kind(file).is_none()),
        )
    }

    /// The datum Lexicons this nodule brings to the node's schema registry, split for
    /// registration under the envelope's `authority`: the datums it holds (under the authority,
    /// a datum Lexicon, whose `main` is a record or an object) are its to register; every other
    /// file is carried so the held ones can resolve references to vendored copies and shared type
    /// libraries, but is never registered by this nodule. A Lexicon without a `main` datum is a
    /// type library and is always carried. A vendored copy of a datum is the holder's to register,
    /// so registering it here would shadow the holder's.
    fn registrable_datums(&self, authority: Option<&str>) -> RegistrableDatums<'_> {
        let mut split = RegistrableDatums::default();
        for (nsid, file) in self
            .lexicons
            .iter()
            .filter(|(_, file)| operation_kind(file).is_none())
        {
            let id = nsid.as_str();
            // Rust-declared datums publish an `object` main (their stored values carry no
            // `$type`, which a Lexicon `record` would require); hand-written ones may state a
            // `record`. Either is a datum; a file with no `main` is a type library.
            let is_datum = matches!(
                file.get("defs")
                    .and_then(|defs| defs.get("main"))
                    .and_then(|main| main.get("type"))
                    .and_then(Value::as_str),
                Some("record" | "object")
            );
            if is_datum
                && authority.is_some_and(|authority| DispatchScope::authority_owns(authority, id))
            {
                split.held.push((id, file, self.sidecars.get(nsid)));
            } else {
                split.carried.push(file);
            }
        }
        split
    }

    /// The gate before hosting, under the NSID `authority` of the envelope that carries the
    /// description (`None`: it holds no prefix, so everything it declares is a vendored copy).
    /// Requests are well formed and name contracts other cartridges hold; every Lexicon file and
    /// reference outside the authority is implemented or requested; the handler serves only
    /// operations declared here; nothing this build cannot serve is accepted. Bindings are not
    /// read.
    fn admit(&self, authority: Option<&str>) -> Result<Admitted> {
        self.cartridge
            .validate()
            .map_err(|error| NoduleError::invalid("cartridge", error.to_string()))?;
        let nodule =
            self.cartridge.nodule.as_ref().ok_or_else(|| {
                NoduleError::invalid("nodule", "a cartridge must contain a nodule")
            })?;
        let admission = Admission {
            nodule,
            resolved: self,
            authority,
            lexicons: parse_lexicons(self)?,
            implemented: nodule
                .routines
                .values()
                .map(|routine| routine.implements.as_str())
                .collect(),
        };
        admission.requests()?;
        admission.lexicon_files()?;
        admission.sidecar()?;
        admission.lexicon_refs()?;
        admission.private_state()?;
        let (operations, artifacts) = admission.services()?;
        admission.stores()?;
        admission.initializers(&operations)?;
        admission.settings()?;
        Ok(Admitted {
            operations,
            authority: authority.map(str::to_owned),
            artifacts,
        })
    }
}

fn parse_lexicons(resolved: &ResolvedCartridge) -> Result<BTreeMap<&str, LexiconFile<'_>>> {
    let mut lexicons = BTreeMap::new();
    for (index, (key, file)) in resolved.lexicons.iter().enumerate() {
        let path = format!("lexicons[{index}]");
        if file.get("lexicon") != Some(&Value::from(1)) {
            return Err(NoduleError::invalid(
                format!("{path}.lexicon"),
                "the language version must be 1",
            ));
        }
        let id = key.as_str();
        if !is_nsid(id) || file.get("id").and_then(Value::as_str) != Some(id) {
            return Err(NoduleError::invalid(
                format!("{path}.id"),
                "a Lexicon file states the NSID named by its pin",
            ));
        }
        let defs = file
            .get("defs")
            .and_then(Value::as_object)
            .filter(|defs| !defs.is_empty())
            .ok_or_else(|| {
                NoduleError::invalid(format!("{path}.defs"), "non-empty definitions are required")
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

/// One admission in progress: the description plus what its Lexicons established, which every
/// contribution check reads. `implemented` is the NSIDs of [`Nodule::implements`], known before
/// their own check so a reference may name one.
struct Admission<'a> {
    nodule: &'a Nodule,
    resolved: &'a ResolvedCartridge,
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
            .is_some_and(|authority| DispatchScope::authority_owns(authority, nsid))
    }

    /// Whether the nodule holds `nsid`: under its authority and declared in its Lexicons.
    fn holds(&self, nsid: &str) -> bool {
        self.owns(nsid) && self.lexicons.contains_key(nsid)
    }

    /// A request names a contract another cartridge holds: one under this nodule's own authority
    /// would be a self-edge.
    fn requests(&self) -> Result<()> {
        for (index, request) in self.resolved.operation_requests().into_iter().enumerate() {
            if self.owns(&request.contract) {
                return Err(NoduleError::invalid(
                    format!("requires[{index}].contract"),
                    format!(
                        "`{}` is under this cartridge's authority; requests name contracts other cartridges hold",
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
        for (index, file) in self.resolved.lexicons.values().enumerate() {
            let id = file
                .get("id")
                .and_then(Value::as_str)
                .expect("lexicons() verified every file states its NSID");
            if let Some(library) = provided_library(id) {
                let copy =
                    Value::Object(file.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                if copy != *library {
                    return Err(NoduleError::invalid(
                        format!("lexicons[{index}]"),
                        format!(
                            "`{id}` is a type library the node provides; a vendored copy must equal the node's"
                        ),
                    ));
                }
                continue;
            }
            if !self.owns(id) && !self.is_requested(id) && !self.implemented.contains(id) {
                return Err(NoduleError::invalid(
                    format!("lexicons[{index}].id"),
                    format!(
                        "`{id}` is outside this cartridge's authority{} and is neither requested nor implemented",
                        self.authority
                            .map(|authority| format!(" `{authority}`"))
                            .unwrap_or_default()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// A sidecar document belongs to a record this nodule holds: a vendored copy is the holder's
    /// to describe, and a document for a record nothing declares would never be read.
    fn sidecar(&self) -> Result<()> {
        let held = self.resolved.registrable_datums(self.authority).held;
        for nsid in self.resolved.sidecars.keys() {
            if !held.iter().any(|(id, _, _)| id == &nsid.to_string()) {
                return Err(NoduleError::invalid(
                    format!("sidecars.{nsid}"),
                    format!("`{nsid}` is not a record this cartridge holds"),
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
        for (key, state) in &self.nodule.local_state {
            if key.is_empty() {
                return Err(NoduleError::invalid(
                    "localState",
                    "a local state key is not empty",
                ));
            }
            if let StateKind::Unknown(kind) = &state.kind {
                return Err(NoduleError::invalid(
                    format!("localState[{key}].kind"),
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
        let mut operations = BTreeMap::new();
        for (routine_key, routine) in &nodule.routines {
            let path = format!("nodule.routines.{routine_key}");
            let component = match &routine.r#impl {
                big_cartridges_core::RoutineImpl::ComponentExport(export) => export,
                big_cartridges_core::RoutineImpl::Unknown(_) => {
                    return Err(NoduleError::invalid(
                        format!("{path}.impl"),
                        "this runtime does not understand the routine implementation",
                    ));
                }
            };
            let Some(component_datum) = self.resolved.components.get(&component.component) else {
                return Err(NoduleError::invalid(
                    format!("{path}.impl.component"),
                    format!("`{}` is not a resolved component", component.component),
                ));
            };
            if !matches!(component_datum, big_cartridges_core::Component::Wasm(_)) {
                return Err(NoduleError::invalid(
                    format!("{path}.impl.component"),
                    "the routine component must be a wasm component",
                ));
            }
            artifacts.insert(component.component.clone());

            let nsid = routine.implements.as_str();
            if !is_nsid(nsid) {
                return Err(NoduleError::invalid(
                    format!("{path}.implements"),
                    format!("`{nsid}` is not an NSID"),
                ));
            }
            if self
                .resolved
                .operation_requests()
                .into_iter()
                .any(|request| request.contract == nsid)
            {
                return Err(NoduleError::invalid(
                    format!("{path}.implements"),
                    format!("`{nsid}` is both requested and implemented"),
                ));
            }
            let kind = match self.lexicons.get(nsid) {
                Some(lexicon) => {
                    let Some(kind) = lexicon.operation else {
                        return Err(NoduleError::invalid(
                            format!("{path}.implements"),
                            format!("`{nsid}` is declared here but is not an operation"),
                        ));
                    };
                    Some(kind)
                }
                None if self.owns(nsid) => {
                    return Err(NoduleError::invalid(
                        format!("{path}.implements"),
                        format!("`{nsid}` is under this cartridge's authority but is not declared"),
                    ));
                }
                None => None,
            };
            if operations.insert(nsid.to_owned(), kind).is_some() {
                return Err(NoduleError::invalid(
                    format!("{path}.implements"),
                    format!("`{nsid}` is implemented twice"),
                ));
            }
        }
        Ok((operations, artifacts))
    }

    /// Revisioned stores: schemas that resolve, a mode this build serves, and a source that is
    /// an implemented operation.
    fn stores(&self) -> Result<()> {
        for (index, (key, store)) in self.nodule.revisioned_stores.iter().enumerate() {
            let path = format!("revisionedStores[{index}]");
            if key.is_empty() {
                return Err(NoduleError::invalid(
                    format!("{path}.key"),
                    "a key is not empty",
                ));
            }
            self.resolve_ref(None, store.selector.as_str(), &format!("{path}.selector"))?;
            self.resolve_ref(None, store.entry.as_str(), &format!("{path}.entry"))?;
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

        Ok(())
    }

    /// Initializers: each has a unique key and invokes an implemented or required query or
    /// procedure, in a run mode this build has.
    fn initializers(&self, implemented: &Implemented) -> Result<()> {
        for (index, (key, initializer)) in self.nodule.initialization.iter().enumerate() {
            let path = format!("initialization[{index}]");
            if key.is_empty() {
                return Err(NoduleError::invalid(
                    format!("{path}.key"),
                    "a key is not empty",
                ));
            }
            self.invocation_target(
                initializer.target.as_str(),
                implemented,
                &format!("{path}.target"),
            )?;
            if let RunMode::Unknown(mode) = &initializer.mode {
                return Err(NoduleError::invalid(
                    format!("{path}.mode"),
                    format!("this runtime has no `{mode}` run mode"),
                ));
            }
        }
        Ok(())
    }

    /// Settings: each listed NSID is a datum this nodule holds (declared in its Lexicons under its
    /// own authority, whose `main` is a record or an object), named once. A vendored copy would
    /// leave the settings to another cartridge's Lexicon, which this nodule could not register.
    fn settings(&self) -> Result<()> {
        let mut seen = BTreeSet::new();
        for (index, nsid) in self.resolved.cartridge.settings.iter().enumerate() {
            let path = format!("settings[{index}]");
            if !seen.insert(nsid.as_str()) {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is listed twice"),
                ));
            }
            if !self.holds(nsid.as_str()) {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is not a Lexicon under this cartridge's authority"),
                ));
            }
            let lexicon = &self.lexicons[nsid.as_str()];
            let main = lexicon.defs.get("main").and_then(|main| main.get("type"));
            if lexicon.operation.is_some()
                || !matches!(main.and_then(Value::as_str), Some("record" | "object"))
            {
                return Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is not a datum: its `main` must be a record or an object"),
                ));
            }
        }
        Ok(())
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
        // A type library the node provides is always resolved, requested or not.
        if let Some(library) = provided_library(file) {
            let def = def.unwrap_or("main");
            return if library["defs"].get(def).is_some() {
                Ok(())
            } else {
                Err(NoduleError::invalid(
                    path,
                    format!("`{file}` declares no definition `{def}`"),
                ))
            };
        }
        if !self.owns(file) {
            return if self.is_requested(file) || self.implemented.contains(file) {
                Ok(())
            } else {
                Err(NoduleError::invalid(
                    path,
                    format!(
                        "`{file}` is outside this cartridge's authority and is neither implemented nor requested"
                    ),
                ))
            };
        }
        let Some(lexicon) = self.lexicons.get(file) else {
            return Err(NoduleError::invalid(
                path,
                format!("`{file}` is under this cartridge's authority but not declared"),
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
        self.resolved
            .cartridge
            .permbinds
            .iter()
            .any(|(binding, permbind)| match permbind {
                Permbind::Method(method) => method.nsid.as_str() == nsid,
                Permbind::Datum(datum) => datum.nsid.as_str() == nsid,
                Permbind::Drawers(_) => binding == nsid,
                Permbind::Unknown(_) => false,
            })
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
                .resolved
                .operation_requests()
                .into_iter()
                .find(|request| request.contract == nsid)
            {
                Some(request) if request.is_operation_call() => Ok(()),
                Some(_request) => Err(NoduleError::invalid(
                    path,
                    format!("`{nsid}` is requested by a non-method permbind"),
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

/// The type library the node provides under `nsid`, if it is one (see
/// [`node_type_libraries`](big_cabinet_core::reference::node_type_libraries)).
fn provided_library(nsid: &str) -> Option<&'static Value> {
    big_cabinet_core::reference::node_type_libraries()
        .iter()
        .find(|library| library["id"] == nsid)
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

#[cfg(test)]
mod tests;
