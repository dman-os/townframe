//! Package declarations (ADR 030 §§11–15): what a micro package *could* do, inspectable
//! without instantiating it. Neutral vocabulary: this module compiles without any backend
//! feature so manifest and contract crates stay portable. Everything here is plain JSON wire
//! data — a manifest read from a component's `dayframe.manifest` section and one built by a
//! linked Rust crate are the same value.
//!
//! Forward compatibility is part of the shape: the manifest and its entrypoints carry object
//! members they do not know, open string vocabularies keep unknown strings as `Other`, and an
//! unknown publication kind is kept verbatim. A newer manifest therefore round-trips through
//! older tooling unchanged, while hosts act only on what they understand.
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub use clay_collection::Nsid;
use serde::Deserialize;
use serde::Serialize;

/// The NSID of the manifest format (ADR 028 §4): the `schema` of a plug's micro contribution
/// (`org.example.daybook.plug.micro`). A manifest carries no version of its own: a breaking
/// change to the format is a new NSID.
pub const MANIFEST_NSID: &str = "org.example.clay.micro.manifest";

/// The declared registration collection a shell reads its roots from: micros register a
/// [`crate::host::RootRow`] (usually as a manifest-declared row) and the shell redeems the
/// presentation each one attaches. It lives with the manifest vocabulary, not in `host`, because
/// a guest micro's manifest names it in a request and a guest cannot link `host`.
pub const SHELL_ROOTS: &str = "org.example.clay.shell.roots";

/// The namespace the stand-in loaders and native built-ins install micros under, until Clay runs
/// on the node and the node's own DID takes its place (`big_plugs`' importer maps a sidecar
/// `id` to `<node did>/<id>`). `did:example` is the DID method the W3C reserves for
/// documentation: syntactically a DID, so it passes the same validation as a real namespace,
/// yet it can never name a real publisher, so a stand-in id cannot be mistaken for one.
pub const LOCAL_NAMESPACE: &str = "did:example:clay-local";

/// Stable provider identity used by configuration and composition persistence: the plug id the
/// micro is installed *as* (ADR 028 §2), text form `<namespace>/<name>` with a DID namespace
/// and a plug name that may contain dots (`did:example:clay-local/dayframe.counter`). The slot
/// is the id, so a fork substituted into it keeps it. Owned by the package envelope, not by
/// the manifest (see [`Sidecar`]). The text form is validated at construction, deserialization
/// included, so a held `MicroId` always splits into a DID and a name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MicroId {
    text: String,
    /// Byte offset of the `/` that ends the namespace. A DID never contains `/`, so the first
    /// one is the separator.
    separator: usize,
}

impl MicroId {
    /// Parse the text form `<did>/<name>`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (namespace, name) = text
            .split_once('/')
            .ok_or_else(|| format!("micro id {text:?} is not `<did>/<name>`"))?;
        Self::installed(namespace, name)
    }

    /// The id of the plug named `name` installed under the DID `namespace`.
    pub fn installed(namespace: &str, name: &str) -> Result<Self, String> {
        if !is_did(namespace) {
            return Err(format!("micro id namespace {namespace:?} is not a DID"));
        }
        if !is_plug_name(name) {
            return Err(format!(
                "micro id name {name:?} is not a plug name: lowercase ascii letters, digits, \
                 `-`, `_` and `.`, starting with a letter, at most 64 bytes, with no leading, \
                 trailing or doubled dot"
            ));
        }
        Ok(Self {
            text: format!("{namespace}/{name}"),
            separator: namespace.len(),
        })
    }

    /// A built-in micro's id under [`LOCAL_NAMESPACE`]. Built-in names are compile-time
    /// constants, so an invalid one is a programming error.
    pub fn local(name: &str) -> Self {
        Self::installed(LOCAL_NAMESPACE, name).expect("a built-in micro's name is a plug name")
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The DID the micro is installed under.
    pub fn namespace(&self) -> &str {
        &self.text[..self.separator]
    }

    /// The plug name: the human-readable part, what a UI shows.
    pub fn name(&self) -> &str {
        &self.text[self.separator + 1..]
    }
}

impl TryFrom<String> for MicroId {
    type Error = String;

    fn try_from(text: String) -> Result<Self, String> {
        Self::parse(&text)
    }
}

impl From<MicroId> for String {
    fn from(id: MicroId) -> Self {
        id.text
    }
}

impl std::fmt::Display for MicroId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.text)
    }
}

/// W3C DID Core syntax, as `big_plugs_core::PlugId` checks its namespace: `did:` method
/// (`1*(a-z / 0-9)`) `:` a non-empty method-specific id of `ALPHA / DIGIT / . - _ :` and
/// uppercase-hex `%` escapes that does not end in `:`. Whether the method is known is a
/// resolution concern, not a syntax one.
fn is_did(did: &str) -> bool {
    let Some((method, id)) = did
        .strip_prefix("did:")
        .and_then(|rest| rest.split_once(':'))
    else {
        return false;
    };
    if method.is_empty()
        || !method
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return false;
    }
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.last() == Some(&b':') || did.len() > 512 {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':') => {
                index += 1;
            }
            b'%' => {
                let is_escape = bytes.get(index + 1..index + 3).is_some_and(|hex| {
                    hex.iter()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'A'..=b'F'))
                });
                if !is_escape {
                    return false;
                }
                index += 3;
            }
            _ => return false,
        }
    }
    true
}

/// A plug name, as `big_plugs_core::PlugId` checks it.
fn is_plug_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes.last() != Some(&b'.')
        && !name.contains("..")
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

/// A fully qualified WIT interface, e.g. `townframe:dayframe/micro-lifecycle@0.4.0`.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InterfaceRef(String);

impl InterfaceRef {
    pub fn new(interface: impl Into<String>) -> Self {
        Self(interface.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The component exports a host drives. `installer` is reserved: phase one never executes it
/// (ADR 030 §10), it only round-trips.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entrypoints {
    pub initialize: InterfaceRef,
    pub presentation: Option<InterfaceRef>,
    pub installer: Option<InterfaceRef>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Entrypoints {
    /// The exports of the current `micro-client` world: lifecycle initialization plus
    /// presentation events.
    pub fn micro_client() -> Self {
        Self {
            initialize: InterfaceRef::new("townframe:dayframe/micro-lifecycle@0.4.0"),
            presentation: Some(InterfaceRef::new(
                "townframe:dayframe/presentation-events@0.4.0",
            )),
            installer: None,
            extra: BTreeMap::new(),
        }
    }
}

/// Declares an open string vocabulary: known members serialize as their kebab-case wire
/// names, and any other string deserializes to `Other` and serializes back unchanged, so a
/// member added by a newer manifest survives older tooling.
macro_rules! open_vocabulary {
    (
        $(#[$meta:meta])*
        $name:ident { $($(#[$member_meta:meta])* $member:ident => $wire:literal),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(from = "String", into = "String")]
        pub enum $name {
            $($(#[$member_meta])* $member,)+
            Other(String),
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                match value.as_str() {
                    $($wire => Self::$member,)+
                    _ => Self::Other(value),
                }
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$member => $wire.into(),)+
                    $name::Other(value) => value,
                }
            }
        }
    };
}

open_vocabulary! {
    /// The two behaviors a micro can publish (ADR 031 §5): a request/response operation, or a
    /// mountable component. The set grows additively.
    PublicationKind {
        Call => "call",
        Presentation => "presentation",
    }
}

open_vocabulary! {
    /// Whether a configuration must supply a requirement before the micro can initialize.
    Necessity {
        Required => "required",
        Optional => "optional",
    }
}

open_vocabulary! {
    /// What a micro asks to do with the data a [`Request`] binds: the open vocabulary of data
    /// contracts (ADR 028 §4; other contract kinds may add members). Writing implies reading,
    /// so there is no separate write-only access.
    Access {
        Read => "read",
        Write => "write",
    }
}

open_vocabulary! {
    /// Who decides what a [`Request`] binds to.
    BindingResolution {
        /// Configuration, with the host's defaults when it says nothing (ADR 031 §11).
        Configured => "configured",
        /// Chosen by the user while the micro runs (a powerbox-style grant). Declared only:
        /// no host issues these yet, and the host never defaults them.
        AtRuntime => "atRuntime",
    }
}

impl Default for Necessity {
    /// Omitted means `required`, the safe reading, as in `big_plugs_core::request`.
    fn default() -> Self {
        Self::Required
    }
}

impl Default for BindingResolution {
    /// Omitted means `configured`, the common case, as in `big_plugs_core::request`.
    fn default() -> Self {
        Self::Configured
    }
}

/// `skip_serializing_if` for [`Request`]'s defaulted members: a default stays off the wire so
/// the canonical form of a manifest is minimal and deterministic, exactly as in `big_plugs`.
fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

open_vocabulary! {
    /// One capability a collection contract offers its consumers (ADR 030 §8).
    CollectionCapability {
        Read => "read",
        Register => "register",
        CausalWrite => "causal-write",
        DomainCommands => "domain-commands",
    }
}

/// A named publication this package can serve once initialized (ADR 031 §5, §8). `initialize`
/// returns publications by `name`, and what it returns is admitted against this declaration.
/// A `Call` names the contract (an NSID) of its request and response Lexicons; a `Presentation`
/// has the one presentation-component contract, so it declares none.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicationDeclaration {
    pub name: String,
    pub kind: PublicationKind,
    pub contract: Option<Nsid>,
}

/// One thing this package asks for, in the one request shape every plug kind shares (ADR 028
/// §4): `{binding, contract, access?, necessity, resolution, minRevision?}`. `contract` is the
/// NSID of what is wanted (a collection contract, or the contract of a call or presentation
/// another micro publishes) and never a concrete supplier: configuration resolves `binding`,
/// the plug-local name code uses, to one. Declaring a request grants no authority.
///
/// The wire shape mirrors `big_plugs_core::request::Request` member for member (camelCase,
/// open `access`/`necessity`/`resolution` strings), and this type is replaced by it once that
/// crate is reachable from this branch.
///
/// Both [`MicroManifest::requires`] and [`MicroManifest::collection_requests`] hold this
/// shape; they differ in `access`, which is absent for a requirement (the grant is "may call"
/// or "may mount") and required for a collection request (`read` or `write`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub binding: String,
    pub contract: Nsid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    /// A required binding that resolves to nothing keeps the micro from starting. Omitted
    /// means `required`.
    #[serde(default, skip_serializing_if = "is_default")]
    pub necessity: Necessity,
    /// Omitted means `configured`.
    #[serde(default, skip_serializing_if = "is_default")]
    pub resolution: BindingResolution,
    /// The least Lexicon `revision` of `contract` the package needs (ADR 028 §4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_revision: Option<u32>,
}

/// One requested facet access right (manifest-as-allowlist, ADR 015 §8). Neutral data; the
/// native backend lowers it into the node seam's `RoutineFacetAccess` vocabulary (D6). The
/// request names the facet's datum by NSID `tag` and never the plug or document that holds it:
/// configuration picks the document, so a substituted plug keeps working (ADR 028 §4).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FacetAccess {
    pub tag: Nsid,
    pub key_id: Option<String>,
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub delete: bool,
}

/// A configuration document the package asks for under a local binding name, with the facet
/// ACL it requests. A request, not a grant: configuration picks the document and what access
/// is actually allowed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationRequest {
    pub binding: String,
    pub facets: Vec<FacetAccess>,
}

/// A collection shape this package can serve or exposes. `id` is the contract's NSID, which
/// [`Request::contract`] names. Selector, key, value, and write context are contract-specific
/// CBOR named by schema NSID; host and guest libraries interpret them, the manifest only names
/// them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionContract {
    pub id: Nsid,
    pub selector_schema: Nsid,
    pub key_schema: Nsid,
    pub value_schema: Nsid,
    pub write_context_schema: Option<Nsid>,
    pub offered: Vec<CollectionCapability>,
}

/// A lazy endpoint attached to a contribution row: the value's attachment `index` refers to
/// one of this package's own declared publications, which installation turns into a
/// `LazyEndpoint` (ADR 031 §7). The row stores the endpoint as data; it never retains the
/// provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttachmentDeclaration {
    pub index: u32,
    pub publication: String,
}

/// A row this package declares into a collection (ADR 031 §3, §7). Installation admits it and
/// writes it into the package's manifest installation scope, so it is visible while the
/// package is inactive. `binding` names one of the manifest's own
/// [`MicroManifest::collection_requests`] with `write` access, and the row lands in whichever
/// collection configuration resolved that binding to (a package never names a concrete
/// collection, ADR 028 §4). `key` is the row's key and `payload` the static row body, which
/// the collection's own contract interprets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContributionDeclaration {
    pub id: String,
    pub binding: String,
    pub key: String,
    pub payload: serde_json::Value,
    pub attachments: Vec<AttachmentDeclaration>,
}

/// A package's full static declaration: what it can provide, require, request, serve,
/// contribute, and how it would like to be activated by default. It describes possibilities
/// only — configured instance identity, enablement, concrete suppliers, granted documents,
/// target acceptance, and live rows are configuration or runtime facts (ADR 030 §15). The
/// package's own identity and version are not here: the plug envelope owns them (ADR 028), and
/// the format identity is the [`MANIFEST_NSID`] the contribution is filed under.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MicroManifest {
    pub entrypoints: Entrypoints,
    pub publications: Vec<PublicationDeclaration>,
    pub requires: Vec<Request>,
    pub configuration_requests: Vec<ConfigurationRequest>,
    pub collection_contracts: Vec<CollectionContract>,
    pub collection_requests: Vec<Request>,
    pub contributions: Vec<ContributionDeclaration>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// A manifest with the identity its plug envelope will own, in the standalone on-disk form: the
/// `<name>.manifest.json` sidecar, flat JSON of the manifest's members plus `id`,
/// `package_version` and `authority`. `big_plugs`' micro importer reads exactly this (`id` becomes the plug
/// name, `package_version` the semver), and the loader splits it back into the identity and the
/// manifest the plug's micro section carries. STAND-IN until the envelope delivers both.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sidecar {
    /// The plug name (`dayframe.counter`), not a full [`MicroId`]: the namespace is whoever
    /// installs the package (`big_plugs`' importer prefixes the node DID), so the same sidecar
    /// installs under any namespace. [`Sidecar::validate`] checks it is a plug name.
    pub id: String,
    pub package_version: u32,
    /// The reverse-DNS NSID prefix the plug holds (the envelope's `authority`, ADR 028 §5). The
    /// package may declare only NSIDs under it; everything else must be a request. `None`
    /// holds no prefix, so such a package declares no NSID at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<String>,
    #[serde(flatten)]
    pub manifest: MicroManifest,
}

impl Sidecar {
    pub fn new(id: impl Into<String>, package_version: u32, manifest: MicroManifest) -> Self {
        Self {
            id: id.into(),
            package_version,
            authority: None,
            manifest,
        }
    }

    pub fn with_authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = Some(authority.into());
        self
    }

    /// [`MicroManifest::validate`] plus the authority rule: every NSID the package declares
    /// lies under its authority, namely its collection contracts. The contract of a call the
    /// package publishes is not declared by it and may be any NSID: implementing a contract is
    /// not owning it (ADR 028 §5), and a fork or a provider implements the NSID of whoever
    /// defined the call (the toolbar owns its action contract, providers implement it).
    /// Implementing grants the implementer no access, so it needs no request. A declared contract's selector, key, value and write-context schemas may
    /// instead be outside the authority when the package requests them (ADR 028 §4: anything
    /// outside the authority must be requested, whatever refers to it), i.e. a request's
    /// `contract` names the schema. What a package merely requests (collection requests,
    /// requirements, facet tags) may name anyone's NSIDs; runtime-defined NSIDs
    /// (`org.example.clay.*`) are only ever requested, never declared.
    pub fn validate(&self) -> Result<(), String> {
        if !is_plug_name(&self.id) {
            return Err(format!("package id {:?} is not a plug name", self.id));
        }
        self.manifest.validate()?;
        let owned = |nsid: &Nsid, what: &str| match &self.authority {
            Some(authority) if nsid.is_under(authority) => Ok(()),
            Some(authority) => Err(format!(
                "{what} {nsid} is outside the package's authority {authority}"
            )),
            None => Err(format!(
                "{what} {nsid} is declared by a package with no authority"
            )),
        };
        let manifest = &self.manifest;
        let owned_or_requested = |nsid: &Nsid, what: &str| {
            let requested = manifest
                .requires
                .iter()
                .chain(&manifest.collection_requests)
                .any(|request| request.contract == *nsid);
            if requested { Ok(()) } else { owned(nsid, what) }
        };
        for contract in &self.manifest.collection_contracts {
            owned(&contract.id, "collection contract")?;
            owned_or_requested(&contract.selector_schema, "selector schema")?;
            owned_or_requested(&contract.key_schema, "key schema")?;
            owned_or_requested(&contract.value_schema, "value schema")?;
            if let Some(schema) = &contract.write_context_schema {
                owned_or_requested(schema, "write-context schema")?;
            }
        }
        Ok(())
    }
}

impl MicroManifest {
    /// Check the manifest's internal references, which serde alone cannot: every named member
    /// is unique within its list, a `call` publication names its contract and a
    /// `presentation` publication names none, a requirement declares no access and a
    /// collection request declares one, every contribution targets a `write` collection
    /// request by binding, and every contribution attachment names one of the manifest's own
    /// publications. Unknown publication kinds pass: hosts carry them without serving them. The
    /// error names the offending member.
    pub fn validate(&self) -> Result<(), String> {
        fn unique<'a>(member: &str, ids: impl Iterator<Item = &'a str>) -> Result<(), String> {
            let mut seen = BTreeSet::new();
            for id in ids {
                if !seen.insert(id) {
                    return Err(format!("duplicate {member} {id:?}"));
                }
            }
            Ok(())
        }

        unique(
            "publications name",
            self.publications.iter().map(|it| it.name.as_str()),
        )?;
        unique(
            "requires binding",
            self.requires.iter().map(|it| it.binding.as_str()),
        )?;
        unique(
            "configuration_requests binding",
            self.configuration_requests
                .iter()
                .map(|it| it.binding.as_str()),
        )?;
        unique(
            "collection_contracts id",
            self.collection_contracts.iter().map(|it| it.id.as_str()),
        )?;
        unique(
            "collection_requests binding",
            self.collection_requests
                .iter()
                .map(|it| it.binding.as_str()),
        )?;
        unique(
            "contributions id",
            self.contributions.iter().map(|it| it.id.as_str()),
        )?;
        for publication in &self.publications {
            match (&publication.kind, &publication.contract) {
                (PublicationKind::Call, None) => {
                    return Err(format!(
                        "call publication {:?} declares no contract",
                        publication.name
                    ));
                }
                (PublicationKind::Presentation, Some(_)) => {
                    return Err(format!(
                        "presentation publication {:?} declares a contract; presentations \
                         have the one presentation-component contract",
                        publication.name
                    ));
                }
                _ => {}
            }
        }
        for required in &self.requires {
            if required.access.is_some() {
                return Err(format!(
                    "requirement {:?} declares an access; a requirement's grant is \
                     \"may call\" or \"may mount\"",
                    required.binding
                ));
            }
        }
        for request in &self.collection_requests {
            if request.access.is_none() {
                return Err(format!(
                    "collection request {:?} declares no access",
                    request.binding
                ));
            }
        }
        for contribution in &self.contributions {
            let Some(request) = self
                .collection_requests
                .iter()
                .find(|request| request.binding == contribution.binding)
            else {
                return Err(format!(
                    "contribution {:?} targets undeclared collection binding {:?}",
                    contribution.id, contribution.binding
                ));
            };
            if request.access != Some(Access::Write) {
                return Err(format!(
                    "contribution {:?} targets binding {:?}, which does not request write access",
                    contribution.id, contribution.binding
                ));
            }
            for attachment in &contribution.attachments {
                if !self
                    .publications
                    .iter()
                    .any(|publication| publication.name == attachment.publication)
                {
                    return Err(format!(
                        "contribution {:?} attaches undeclared publication {:?}",
                        contribution.id, attachment.publication
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "contract")]
mod builder;
#[cfg(feature = "contract")]
pub use builder::Attachments;
#[cfg(feature = "contract")]
pub use builder::ManifestBuilder;

#[cfg(test)]
mod tests;
