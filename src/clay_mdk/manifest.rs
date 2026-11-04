//! Package declarations (ADR 024 §§11–15): what a micro package *could* do, inspectable
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

use serde::Deserialize;
use serde::Serialize;

pub use clay_guest_session::capabilities::ContractIdentity;

/// Version of the manifest vocabulary itself, written into [`MicroManifest::schema`].
pub const MANIFEST_SCHEMA: u32 = 1;

/// Stable provider identity used by configuration and composition persistence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MicroId(String);

impl MicroId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
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
/// (ADR 024 §10), it only round-trips.
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
    /// The two behaviors a micro can publish (ADR 025 §5): a request/response operation, or a
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
    /// One capability a collection contract offers its consumers (ADR 024 §8).
    CollectionCapability {
        Read => "read",
        Register => "register",
        CausalWrite => "causal-write",
        DomainCommands => "domain-commands",
    }
}

/// A named publication this package can serve once initialized (ADR 025 §5, §8). `initialize`
/// returns publications by `name`, and what it returns is admitted against this declaration.
/// A `Call` names the contract of its request and response Lexicons; a `Presentation` has the
/// one presentation-component contract, so it declares none.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicationDeclaration {
    pub name: String,
    pub kind: PublicationKind,
    pub contract: Option<ContractIdentity>,
}

/// A contract this package consumes. `binding` is the local name configuration routes to a
/// concrete supplier and `initial-context.require(binding)` resolves; declaring it grants no
/// authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequiredContract {
    pub binding: String,
    pub contract: ContractIdentity,
    pub necessity: Necessity,
}

/// One requested facet access right (manifest-as-allowlist, ADR 015 §8). Neutral data; the
/// native backend lowers it into the node seam's `RoutineFacetAccess` vocabulary (D6).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FacetAccess {
    pub owner_plug_id: Option<String>,
    pub tag: String,
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

/// A collection shape this package can serve or exposes. Selector, key, value, and write
/// context are contract-specific CBOR named by schema id; host and guest libraries interpret
/// them, the manifest only names them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionContract {
    pub id: String,
    pub selector_schema: String,
    pub key_schema: String,
    pub value_schema: String,
    pub write_context_schema: Option<String>,
    pub offered: Vec<CollectionCapability>,
}

/// A lazy endpoint attached to a contribution row: the value's attachment `index` refers to
/// one of this package's own declared publications, which installation turns into a
/// `LazyEndpoint` (ADR 025 §7). The row stores the endpoint as data; it never retains the
/// provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttachmentDeclaration {
    pub index: u32,
    pub publication: String,
}

/// A row this package declares into a collection (ADR 025 §3, §7). Installation admits it and
/// writes it into the package's manifest installation scope, so it is visible while the
/// package is inactive. `collection` names the collection contract, `key` the row's key and
/// `payload` the static row body, which the collection's own contract interprets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContributionDeclaration {
    pub id: String,
    pub collection: String,
    pub key: String,
    pub payload: serde_json::Value,
    pub attachments: Vec<AttachmentDeclaration>,
}

/// A package's full static declaration: what it can provide, require, request, serve,
/// contribute, and how it would like to be activated by default. It describes possibilities
/// only — configured instance identity, enablement, concrete suppliers, granted documents,
/// target acceptance, and live rows are configuration or runtime facts (ADR 024 §15).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MicroManifest {
    pub schema: u32,
    pub id: MicroId,
    pub package_version: u32,
    pub entrypoints: Entrypoints,
    pub publications: Vec<PublicationDeclaration>,
    pub requires: Vec<RequiredContract>,
    pub configuration_requests: Vec<ConfigurationRequest>,
    pub collection_contracts: Vec<CollectionContract>,
    pub contributions: Vec<ContributionDeclaration>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl MicroManifest {
    /// Check the manifest's internal references, which serde alone cannot: the schema is the
    /// one this build reads, every named member is unique within its list, a `call`
    /// publication names its contract and a `presentation` publication names none, and every
    /// contribution attachment names one of the manifest's own publications. Unknown
    /// publication kinds pass: hosts carry them without serving them. The error names the
    /// offending member.
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

        if self.schema != MANIFEST_SCHEMA {
            return Err(format!(
                "manifest declares schema {}, this build reads schema {MANIFEST_SCHEMA}",
                self.schema
            ));
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
        for contribution in &self.contributions {
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

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
