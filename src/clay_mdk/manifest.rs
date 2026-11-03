//! Package declarations (ADR 024 §§11–15): what a micro package *could* do, inspectable
//! without instantiating it. Neutral vocabulary: this module compiles without any backend
//! feature so manifest and contract crates stay portable. Everything here is plain JSON wire
//! data — a manifest read from a component's `dayframe.manifest` section and one built by a
//! linked Rust crate are the same value.
//!
//! Forward compatibility is part of the shape: the manifest and its entrypoints carry object
//! members they do not know, open string vocabularies keep unknown strings as `Other`, and an
//! unknown [`Trigger`] kind is kept verbatim. A newer manifest therefore round-trips through
//! older tooling unchanged, while hosts act only on what they understand.
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap;

/// An explicit contract implemented by a published facility. The identity names the dependency
/// in configuration; native Rust types are adapters, not transport values (ADR 015 §7).
pub trait Provision: Clone + Send + Sync + 'static {
    const ID: &'static str;
    const VERSION: u32 = 1;
}

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

/// A contract identity plus its compatibility version.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ContractRef {
    pub id: String,
    pub version: u32,
}

impl ContractRef {
    pub fn of<T: Provision>() -> Self {
        Self {
            id: T::ID.into(),
            version: T::VERSION,
        }
    }
}

/// A fully qualified WIT interface, e.g. `townframe:dayframe/micro-lifecycle@0.3.0`.
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
            initialize: InterfaceRef::new("townframe:dayframe/micro-lifecycle@0.3.0"),
            presentation: Some(InterfaceRef::new(
                "townframe:dayframe/presentation-events@0.3.0",
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
    /// Whether obtaining a provided resource keeps the providing micro live (ADR 024 §2).
    /// Declared by the provider; a holder cannot promote weak to strong.
    Retention {
        Strong => "strong",
        Weak => "weak",
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

/// A contract this package can provide once initialized. `id` is the stable member id; the
/// declaration is a possibility, not a live provision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProvidedContract {
    pub id: String,
    pub contract: ContractRef,
    pub retention: Retention,
}

/// A contract this package consumes. `binding` is the local name configuration routes to a
/// concrete supplier and `initial-context.require(binding)` resolves; declaring it grants no
/// authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequiredContract {
    pub binding: String,
    pub contract: ContractRef,
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

/// An attachment slot a contribution's live row fills during successful initialization. The
/// manifest declares only the slot's index and contract: payload never carries authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilitySlotDeclaration {
    pub index: u32,
    pub contract: ContractRef,
}

/// A contribution this package can make to an extension point. `target_binding` names one of
/// its own [`RequiredContract::binding`]s, so configuration — not the package — decides which
/// configured instance receives it. `payload` is static data the extension contract's acceptor
/// understands, readable before the contributor ever runs (e.g. a toolbar item's label).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContributionDeclaration {
    pub id: String,
    pub target_binding: String,
    pub extension_contract: ContractRef,
    pub payload: serde_json::Value,
    pub capability_slots: Vec<CapabilitySlotDeclaration>,
}

/// A default activation registration installation writes for this package (ADR 024 §3). The
/// subject is always the declaring package's configured instance, so it is not spelled out;
/// `declaration_id` names the [`ContributionDeclaration`] the registration activates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivationRegistrationTemplate {
    pub id: String,
    pub trigger: Trigger,
    pub declaration_id: String,
}

/// When an activation registration pulls its subject live (ADR 024 §4). On the wire an object
/// discriminated by `kind`. An object this build does not fully understand — an unrecognised
/// kind, or a known kind carrying members beyond its own — deserializes to `Unknown` with its
/// remaining members kept verbatim and serializes back unchanged; hosts never activate it.
/// Round-trip fidelity wins over activating a trigger whose refinements would be ignored.
#[derive(Clone, Debug, PartialEq)]
pub enum Trigger {
    /// `target-live`: while the instance configured for `target_binding` is live and accepts
    /// registrations for `extension_contract`.
    TargetLive {
        target_binding: String,
        extension_contract: ContractRef,
    },
    /// `explicit-root`: the registration itself roots the subject.
    ExplicitRoot,
    /// A trigger this build does not fully understand, with its members besides `kind` carried
    /// as they arrived.
    Unknown {
        kind: String,
        fields: BTreeMap<String, serde_json::Value>,
    },
}

impl Serialize for Trigger {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self {
            Self::TargetLive {
                target_binding,
                extension_contract,
            } => {
                map.serialize_entry("kind", "target-live")?;
                map.serialize_entry("target_binding", target_binding)?;
                map.serialize_entry("extension_contract", extension_contract)?;
            }
            Self::ExplicitRoot => map.serialize_entry("kind", "explicit-root")?,
            Self::Unknown { kind, fields } => {
                map.serialize_entry("kind", kind)?;
                for (name, value) in fields {
                    map.serialize_entry(name, value)?;
                }
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Trigger {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Split off the discriminator first and keep everything else as JSON, so a trigger that
        // falls through to `Unknown` is stored with exactly the members it arrived with. A
        // known kind is only parsed when it carries no member outside its own; one that does
        // is a newer refinement and stays `Unknown`. A known kind whose own members are
        // malformed is an error, not an unknown trigger.
        #[derive(Deserialize)]
        struct Tagged {
            kind: String,
            #[serde(flatten)]
            fields: BTreeMap<String, serde_json::Value>,
        }
        #[derive(Deserialize)]
        struct TargetLive {
            target_binding: String,
            extension_contract: ContractRef,
        }
        let Tagged { kind, fields } = Tagged::deserialize(deserializer)?;
        let only = |known: &[&str]| fields.keys().all(|name| known.contains(&name.as_str()));
        match kind.as_str() {
            "target-live" if only(&["target_binding", "extension_contract"]) => {
                let fields = serde_json::Value::Object(fields.into_iter().collect());
                let TargetLive {
                    target_binding,
                    extension_contract,
                } = serde_json::from_value(fields)
                    .map_err(<D::Error as serde::de::Error>::custom)?;
                Ok(Self::TargetLive {
                    target_binding,
                    extension_contract,
                })
            }
            "explicit-root" if fields.is_empty() => Ok(Self::ExplicitRoot),
            _ => Ok(Self::Unknown { kind, fields }),
        }
    }
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
    pub provides: Vec<ProvidedContract>,
    pub requires: Vec<RequiredContract>,
    pub configuration_requests: Vec<ConfigurationRequest>,
    pub collection_contracts: Vec<CollectionContract>,
    pub contributions: Vec<ContributionDeclaration>,
    pub activation_defaults: Vec<ActivationRegistrationTemplate>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl MicroManifest {
    /// Check the manifest's internal references, which serde alone cannot: the schema is the
    /// one this build reads, every named member is unique within its list, every contribution
    /// targets one of the manifest's own requirement bindings, and every activation default
    /// names a declared contribution — for a `target-live` trigger, through that
    /// contribution's own target binding and extension contract. Unknown triggers pass: hosts
    /// carry them without activating them. The error names the offending member.
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
        unique("provides id", self.provides.iter().map(|it| it.id.as_str()))?;
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
        unique(
            "activation_defaults id",
            self.activation_defaults.iter().map(|it| it.id.as_str()),
        )?;
        for contribution in &self.contributions {
            if !self
                .requires
                .iter()
                .any(|required| required.binding == contribution.target_binding)
            {
                return Err(format!(
                    "contribution {:?} targets undeclared requirement binding {:?}",
                    contribution.id, contribution.target_binding
                ));
            }
        }
        for template in &self.activation_defaults {
            let Some(contribution) = self
                .contributions
                .iter()
                .find(|contribution| contribution.id == template.declaration_id)
            else {
                return Err(format!(
                    "activation default {:?} names undeclared contribution {:?}",
                    template.id, template.declaration_id
                ));
            };
            if let Trigger::TargetLive {
                target_binding,
                extension_contract,
            } = &template.trigger
                && (*target_binding != contribution.target_binding
                    || *extension_contract != contribution.extension_contract)
            {
                return Err(format!(
                    "activation default {:?} trigger does not match the target binding and \
                     extension contract of contribution {:?}",
                    template.id, contribution.id
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
