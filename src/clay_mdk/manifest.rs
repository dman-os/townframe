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
mod tests {
    use super::*;

    fn shelf() -> ContractRef {
        ContractRef {
            id: "dayframe.shelf".into(),
            version: 1,
        }
    }

    fn canonical_manifest() -> MicroManifest {
        MicroManifest {
            schema: MANIFEST_SCHEMA,
            id: MicroId::new("dayframe.counter"),
            package_version: 1,
            entrypoints: Entrypoints::micro_client(),
            provides: vec![ProvidedContract {
                id: "controller".into(),
                contract: ContractRef {
                    id: "wasm.controller".into(),
                    version: 1,
                },
                retention: Retention::Strong,
            }],
            requires: vec![RequiredContract {
                binding: "shelf".into(),
                contract: shelf(),
                necessity: Necessity::Optional,
            }],
            configuration_requests: vec![ConfigurationRequest {
                binding: "counter".into(),
                facets: vec![FacetAccess {
                    owner_plug_id: None,
                    tag: "org.dayframe.counter.config".into(),
                    key_id: Some("count".into()),
                    read: true,
                    write: false,
                    create: false,
                    delete: false,
                }],
            }],
            collection_contracts: vec![CollectionContract {
                id: "tallies".into(),
                selector_schema: "dayframe.counter.tally-selector".into(),
                key_schema: "dayframe.counter.tally-key".into(),
                value_schema: "dayframe.counter.tally".into(),
                write_context_schema: None,
                offered: vec![CollectionCapability::Read, CollectionCapability::Register],
            }],
            contributions: vec![nested_contribution()],
            activation_defaults: vec![
                ActivationRegistrationTemplate {
                    id: "toolbar-button".into(),
                    trigger: Trigger::TargetLive {
                        target_binding: "shelf".into(),
                        extension_contract: shelf(),
                    },
                    declaration_id: "toolbar-button".into(),
                },
                ActivationRegistrationTemplate {
                    id: "always-on".into(),
                    trigger: Trigger::ExplicitRoot,
                    declaration_id: "toolbar-button".into(),
                },
            ],
            extra: BTreeMap::new(),
        }
    }

    fn nested_contribution() -> ContributionDeclaration {
        ContributionDeclaration {
            id: "toolbar-button".into(),
            target_binding: "shelf".into(),
            extension_contract: shelf(),
            payload: serde_json::json!({
                "badges": [1, null, "beta"],
                "enabled": true,
                "label": { "en": "Counter" },
                "order": 2,
                "slot": "toolbar"
            }),
            capability_slots: vec![CapabilitySlotDeclaration {
                index: 0,
                contract: ContractRef {
                    id: "dayframe.surface-mount".into(),
                    version: 1,
                },
            }],
        }
    }

    // ── Roundtrips ──

    #[test]
    fn manifest_roundtrips_through_json_unchanged() {
        let manifest = canonical_manifest();
        let json = serde_json::to_string(&manifest).unwrap();
        assert_eq!(
            serde_json::from_str::<MicroManifest>(&json).unwrap(),
            manifest
        );
    }

    /// A manifest written by newer tooling — an extra top-level member, an extra entrypoint,
    /// a trigger kind and vocabulary strings this build does not know — must deserialize and
    /// re-serialize to the same JSON, with the unknown parts held as data rather than dropped.
    #[test]
    fn unknown_members_round_trip_unchanged() {
        let mut wire = serde_json::to_value(canonical_manifest()).unwrap();
        wire["future_member"] = serde_json::json!({ "nested": [1, "two", null] });
        wire["entrypoints"]["future_entry"] =
            serde_json::json!("townframe:dayframe/future-interface@9.0.0");
        wire["activation_defaults"][0]["trigger"] = serde_json::json!({
            "kind": "command-invoked",
            "command": "counter.open",
            "arguments": { "focus": true }
        });
        wire["provides"][0]["retention"] = serde_json::json!("pinned");
        wire["requires"][0]["necessity"] = serde_json::json!("eventual");
        wire["collection_contracts"][0]["offered"][1] = serde_json::json!("stream");

        let manifest: MicroManifest = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            manifest.extra["future_member"],
            serde_json::json!({ "nested": [1, "two", null] })
        );
        assert_eq!(
            manifest.entrypoints.extra["future_entry"],
            serde_json::json!("townframe:dayframe/future-interface@9.0.0")
        );
        assert_eq!(
            manifest.activation_defaults[0].trigger,
            Trigger::Unknown {
                kind: "command-invoked".into(),
                fields: BTreeMap::from([
                    ("arguments".into(), serde_json::json!({ "focus": true })),
                    ("command".into(), serde_json::json!("counter.open")),
                ]),
            }
        );
        assert_eq!(
            manifest.provides[0].retention,
            Retention::Other("pinned".into())
        );
        assert_eq!(
            manifest.requires[0].necessity,
            Necessity::Other("eventual".into())
        );
        assert_eq!(
            manifest.collection_contracts[0].offered,
            vec![
                CollectionCapability::Read,
                CollectionCapability::Other("stream".into())
            ]
        );
        assert_eq!(serde_json::to_value(&manifest).unwrap(), wire);
    }

    /// A known trigger kind refined by a member this build does not know (a newer manifest's
    /// `when` condition, say) must not activate as the bare kind with the refinement silently
    /// dropped: it is carried as `Unknown` and round-trips unchanged. A known kind missing
    /// one of its own members is malformed, not unknown.
    #[test]
    fn known_trigger_kind_with_unknown_members_is_carried_as_unknown() {
        let refined_target_live = serde_json::json!({
            "kind": "target-live",
            "target_binding": "shelf",
            "extension_contract": { "id": "dayframe.shelf", "version": 1 },
            "when": "focused"
        });
        let refined_explicit_root = serde_json::json!({
            "kind": "explicit-root",
            "priority": 3
        });
        for (wire, kind) in [
            (refined_target_live, "target-live"),
            (refined_explicit_root, "explicit-root"),
        ] {
            let trigger: Trigger = serde_json::from_value(wire.clone()).unwrap();
            let Trigger::Unknown {
                kind: carried_kind, ..
            } = &trigger
            else {
                panic!("{kind} with an unknown member parsed as a known trigger: {trigger:?}");
            };
            assert_eq!(carried_kind, kind);
            assert_eq!(serde_json::to_value(&trigger).unwrap(), wire);
        }

        let missing_member = serde_json::json!({
            "kind": "target-live",
            "target_binding": "shelf"
        });
        assert!(serde_json::from_value::<Trigger>(missing_member).is_err());
    }

    // ── Validation ──

    #[test]
    fn validate_accepts_coherent_manifests() {
        canonical_manifest().validate().unwrap();

        let mut with_unknown_trigger = canonical_manifest();
        with_unknown_trigger.activation_defaults[0].trigger = Trigger::Unknown {
            kind: "command-invoked".into(),
            fields: BTreeMap::from([("command".into(), serde_json::json!("counter.open"))]),
        };
        with_unknown_trigger.validate().unwrap();
    }

    #[test]
    fn validate_rejects_incoherent_manifests() {
        type Mutation = fn(&mut MicroManifest);
        let cases: &[(Mutation, &str)] = &[
            (
                |manifest| manifest.schema = MANIFEST_SCHEMA + 1,
                "manifest declares schema 2, this build reads schema 1",
            ),
            (
                |manifest| manifest.provides.push(manifest.provides[0].clone()),
                "duplicate provides id \"controller\"",
            ),
            (
                |manifest| manifest.requires.push(manifest.requires[0].clone()),
                "duplicate requires binding \"shelf\"",
            ),
            (
                |manifest| {
                    manifest
                        .configuration_requests
                        .push(manifest.configuration_requests[0].clone())
                },
                "duplicate configuration_requests binding \"counter\"",
            ),
            (
                |manifest| {
                    manifest
                        .collection_contracts
                        .push(manifest.collection_contracts[0].clone())
                },
                "duplicate collection_contracts id \"tallies\"",
            ),
            (
                |manifest| manifest.contributions.push(nested_contribution()),
                "duplicate contributions id \"toolbar-button\"",
            ),
            (
                |manifest| {
                    manifest
                        .activation_defaults
                        .push(manifest.activation_defaults[1].clone())
                },
                "duplicate activation_defaults id \"always-on\"",
            ),
            (
                |manifest| manifest.contributions[0].target_binding = "missing".into(),
                "contribution \"toolbar-button\" targets undeclared requirement binding \
                 \"missing\"",
            ),
            (
                |manifest| manifest.activation_defaults[1].declaration_id = "missing".into(),
                "activation default \"always-on\" names undeclared contribution \"missing\"",
            ),
            (
                |manifest| {
                    manifest.requires.push(RequiredContract {
                        binding: "other".into(),
                        contract: shelf(),
                        necessity: Necessity::Optional,
                    });
                    manifest.activation_defaults[0].trigger = Trigger::TargetLive {
                        target_binding: "other".into(),
                        extension_contract: shelf(),
                    };
                },
                "activation default \"toolbar-button\" trigger does not match the target \
                 binding and extension contract of contribution \"toolbar-button\"",
            ),
            (
                |manifest| {
                    manifest.activation_defaults[0].trigger = Trigger::TargetLive {
                        target_binding: "shelf".into(),
                        extension_contract: ContractRef {
                            id: "dayframe.shelf".into(),
                            version: 2,
                        },
                    };
                },
                "activation default \"toolbar-button\" trigger does not match the target \
                 binding and extension contract of contribution \"toolbar-button\"",
            ),
        ];
        for &(mutate, expected) in cases {
            let mut manifest = canonical_manifest();
            mutate(&mut manifest);
            assert_eq!(manifest.validate().unwrap_err(), expected);
        }
    }

    #[test]
    fn contract_ref_of_uses_the_contracts_compile_time_identity() {
        #[derive(Clone)]
        struct Contract;
        impl Provision for Contract {
            const ID: &'static str = "test.contract";
        }
        assert_eq!(
            ContractRef::of::<Contract>(),
            ContractRef {
                id: "test.contract".into(),
                version: 1,
            }
        );
    }

    // ── Wire-shape goldens ──
    //
    // Manifests cross process boundaries: these snapshots pin the canonical JSON shape so a
    // serde attribute or field change cannot silently break a linked guest's manifest.

    #[test]
    fn micro_id_is_a_bare_json_string() {
        let id = MicroId::new("dayframe.counter");
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"dayframe.counter\"");
        insta::assert_snapshot!(serde_json::to_string_pretty(&id).unwrap());
    }

    #[test]
    fn manifest_canonical_wire_shape() {
        insta::assert_snapshot!(serde_json::to_string_pretty(&canonical_manifest()).unwrap());
    }

    #[test]
    fn contribution_nested_payload_wire_shape() {
        insta::assert_snapshot!(serde_json::to_string_pretty(&nested_contribution()).unwrap());
    }

    #[test]
    fn facet_access_acl_wire_shape() {
        let acl = FacetAccess {
            owner_plug_id: Some("dayframe.organizer".into()),
            tag: "org.dayframe.counter.config".into(),
            key_id: Some("count".into()),
            read: true,
            write: true,
            create: false,
            delete: false,
        };
        insta::assert_snapshot!(serde_json::to_string_pretty(&acl).unwrap());
    }
}
