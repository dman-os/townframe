//! Compile-time micro declarations (ADR 015 §9): identity, contract references, and data
//! access, written in Rust so compatibility is type-checked. Neutral vocabulary: this module
//! compiles without any backend feature so manifest and contract crates stay portable; the
//! native backend translates it into the composition runtime's descriptors. Everything here is
//! plain data and serializes, so a manifest that arrives over the wire instead of from a linked
//! crate is the same value.
use serde::Deserialize;
use serde::Serialize;

/// An explicit contract implemented by a published facility. The identity names the dependency
/// in configuration; native Rust types are adapters, not transport values (ADR 015 §7).
pub trait Provision: Clone + Send + Sync + 'static {
    const ID: &'static str;
    const VERSION: u32 = 1;
}

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

/// A declared requirement or provision: contract identity plus compatibility version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProvisionRef {
    pub id: String,
    pub version: u32,
}

impl ProvisionRef {
    pub fn of<T: Provision>() -> Self {
        Self {
            id: T::ID.into(),
            version: T::VERSION,
        }
    }
}

/// One declared facet access right (manifest-as-allowlist, ADR 015 §8). Neutral data; the
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

/// The configuration document a micro declares: its binding name plus the facet ACL the host
/// validates before granting access.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConfigGrant {
    pub name: String,
    pub facets: Vec<FacetAccess>,
}

/// A contribution a micro declares rather than makes: `provision` names one of its declared
/// requirements, `payload` is what that contract's acceptor understands (see the native
/// backend's `Contributable`). This is how a micro that cannot write a host collection itself —
/// a wasm guest today — still lands in one; the composition backend routes it by contract id
/// after the micro activates, handing the acceptor the guest's published surface.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Contribution {
    pub provision: String,
    pub payload: serde_json::Value,
}

/// A micro's full compile-time declaration. Native micros contribute through the authoring
/// context and leave `contributions` empty.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MicroManifest {
    pub id: MicroId,
    pub version: u32,
    pub requirements: Vec<ProvisionRef>,
    pub provisions: Vec<ProvisionRef>,
    pub config: Option<ConfigGrant>,
    pub contributions: Vec<Contribution>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_manifest() -> MicroManifest {
        MicroManifest {
            id: MicroId::new("dayframe.counter"),
            version: 1,
            requirements: vec![ProvisionRef {
                id: "dayframe.shelf".into(),
                version: 1,
            }],
            provisions: vec![],
            config: Some(ConfigGrant {
                name: "counter".into(),
                facets: vec![FacetAccess {
                    owner_plug_id: None,
                    tag: "org.dayframe.counter.config".into(),
                    key_id: Some("count".into()),
                    read: true,
                    write: false,
                    create: false,
                    delete: false,
                }],
            }),
            contributions: vec![nested_contribution()],
        }
    }

    fn nested_contribution() -> Contribution {
        Contribution {
            provision: "dayframe.shelf".into(),
            payload: serde_json::json!({
                "slot": "toolbar",
                "order": 2,
                "enabled": true,
                "label": { "en": "Counter" },
                "badges": [1, null, "beta"]
            }),
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

    #[test]
    fn provision_ref_of_uses_the_contracts_compile_time_identity() {
        #[derive(Clone)]
        struct Contract;
        impl Provision for Contract {
            const ID: &'static str = "test.contract";
        }
        assert_eq!(
            ProvisionRef::of::<Contract>(),
            ProvisionRef {
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
