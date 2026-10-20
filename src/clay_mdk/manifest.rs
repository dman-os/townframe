//! Compile-time micro declarations (ADR 015 §9): identity, contract references, and data
//! access, written in Rust so compatibility is type-checked. Neutral vocabulary: this module
//! compiles without any backend feature so manifest and contract crates stay portable; the
//! native backend translates it into the composition runtime's descriptors.

/// An explicit contract implemented by a published facility. The identity names the dependency
/// in configuration; native Rust types are adapters, not transport values (ADR 015 §7).
pub trait Provision: Clone + Send + Sync + 'static {
    const ID: &'static str;
    const VERSION: u32 = 1;
}

/// Stable provider identity used by configuration and composition persistence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigGrant {
    pub name: String,
    pub facets: Vec<FacetAccess>,
}

/// A micro's full compile-time declaration. `contributions` are host-side insertion data for
/// providers whose contributions the composition root binds (for example a wasm package that
/// cannot itself reach a native collection yet); native micros contribute through the
/// authoring context instead and leave this empty.
#[derive(Clone, Debug, PartialEq)]
pub struct MicroManifest<C = ()> {
    pub id: MicroId,
    pub version: u32,
    pub requirements: Vec<ProvisionRef>,
    pub provisions: Vec<ProvisionRef>,
    pub config: Option<ConfigGrant>,
    pub contributions: Vec<C>,
}
