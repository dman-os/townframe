//! The cartridge catalog's portable package envelope (ADR 027 §1–2).
//!
//! A [`Cartridge`] is one immutable package revision as the catalog carries it: identity, version
//! label, distribution dependencies, content-addressed artifacts, and the contributions the
//! package makes. The catalog validates the envelope and its distribution references; it does not
//! interpret a contribution. The nodule description and the micro descriptions are carried as
//! opaque objects tagged with the schema they claim to conform to, so the catalog can be consumed
//! by BigNode and by Clay without depending on either runtime's types, and a contribution kind
//! this build does not know survives a round trip untouched.
//!
//! Enablement, configuration, grants and readiness are not here by design: they belong to the
//! runtime that consumes the contribution (ADR 027 §4), and a catalog boolean that claimed to
//! summarize both consumers would state a fact neither owns.

use big_cabinet_core::datum::DatumShape;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::Nsid;
use big_cabinet_core::datum::Shape;

use big_cabinet_core::datum::is_nsid_authority;
use big_cabinet_core::reference::DatumPin;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

pub mod catalog;
pub mod config;
pub mod protocol;
pub mod registry;
pub mod request;
pub use request::Necessity;
pub use request::Resolution;

pub mod signature;

pub const AUTHORITY: &str = "org.example.daybook";

mod manifest;
pub use manifest::Component;
pub use manifest::ComponentExport;
pub use manifest::Initializer;
pub use manifest::LocalState;
pub use manifest::Nodule;
pub use manifest::Permbind;
pub use manifest::PermbindDatum;
pub use manifest::PermbindDrawers;
pub use manifest::PermbindMethod;
pub use manifest::RevisionedStore;
pub use manifest::Routine;
pub use manifest::RoutineImpl;
pub use manifest::RoutineUse;
pub use manifest::RunMode;
pub use manifest::StateKind;
pub use manifest::StoreMode;
pub use manifest::WasmComponent;

/// `org.example.daybook.cartridge/main`: one package revision. The revision is not a field, it is the
/// content hash of the envelope ([`Cartridge::revision`]), so a pin names exactly these bytes and two
/// catalogs that imported the same package agree without coordination.
#[macros::datum(authority = AUTHORITY)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cartridge {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub id: CartridgeId,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub version: Version,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dependencies: BTreeMap<CartridgeId, VersionReq>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub lexicons: BTreeMap<Nsid, DatumPin>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sidecars: BTreeMap<Nsid, DatumPin>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub components: BTreeMap<String, DatumPin>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub permbinds: BTreeMap<String, Permbind>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub settings: BTreeSet<Nsid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodule: Option<Nodule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub micro: Option<MicroContribution>,
    /// The publisher's signature over [`Cartridge::revision`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<signature::CartridgeSignature>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl Cartridge {
    pub fn new(id: CartridgeId, version: Version) -> Self {
        Self {
            id,
            version,
            authority: None,
            title: None,
            description: None,
            dependencies: BTreeMap::new(),
            lexicons: BTreeMap::new(),
            sidecars: BTreeMap::new(),
            components: BTreeMap::new(),
            permbinds: BTreeMap::new(),
            settings: BTreeSet::new(),
            nodule: None,
            micro: None,
            signature: None,
            extra: Default::default(),
        }
    }

    /// The immutable revision this envelope is: blake3 over its canonical JSON, including every
    /// member this build does not know about. The signature is excluded because it signs the revision.
    pub fn revision(&self) -> Revision {
        let mut value = serde_json::to_value(self).expect("an envelope serializes");
        value
            .as_object_mut()
            .expect("an envelope serializes as an object")
            .remove("signature");
        let canonical = json_canon::to_vec(&value).expect("an envelope serializes");
        Revision(*blake3::hash(&canonical).as_bytes())
    }

    pub fn validate(&self) -> Result<(), CartridgeError> {
        if let Some(authority) = &self.authority
            && !is_nsid_authority(authority)
        {
            return Err(CartridgeError::invalid(
                "authority",
                format!("`{authority}` is not an NSID authority (two or more reverse-DNS labels)"),
            ));
        }
        for id in self.dependencies.keys() {
            if id == &self.id {
                return Err(CartridgeError::invalid(
                    "dependencies",
                    "a package cannot depend on itself",
                ));
            }
        }
        if let Some(micro) = &self.micro {
            if micro.component.is_empty() {
                return Err(CartridgeError::invalid(
                    "micro.component",
                    "a micro names the component key of its component",
                ));
            }
            if !self.components.contains_key(&micro.component) {
                return Err(CartridgeError::invalid(
                    "micro.component",
                    format!("`{}` is not a component of this cartridge", micro.component),
                ));
            }
        }
        self.validate_routine_uses()
    }

    fn validate_routine_uses(&self) -> Result<(), CartridgeError> {
        let Some(nodule) = &self.nodule else {
            return Ok(());
        };
        let mut names = BTreeSet::new();
        for name in self.permbinds.keys() {
            names.insert(name.as_str());
        }
        for name in nodule
            .local_state
            .keys()
            .chain(nodule.revisioned_stores.keys())
        {
            if !names.insert(name.as_str()) {
                return Err(CartridgeError::invalid(
                    format!("nodule.{name}"),
                    "permbind, local state and revisioned store keys must be unique",
                ));
            }
        }
        for (routine_key, routine) in &nodule.routines {
            if !routine.implements.as_str().is_empty() {
                for use_key in routine.uses.keys() {
                    if !names.contains(use_key.as_str()) {
                        return Err(CartridgeError::invalid(
                            format!("nodule.routines.{routine_key}.uses.{use_key}"),
                            "use must name a permbind, local state entry or revisioned store",
                        ));
                    }
                    if let Some(permbind) = self.permbinds.get(use_key) {
                        Self::validate_routine_use(
                            routine_key,
                            use_key,
                            &routine.uses[use_key],
                            permbind,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_routine_use(
        routine_key: &str,
        use_key: &str,
        usage: &RoutineUse,
        permbind: &Permbind,
    ) -> Result<(), CartridgeError> {
        let allowed = match permbind {
            Permbind::Datum(value) => [
                (usage.read, value.read.as_ref()),
                (usage.write, value.write.as_ref()),
                (usage.create, value.create.as_ref()),
                (usage.delete, value.delete.as_ref()),
            ],
            Permbind::Drawers(value) => [
                (usage.read, value.read.as_ref()),
                (usage.write, value.write.as_ref()),
                (usage.create, value.create.as_ref()),
                (usage.delete, value.delete.as_ref()),
            ],
            Permbind::Method(_) | Permbind::Unknown(_) => [
                (usage.read, None),
                (usage.write, None),
                (usage.create, None),
                (usage.delete, None),
            ],
        };
        if allowed
            .iter()
            .any(|(requested, declared)| *requested && declared.is_none())
        {
            return Err(CartridgeError::invalid(
                format!("nodule.routines.{routine_key}.uses.{use_key}"),
                "routine use asks for access the permbind does not declare",
            ));
        }
        Ok(())
    }

    pub fn nodule(&self) -> Option<&Nodule> {
        self.nodule.as_ref()
    }

    pub fn micro(&self) -> Option<&MicroContribution> {
        self.micro.as_ref()
    }

    pub fn component(&self, key: &str) -> Option<&DatumPin> {
        self.components.get(key)
    }
}

/// A cartridge envelope together with the datums named by its pins.
///
/// Registry resolution is deliberately outside this type. The constructor accepts the resolved
/// payloads directly so admission can validate the complete manifest without performing I/O.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCartridge {
    pub cartridge: Cartridge,
    pub lexicons: BTreeMap<Nsid, IndexMap<String, serde_json::Value>>,
    pub sidecars: BTreeMap<Nsid, IndexMap<String, serde_json::Value>>,
    pub components: BTreeMap<String, Component>,
}

impl ResolvedCartridge {
    pub fn new(
        cartridge: Cartridge,
        lexicons: BTreeMap<Nsid, IndexMap<String, serde_json::Value>>,
        sidecars: BTreeMap<Nsid, IndexMap<String, serde_json::Value>>,
        components: BTreeMap<String, Component>,
    ) -> Self {
        Self {
            cartridge,
            lexicons,
            sidecars,
            components,
        }
    }
}

/// `org.example.daybook.cartridge.micro`: the package's one GUI contribution. It has no key of its own:
/// the micro's identity is the cartridge id it is installed as.
///
/// `component` is the key of the [`Artifact`] holding the micro's wasm component; it is the one
/// member the catalog and the host loader read, so it is typed and [`Cartridge::validate`] checks
/// the artifact exists. `schema` is the id of the manifest format `description` conforms to, and
/// `description` is Clay's manifest minus the members the envelope already states (`id`,
/// `package_version`). Clay's runtime owns that manifest's interpretation, so the catalog carries
/// it as an open object and does not depend on Clay's crates.
#[macros::datum(authority = Cartridge::ID, name = "micro")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MicroContribution {
    pub component: String,
    pub schema: String,
    pub description: IndexMap<String, serde_json::Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, thiserror::Error)]
pub enum CartridgeError {
    #[error("cartridge envelope invalid at `{path}`: {reason}")]
    Invalid { path: String, reason: String },
    /// One version of a package names one revision. A second envelope claiming the same version is
    /// a republish under an existing label, which would make the label mean two different byte
    /// strings; the catalog refuses it instead of picking one.
    #[error(
        "cartridge `{id}` {version} is already catalogued at revision {existing}; this envelope is \
         {incoming}"
    )]
    Conflict {
        id: CartridgeId,
        version: Box<Version>,
        existing: Revision,
        incoming: Revision,
    },
}

impl CartridgeError {
    fn invalid(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

/// The package identity, `<did>/<name>`, stable across every revision (ADR 028 §2). The namespace
/// is the publisher's DID, of any method, and the name is a lowercase segment. A DID is a
/// valid datum key slot as it stands (`:` and `%` need no escaping there), and neither a DID nor a
/// name contains `/`, so the first `/` always splits the text form back into its two parts. That
/// text form is the one spelling used in datum key slots (`cartridges.install/<cartridge>`,
/// `nodule.artifact/<cartridge>/<key>`), persisted rows and workload ids; there is no second encoding.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CartridgeId {
    pub namespace: String,
    pub name: String,
}

/// A DID as a cartridge namespace is bounded to this many bytes. The DID grammar sets no limit, but a
/// cartridge id is embedded in every datum key, persisted row and workload id, so an unbounded
/// publisher string would be an unbounded key. 512 holds every `did:key` and `did:plc`, and a
/// `did:web` whose host alone may be a full 253-octet DNS name plus a path.
const MAX_DID_OCTETS: usize = 512;

impl CartridgeId {
    /// Both parts are validated here; `namespace` is a DID per the W3C DID Core syntax.
    pub fn new(namespace: &str, name: &str) -> Result<Self, InvalidCartridgeId> {
        if !is_did(namespace) || !is_segment(name) {
            return Err(InvalidCartridgeId(format!("{namespace}/{name}")));
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        })
    }
}

/// W3C DID Core syntax: `did:` method-name `:` method-specific-id, where the method is
/// `1*(a-z / 0-9)` and the id is `*( *idchar ":" ) 1*idchar` with
/// `idchar = ALPHA / DIGIT / "." / "-" / "_" / pct-encoded`. That is: non-empty, may contain `:`
/// (empty `::` segments included), may not end in `:`, and `%` must start a two-hex-digit escape.
/// Escapes must use uppercase hex, the RFC 3986 §6.2.2.1 canonical form: accepting `%3a` beside
/// `%3A` would give one DID two cartridge ids, and an id must name exactly one publisher.
/// Methods are open: whether a method is known is a resolution concern, not a syntax one.
fn is_did(did: &str) -> bool {
    if did.len() > MAX_DID_OCTETS {
        return false;
    }
    let Some(rest) = did.strip_prefix("did:") else {
        return false;
    };
    let Some((method, id)) = rest.split_once(':') else {
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
    if bytes.is_empty() || bytes.last() == Some(&b':') {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':') => {
                index += 1
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

/// A cartridge name: lowercase letters, digits, `-`, `_` and `.`, starting with a letter. Dots are
/// allowed as npm allows them (Clay micro ids read `dayframe.counter`), but never last or doubled,
/// so a name is never a `.`/`..` key-slot segment and `a..b` cannot alias anything.
fn is_segment(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes.last() != Some(&b'.')
        && !segment.contains("..")
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

impl fmt::Display for CartridgeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("`{0}` is not a cartridge id: expected `<did>/<name>`, a DID and a lowercase ascii name")]
pub struct InvalidCartridgeId(pub String);

impl FromStr for CartridgeId {
    type Err = InvalidCartridgeId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (namespace, name) = value
            .split_once('/')
            .ok_or_else(|| InvalidCartridgeId(value.to_owned()))?;
        Self::new(namespace, name).map_err(|_| InvalidCartridgeId(value.to_owned()))
    }
}

/// The DID namespace test fixtures publish under, as a literal so [`example_cartridge!`] can `concat!`
/// it into const contexts.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! example_namespace {
    () => {
        "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z"
    };
}

/// The text form of the fixture cartridge `<name>` (`<example namespace>/<name>`), usable in `const`s
/// and string positions so fixtures do not repeat DID literals.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! example_cartridge {
    ($name:literal) => {
        concat!($crate::example_namespace!(), "/", $name)
    };
}

/// Cartridge ids for tests across the crates that consume them.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use super::Cartridge;
    use super::CartridgeId;

    pub const EXAMPLE_NAMESPACE: &str = example_namespace!();

    /// `EXAMPLE_NAMESPACE/<name>`.
    pub fn cartridge_id(name: &str) -> CartridgeId {
        CartridgeId::new(EXAMPLE_NAMESPACE, name).expect("fixture cartridge ids are valid")
    }

    /// The seed of the Ed25519 key whose `did:key` is [`EXAMPLE_NAMESPACE`]: a published test
    /// vector, not a secret.
    pub const EXAMPLE_SEED: [u8; 32] = [7; 32];

    /// Signs `cartridge` with the key whose `did:key` is `seed`'s, which is the namespace of every
    /// fixture id when `seed` is [`EXAMPLE_SEED`].
    pub fn sign_with_seed(mut cartridge: Cartridge, seed: &[u8; 32]) -> Cartridge {
        use ed25519_dalek::Signer;
        let key = ed25519_dalek::SigningKey::from_bytes(seed);
        cartridge.sign_with(|message| key.sign(message).to_bytes());
        cartridge
    }

    /// `cartridge` signed by the owner of [`EXAMPLE_NAMESPACE`].
    pub fn sign_as_example(cartridge: Cartridge) -> Cartridge {
        sign_with_seed(cartridge, &EXAMPLE_SEED)
    }
}

/// 32 blake3 bytes as multibase base58btc, the text form every other 32-byte key in this tree has.
macro_rules! hash32 {
    ($(#[$doc:meta])* $name:ident, $invalid:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; 32]);

        impl $name {
            pub fn of(bytes: &[u8]) -> Self {
                Self(*blake3::hash(bytes).as_bytes())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&utils_rs::hash::encode_base58_multibase(self.0))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}({self})", stringify!($name))
            }
        }

        #[derive(Debug, thiserror::Error)]
        #[error("`{0}` is not a blake3 digest: expected 32 bytes as multibase base58btc")]
        pub struct $invalid(pub String);

        impl FromStr for $name {
            type Err = $invalid;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let bytes = utils_rs::hash::decode_base58_multibase(value)
                    .map_err(|_| $invalid(value.to_owned()))?;
                let bytes: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| $invalid(value.to_owned()))?;
                Ok(Self(bytes))
            }
        }

        text_wire!($name);
    };
}

/// A type whose wire form is its `Display`/`FromStr` text, and which the datum machinery stores
/// as a plain string.
macro_rules! text_wire {
    ($name:ident) => {
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }

        impl DatumShape for $name {
            const SHAPE: Shape = Shape::Value;
        }

        #[cfg(feature = "schemars")]
        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn schema_id() -> std::borrow::Cow<'static, str> {
                concat!(module_path!(), "::", stringify!($name)).into()
            }

            fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({ "type": "string" })
            }
        }
    };
}

hash32!(
    /// The digest of an artifact's bytes.
    Digest,
    InvalidDigest
);
hash32!(
    /// The content hash of a whole envelope: the revision a configuration selects and a dispatch pins.
    Revision,
    InvalidRevision
);

text_wire!(CartridgeId);

/// A semver release label, as the envelope spells it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(pub semver::Version);

impl fmt::Display for Version {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl FromStr for Version {
    type Err = semver::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

text_wire!(Version);

/// A semver requirement on a dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VersionReq(pub semver::VersionReq);

impl fmt::Display for VersionReq {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl FromStr for VersionReq {
    type Err = semver::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

text_wire!(VersionReq);

#[cfg(test)]
mod tests;
