//! The plug catalog's portable package envelope (ADR 027 §1–2).
//!
//! A [`Plug`] is one immutable package revision as the catalog carries it: identity, version
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
use big_cabinet_core::datum::Shape;
use big_cabinet_core::datum::is_nsid;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

pub mod catalog;
pub mod protocol;
pub mod registry;
pub mod request;
pub mod signature;

pub const AUTHORITY: &str = "org.example.daybook";

/// `org.example.daybook.plug/main`: one package revision. The revision is not a field, it is the
/// content hash of the envelope ([`Plug::revision`]), so a pin names exactly these bytes and two
/// catalogs that imported the same package agree without coordination.
#[macros::datum(authority = AUTHORITY)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Plug {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub id: PlugId,
    /// The semver label a release is discovered and depended upon by. Not an operation or
    /// contract identity: a contract that changes meaning takes a new NSID (ADR 027 §6).
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub version: Version,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<Dependency>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributions: Vec<Contribution>,
    /// The publisher's signature over [`Plug::revision`] (see [`signature`]). Absent on an
    /// envelope that has not been signed yet; the registry accepts only signed ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<signature::PlugSignature>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl Plug {
    pub fn new(id: PlugId, version: Version) -> Self {
        Self {
            id,
            version,
            title: None,
            description: None,
            dependencies: Vec::new(),
            artifacts: Vec::new(),
            contributions: Vec::new(),
            signature: None,
            extra: Default::default(),
        }
    }

    /// The immutable revision this envelope is: blake3 over its canonical JSON, including every
    /// member this build does not know about, so a peer that added a field produced a different
    /// revision and a pin never silently means newer bytes. The signature is the one member left
    /// out: it signs the revision, so it cannot be part of it, and a version's identity must not
    /// change when it is signed or re-signed.
    pub fn revision(&self) -> Revision {
        let mut value = serde_json::to_value(self).expect("an envelope serializes");
        value
            .as_object_mut()
            .expect("an envelope serializes as an object")
            .remove("signature");
        let canonical = json_canon::to_vec(&value).expect("an envelope serializes");
        Revision(*blake3::hash(&canonical).as_bytes())
    }

    /// The distribution checks the catalog owns. Contribution payloads are not read: a nodule or
    /// micro description is checked by the runtime that admits it.
    pub fn validate(&self) -> Result<(), PlugError> {
        let mut dependencies = BTreeSet::new();
        for (index, dependency) in self.dependencies.iter().enumerate() {
            let path = format!("dependencies[{index}]");
            if dependency.id == self.id {
                return Err(PlugError::invalid(
                    path,
                    "a package cannot depend on itself",
                ));
            }
            if !dependencies.insert(&dependency.id) {
                return Err(PlugError::invalid(
                    path,
                    format!("`{}` is depended upon twice", dependency.id),
                ));
            }
        }
        let mut artifacts = BTreeSet::new();
        for (index, artifact) in self.artifacts.iter().enumerate() {
            let path = format!("artifacts[{index}]");
            if artifact.key.is_empty() {
                return Err(PlugError::invalid(path, "an artifact key is not empty"));
            }
            if !artifacts.insert(artifact.key.as_str()) {
                return Err(PlugError::invalid(
                    path,
                    format!("artifact key `{}` is declared twice", artifact.key),
                ));
            }
        }
        let mut nodules = 0;
        let mut micros = 0;
        for (index, contribution) in self.contributions.iter().enumerate() {
            let path = format!("contributions[{index}]");
            match contribution {
                Contribution::Nodule(nodule) => {
                    nodules += 1;
                    if nodules > 1 {
                        return Err(PlugError::invalid(
                            path,
                            "a plug has one node-facing nodule; its routines and operations are \
                             contributions inside it, not further nodules",
                        ));
                    }
                    check_schema(&path, &nodule.schema)?;
                }
                Contribution::Micro(micro) => {
                    micros += 1;
                    if micros > 1 {
                        return Err(PlugError::invalid(
                            path,
                            "a plug has one micro; its micro id is the plug id it is installed as, \
                             so a second micro is a second plug",
                        ));
                    }
                    check_schema(&path, &micro.schema)?;
                }
                Contribution::Unknown(_) => {}
            }
        }
        Ok(())
    }

    pub fn nodule(&self) -> Option<&NoduleContribution> {
        self.contributions
            .iter()
            .find_map(|contribution| match contribution {
                Contribution::Nodule(nodule) => Some(nodule),
                _ => None,
            })
    }

    pub fn micro(&self) -> Option<&MicroContribution> {
        self.contributions
            .iter()
            .find_map(|contribution| match contribution {
                Contribution::Micro(micro) => Some(micro),
                _ => None,
            })
    }

    pub fn artifact(&self, key: &str) -> Option<&Artifact> {
        self.artifacts.iter().find(|artifact| artifact.key == key)
    }
}

fn check_schema(path: &str, schema: &str) -> Result<(), PlugError> {
    if !is_nsid(schema) {
        return Err(PlugError::invalid(
            format!("{path}.schema"),
            format!("`{schema}` is not the NSID of a description datum"),
        ));
    }
    Ok(())
}

/// A distribution dependency: the package has to be present for this one to be complete. Which
/// of its operations are consumed, and by whom, is stated by the contribution that consumes them.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dependency {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub id: PlugId,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub version: VersionReq,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Bytes the package ships, named inside the package and pinned by digest. A contribution refers
/// to an artifact by key; a dispatch pins the digest the key resolved to at that revision.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub key: String,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub digest: Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What the package contributes, each member tagged by the consumer it is for. Open: a kind this
/// catalog build does not know is carried for a consumer that does.
#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum Contribution {
    Nodule(NoduleContribution),
    Micro(MicroContribution),
    #[datum(other)]
    Unknown(big_cabinet_core::datum::UnknownMember),
}

/// `org.example.daybook.plug.nodule`: the package's one node-facing facet. `schema` is the id of
/// the description datum the payload conforms to, so the nodule runtime can tell a description
/// it understands from one it must carry; the catalog reads neither.
#[macros::datum(authority = Plug::ID, name = "nodule")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleContribution {
    pub schema: String,
    pub description: IndexMap<String, serde_json::Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.plug.micro`: the package's one GUI contribution. It has no key of its own:
/// the micro's identity is the plug id it is installed as. `schema` is the id of the description
/// the payload conforms to, and `description` is Clay's manifest minus the members the envelope
/// already states (`id`, `package_version`).
#[macros::datum(authority = Plug::ID, name = "micro")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MicroContribution {
    pub schema: String,
    pub description: IndexMap<String, serde_json::Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, thiserror::Error)]
pub enum PlugError {
    #[error("plug envelope invalid at `{path}`: {reason}")]
    Invalid { path: String, reason: String },
    /// One version of a package names one revision. A second envelope claiming the same version is
    /// a republish under an existing label, which would make the label mean two different byte
    /// strings; the catalog refuses it instead of picking one.
    #[error(
        "plug `{id}` {version} is already catalogued at revision {existing}; this envelope is \
         {incoming}"
    )]
    Conflict {
        id: PlugId,
        version: Box<Version>,
        existing: Revision,
        incoming: Revision,
    },
}

impl PlugError {
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
/// text form is the one spelling used in datum key slots (`nodule.install/<plug>`,
/// `nodule.artifact/<plug>/<key>`), persisted rows and workload ids; there is no second encoding.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlugId {
    pub namespace: String,
    pub name: String,
}

/// A DID as a plug namespace is bounded to this many bytes. The DID grammar sets no limit, but a
/// plug id is embedded in every datum key, persisted row and workload id, so an unbounded
/// publisher string would be an unbounded key. 512 holds every `did:key` and `did:plc`, and a
/// `did:web` whose host alone may be a full 253-octet DNS name plus a path.
const MAX_DID_OCTETS: usize = 512;

impl PlugId {
    /// Both parts are validated here; `namespace` is a DID per the W3C DID Core syntax.
    pub fn new(namespace: &str, name: &str) -> Result<Self, InvalidPlugId> {
        if !is_did(namespace) || !is_segment(name) {
            return Err(InvalidPlugId(format!("{namespace}/{name}")));
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
/// `%3A` would give one DID two plug ids, and an id must name exactly one publisher.
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

/// A plug name: lowercase letters, digits, `-`, `_` and `.`, starting with a letter. Dots are
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

impl fmt::Display for PlugId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("`{0}` is not a plug id: expected `<did>/<name>`, a DID and a lowercase ascii name")]
pub struct InvalidPlugId(pub String);

impl FromStr for PlugId {
    type Err = InvalidPlugId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (namespace, name) = value
            .split_once('/')
            .ok_or_else(|| InvalidPlugId(value.to_owned()))?;
        Self::new(namespace, name).map_err(|_| InvalidPlugId(value.to_owned()))
    }
}

/// The DID namespace test fixtures publish under, as a literal so [`example_plug!`] can `concat!`
/// it into const contexts.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! example_namespace {
    () => {
        "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z"
    };
}

/// The text form of the fixture plug `<name>` (`<example namespace>/<name>`), usable in `const`s
/// and string positions so fixtures do not repeat DID literals.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! example_plug {
    ($name:literal) => {
        concat!($crate::example_namespace!(), "/", $name)
    };
}

/// Plug ids for tests across the crates that consume them.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use super::Plug;
    use super::PlugId;

    pub const EXAMPLE_NAMESPACE: &str = example_namespace!();

    /// `EXAMPLE_NAMESPACE/<name>`.
    pub fn plug_id(name: &str) -> PlugId {
        PlugId::new(EXAMPLE_NAMESPACE, name).expect("fixture plug ids are valid")
    }

    /// The seed of the Ed25519 key whose `did:key` is [`EXAMPLE_NAMESPACE`]: a published test
    /// vector, not a secret.
    pub const EXAMPLE_SEED: [u8; 32] = [7; 32];

    /// Signs `plug` with the key whose `did:key` is `seed`'s, which is the namespace of every
    /// fixture id when `seed` is [`EXAMPLE_SEED`].
    pub fn sign_with_seed(mut plug: Plug, seed: &[u8; 32]) -> Plug {
        use ed25519_dalek::Signer;
        let key = ed25519_dalek::SigningKey::from_bytes(seed);
        plug.sign_with(|message| key.sign(message).to_bytes());
        plug
    }

    /// `plug` signed by the owner of [`EXAMPLE_NAMESPACE`].
    pub fn sign_as_example(plug: Plug) -> Plug {
        sign_with_seed(plug, &EXAMPLE_SEED)
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

text_wire!(PlugId);

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
