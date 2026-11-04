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
            extra: Default::default(),
        }
    }

    /// The immutable revision this envelope is: blake3 over its canonical JSON, including every
    /// member this build does not know about, so a peer that added a field produced a different
    /// revision and a pin never silently means newer bytes.
    pub fn revision(&self) -> Revision {
        let canonical = json_canon::to_vec(self).expect("an envelope serializes");
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
        let mut micros = BTreeSet::new();
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
                    if micro.key.is_empty() {
                        return Err(PlugError::invalid(path, "a micro key is not empty"));
                    }
                    if !micros.insert(micro.key.as_str()) {
                        return Err(PlugError::invalid(
                            path,
                            format!("micro key `{}` is declared twice", micro.key),
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

    pub fn micros(&self) -> impl Iterator<Item = &MicroContribution> {
        self.contributions
            .iter()
            .filter_map(|contribution| match contribution {
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

/// `org.example.daybook.plug.micro`: one GUI contribution, keyed inside the package.
#[macros::datum(authority = Plug::ID, name = "micro")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MicroContribution {
    pub key: String,
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

/// The package identity, `@namespace/name`, stable across every revision. The same spelling the
/// legacy manifests use, so an existing package keeps its identity when it is catalogued here.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlugId {
    pub namespace: String,
    pub name: String,
}

impl PlugId {
    pub fn new(namespace: &str, name: &str) -> Result<Self, InvalidPlugId> {
        if !is_segment(namespace) || !is_segment(name) {
            return Err(InvalidPlugId(format!("@{namespace}/{name}")));
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        })
    }
}

fn is_segment(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

impl fmt::Display for PlugId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "@{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("`{0}` is not a plug id: expected `@namespace/name` in lowercase ascii")]
pub struct InvalidPlugId(pub String);

impl FromStr for PlugId {
    type Err = InvalidPlugId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (namespace, name) = value
            .strip_prefix('@')
            .and_then(|rest| rest.split_once('/'))
            .ok_or_else(|| InvalidPlugId(value.to_owned()))?;
        Self::new(namespace, name).map_err(|_| InvalidPlugId(value.to_owned()))
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
mod tests {
    use super::*;
    use big_cabinet_core::datum::DatumMember;
    use serde_json::json;

    fn plug() -> Plug {
        let mut plug = Plug::new("@example/photos".parse().unwrap(), "1.2.3".parse().unwrap());
        plug.title = Some("Photos".into());
        plug.dependencies.push(Dependency {
            id: "@example/search-api".parse().unwrap(),
            version: "^2".parse().unwrap(),
            extra: Default::default(),
        });
        plug.artifacts.push(Artifact {
            key: "routines".into(),
            digest: Digest::of(b"wasm"),
            size: Some(4),
            media_type: Some("application/wasm".into()),
            extra: Default::default(),
        });
        plug.contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: "org.example.daybook.nodule".into(),
                description: [("id".to_owned(), json!("org.example.photos"))]
                    .into_iter()
                    .collect(),
                extra: Default::default(),
            }));
        plug.contributions
            .push(Contribution::Micro(MicroContribution {
                key: "gallery".into(),
                schema: "org.example.clay.micro".into(),
                description: [("entry".to_owned(), json!("gallery.js"))]
                    .into_iter()
                    .collect(),
                extra: Default::default(),
            }));
        plug
    }

    #[test]
    fn the_envelope_spells_ids_versions_and_digests_as_text() {
        let plug = plug();
        plug.validate().unwrap();
        let value = serde_json::to_value(&plug).unwrap();
        assert_eq!(value["id"], "@example/photos");
        assert_eq!(value["version"], "1.2.3");
        assert_eq!(value["dependencies"][0]["version"], "^2");
        let digest = value["artifacts"][0]["digest"].as_str().unwrap();
        assert!(digest.starts_with('z'), "{digest}");
        assert_eq!(digest.parse::<Digest>().unwrap(), Digest::of(b"wasm"));
        assert_eq!(value["contributions"][0]["$type"], NoduleContribution::ID);
        assert_eq!(value["contributions"][1]["$type"], MicroContribution::ID);
        assert_eq!(serde_json::from_value::<Plug>(value).unwrap(), plug);
        assert_eq!(plug.nodule().unwrap().schema, "org.example.daybook.nodule");
        assert_eq!(plug.micros().count(), 1);
        assert_eq!(
            plug.artifact("routines").unwrap().media_type.as_deref(),
            Some("application/wasm")
        );
    }

    #[test]
    fn a_plug_id_is_lowercase_namespace_and_name() {
        assert_eq!(
            "@example/photos".parse::<PlugId>().unwrap().to_string(),
            "@example/photos"
        );
        for bad in [
            "example/photos",
            "@Example/photos",
            "@example/",
            "@/x",
            "@a b/c",
            "@a/b/c",
        ] {
            assert!(bad.parse::<PlugId>().is_err(), "{bad}");
        }
        assert!("1.2".parse::<Version>().is_err());
        assert!("nonsense".parse::<VersionReq>().is_err());
    }

    #[test]
    fn the_revision_is_the_content_hash_and_sees_unknown_members() {
        let plug = plug();
        let revision = plug.revision();
        assert_eq!(plug.clone().revision(), revision);
        let mut carried = plug.clone();
        carried.extra.insert("futureField".into(), json!(1));
        assert_ne!(carried.revision(), revision);
        let text = revision.to_string();
        assert_eq!(text.parse::<Revision>().unwrap(), revision);
        assert!("z".parse::<Revision>().is_err());
    }

    #[test]
    fn an_unknown_contribution_is_carried_not_refused() {
        let value = json!({
            "id": "@example/photos",
            "version": "0.1.0",
            "contributions": [
                { "$type": "org.example.daybook.plug.theme", "palette": ["#fff"] },
            ],
            "futureField": true,
        });
        let plug: Plug = serde_json::from_value(value.clone()).unwrap();
        plug.validate().unwrap();
        assert!(matches!(plug.contributions[0], Contribution::Unknown(_)));
        assert_eq!(plug.nodule(), None);
        assert_eq!(serde_json::to_value(&plug).unwrap(), value);
    }

    #[test]
    fn validation_owns_distribution_not_contribution_content() {
        let mut two_nodules = plug();
        two_nodules
            .contributions
            .push(two_nodules.contributions[0].clone());
        let error = two_nodules.validate().unwrap_err().to_string();
        assert!(error.contains("one node-facing nodule"), "{error}");

        let mut duplicate_micro = plug();
        duplicate_micro
            .contributions
            .push(duplicate_micro.contributions[1].clone());
        let error = duplicate_micro.validate().unwrap_err().to_string();
        assert!(error.contains("micro key `gallery`"), "{error}");

        let mut duplicate_artifact = plug();
        duplicate_artifact
            .artifacts
            .push(duplicate_artifact.artifacts[0].clone());
        let error = duplicate_artifact.validate().unwrap_err().to_string();
        assert!(error.contains("artifact key `routines`"), "{error}");

        let mut self_dependency = plug();
        self_dependency.dependencies[0].id = self_dependency.id.clone();
        let error = self_dependency.validate().unwrap_err().to_string();
        assert!(error.contains("depend on itself"), "{error}");

        let mut bad_schema = plug();
        let Contribution::Nodule(nodule) = &mut bad_schema.contributions[0] else {
            unreachable!()
        };
        nodule.schema = "not an nsid".into();
        let error = bad_schema.validate().unwrap_err().to_string();
        assert!(error.contains("contributions[0].schema"), "{error}");

        // The description is a consumer's business: nonsense passes the catalog.
        let mut nonsense_description = plug();
        let Contribution::Nodule(nodule) = &mut nonsense_description.contributions[0] else {
            unreachable!()
        };
        nodule.description = [("anything".to_owned(), json!(null))].into_iter().collect();
        nonsense_description.validate().unwrap();
    }

    #[cfg(feature = "schemars")]
    mod declaration {
        use super::*;
        use big_cabinet_core::datum::Bundle;
        use big_cabinet_core::datum::Declaration;
        use big_cabinet_core::datum::Universe;

        #[test]
        fn the_envelope_datums_declare_publish_and_admit() {
            let universe = Universe::collect([
                Declaration::of::<Plug>().unwrap(),
                Declaration::of::<NoduleContribution>().unwrap(),
                Declaration::of::<MicroContribution>().unwrap(),
            ]);
            let ids: Vec<&str> = universe
                .datums
                .iter()
                .map(|datum| datum.id.as_str())
                .collect();
            assert_eq!(
                ids,
                [
                    "org.example.daybook.plug",
                    "org.example.daybook.plug.nodule",
                    "org.example.daybook.plug.micro",
                ]
            );
            let bundle = Bundle::new(&universe, Vec::new());
            let lexicons = bundle.lexicons().unwrap();
            assert_eq!(lexicons.len(), 3);
            let plug = &lexicons[0].file;
            let contributions = &plug["defs"]["main"]["properties"]["contributions"]["items"];
            assert_eq!(contributions["type"], "union", "{plug}");
            assert_eq!(contributions.get("closed"), None, "{plug}");
            assert_eq!(
                contributions["refs"],
                json!([NoduleContribution::ID, MicroContribution::ID]),
                "{plug}"
            );
            // A description is `unknown` to the catalog's Lexicon: any data object.
            let nodule = &lexicons[1].file;
            assert_eq!(
                nodule["defs"]["main"]["properties"]["description"]["type"], "unknown",
                "{nodule}"
            );
            for (index, lexicon) in lexicons.iter().enumerate() {
                let carried: Vec<&serde_json::Value> = lexicons
                    .iter()
                    .enumerate()
                    .filter(|(other, _)| *other != index)
                    .map(|(_, other)| &other.file)
                    .collect();
                let compiled = bundle
                    .validate_lexicon(&lexicon.id, &lexicon.file, &carried)
                    .unwrap_or_else(|error| panic!("{}: {error}", lexicon.id));
                if lexicon.id == Plug::ID {
                    let mut value = serde_json::to_value(super::plug()).unwrap();
                    value["$type"] = json!(Plug::ID);
                    for size in [json!(null), json!(0), json!(4), json!(i64::MAX)] {
                        value["artifacts"][0]["size"] = size;
                        let stored =
                            big_cabinet_core::datum::Stored::check(&compiled, value.clone())
                                .unwrap()
                                .into_value();
                        assert_eq!(stored, value);
                        let envelope: Plug = serde_json::from_value(stored).unwrap();
                        envelope.validate().unwrap();
                        assert_eq!(
                            serde_json::to_value(envelope.artifacts[0].size).unwrap(),
                            value["artifacts"][0]["size"]
                        );
                    }
                    for size in [json!(-1), json!(0.5), json!(u64::MAX), json!("4")] {
                        value["artifacts"][0]["size"] = size;
                        assert!(
                            big_cabinet_core::datum::Stored::check(&compiled, value.clone())
                                .is_err()
                        );
                    }
                }
            }
        }
    }
}
