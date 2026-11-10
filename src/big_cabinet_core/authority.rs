use crate::dmeta::Bytes;
use crate::dmeta::DatumClaim;
use crate::dmeta::MultibaseKey;
use crate::dmeta::SignedDatumClaim;
use crate::dmeta::Uuid7;
use method_core::ContextError;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use service_core_rs::HttpMetadata;
use service_core_rs::declare_service_protocol;
use service_core_rs::xrpc::bearer_header_value;
use service_core_rs::xrpc::bearer_token;

pub const OPERATOR_OBJECT: &str = "is operator";

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapId(uuid::Uuid);

impl CapId {
    pub fn new_random() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl std::fmt::Debug for CapId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CapId([redacted])")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct Rights(u32);

impl Rights {
    pub const USE: Self = Self(1);
    pub const NARROW: Self = Self(2);
    pub const MINT_CHILD: Self = Self(4);
    pub const DELEGATE: Self = Self(8);

    const NAMES: [(Self, &'static str); 4] = [
        (Self::USE, "use"),
        (Self::NARROW, "narrow"),
        (Self::MINT_CHILD, "mintChild"),
        (Self::DELEGATE, "delegate"),
    ];

    pub const fn all() -> Self {
        Self(Self::USE.0 | Self::NARROW.0 | Self::MINT_CHILD.0 | Self::DELEGATE.0)
    }

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn is_subset_of(self, other: Self) -> bool {
        other.contains(self)
    }

    fn named(name: &str) -> Option<Self> {
        Self::NAMES
            .iter()
            .find(|(_, right)| *right == name)
            .map(|(right, _)| *right)
    }

    pub fn bits_i64(self) -> i64 {
        i64::from(self.0)
    }

    pub fn from_bits(bits: i64) -> Option<Self> {
        u32::try_from(bits)
            .ok()
            .filter(|bits| bits & !Self::all().0 == 0)
            .map(Self)
    }
}

impl std::ops::BitOr for Rights {
    type Output = Self;
    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::fmt::Display for Rights {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (right, name) in Self::NAMES {
            if self.contains(right) {
                if !first {
                    formatter.write_str("+")?;
                }
                formatter.write_str(name)?;
                first = false;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for Rights {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl Serialize for Rights {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let names: Vec<&str> = Self::NAMES
            .iter()
            .filter(|(right, _)| self.contains(*right))
            .map(|(_, name)| *name)
            .collect();
        let mut seq = serializer.serialize_seq(Some(names.len()))?;
        for name in names {
            seq.serialize_element(name)?;
        }
        seq.end()
    }
}

/// A list of right names, as `Serialize` writes it.
#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Rights {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Rights".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Rights").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "array", "items": { "type": "string" } })
    }
}

impl<'de> Deserialize<'de> for Rights {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let names = Vec::<String>::deserialize(deserializer)?;
        let mut rights = Self::default();
        for name in names {
            let right = Self::named(&name).ok_or_else(|| {
                serde::de::Error::custom(format!("unknown capability right {name}"))
            })?;
            rights = rights | right;
        }
        Ok(rights)
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Authorization {
    pub authority: CapId,
    pub signer: CapId,
}

impl Authorization {
    pub const fn identity(capability: CapId) -> Self {
        Self {
            authority: capability,
            signer: capability,
        }
    }

    /// The wire text of a capability id.
    pub fn id_text(id: CapId) -> String {
        match serde_json::to_value(id).expect("a capability id serializes") {
            serde_json::Value::String(text) => text,
            other => unreachable!("a capability id serializes as a string, not {other}"),
        }
    }

    /// The caller's credential as an HTTP `Authorization: Bearer <authority>.<signer>` header
    /// value, as atproto XRPC presents one. The token is the authority and signer capability ids
    /// joined by a dot (`<uuid>.<uuid>`), which are token68-safe. Like any bearer token it is a
    /// secret: whoever holds both ids acts with that capability.
    pub fn header_value(&self) -> String {
        bearer_header_value(&format!(
            "{}.{}",
            Self::id_text(self.authority),
            Self::id_text(self.signer)
        ))
    }

    /// Parses a header value written by [`Self::header_value`]; `None` for any other scheme or a
    /// malformed token.
    pub fn from_header_value(value: &str) -> Option<Self> {
        let id = |text: &str| -> Option<CapId> {
            serde_json::from_value(serde_json::Value::String(text.to_owned())).ok()
        };
        let token = bearer_token(value)?;
        let (authority, signer) = token.split_once('.')?;
        Some(Self {
            authority: id(authority)?,
            signer: id(signer)?,
        })
    }

    /// The HTTP metadata of a request made by this caller: the credential the request context
    /// travels with, in the header the XRPC server parses it from.
    pub fn metadata(&self) -> HttpMetadata {
        HttpMetadata {
            authorization: Some(self.header_value()),
            idempotency_key: None,
        }
    }

    /// The caller's credential carried by a request. Absent or unparseable is
    /// [`ContextError::Unauthenticated`], which every service error carries through
    /// `From<ContextError>`.
    pub fn from_metadata(metadata: &HttpMetadata) -> Result<Self, ContextError> {
        metadata
            .authorization
            .as_deref()
            .and_then(Self::from_header_value)
            .ok_or(ContextError::Unauthenticated)
    }
}

#[cfg(test)]
mod bearer_tests {
    use super::*;

    #[test]
    fn a_bearer_header_round_trips_an_authorization_and_rejects_anything_else() {
        let authorization = Authorization {
            authority: CapId::new_random(),
            signer: CapId::new_random(),
        };
        let header = authorization.header_value();
        assert_eq!(
            Authorization::from_header_value(&header),
            Some(authorization)
        );
        assert_eq!(
            Authorization::from_header_value(&header.replacen("Bearer", "bearer", 1)),
            Some(authorization)
        );
        assert_eq!(
            Authorization::from_metadata(&authorization.metadata()),
            Ok(authorization)
        );
        assert_eq!(
            Authorization::from_metadata(&HttpMetadata::default()),
            Err(ContextError::Unauthenticated)
        );
        let token = header.strip_prefix("Bearer ").unwrap();
        for malformed in [
            token.to_owned(),
            format!("Basic {token}"),
            "Bearer ".to_owned(),
            "Bearer not-a-uuid.not-a-uuid".to_owned(),
            format!("Bearer {token}.{token}"),
            format!("Bearer {}", token.replace('.', ":")),
        ] {
            assert_eq!(
                Authorization::from_header_value(&malformed),
                None,
                "{malformed}"
            );
        }
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAuthority {
    pub capability: Capability,
    pub actor: Option<Actor>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveAuthority {
    pub capability: CapId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveCapability {
    pub capability: CapId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CapabilityStatus {
    Live,
    Retracted,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capability {
    pub capability_id: CapId,
    pub object: String,
    pub rights: Rights,
    pub issuer_id: Uuid7,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Namespace {
    pub namespace_id: Uuid7,
    pub capability: CapId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    pub actor_id: Uuid7,
    pub url: String,
    pub public_key: MultibaseKey,
    pub certificate: Bytes,
    pub parent_actor_id: Option<Uuid7>,
    pub capability: CapId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum AuthorityServiceError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("capability is unknown, retracted, or descends from a retracted capability")]
    InvalidCapability,
    #[error("capability lacks the right this operation needs")]
    Unauthorized,
    #[error("the request asks for more than the presented capability holds")]
    Escalation,
    #[error("AuthorityActors did not associate this capability with an actor")]
    UnrecognizedActor,
    #[error("actor path is not its parent's path plus one segment")]
    InvalidActorPath,
    #[error("actor is not registered")]
    ActorNotFound,
    #[error("authority state is corrupt: {0}")]
    StateCorrupt(String),
    #[error("authority storage failed: {0}")]
    Storage(String),
    #[error("claim is invalid or could not be signed: {0}")]
    Claim(String),
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenActor {
    pub parent: CapId,
    pub path: String,
    pub rights: Rights,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveActor {
    pub capability: CapId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintCapability {
    pub issuer: CapId,
    pub parent: Option<CapId>,
    pub dependencies: Vec<CapId>,
    pub object: String,
    pub rights: Rights,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttenuateCapability {
    pub capability: CapId,
    pub rights: Rights,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectCapability {
    pub capability: CapId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetractCapability {
    pub via: CapId,
    pub capability: CapId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignDatumClaim {
    pub actor: CapId,
    pub claim: DatumClaim,
}

method_core::methods! {
    error = AuthorityServiceError, nsid_prefix = "org.example.daybook.authority.";
    OpenActor => Actor, procedure, "openActor";
    ResolveActor => Actor, query, "resolveActor";
    ResolveAuthority => ResolvedAuthority, query, "resolveAuthority";
    MintCapability => Capability, procedure, "mintCapability";
    AttenuateCapability => Capability, procedure, "attenuateCapability";
    InspectCapability => Capability, query, "inspectCapability";
    RetractCapability => (), procedure, "retractCapability";
    SignDatumClaim => SignedDatumClaim, procedure, "signDatumClaim";
}

service_core_rs::service_method!(ObserveCapability => stream CapabilityStatus, AuthorityServiceError, "org.example.daybook.authority.observeCapability");

declare_service_protocol! {
    pub enum AuthorityProtocol => AuthorityMessage {
        unary {
            OpenActor(OpenActor),
            ResolveActor(ResolveActor),
            ResolveAuthority(ResolveAuthority),
            MintCapability(MintCapability),
            AttenuateCapability(AttenuateCapability),
            InspectCapability(InspectCapability),
            RetractCapability(RetractCapability),
            SignDatumClaim(SignDatumClaim)
        }
        stream { ObserveCapability(ObserveCapability) }
    }
}
