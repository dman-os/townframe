use crate::dmeta::Bytes;
use crate::dmeta::FacetClaim;
use crate::dmeta::MultibaseKey;
use crate::dmeta::SignedFacetClaim;
use crate::dmeta::Uuid7;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use service_core_rs::ContextError;
use service_core_rs::Operation;
use service_core_rs::XrpcOperation;
use service_core_rs::declare_service_protocol;

pub const OPERATOR_OBJECT: &str = "is operator";

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAuthority {
    pub capability: Capability,
    pub actor: Option<Actor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveAuthority {
    pub capability: CapId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveCapability {
    pub capability: CapId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CapabilityStatus {
    Live,
    Retracted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capability {
    pub capability_id: CapId,
    pub object: String,
    pub rights: Rights,
    pub issuer_id: Uuid7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Namespace {
    pub namespace_id: Uuid7,
    pub capability: CapId,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenActor {
    pub parent: CapId,
    pub path: String,
    pub rights: Rights,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveActor {
    pub capability: CapId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintCapability {
    pub issuer: CapId,
    pub parent: Option<CapId>,
    pub dependencies: Vec<CapId>,
    pub object: String,
    pub rights: Rights,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttenuateCapability {
    pub capability: CapId,
    pub rights: Rights,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectCapability {
    pub capability: CapId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetractCapability {
    pub via: CapId,
    pub capability: CapId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignFacetClaim {
    pub actor: CapId,
    pub claim: FacetClaim,
}

macro_rules! operation {
    ($request:ident => $output:ty, $kind:ident, $nsid:literal) => {
        impl Operation for $request {
            type Output = $output;
            type Error = AuthorityServiceError;
            const XRPC: XrpcOperation =
                XrpcOperation::$kind(concat!("org.example.daybook.authority.", $nsid))
                    .with_schemas(stringify!($output), stringify!(AuthorityServiceError));
        }
    };
}
operation!(OpenActor => Actor, procedure, "openActor");
operation!(ResolveActor => Actor, query, "resolveActor");
operation!(ResolveAuthority => ResolvedAuthority, query, "resolveAuthority");
operation!(MintCapability => Capability, procedure, "mintCapability");
operation!(AttenuateCapability => Capability, procedure, "attenuateCapability");
operation!(InspectCapability => Capability, query, "inspectCapability");
operation!(RetractCapability => (), procedure, "retractCapability");
operation!(SignFacetClaim => SignedFacetClaim, procedure, "signFacetClaim");

service_core_rs::service_operation!(ObserveCapability => stream CapabilityStatus, AuthorityServiceError, "org.example.daybook.authority.observeCapability");

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
            SignFacetClaim(SignFacetClaim)
        }
        stream { ObserveCapability(ObserveCapability) }
    }
}
