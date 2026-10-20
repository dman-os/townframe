//! What the authority service says on the wire: its request types, outputs,
//! error vocabulary, and the protocol that carries them.
//!
//! This module is the whole public surface of authority. A consumer holds a
//! `Client<AuthorityProtocol>` and cannot reach the state directly, whether the
//! service runs in-process, in another process on the node, or over HTTP.

use big_cabinet_core::dmeta::{Bytes, FacetClaim, MultibaseKey, SignedFacetClaim, Uuid7};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use service_utils_rs::{ContextError, Operation, XrpcOperation, service_protocol};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapId(uuid::Uuid);

impl CapId {
    pub(super) fn new_random() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub(super) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    pub(super) fn as_bytes(&self) -> &[u8; 16] {
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

    pub(super) fn bits_i64(self) -> i64 {
        i64::from(self.0)
    }

    /// Rights read back from storage. Bits outside the vocabulary are a corrupt
    /// row, not a right this build forgot.
    pub(super) fn from_bits(bits: i64) -> Option<Self> {
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

/// One capability as the service reports it. `object` is opaque here: only the
/// actor that named it knows what it means.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capability {
    pub id: CapId,
    pub object: String,
    pub rights: Rights,
    pub issuer: Uuid7,
}

/// An actor binding, carrying exactly what dmeta's `ActorMetadata` needs plus
/// the actor capability that was presented for it or opened with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    pub uuid: Uuid7,
    pub url: String,
    pub public_key: MultibaseKey,
    /// Node-signed attribution certificate (ADR 015 §5) for this binding.
    pub certificate: Bytes,
    /// The actor that opened this one. The operator has none.
    pub parent_actor: Option<Uuid7>,
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

macro_rules! operation {
    ($request:ident => $output:ty, $kind:ident, $nsid:literal, $schema:literal) => {
        impl Operation for $request {
            type Output = $output;
            type Error = AuthorityServiceError;
            const XRPC: XrpcOperation =
                XrpcOperation::$kind(concat!("org.example.daybook.authority.", $nsid))
                    .with_schemas($schema, "AuthorityServiceError");
        }
    };
}

/// Open the child actor at `path`, creating it on first use. `parent` must be
/// the parent actor's own capability with `mintChild`, and `path` must be the
/// parent's path plus one segment, so the path alone names the parent and two
/// parents never contend for one child.
///
/// The actor (UUID, key, URL, certificate) is created once and is the same on
/// every later open. Its capability is a handle: an open reuses a live actor
/// capability with the same parent and rights, and mints a new one otherwise,
/// so a parent rotates a child's capability by retracting it and opening again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenActor {
    pub parent: CapId,
    pub path: String,
    pub rights: Rights,
}

operation!(OpenActor => Actor, procedure, "openActor", "Actor");

/// Which actor is this actor capability? Needs `use`: presenting an actor's
/// capability is how a caller identifies as it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveActor {
    pub capability: CapId,
}

operation!(ResolveActor => Actor, query, "resolveActor", "Actor");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintCapability {
    pub issuer: CapId,
    pub object: String,
    pub rights: Rights,
}

operation!(MintCapability => Capability, procedure, "mintCapability", "Capability");

/// Derive an object capability with fewer rights and the same object and
/// issuer. Needs `narrow`. A holder can hand on less than it holds, never
/// something else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttenuateCapability {
    pub capability: CapId,
    pub rights: Rights,
}

operation!(AttenuateCapability => Capability, procedure, "attenuateCapability", "Capability");

/// Is this capability live, and what does it say? Holding the id is enough.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectCapability {
    pub capability: CapId,
}

operation!(InspectCapability => Capability, query, "inspectCapability", "Capability");

/// Retract a capability and, with it, everything derived from it. `via` must
/// be the capability itself or one of its ancestors. The operator root is not
/// retractable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetractCapability {
    pub via: CapId,
    pub capability: CapId,
}

operation!(RetractCapability => (), procedure, "retractCapability", "Void");

/// Record that `subject` may hold `rights` over `object` in the namespace of
/// the actor that names it. Needs `delegate` on `via`:
///
/// - through the issuer's own actor capability, any object it may name, with
///   rights inside its capability's object rights; the grant stands until the
///   issuer revokes it;
/// - through an object capability, exactly that capability's object, with
///   rights inside its rights; the grant stands while `via` stays live, so
///   retracting the capability a grant was written through withdraws it.
///
/// Granting again through the same `via` replaces the rights it recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantAccess {
    pub via: CapId,
    pub subject: Uuid7,
    pub object: String,
    pub rights: Rights,
}

operation!(GrantAccess => (), procedure, "grantAccess", "Void");

/// Withdraw grants of `object` to `subject`. Needs `delegate` on `via`. The
/// issuer's actor capability withdraws every such grant in its namespace; an
/// object capability withdraws those written through it or through
/// capabilities derived from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeAccess {
    pub via: CapId,
    pub subject: Uuid7,
    pub object: String,
}

operation!(RevokeAccess => (), procedure, "revokeAccess", "Void");

/// The rights `subject` holds over `object` in the issuer's namespace: the
/// union of its standing grants, exact object match only. `via` must be the
/// issuer's own actor capability with `use`, because only the issuer
/// interprets its objects and decides what to mint from the answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckGrant {
    pub via: CapId,
    pub subject: Uuid7,
    pub object: String,
}

operation!(CheckGrant => Rights, query, "checkGrant", "Rights");

/// Sign a dmeta v2 facet claim as the actor `actor` is the capability of. Needs
/// the actor's own capability with `use`, and the claim's `actorUuid` must be
/// that actor. Which drawer authorizes the claim's document is DocsRepo's
/// check, made before this call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignFacetClaim {
    pub actor: CapId,
    pub claim: FacetClaim,
}

operation!(SignFacetClaim => SignedFacetClaim, procedure, "signFacetClaim", "SignedFacetClaim");

service_protocol! {
    pub enum AuthorityProtocol => AuthorityMessage {
        unary {
            OpenActor(OpenActor),
            ResolveActor(ResolveActor),
            MintCapability(MintCapability),
            AttenuateCapability(AttenuateCapability),
            InspectCapability(InspectCapability),
            RetractCapability(RetractCapability),
            GrantAccess(GrantAccess),
            RevokeAccess(RevokeAccess),
            CheckGrant(CheckGrant),
            SignFacetClaim(SignFacetClaim)
        }
        stream {}
    }
}
