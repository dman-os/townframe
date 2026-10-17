use super::{
    ActorPublicKey, AuthorityError, DrawerId, Signature, TOKEN_VERSION, TokenId, verify_signature,
};
use ed25519_dalek::VerifyingKey;
use serde::ser::SerializeStructVariant;
use serde::{Deserialize, Serialize};

/// The coarse, drawer-oriented grants understood by Cabinet.
///
/// `Grant` deliberately keeps its v1 token-wire representation separate from
/// the public `DrawerId` identity type. The old representation encoded each
/// drawer ID as a JSON byte array; using `ObjKey`'s derived human-readable
/// serde implementation here would instead emit a base58 string and silently
/// change signed payloads. `TokenWireGrant` therefore carries the exact raw
/// bytes on the wire while accepting variable-length real document IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Grant {
    ReadDrawer(DrawerId),
    WriteDrawer(DrawerId),
    ManageDrawer(DrawerId),
    CreateDocument(DrawerId),
    RemoveDocumentFromDrawer(DrawerId),
    AddChildDrawer(DrawerId),
    Delegate {
        drawer: DrawerId,
        grants: Vec<Grant>,
    },
}

#[derive(Deserialize)]
enum TokenWireGrant {
    ReadDrawer(Vec<u8>),
    WriteDrawer(Vec<u8>),
    ManageDrawer(Vec<u8>),
    CreateDocument(Vec<u8>),
    RemoveDocumentFromDrawer(Vec<u8>),
    AddChildDrawer(Vec<u8>),
    Delegate {
        drawer: Vec<u8>,
        grants: Vec<TokenWireGrant>,
    },
}

impl TokenWireGrant {
    fn into_grant(self) -> Grant {
        match self {
            Self::ReadDrawer(drawer) => Grant::ReadDrawer(DrawerId::new(drawer)),
            Self::WriteDrawer(drawer) => Grant::WriteDrawer(DrawerId::new(drawer)),
            Self::ManageDrawer(drawer) => Grant::ManageDrawer(DrawerId::new(drawer)),
            Self::CreateDocument(drawer) => Grant::CreateDocument(DrawerId::new(drawer)),
            Self::RemoveDocumentFromDrawer(drawer) => {
                Grant::RemoveDocumentFromDrawer(DrawerId::new(drawer))
            }
            Self::AddChildDrawer(drawer) => Grant::AddChildDrawer(DrawerId::new(drawer)),
            Self::Delegate { drawer, grants } => Grant::Delegate {
                drawer: DrawerId::new(drawer),
                grants: grants.into_iter().map(Self::into_grant).collect(),
            },
        }
    }
}

impl Serialize for Grant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::ReadDrawer(drawer) => {
                serializer.serialize_newtype_variant("Grant", 0, "ReadDrawer", drawer.as_bytes())
            }
            Self::WriteDrawer(drawer) => {
                serializer.serialize_newtype_variant("Grant", 1, "WriteDrawer", drawer.as_bytes())
            }
            Self::ManageDrawer(drawer) => {
                serializer.serialize_newtype_variant("Grant", 2, "ManageDrawer", drawer.as_bytes())
            }
            Self::CreateDocument(drawer) => serializer.serialize_newtype_variant(
                "Grant",
                3,
                "CreateDocument",
                drawer.as_bytes(),
            ),
            Self::RemoveDocumentFromDrawer(drawer) => serializer.serialize_newtype_variant(
                "Grant",
                4,
                "RemoveDocumentFromDrawer",
                drawer.as_bytes(),
            ),
            Self::AddChildDrawer(drawer) => serializer.serialize_newtype_variant(
                "Grant",
                5,
                "AddChildDrawer",
                drawer.as_bytes(),
            ),
            Self::Delegate { drawer, grants } => {
                let mut variant = serializer.serialize_struct_variant("Grant", 6, "Delegate", 2)?;
                variant.serialize_field("drawer", drawer.as_bytes())?;
                variant.serialize_field("grants", grants)?;
                variant.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Grant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        TokenWireGrant::deserialize(deserializer).map(TokenWireGrant::into_grant)
    }
}

impl Grant {
    pub(super) fn drawer(&self) -> &DrawerId {
        match self {
            Self::ReadDrawer(drawer)
            | Self::WriteDrawer(drawer)
            | Self::ManageDrawer(drawer)
            | Self::CreateDocument(drawer)
            | Self::RemoveDocumentFromDrawer(drawer)
            | Self::AddChildDrawer(drawer) => drawer,
            Self::Delegate { drawer, .. } => drawer,
        }
    }
}

#[derive(Serialize)]
struct UnsignedToken<'a> {
    version: u8,
    token_id: TokenId,
    subject: ActorPublicKey,
    grants: &'a [Grant],
    parent: Option<TokenId>,
    issuer: ActorPublicKey,
    root: bool,
}

/// A node-signed, serializable capability that can cross an address-space
/// boundary. Its signature covers every field except `signature` itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilityToken {
    pub version: u8,
    pub token_id: TokenId,
    pub subject: ActorPublicKey,
    pub grants: Vec<Grant>,
    pub parent: Option<TokenId>,
    pub issuer: ActorPublicKey,
    /// A signed root marker. AuthorityRepo additionally checks the persisted
    /// root token ID before permitting current root operations.
    pub root: bool,
    pub signature: Signature,
}

impl CapabilityToken {
    fn unsigned(&self) -> UnsignedToken<'_> {
        UnsignedToken {
            version: self.version,
            token_id: self.token_id,
            subject: self.subject,
            grants: &self.grants,
            parent: self.parent,
            issuer: self.issuer,
            root: self.root,
        }
    }

    pub(super) fn payload(&self) -> Result<Vec<u8>, AuthorityError> {
        serde_json::to_vec(&self.unsigned()).map_err(AuthorityError::Encoding)
    }

    /// Verify the portable node signature without consulting local revocation.
    /// This is the historical cryptographic check used for signed claims.
    pub fn verify(&self, node_public_key: &ActorPublicKey) -> Result<(), AuthorityError> {
        if self.version != TOKEN_VERSION || self.issuer != *node_public_key {
            return Err(AuthorityError::InvalidToken);
        }
        let key =
            VerifyingKey::from_bytes(node_public_key).map_err(|_| AuthorityError::InvalidToken)?;
        verify_signature(&key, &self.payload()?, &self.signature)
    }
}
