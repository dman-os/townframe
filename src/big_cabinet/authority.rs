//! Actor identities, capabilities, grants, and signing for Cabinet, exposed as a
//! `service_utils_rs` service.
//!
//! The public surface is [`AuthorityProtocol`] plus the [`boot`] function that
//! hosts it. Four parts stay independent inside the service so each can be lifted
//! out later without untangling the others (ADR 017): a capability table, an
//! actor registry, a grant ledger namespaced by issuing actor, and a signer that
//! signs as the actor whose own capability is presented. Actors name objects and
//! decide containment; nothing here interprets an object string.

mod actor;
mod cap;
mod protocol;
mod service;
mod signer;
mod store;

#[cfg(test)]
mod tests;

pub use protocol::{
    Actor, AttenuateCapability, AuthorityProtocol, AuthorityServiceError, CapId, Capability,
    CheckGrant, GrantAccess, InspectCapability, MintCapability, OpenActor, ResolveActor,
    RetractCapability, RevokeAccess, Rights, SignFacetClaim,
};
pub use service::{Authority, AuthorityHandler, AuthorityService, boot};

use big_cabinet_core::dmeta::{AttributionError, DmetaError};
use secrets_rs::SecretsError;
use thiserror::Error;

/// An Ed25519 public key in raw bytes. The wire spelling of an identity is
/// dmeta's `MultibaseKey` text; this alias is the internal storage form.
pub type ActorPublicKey = [u8; 32];

/// The keyring service holding the wrapping key, and the label every sealed
/// key is bound to.
const NODE_SERVICE: &str = "big_cabinet.authority";
const WRAPPING_KEY: &str = "wrapping";

/// The object string of the operator's root capability.
pub const OPERATOR_OBJECT: &str = "is operator";
/// Every way an authority operation or boot fails. Operations report the wire
/// form, [`AuthorityServiceError`]; boot reports this one, whose sources are
/// kept for the embedder.
#[derive(Debug, Error)]
pub enum AuthorityError {
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
    #[error("persisted authority state is corrupt: {0}")]
    StateCorrupt(String),
    #[error("authority storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("secret store failed: {0}")]
    Secrets(#[from] SecretsError),
    #[error("facet claim is invalid: {0}")]
    Claim(#[from] DmetaError),
    #[error("attribution certificate is invalid: {0}")]
    Attribution(#[from] AttributionError),
}

impl From<AuthorityError> for AuthorityServiceError {
    fn from(error: AuthorityError) -> Self {
        match error {
            AuthorityError::InvalidCapability => Self::InvalidCapability,
            AuthorityError::Unauthorized => Self::Unauthorized,
            AuthorityError::Escalation => Self::Escalation,
            AuthorityError::UnrecognizedActor => Self::UnrecognizedActor,
            AuthorityError::InvalidActorPath => Self::InvalidActorPath,
            AuthorityError::ActorNotFound => Self::ActorNotFound,
            AuthorityError::StateCorrupt(message) => Self::StateCorrupt(message),
            AuthorityError::Storage(error) => Self::Storage(error.to_string()),
            // A sealed key that does not open is the same fault as a corrupt row.
            AuthorityError::Secrets(error) => Self::StateCorrupt(error.to_string()),
            AuthorityError::Claim(error) => Self::Claim(error.to_string()),
            AuthorityError::Attribution(error) => Self::Claim(error.to_string()),
        }
    }
}
