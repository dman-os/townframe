mod interlude {
    pub use utils_rs::prelude::*;
}

pub mod authority;

pub use authority::{
    ActorClaim, ActorPublicKey, AuthorityError, AuthorityRepo, CapabilityToken, DrawerId, Grant,
    Signature, TokenId,
};
