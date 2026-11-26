mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::ids::*;
    pub use structstruck::strike;

    pub use std::collections::HashMap as Map;
    pub use std::collections::HashSet as Set;
}

pub use ids::BuckId;
pub use ids::BuckLevel;
pub use ids::ByteKey;
pub use ids::ObjKey;
pub use ids::PartKey;
pub use ids::PeerKey;

#[cfg(any(test, feature = "test-support"))]
pub mod bench;
pub mod driver;
pub mod durable_outbox;
pub mod fingerprint;
mod ids;
pub mod kit;
pub mod long_poll;
pub mod okv;
pub mod part_store;
pub mod replay;
pub mod rev;
pub mod sessions;
#[cfg(test)]
mod scaling;
pub mod sync;

#[derive(Default, Debug, Clone, Copy)]
pub enum AllowedStrat {
    Cursor,
    #[default]
    CursorAndBucket,
}
