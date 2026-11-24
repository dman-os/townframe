mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::ids::*;
    pub use structstruck::strike;

    pub use std::collections::HashMap as Map;
    pub use std::collections::HashSet as Set;
}

pub use ids::BuckId;
pub use ids::BuckLevel;
pub use ids::ObjKey;
pub use ids::PartKey;
pub use ids::PeerKey;

pub mod driver;
pub mod fingerprint;
mod ids;
mod kit;
pub mod okv;
pub mod part_store;
pub mod replay;
pub mod rev;
pub mod sync;
mod utils;

use crate::interlude::*;

#[derive(Default, Debug, Clone, Copy)]
pub enum AllowedStrat {
    Cursor,
    #[default]
    CursorAndBucket,
}

/// The driver's injected clock; the machine never reads time itself. Its instant is the
/// web-safe `kit::scheduler::Instant`.
pub type Clock = std::sync::Arc<dyn Fn() -> kit::scheduler::Instant + Send + Sync>;
