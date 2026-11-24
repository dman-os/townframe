mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::ids::*;
    pub use structstruck::strike;

    pub use std::collections::HashMap as Map;
    pub use std::collections::HashSet as Set;
}

pub use ids::ObjKey;
pub use ids::PartKey;
pub use ids::PeerKey;

mod fingerprint;
mod ids;
mod kit;
pub mod okv;
pub mod part_store;
pub mod replay;
pub mod rev;
mod sync;
mod utils;

use crate::interlude::*;

#[derive(Default, Debug, Clone, Copy)]
pub enum AllowedStrat {
    Cursor,
    #[default]
    CursorAndBucket,
}

// FIXME(port): becomes an injected clock with big_repo's `Clock` shape when the machine is
// ported; its instant is the web-safe `kit::scheduler::Instant`.
pub type Clock = Box<dyn Fn() -> kit::scheduler::Instant>;
