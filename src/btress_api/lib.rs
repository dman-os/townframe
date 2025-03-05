#![allow(unused)]

mod interlude {
    pub use utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};

    #[cfg(test)]
    pub use utils_rs::testing::*;
}

use crate::interlude::*;
use utils_rs::api;

pub struct Context {
    config: Config,
    db: api::StdDb,
}
pub type SharedContext = Arc<Context>;

#[derive(Debug)]
pub struct Config {}

mod user;

fn start() {}
