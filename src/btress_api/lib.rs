#![allow(unused)]

mod interlude {
    pub use utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};

    #[cfg(test)]
    pub use crate::utils::testing::*;
    #[cfg(test)]
    pub use utils_rs::testing::*;
}

use crate::interlude::*;
use utils_rs::api;

pub struct Context {
    config: Config,
    db: api::StdDb,
    kanidm: kanidm_client::KanidmClient,
    argon2: Arc<argon2::Argon2<'static>>,
}

pub type SharedContext = Arc<Context>;

#[derive(educe::Educe, Clone)]
#[educe(Deref, DerefMut)]
pub struct ServiceContext(pub SharedContext);

#[derive(educe::Educe, Clone)]
#[educe(Deref, DerefMut)]
pub struct SharedServiceContext(pub ServiceContext);

#[derive(Debug)]
pub struct Config {
    pub pass_salt_hash: Arc<argon2::password_hash::SaltString>,
}

mod user;
mod utils;

fn start() {}
