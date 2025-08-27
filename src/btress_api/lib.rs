#![allow(unused)]

mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};
    pub use async_trait::async_trait;

    // #[cfg(test)]
    // pub use crate::utils::testing::*;
    // #[cfg(test)]
    // pub use api_utils_rs::testing::*;
}

use crate::interlude::*;
use api_utils_rs::api;

pub struct Context {
    config: Config,
    db: api::StdDb,
    // kanidm: kanidm_client::KanidmClient,
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

mod gen;
mod user;
mod utils;

fn init() -> Res<()> {
    CX.set(Arc::new(Context {
        argon2: Arc::new(argon2::Argon2::default()),
        config: Config {
            pass_salt_hash: Arc::new(argon2::password_hash::SaltString::generate(
                &mut argon2::password_hash::rand_core::OsRng,
            )),
        },
        db: StdDb::PgWasi {},
    }))
    .map_err(|_| ferr!("double component intialization"))?;
    Ok(())
}

fn cx() -> SharedContext {
    crate::CX
        .get()
        .expect("component was not initialized")
        .clone()
}

pub const CX: tokio::sync::OnceCell<SharedContext> = tokio::sync::OnceCell::const_new();

mod wit {
    wit_bindgen::generate!({
        world: "api",
        // generate_all,
        async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,

            "townframe:api-utils/utils": api_utils_rs::wit::townframe::api_utils::utils,

            "townframe:btress-api/user": crate::gen::user,
            "townframe:btress-api/user-create": crate::gen::user::user_create,
        }
    });
}

// wit::export!(Component);

struct Component;

impl wit::exports::townframe::btress_api::ctx::Guest for Component {
    #[allow(async_fn_in_trait)]
    async fn init() -> Result<(), String> {
        crate::init().map_err(|err| format!("{err:?}"))?;
        Ok(())
    }
}
impl user::create::Guest for Component {
    type Handler = user::create::UserCreate;
}

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());
