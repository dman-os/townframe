#![allow(unused)]

mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};
    pub use async_trait::async_trait;

    pub use crate::wit::wasi::clocks::wall_clock;
    pub use crate::wit::wasi::clocks::wall_clock::Datetime;

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

fn start() {}

mod wit {
    wit_bindgen::generate!({
        path: "../btress_api/wit",
        world: "api",
        generate_all,
        async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "townframe:btress-api/utils/errors-validation": api_utils_rs::validation_errs::ValidationErrors,
            "townframe:btress-api/user": crate::gen::user,
            "townframe:btress-api/user-create": crate::gen::user::user_create,
        }
    });
    use crate::interlude::utoipa;
    use crate::interlude::OffsetDateTime;
    use wasi::clocks::wall_clock::Datetime;

    impl From<OffsetDateTime> for Datetime {
        fn from(value: OffsetDateTime) -> Self {
            Self {
                seconds: todo!(),
                nanoseconds: todo!(),
            }
        }
    }
    impl utoipa::ToSchema for Datetime {}
    impl utoipa::PartialSchema for Datetime {
        fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
            <OffsetDateTime as utoipa::PartialSchema>::schema()
        }
    }
}
struct Comp;
impl wit::user_create::Guest for Comp {
    #[allow(async_fn_in_trait)]
    async fn call(
        inp: wit::user_create::Input,
    ) -> Result<wit::user_create::Output, wit::user_create::Error> {
        todo!()
    }
}

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());
