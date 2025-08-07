#![allow(unused)]

mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub use crate::{Context, SharedContext};
    pub use async_trait::async_trait;

    #[cfg(test)]
    pub use crate::utils::testing::*;
    #[cfg(test)]
    pub use api_utils_rs::testing::*;
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

mod user;
mod utils;

fn start() {}

mod wit {
    wit_bindgen::generate!({
        path: "../btress_api/wit",
        world: "api",
        generate_all,
        async: true,
    });
    pub use exports::townframe::btress_api::user_create;
    pub use exports::townframe::btress_api::user_update;
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

fn play() -> Res<()> {
    use std::fmt::Write;

    let reg = api_utils_rs::gen::TypeReg::default();

    let features = vec![user::feature(&reg)];

    let mut out = String::new();
    let buf = &mut out;
    write!(
        buf,
        r#"
use super::*;   

"#
    )?;
    for feature in features {
        api_utils_rs::gen::handler_rust::feature_module(&reg, buf, &feature)?;
    }

    let mut out = String::new();
    let buf = &mut out;

    // writeln!(buf, "package townframe:btress-api;")?;
    // writeln!(buf)?;
    // writeln!(
    //     buf,
    //     "interface {name} {{",
    //     name = AsKebabCase(&endpoint.id[..])
    // )?;
    // {
    //     let mut buf = indenter::indented(buf).with_str("    ");
    //     component_wit::endpoint_interface(&reg, &mut buf, &endpoint)?;
    //     writeln!(buf)?;
    //     writeln!(buf, "type input = string;")?;
    //     writeln!(buf, "type output = string;")?;
    //     writeln!(buf, "call: func(inp: input) -> result<output, error>;")?;
    // }
    // writeln!(buf, "}}")?;
    //
    // std::fs::write("../btress_api/wit/user.wit", &out)?;
    // println!("{out}");
    Ok(())
}

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());
