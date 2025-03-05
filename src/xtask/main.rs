#[allow(unused)]
mod interlude {
    pub use std::future::Future;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;

    pub use color_eyre::eyre;
    pub use eyre::{format_err as ferr, Context, Result as Res, WrapErr};
    pub use tracing::{debug, error, info, trace, warn};
    pub use tracing_unwrap::*;
}
use clap::builder::styling::AnsiColor;

use crate::interlude::*;

mod utils;

fn main() -> Res<()> {
    dotenv_flow::dotenv_flow().ok();
    utils::setup_tracing()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(main_main())
}

async fn main_main() -> Res<()> {
    let _cwd = std::env::current_dir()?;

    use clap::Parser;
    let args = Args::parse();
    match args.command {
        Commands::Play {} => {
            #[derive(Debug, serde::Serialize, serde::Deserialize)]
            pub struct PrimitivesPartial {
                pub my_prim: Option<String>,
            }
            #[derive(Debug, serde::Serialize, serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Branch2Partial {
                pub branch2: Option<String>,
            }
            #[derive(Debug, serde::Serialize, serde::Deserialize)]
            #[serde(untagged)]
            pub enum CompositesEitherEither {
                PrimitivesPartial(PrimitivesPartial),
                Branch2Partial(Branch2Partial),
            }
            let value: CompositesEitherEither = serde_json::from_str(
                r#"
{
    "branch2": "bytes"
}
            "#,
            )
            .unwrap();
            println!("{value:?}");
        }
        Commands::SeedKanidm {} => {
            let client = kanidm_client::KanidmClientBuilder::new()
                .address("https://localhost:8443".into())
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .map_err(|err| ferr!("{err:?}"))?;
            {
                let pass = std::env::var("KANIDM_ADMIN_PASSWORD").expect(
                    "env KANIDM_ADMIN_PASSWORD required, make sure to run ghjk x kanidm-recover",
                );
                client
                    .auth_simple_password("idm_admin", &pass)
                    .await
                    .map_err(|err| ferr!("{err:?}"))?;
            }
            let tframe_admin = "tframe_admin";
            let tframe_group = "tframe_users";

            client
                .idm_service_account_create(tframe_admin, tframe_admin, "idm_admin")
                .await
                .map_err(|err| ferr!("{err:?}"))?;
            client
                .idm_group_create(tframe_group, Some(tframe_admin))
                .await
                .map_err(|err| ferr!("{err:?}"))?;
            client
                .idm_group_add_members(tframe_group, &[tframe_admin])
                .await
                .map_err(|err| ferr!("{err:?}"))?;
            {
                let oauth_name = "granary";
                client
                    .idm_oauth2_rs_public_create(
                        oauth_name,
                        oauth_name,
                        "http://localhost:3000/redirect/signin",
                    )
                    .await
                    .map_err(|err| ferr!("{err:?}"))?;
                client
                    .idm_oauth2_rs_enable_pkce(oauth_name)
                    .await
                    .map_err(|err| ferr!("{err:?}"))?;
                client
                    .idm_oauth2_rs_update_scope_map(
                        oauth_name,
                        tframe_group,
                        vec!["openid", "profile", "email", "groups"],
                    )
                    .await
                    .map_err(|err| ferr!("{err:?}"))?;
                client
                    .idm_oauth2_rs_enable_public_localhost_redirect(oauth_name)
                    .await
                    .map_err(|err| ferr!("{err:?}"))?;
            }
            //
        }
    }

    Ok(())
}

const CLAP_STYLE: clap::builder::Styles = clap::builder::Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Debug, clap::Parser)]
#[clap(
    version,
    about,
    styles = CLAP_STYLE
)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    // #[clap(visible_alias = "r")]
    // SeedZitadel {},
    // SeedZitadel {},
    SeedKanidm {},
    Play {},
}
