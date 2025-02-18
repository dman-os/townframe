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
        Commands::SeedZitadel {} => {
            let mut client_mgmt =
                zitadel::api::clients::ClientBuilder::new("http://localhost:8181")
                    .build_management_client()
                    .await
                    .unwrap_or_log();
            let proj = client_mgmt
                .add_project(zitadel::api::zitadel::management::v1::AddProjectRequest {
                    name: "townframe".into(),
                    ..Default::default()
                })
                .await
                .wrap_err("error creating project")?
                .into_inner();

            use zitadel::api::zitadel::app::v1::*;
            let app = client_mgmt
                .add_oidc_app(zitadel::api::zitadel::management::v1::AddOidcAppRequest {
                    name: "granary_web".into(),
                    project_id: proj.id.clone(),
                    dev_mode: true,
                    redirect_uris: vec!["http://localhost:3000/redirect/signin".into()],
                    app_type: OidcAppType::UserAgent.into(),
                    // PKCE auth according to
                    // https://github.com/zitadel/zitadel/blob/5bbb953ffbd2a20a333704217c7077a78f96dfe5/console/src/app/pages/projects/apps/authmethods.ts#L24
                    response_types: vec![OidcResponseType::Code.into()],
                    grant_types: vec![OidcGrantType::AuthorizationCode.into()],
                    auth_method_type: OidcAuthMethodType::None.into(),
                    ..Default::default()
                })
                .await
                .wrap_err("error creating app")?
                .into_inner();
            println!("{app:#?}");
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
    #[clap(visible_alias = "r")]
    SeedZitadel {},
}
