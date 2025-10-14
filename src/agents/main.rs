#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;
    pub use restate_sdk::prelude::*;

    pub(crate) use crate::{Ctx, SharedCtx};
    pub use autosurgeon::{Hydrate, Reconcile};

    pub use std::str::FromStr;
}

use crate::interlude::*;

mod docs;
mod gen;

use crate::docs::DocsPipeline;

struct Ctx {
    acx: utils_rs::am::AmCtx,
    llm_provider: Box<dyn llm::LLMProvider>,
}

struct Config {
    ollama_url: String,
    ollama_model: String,
}

impl Ctx {
    async fn init(config: Config) -> Res<SharedCtx> {
        let acx = utils_rs::am::AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "daybook_agents".to_string(),
                storage_dir: "/tmp/samod-sync-agents".into(),
            },
            Option::<samod::AlwaysAnnounce>::None,
        )
        .await?;
        let llm_provider = llm::builder::LLMBuilder::new()
            .backend(llm::builder::LLMBackend::Ollama)
            .base_url(config.ollama_url)
            .model(config.ollama_model)
            .build()
            .wrap_err("error building llm provider")?;
        let cx = Arc::new(Self { acx, llm_provider });

        Ok(cx)
    }
}
type SharedCtx = Arc<Ctx>;

fn main() -> Res<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(app_main())
}

async fn app_main() -> Res<()> {
    utils_rs::setup_tracing()?;

    let config = Config {
        ollama_url: "http://127.0.0.1:1143".into(),
        ollama_model: "gemma3".into(),
    };
    let cx = Ctx::init(config).await?;

    let addr = std::net::SocketAddr::from((
        std::net::Ipv4Addr::UNSPECIFIED,
        utils_rs::get_env_var("PORT")
            .and_then(|str| {
                str.parse()
                    .map_err(|err| ferr!("error parsing port env var ({str}): {err}"))
            })
            .unwrap_or(9090),
    ));

    HttpServer::new(
        restate_sdk::endpoint::Endpoint::builder()
            .bind(docs::DocPipelineImpl { cx: cx.clone() }.serve())
            .build(),
    )
    .listen_and_serve(addr)
    .await;

    Ok(())
}
