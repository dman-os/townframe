#[allow(unused)]
mod interlude {
    pub use std::future::Future;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;

    pub use utils_rs::prelude::*;
}
mod gen;

use clap::builder::styling::AnsiColor;

use crate::interlude::*;

fn main() -> Res<()> {
    dotenv_flow::dotenv_flow().ok();
    utils_rs::setup_tracing()?;
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
        Commands::Gen {} => {
            gen::cli()?;
        }
        Commands::Play {} => {
            use ollama_rs::generation::completion::request::GenerationRequest;
            use ollama_rs::generation::images::Image;

            tokio::try_join!(
                async {
                    let generation_request =
                        GenerationRequest::new("gemma3".into(), "Extract the text.")
                        .images(
                            vec![Image::from_base64(data_encoding::BASE64.encode(
                                &tokio::fs::read("/tmp/photo_2025-12-25_22-56-09.jpg").await?,
                            ))],
                        );

                    let ollama = ollama_rs::Ollama::new("http://localhost", 11434);
                    let ollama_response = ollama.generate(generation_request).await?;
                    println!("{ollama_response:#?}");
                    eyre::Ok(())
                },
                async {
                    let generation_request =
                        GenerationRequest::new("gemma3".into(), "Extract the text.")
                        .images(
                            vec![Image::from_base64(data_encoding::BASE64.encode(
                                &tokio::fs::read("/tmp/photo_2025-12-26_00-05-33.jpg").await?,
                            ))],
                        );

                    let ollama = ollama_rs::Ollama::new("http://localhost", 11434);
                    let ollama_response = ollama.generate(generation_request).await?;
                    println!("{ollama_response:#?}");
                    eyre::Ok(())
                },
                async {
                    let generation_request =
                        GenerationRequest::new("gemma3".into(), "Extract the text.")
                        .images(
                            vec![Image::from_base64(data_encoding::BASE64.encode(
                                &tokio::fs::read("/tmp/photo_2025-12-26_17-22-49.jpg").await?,
                            ))],
                        );

                    let ollama = ollama_rs::Ollama::new("http://localhost", 11434);
                    let ollama_response = ollama.generate(generation_request).await?;
                    println!("{ollama_response:#?}");
                    eyre::Ok(())
                },
            )?;
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
    // SeedKanidm {},
    Play {},
    Gen {},
}
