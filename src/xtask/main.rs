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
                        GenerationRequest::new("gemma3".into(), "Extract the text.").images(vec![
                            Image::from_base64(data_encoding::BASE64.encode(
                                &tokio::fs::read("/tmp/photo_2025-12-25_22-56-09.jpg").await?,
                            )),
                        ]);

                    let ollama = ollama_rs::Ollama::new("http://localhost", 11434);
                    let ollama_response = ollama.generate(generation_request).await?;
                    println!("{ollama_response:#?}");
                    eyre::Ok(())
                },
                async {
                    let generation_request =
                        GenerationRequest::new("gemma3".into(), "Extract the text.").images(vec![
                            Image::from_base64(data_encoding::BASE64.encode(
                                &tokio::fs::read("/tmp/photo_2025-12-26_00-05-33.jpg").await?,
                            )),
                        ]);

                    let ollama = ollama_rs::Ollama::new("http://localhost", 11434);
                    let ollama_response = ollama.generate(generation_request).await?;
                    println!("{ollama_response:#?}");
                    eyre::Ok(())
                },
                async {
                    let generation_request =
                        GenerationRequest::new("gemma3".into(), "Extract the text.").images(vec![
                            Image::from_base64(data_encoding::BASE64.encode(
                                &tokio::fs::read("/tmp/photo_2025-12-26_17-22-49.jpg").await?,
                            )),
                        ]);

                    let ollama = ollama_rs::Ollama::new("http://localhost", 11434);
                    let ollama_response = ollama.generate(generation_request).await?;
                    println!("{ollama_response:#?}");
                    eyre::Ok(())
                },
            )?;
        }
        Commands::OcrDemo {
            image,
            det_model,
            rec_model,
            dict_path,
        } => {
            use oar_ocr::oarocr::OAROCRBuilder;
            use oar_ocr::utils::load_image;

            if !image.exists() {
                eyre::bail!("Image path does not exist: {}", image.display());
            }
            if !det_model.exists() {
                eyre::bail!(
                    "Detection model path does not exist: {}",
                    det_model.display()
                );
            }
            if !rec_model.exists() {
                eyre::bail!(
                    "Recognition model path does not exist: {}",
                    rec_model.display()
                );
            }
            if !dict_path.exists() {
                eyre::bail!("Dictionary path does not exist: {}", dict_path.display());
            }

            let ocr = OAROCRBuilder::new(&det_model, &rec_model, &dict_path).build()?;
            let image_data = load_image(&image)?;
            let results = ocr.predict(vec![image_data])?;

            for result in results {
                println!("{result}");
                println!("----");
                println!("Text:\n{}", result.concatenated_text("\n"));
            }
        }
        Commands::EmbedDemo { text, batch_size } => {
            use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};

            let input_texts = if text.is_empty() {
                vec![
                    "query: local embedding stack demo".to_string(),
                    "passage: townframe uses local models for OCR and embeddings.".to_string(),
                    "passage: fastembed-rs runs inference through ONNX Runtime.".to_string(),
                ]
            } else {
                text
            };

            let init_options = TextInitOptions::new(EmbeddingModel::NomicEmbedTextV15Q)
                .with_show_download_progress(true);
            let mut embedding_model = TextEmbedding::try_new(init_options)
                .map_err(|err| eyre::eyre!("failed to initialize fastembed model: {err}"))?;
            let embeddings = embedding_model
                .embed(input_texts.clone(), batch_size)
                .map_err(|err| eyre::eyre!("failed to embed text: {err}"))?;

            println!("Embedded {} item(s)", embeddings.len());
            for (index, (input_text, embedding)) in
                input_texts.iter().zip(embeddings.iter()).enumerate()
            {
                println!("item[{index}] text: {input_text}");
                println!("item[{index}] dim: {}", embedding.len());
                let preview_dimensions = embedding.iter().take(8).collect::<Vec<_>>();
                println!("item[{index}] first 8 dims: {preview_dimensions:?}");
                println!("----");
            }
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
    OcrDemo {
        #[clap(long)]
        image: PathBuf,
        #[clap(long, default_value = "target/models/detection/v5/det.onnx")]
        det_model: PathBuf,
        #[clap(long, default_value = "target/models/languages/latin/rec.onnx")]
        rec_model: PathBuf,
        #[clap(long, default_value = "target/models/languages/latin/dict.txt")]
        dict_path: PathBuf,
    },
    EmbedDemo {
        #[clap(long = "text")]
        text: Vec<String>,
        #[clap(long)]
        batch_size: Option<usize>,
    },
}
