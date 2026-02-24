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
        Commands::EmbedSimilarityDemo {
            image,
            text,
            text_compare,
            image_compare,
        } => {
            let cache_dir = mltools::models::test_cache_dir();
            let observer = mltools::models::MobileDefaultObserver::new(|event| match event {
                mltools::models::MobileDefaultEvent::DownloadStarted { source, file } => {
                    eprintln!("[download:{source}] {file} started");
                }
                mltools::models::MobileDefaultEvent::DownloadProgress {
                    source,
                    file,
                    downloaded_bytes,
                    total_bytes,
                } => match total_bytes {
                    Some(total_bytes) if total_bytes > 0 => {
                        let percent = (downloaded_bytes as f64 / total_bytes as f64) * 100.0;
                        eprintln!(
                            "[download:{source}] {file} {:.1}% ({downloaded_bytes}/{total_bytes} bytes)",
                            percent
                        );
                    }
                    _ => {
                        eprintln!("[download:{source}] {file} {downloaded_bytes} bytes downloaded");
                    }
                },
                mltools::models::MobileDefaultEvent::DownloadCompleted { source, file } => {
                    eprintln!("[download:{source}] {file} completed");
                }
                mltools::models::MobileDefaultEvent::DownloadFailed {
                    source,
                    file,
                    message,
                } => {
                    eprintln!("[download:{source}] {file} failed: {message}");
                }
            });
            let config =
                mltools::models::mobile_default_with_observer(&cache_dir, Some(&observer)).await?;
            let ctx = mltools::Ctx::new(config).await;

            if let Some(text_compare) = text_compare {
                let text = text.ok_or_eyre(
                    "embed-similarity-demo requires --text when --text-compare is provided",
                )?;
                let text_embedding = mltools::embed_text(ctx.as_ref(), &text).await?;
                let text_embedding_again = mltools::embed_text(ctx.as_ref(), &text).await?;
                let text_self_similarity = sqlite_vec_cosine_similarity(
                    &text_embedding.vector,
                    &text_embedding_again.vector,
                )
                .await?;
                let text_self_similarity_direct =
                    direct_cosine_similarity(&text_embedding.vector, &text_embedding_again.vector)?;
                let text_compare_embedding =
                    mltools::embed_text(ctx.as_ref(), &text_compare).await?;
                let text_compare_embedding_again =
                    mltools::embed_text(ctx.as_ref(), &text_compare).await?;

                let similarity = sqlite_vec_cosine_similarity(
                    &text_embedding.vector,
                    &text_compare_embedding.vector,
                )
                .await?;
                let similarity_direct = direct_cosine_similarity(
                    &text_embedding.vector,
                    &text_compare_embedding.vector,
                )?;
                let text_compare_self_similarity = sqlite_vec_cosine_similarity(
                    &text_compare_embedding.vector,
                    &text_compare_embedding_again.vector,
                )
                .await?;
                let text_compare_self_similarity_direct = direct_cosine_similarity(
                    &text_compare_embedding.vector,
                    &text_compare_embedding_again.vector,
                )?;

                println!("text_a: {text}");
                println!("text_b: {text_compare}");
                println!(
                    "text_model_a: {} ({}d)",
                    text_embedding.model_id, text_embedding.dimensions
                );
                println!(
                    "text_model_b: {} ({}d)",
                    text_compare_embedding.model_id, text_compare_embedding.dimensions
                );
                println!("text_text_similarity: {:.6}", similarity);
                println!("text_text_similarity_direct: {:.6}", similarity_direct);
                println!("text_a_self_similarity: {:.6}", text_self_similarity);
                println!(
                    "text_a_self_similarity_direct: {:.6}",
                    text_self_similarity_direct
                );
                println!(
                    "text_b_self_similarity: {:.6}",
                    text_compare_self_similarity
                );
                println!(
                    "text_b_self_similarity_direct: {:.6}",
                    text_compare_self_similarity_direct
                );
                println!("summary: {}", similarity_summary(similarity));
            } else if let Some(image_compare) = image_compare {
                let image = image.ok_or_eyre(
                    "embed-similarity-demo requires --image when --image-compare is provided",
                )?;
                let image_a = mltools::embed_image(ctx.as_ref(), &image).await?;
                let image_a_again = mltools::embed_image(ctx.as_ref(), &image).await?;
                let image_b = mltools::embed_image(ctx.as_ref(), &image_compare).await?;
                let image_b_again = mltools::embed_image(ctx.as_ref(), &image_compare).await?;

                let image_image_similarity =
                    sqlite_vec_cosine_similarity(&image_a.vector, &image_b.vector).await?;
                let image_image_similarity_direct =
                    direct_cosine_similarity(&image_a.vector, &image_b.vector)?;
                let image_a_self_similarity =
                    sqlite_vec_cosine_similarity(&image_a.vector, &image_a_again.vector).await?;
                let image_a_self_similarity_direct =
                    direct_cosine_similarity(&image_a.vector, &image_a_again.vector)?;
                let image_b_self_similarity =
                    sqlite_vec_cosine_similarity(&image_b.vector, &image_b_again.vector).await?;
                let image_b_self_similarity_direct =
                    direct_cosine_similarity(&image_b.vector, &image_b_again.vector)?;

                println!("image_a: {}", image.display());
                println!("image_b: {}", image_compare.display());
                println!(
                    "image_model_a: {} ({}d)",
                    image_a.model_id, image_a.dimensions
                );
                println!(
                    "image_model_b: {} ({}d)",
                    image_b.model_id, image_b.dimensions
                );
                println!("image_image_similarity: {:.6}", image_image_similarity);
                println!(
                    "image_image_similarity_direct: {:.6}",
                    image_image_similarity_direct
                );
                println!("image_a_self_similarity: {:.6}", image_a_self_similarity);
                println!(
                    "image_a_self_similarity_direct: {:.6}",
                    image_a_self_similarity_direct
                );
                println!("image_b_self_similarity: {:.6}", image_b_self_similarity);
                println!(
                    "image_b_self_similarity_direct: {:.6}",
                    image_b_self_similarity_direct
                );
                println!("summary: {}", similarity_summary(image_image_similarity));
            } else {
                let text = text.ok_or_eyre(
                    "embed-similarity-demo requires --text unless --text-compare or --image-compare is provided",
                )?;
                let cross_modal_text = format!("search_query: {text}");
                let text_embedding = mltools::embed_text(ctx.as_ref(), &text).await?;
                let text_embedding_again = mltools::embed_text(ctx.as_ref(), &text).await?;
                let text_self_similarity = sqlite_vec_cosine_similarity(
                    &text_embedding.vector,
                    &text_embedding_again.vector,
                )
                .await?;
                let text_self_similarity_direct =
                    direct_cosine_similarity(&text_embedding.vector, &text_embedding_again.vector)?;
                let image = image.ok_or_eyre(
                    "embed-similarity-demo requires --image unless --text-compare is provided",
                )?;
                let image_embedding = mltools::embed_image(ctx.as_ref(), &image).await?;
                let image_embedding_again = mltools::embed_image(ctx.as_ref(), &image).await?;
                let (builtin_image_embedding, builtin_text_embedding) =
                    fastembed_builtin_nomic_cross_modal_embeddings(
                        &image,
                        &cross_modal_text,
                        cache_dir.join("fastembed-builtin"),
                    )
                    .await?;
                let (clip_image_embedding, clip_text_embedding) =
                    fastembed_builtin_clip_cross_modal_embeddings(
                        &image,
                        &text,
                        cache_dir.join("fastembed-builtin"),
                    )
                    .await?;
                let similarity = sqlite_vec_cosine_similarity(
                    &image_embedding.vector,
                    &mltools::embed_text(ctx.as_ref(), &cross_modal_text)
                        .await?
                        .vector,
                )
                .await?;
                let cross_modal_text_embedding =
                    mltools::embed_text(ctx.as_ref(), &cross_modal_text).await?;
                let similarity_direct = direct_cosine_similarity(
                    &image_embedding.vector,
                    &cross_modal_text_embedding.vector,
                )?;
                let image_self_similarity = sqlite_vec_cosine_similarity(
                    &image_embedding.vector,
                    &image_embedding_again.vector,
                )
                .await?;
                let image_self_similarity_direct = direct_cosine_similarity(
                    &image_embedding.vector,
                    &image_embedding_again.vector,
                )?;
                let builtin_similarity =
                    sqlite_vec_cosine_similarity(&builtin_image_embedding, &builtin_text_embedding)
                        .await?;
                let builtin_similarity_direct =
                    direct_cosine_similarity(&builtin_image_embedding, &builtin_text_embedding)?;
                let clip_similarity =
                    sqlite_vec_cosine_similarity(&clip_image_embedding, &clip_text_embedding)
                        .await?;
                let clip_similarity_direct =
                    direct_cosine_similarity(&clip_image_embedding, &clip_text_embedding)?;

                println!("image: {}", image.display());
                println!("text: {text}");
                println!("text_effective_for_cross_modal: {cross_modal_text}");
                println!(
                    "image_model: {} ({}d)",
                    image_embedding.model_id, image_embedding.dimensions
                );
                println!(
                    "text_model: {} ({}d)",
                    cross_modal_text_embedding.model_id, cross_modal_text_embedding.dimensions
                );
                println!("cosine_similarity: {:.6}", similarity);
                println!("cosine_similarity_direct: {:.6}", similarity_direct);
                println!(
                    "fastembed_builtin_cosine_similarity: {:.6}",
                    builtin_similarity
                );
                println!(
                    "fastembed_builtin_cosine_similarity_direct: {:.6}",
                    builtin_similarity_direct
                );
                println!(
                    "fastembed_builtin_clip_cosine_similarity: {:.6}",
                    clip_similarity
                );
                println!(
                    "fastembed_builtin_clip_cosine_similarity_direct: {:.6}",
                    clip_similarity_direct
                );
                println!("image_self_similarity: {:.6}", image_self_similarity);
                println!(
                    "image_self_similarity_direct: {:.6}",
                    image_self_similarity_direct
                );
                println!("text_self_similarity: {:.6}", text_self_similarity);
                println!(
                    "text_self_similarity_direct: {:.6}",
                    text_self_similarity_direct
                );
                println!("summary: {}", similarity_summary(similarity));
            }
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
    }

    Ok(())
}

fn init_sqlite_vec() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| unsafe {
        let entry_point: unsafe extern "C" fn(
            *mut libsqlite3_sys::sqlite3,
            *mut *mut std::ffi::c_char,
            *const libsqlite3_sys::sqlite3_api_routines,
        ) -> i32 = std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ());
        libsqlite3_sys::sqlite3_auto_extension(Some(entry_point));
    });
}

async fn sqlite_vec_cosine_similarity(image_vector: &[f32], text_vector: &[f32]) -> Res<f32> {
    if image_vector.is_empty() || text_vector.is_empty() {
        eyre::bail!("cannot compare empty embeddings");
    }
    if image_vector.len() != text_vector.len() {
        eyre::bail!(
            "embedding dimension mismatch: image={} text={}",
            image_vector.len(),
            text_vector.len()
        );
    }

    init_sqlite_vec();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .wrap_err("error opening sqlite memory db for sqlite-vec similarity")?;

    let _: String = sqlx::query_scalar("select vec_version()")
        .fetch_one(&pool)
        .await
        .wrap_err("sqlite-vec extension not available")?;

    let image_vector_json = serde_json::to_string(image_vector)
        .wrap_err("error serializing image embedding to json")?;
    let text_vector_json =
        serde_json::to_string(text_vector).wrap_err("error serializing text embedding to json")?;

    let cosine_distance: f64 = sqlx::query_scalar(
        "SELECT vec_distance_cosine(vec_f32(?1), vec_f32(?2)) AS cosine_distance",
    )
    .bind(image_vector_json)
    .bind(text_vector_json)
    .fetch_one(&pool)
    .await
    .wrap_err("error computing cosine distance with sqlite-vec")?;

    let similarity = 1.0f64 - cosine_distance;
    Ok(similarity as f32)
}

async fn fastembed_builtin_nomic_cross_modal_embeddings(
    image: &Path,
    text_with_prefix: &str,
    cache_dir: PathBuf,
) -> Res<(Vec<f32>, Vec<f32>)> {
    let image_path = image.to_path_buf();
    let text_with_prefix = text_with_prefix.to_string();

    tokio::task::spawn_blocking(move || -> Res<(Vec<f32>, Vec<f32>)> {
        use fastembed::{
            EmbeddingModel, ImageEmbedding, ImageEmbeddingModel, ImageInitOptions, TextEmbedding,
            TextInitOptions,
        };

        let mut image_embedder = ImageEmbedding::try_new(
            ImageInitOptions::new(ImageEmbeddingModel::NomicEmbedVisionV15)
                .with_cache_dir(cache_dir.clone())
                .with_show_download_progress(true),
        )
        .map_err(|err| eyre::eyre!("failed to initialize built-in vision model: {err}"))?;
        let mut text_embedder = TextEmbedding::try_new(
            TextInitOptions::new(EmbeddingModel::NomicEmbedTextV15Q)
                .with_cache_dir(cache_dir)
                .with_show_download_progress(true),
        )
        .map_err(|err| eyre::eyre!("failed to initialize built-in text model: {err}"))?;

        let mut image_vectors = image_embedder
            .embed(vec![image_path], None)
            .map_err(|err| eyre::eyre!("fastembed built-in image embed failed: {err}"))?;
        let mut text_vectors = text_embedder
            .embed(vec![text_with_prefix], None)
            .map_err(|err| eyre::eyre!("fastembed built-in text embed failed: {err}"))?;

        let image_vector = image_vectors
            .pop()
            .ok_or_eyre("fastembed built-in image embed returned no vector")?;
        let text_vector = text_vectors
            .pop()
            .ok_or_eyre("fastembed built-in text embed returned no vector")?;
        Ok((image_vector, text_vector))
    })
    .await
    .wrap_err("fastembed built-in cross-modal embed task failed to join")?
}

async fn fastembed_builtin_clip_cross_modal_embeddings(
    image: &Path,
    text: &str,
    cache_dir: PathBuf,
) -> Res<(Vec<f32>, Vec<f32>)> {
    let image_path = image.to_path_buf();
    let text = text.to_string();

    tokio::task::spawn_blocking(move || -> Res<(Vec<f32>, Vec<f32>)> {
        use fastembed::{
            EmbeddingModel, ImageEmbedding, ImageEmbeddingModel, ImageInitOptions, TextEmbedding,
            TextInitOptions,
        };

        let mut image_embedder = ImageEmbedding::try_new(
            ImageInitOptions::new(ImageEmbeddingModel::ClipVitB32)
                .with_cache_dir(cache_dir.clone())
                .with_show_download_progress(true),
        )
        .map_err(|err| eyre::eyre!("failed to initialize built-in CLIP vision model: {err}"))?;
        let mut text_embedder = TextEmbedding::try_new(
            TextInitOptions::new(EmbeddingModel::ClipVitB32)
                .with_cache_dir(cache_dir)
                .with_show_download_progress(true),
        )
        .map_err(|err| eyre::eyre!("failed to initialize built-in CLIP text model: {err}"))?;

        let mut image_vectors = image_embedder
            .embed(vec![image_path], None)
            .map_err(|err| eyre::eyre!("fastembed built-in CLIP image embed failed: {err}"))?;
        let mut text_vectors = text_embedder
            .embed(vec![text], None)
            .map_err(|err| eyre::eyre!("fastembed built-in CLIP text embed failed: {err}"))?;

        let image_vector = image_vectors
            .pop()
            .ok_or_eyre("fastembed built-in CLIP image embed returned no vector")?;
        let text_vector = text_vectors
            .pop()
            .ok_or_eyre("fastembed built-in CLIP text embed returned no vector")?;
        Ok((image_vector, text_vector))
    })
    .await
    .wrap_err("fastembed built-in CLIP cross-modal embed task failed to join")?
}

fn direct_cosine_similarity(image_vector: &[f32], text_vector: &[f32]) -> Res<f32> {
    if image_vector.is_empty() || text_vector.is_empty() {
        eyre::bail!("cannot compare empty embeddings");
    }
    if image_vector.len() != text_vector.len() {
        eyre::bail!(
            "embedding dimension mismatch: image={} text={}",
            image_vector.len(),
            text_vector.len()
        );
    }

    let mut dot = 0.0f64;
    let mut image_norm = 0.0f64;
    let mut text_norm = 0.0f64;
    for (image_value, text_value) in image_vector.iter().zip(text_vector) {
        let image_value = *image_value as f64;
        let text_value = *text_value as f64;
        dot += image_value * text_value;
        image_norm += image_value * image_value;
        text_norm += text_value * text_value;
    }

    let denom = image_norm.sqrt() * text_norm.sqrt();
    if denom == 0.0 {
        eyre::bail!("cannot compare zero-norm embeddings");
    }

    Ok((dot / denom) as f32)
}

fn similarity_summary(score: f32) -> &'static str {
    match score {
        value if value >= 0.70 => "very strong match (label likely describes the image well)",
        value if value >= 0.45 => "moderate match (label is plausibly related)",
        value if value >= 0.20 => "weak match (some overlap, but not a close description)",
        value if value >= 0.0 => "very weak match (mostly unrelated)",
        _ => "negative similarity (label likely mismatches the image)",
    }
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
    EmbedSimilarityDemo {
        #[arg(long)]
        image: Option<PathBuf>,
        #[arg(long)]
        text: Option<String>,
        #[arg(long)]
        text_compare: Option<String>,
        #[arg(long)]
        image_compare: Option<PathBuf>,
    },
    Play {},
    Gen {},
}
