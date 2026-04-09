#[allow(unused)]
mod interlude {
    pub use std::future::Future;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;

    pub use utils_rs::prelude::*;
}

mod gen;

use clap::builder::styling::AnsiColor;
use irpc::{channel::oneshot, rpc_requests, Client, WithChannels};
use irpc_iroh::IrohProtocol;
use serde::{Deserialize, Serialize};

use crate::interlude::*;

const IRPC_PROBE_ALPN: &[u8] = b"townframe/xtask/irpc-probe/0";

#[rpc_requests(message = ProbeMessage)]
#[derive(Debug, Serialize, Deserialize)]
enum ProbeProtocol {
    #[rpc(tx=oneshot::Sender<u64>)]
    #[wrap(PingReq, derive(Clone))]
    Ping(u64),
}

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
        Commands::IrpcDisconnectProbe {} => {
            irpc_disconnect_probe().await?;
        }
        Commands::IrpcInflightDisconnectProbe {} => {
            irpc_inflight_disconnect_probe().await?;
        }
        Commands::IrpcClientCancelProbe {} => {
            irpc_client_cancel_probe().await?;
        }
        Commands::Automerge08MinimalRepro {} => {
            automerge08_minimal_repro()?;
        }
        Commands::BuildPlugOci {
            plug_root,
            out_root,
        } => {
            build_plug_oci(plug_root, out_root).await?;
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
                let image_mime = |path: &std::path::Path| -> Res<&'static str> {
                    match path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| ext.to_ascii_lowercase())
                        .as_deref()
                    {
                        Some("jpg" | "jpeg") => Ok("image/jpeg"),
                        Some("png") => Ok("image/png"),
                        Some("gif") => Ok("image/gif"),
                        Some("bmp") => Ok("image/bmp"),
                        Some("webp") => Ok("image/webp"),
                        _ => eyre::bail!("unsupported image extension for '{}'", path.display()),
                    }
                };
                let image = image.ok_or_eyre(
                    "embed-similarity-demo requires --image when --image-compare is provided",
                )?;
                let image_a =
                    mltools::embed_image(ctx.as_ref(), &image, image_mime(&image)?).await?;
                let image_a_again =
                    mltools::embed_image(ctx.as_ref(), &image, image_mime(&image)?).await?;
                let image_b =
                    mltools::embed_image(ctx.as_ref(), &image_compare, image_mime(&image_compare)?)
                        .await?;
                let image_b_again =
                    mltools::embed_image(ctx.as_ref(), &image_compare, image_mime(&image_compare)?)
                        .await?;

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
                let image_mime = match image
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.to_ascii_lowercase())
                    .as_deref()
                {
                    Some("jpg" | "jpeg") => "image/jpeg",
                    Some("png") => "image/png",
                    Some("gif") => "image/gif",
                    Some("bmp") => "image/bmp",
                    Some("webp") => "image/webp",
                    _ => eyre::bail!("unsupported image extension for '{}'", image.display()),
                };
                let image_embedding =
                    mltools::embed_image(ctx.as_ref(), &image, image_mime).await?;
                let image_embedding_again =
                    mltools::embed_image(ctx.as_ref(), &image, image_mime).await?;
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

async fn build_plug_oci(plug_root: PathBuf, out_root: Option<PathBuf>) -> Res<()> {
    use daybook_types::manifest::PlugManifest;
    use oci_spec::image::{
        Descriptor, ImageIndexBuilder, ImageManifestBuilder, MediaType, OciLayoutBuilder,
    };
    use sha2::{Digest as _, Sha256};

    let cwd = std::env::current_dir()?;
    let plug_root = if plug_root.is_absolute() {
        plug_root
    } else {
        cwd.join(plug_root)
    };
    eyre::ensure!(
        plug_root.join("manifest.rs").exists(),
        "expected manifest bin source at {}",
        plug_root.join("manifest.rs").display()
    );
    eyre::ensure!(
        plug_root.join("Cargo.toml").exists(),
        "expected Cargo.toml at {}",
        plug_root.join("Cargo.toml").display()
    );

    let output = tokio::process::Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            &plug_root.join("Cargo.toml").display().to_string(),
            "--bin",
            "manifest",
        ])
        .output()
        .await?;
    eyre::ensure!(
        output.status.success(),
        "failed running plug manifest bin at '{}': {}",
        plug_root.display(),
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest_json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .wrap_err("error parsing manifest bin stdout as JSON")?;
    let manifest: PlugManifest = serde_json::from_value(manifest_json.clone())
        .wrap_err("error parsing PlugManifest JSON")?;

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .canonicalize()?;
    let out_root = out_root.unwrap_or_else(|| workspace_root.join("target/oci"));
    let artifact_root = out_root.join(manifest.id());
    let blobs_sha_root = artifact_root.join("blobs/sha256");

    if artifact_root.exists() {
        tokio::fs::remove_dir_all(&artifact_root).await?;
    }
    tokio::fs::create_dir_all(&blobs_sha_root).await?;

    let mut component_url_to_digest_hex: HashMap<String, String> = HashMap::new();
    let mut layer_descriptors: Vec<Descriptor> = vec![];
    let mut seen_digests = HashSet::<String>::new();
    let needs_wasm_build = manifest.wflow_bundles.values().any(|bundle| {
        bundle
            .component_urls
            .iter()
            .any(|component_url| matches!(component_url.scheme(), "static" | "build"))
    });
    let wasm_target_dir = workspace_root.join("target/wasm");
    if needs_wasm_build {
        build_plug_wasm_component(&workspace_root, &plug_root, &wasm_target_dir).await?;
    }
    for bundle in manifest.wflow_bundles.values() {
        for component_url in &bundle.component_urls {
            let bytes = match component_url.scheme() {
                "file" => {
                    let file_path = component_url
                        .to_file_path()
                        .map_err(|_| eyre::eyre!("invalid file URL '{}'", component_url))?;
                    tokio::fs::read(&file_path)
                        .await
                        .wrap_err_with(|| format!("error reading '{}'", file_path.display()))?
                }
                "static" => read_static_component_bytes(component_url, &wasm_target_dir)
                    .await
                    .wrap_err_with(|| {
                        format!("error resolving static component URL '{component_url}'")
                    })?,
                "build" => read_build_component_bytes(component_url, &wasm_target_dir)
                    .await
                    .wrap_err_with(|| {
                        format!("error resolving build component URL '{component_url}'")
                    })?,
                scheme => {
                    eyre::bail!(
                        "build-plug-oci only supports file://, static:, and build:// component urls, got '{}' in '{}'",
                        scheme,
                        component_url
                    );
                }
            };
            let digest_hex = format!("{:x}", Sha256::digest(&bytes));
            let digest = format!("sha256:{digest_hex}");
            component_url_to_digest_hex.insert(component_url.to_string(), digest_hex.clone());
            if seen_digests.insert(digest_hex.clone()) {
                tokio::fs::write(blobs_sha_root.join(&digest_hex), &bytes).await?;
                let media_type = match component_url.scheme() {
                    "file" => {
                        let file_path = component_url
                            .to_file_path()
                            .map_err(|_| eyre::eyre!("invalid file URL '{}'", component_url))?;
                        match file_path
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .map(|ext| ext.to_ascii_lowercase())
                            .as_deref()
                        {
                            Some("wasm") => {
                                MediaType::Other(oci_wasm::WASM_LAYER_MEDIA_TYPE.into())
                            }
                            _ => MediaType::Other("application/octet-stream".into()),
                        }
                    }
                    "static" if is_static_wasm_component(component_url) => {
                        MediaType::Other(oci_wasm::WASM_LAYER_MEDIA_TYPE.into())
                    }
                    "build" if is_build_wasm_component(component_url) => {
                        MediaType::Other(oci_wasm::WASM_LAYER_MEDIA_TYPE.into())
                    }
                    _ => MediaType::Other("application/octet-stream".into()),
                };
                let digest: oci_spec::image::Digest = digest.parse()?;
                layer_descriptors.push(Descriptor::new(media_type, bytes.len() as u64, digest));
            }
        }
    }

    let mut oci_manifest_json = manifest_json;
    let bundles = oci_manifest_json
        .get_mut("wflowBundles")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_eyre("manifest JSON missing object at 'wflowBundles'")?;
    for bundle in bundles.values_mut() {
        let urls = bundle
            .get_mut("componentUrls")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_eyre("bundle missing array at 'componentUrls'")?;
        for value in urls.iter_mut() {
            let Some(url) = value.as_str() else {
                eyre::bail!("componentUrls entries must be strings");
            };
            if let Some(digest_hex) = component_url_to_digest_hex.get(url) {
                *value = serde_json::Value::String(format!("oci://sha256:{digest_hex}"));
            }
        }
    }
    let oci_manifest_payload = serde_json::to_vec_pretty(&oci_manifest_json)?;
    let oci_manifest_payload_digest_hex = format!("{:x}", Sha256::digest(&oci_manifest_payload));
    tokio::fs::write(
        blobs_sha_root.join(&oci_manifest_payload_digest_hex),
        &oci_manifest_payload,
    )
    .await?;
    let oci_manifest_layer_digest: oci_spec::image::Digest =
        format!("sha256:{oci_manifest_payload_digest_hex}").parse()?;
    layer_descriptors.push(Descriptor::new(
        MediaType::Other(daybook_core::plugs::OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE.into()),
        oci_manifest_payload.len() as u64,
        oci_manifest_layer_digest,
    ));

    let config_bytes = b"{}".to_vec();
    let config_digest_hex = format!("{:x}", Sha256::digest(&config_bytes));
    tokio::fs::write(blobs_sha_root.join(&config_digest_hex), &config_bytes).await?;
    let config_digest: oci_spec::image::Digest = format!("sha256:{config_digest_hex}").parse()?;
    let config_desc = Descriptor::new(
        MediaType::EmptyJSON,
        config_bytes.len() as u64,
        config_digest,
    );

    let image_manifest = ImageManifestBuilder::default()
        .schema_version(2u32)
        .media_type(MediaType::ImageManifest)
        .artifact_type(MediaType::Other(
            daybook_core::plugs::OCI_PLUG_ARTIFACT_TYPE.into(),
        ))
        .config(config_desc)
        .layers(layer_descriptors)
        .build()?;
    let image_manifest_bytes = image_manifest.to_string_pretty()?.into_bytes();
    let image_manifest_digest_hex = format!("{:x}", Sha256::digest(&image_manifest_bytes));
    tokio::fs::write(
        blobs_sha_root.join(&image_manifest_digest_hex),
        &image_manifest_bytes,
    )
    .await?;

    let image_manifest_digest: oci_spec::image::Digest =
        format!("sha256:{image_manifest_digest_hex}").parse()?;
    let index_manifest_desc = Descriptor::new(
        MediaType::ImageManifest,
        image_manifest_bytes.len() as u64,
        image_manifest_digest,
    );
    let image_index = ImageIndexBuilder::default()
        .schema_version(2u32)
        .media_type(MediaType::ImageIndex)
        .artifact_type(MediaType::Other(
            daybook_core::plugs::OCI_PLUG_ARTIFACT_TYPE.into(),
        ))
        .manifests(vec![index_manifest_desc])
        .build()?;
    image_index
        .to_file_pretty(artifact_root.join("index.json"))
        .wrap_err("error writing OCI index.json")?;

    let oci_layout = OciLayoutBuilder::default()
        .image_layout_version("1.0.0")
        .build()?;
    oci_layout
        .to_file_pretty(artifact_root.join("oci-layout"))
        .wrap_err("error writing oci-layout")?;

    println!("wrote OCI plug artifact to {}", artifact_root.display());
    Ok(())
}

async fn build_plug_wasm_component(
    workspace_root: &Path,
    plug_root: &Path,
    wasm_target_dir: &Path,
) -> Res<()> {
    let mut build = tokio::process::Command::new("cargo");
    build
        .args([
            "build",
            "--release",
            "--target",
            "wasm32-wasip2",
            "--manifest-path",
            &plug_root.join("Cargo.toml").display().to_string(),
        ])
        .current_dir(workspace_root)
        .env("CARGO_TARGET_DIR", wasm_target_dir);
    append_tokio_unstable_rustflags(&mut build);
    let output = build.output().await?;
    eyre::ensure!(
        output.status.success(),
        "failed to build plug wasm for '{}': {}",
        plug_root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

async fn read_static_component_bytes(
    component_url: &url::Url,
    wasm_target_dir: &Path,
) -> Res<Vec<u8>> {
    let static_name = component_url.path().trim_start_matches('/');
    eyre::ensure!(
        !static_name.is_empty(),
        "static component URL must include a path, got '{}'",
        component_url
    );
    if static_name.ends_with(".wasm.zst") {
        let wasm_name = static_name
            .strip_suffix(".zst")
            .ok_or_eyre("static .wasm.zst path parsing error")?;
        let wasm_path = wasm_target_dir
            .join("wasm32-wasip2")
            .join("release")
            .join(wasm_name);
        let wasm_bytes = tokio::fs::read(&wasm_path)
            .await
            .wrap_err_with(|| format!("missing wasm artifact at '{}'", wasm_path.display()))?;
        return Ok(wasm_bytes);
    }
    eyre::bail!(
        "unsupported static component URL '{}'; expected '*.wasm.zst'",
        component_url
    );
}

fn is_static_wasm_component(component_url: &url::Url) -> bool {
    component_url.scheme() == "static" && component_url.path().ends_with(".wasm.zst")
}

async fn read_build_component_bytes(
    component_url: &url::Url,
    wasm_target_dir: &Path,
) -> Res<Vec<u8>> {
    let component_name = component_url.path().trim_start_matches('/');
    eyre::ensure!(
        !component_name.is_empty(),
        "build component URL must include a component filename, got '{}'",
        component_url
    );
    eyre::ensure!(
        component_name.ends_with(".wasm"),
        "unsupported build component URL '{}'; expected '*.wasm'",
        component_url
    );
    let wasm_path = wasm_target_dir
        .join("wasm32-wasip2")
        .join("release")
        .join(component_name);
    tokio::fs::read(&wasm_path)
        .await
        .wrap_err_with(|| format!("missing built wasm artifact at '{}'", wasm_path.display()))
}

fn is_build_wasm_component(component_url: &url::Url) -> bool {
    component_url.scheme() == "build" && component_url.path().ends_with(".wasm")
}

fn append_tokio_unstable_rustflags(cmd: &mut tokio::process::Command) {
    const TOKIO_UNSTABLE_FLAG: &str = "--cfg";
    const TOKIO_UNSTABLE_VALUE: &str = "tokio_unstable";

    let encoded_flag = format!("{TOKIO_UNSTABLE_FLAG}\x1f{TOKIO_UNSTABLE_VALUE}");
    let encoded = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    if !encoded
        .split('\x1f')
        .any(|part| part == TOKIO_UNSTABLE_VALUE)
    {
        let new_encoded = if encoded.is_empty() {
            encoded_flag
        } else {
            format!("{encoded}\x1f{encoded_flag}")
        };
        cmd.env("CARGO_ENCODED_RUSTFLAGS", new_encoded);
    }

    let rustflags = std::env::var("RUSTFLAGS").unwrap_or_default();
    if !rustflags.contains("tokio_unstable") {
        let new_rustflags = if rustflags.is_empty() {
            format!("{TOKIO_UNSTABLE_FLAG} {TOKIO_UNSTABLE_VALUE}")
        } else {
            format!("{rustflags} {TOKIO_UNSTABLE_FLAG} {TOKIO_UNSTABLE_VALUE}")
        };
        cmd.env("RUSTFLAGS", new_rustflags);
    }
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

async fn irpc_disconnect_probe() -> Res<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                ProbeMessage::Ping(msg) => {
                    let WithChannels { inner, tx, .. } = msg;
                    tx.send(inner.0.saturating_add(1))
                        .await
                        .expect("probe responder channel closed unexpectedly");
                }
            }
        }
    });

    let server_local = Client::<ProbeProtocol>::local(tx);
    let server_endpoint = iroh::Endpoint::bind().await?;
    let protocol = IrohProtocol::with_sender(
        server_local
            .as_local()
            .expect("local irpc sender missing for probe"),
    );
    let router = iroh::protocol::Router::builder(server_endpoint)
        .accept(IRPC_PROBE_ALPN, protocol)
        .spawn();
    let server_addr = router.endpoint().addr();

    let client_endpoint = iroh::Endpoint::bind().await?;
    let client = irpc_iroh::client::<ProbeProtocol>(client_endpoint, server_addr, IRPC_PROBE_ALPN);

    let first = client
        .rpc(PingReq(41))
        .await
        .wrap_err("initial rpc failed")?;
    if first != 42 {
        eyre::bail!("probe invariant failed: expected 42, got {first}");
    }

    router.shutdown().await.wrap_err("router shutdown failed")?;

    match client.rpc(PingReq(7)).await {
        Ok(val) => eyre::bail!("expected rpc failure after shutdown, got value={val}"),
        Err(err) => {
            println!("irpc disconnect probe: observed expected error after shutdown: {err}");
        }
    }

    Ok(())
}

async fn irpc_inflight_disconnect_probe() -> Res<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                ProbeMessage::Ping(msg) => {
                    let WithChannels { inner, tx, .. } = msg;
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    // If peer disconnected, this will fail and that's fine for the probe.
                    let _ = tx.send(inner.0.saturating_add(1)).await;
                }
            }
        }
    });

    let server_local = Client::<ProbeProtocol>::local(tx);
    let server_endpoint = iroh::Endpoint::bind().await?;
    let protocol = IrohProtocol::with_sender(
        server_local
            .as_local()
            .expect("local irpc sender missing for probe"),
    );
    let router = iroh::protocol::Router::builder(server_endpoint)
        .accept(IRPC_PROBE_ALPN, protocol)
        .spawn();
    let server_addr = router.endpoint().addr();

    let client_endpoint = iroh::Endpoint::bind().await?;
    let client = irpc_iroh::client::<ProbeProtocol>(client_endpoint, server_addr, IRPC_PROBE_ALPN);

    let call = tokio::spawn({
        let client = client.clone();
        async move {
            let started = std::time::Instant::now();
            let out = client.rpc(PingReq(123)).await;
            (started.elapsed(), out)
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    router.shutdown().await.wrap_err("router shutdown failed")?;

    let (elapsed, out) = call.await.wrap_err("probe task join failed")?;
    match out {
        Ok(value) => {
            eyre::bail!("expected in-flight rpc failure after shutdown, got value={value}")
        }
        Err(err) => {
            println!(
                "irpc in-flight disconnect probe: rpc failed after {:?}: {}",
                elapsed, err
            );
        }
    }
    Ok(())
}

async fn irpc_client_cancel_probe() -> Res<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let (report_tx, mut report_rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                ProbeMessage::Ping(msg) => {
                    let WithChannels { inner, tx, .. } = msg;
                    let report_tx = report_tx.clone();
                    tokio::spawn(async move {
                        let started = std::time::Instant::now();
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        let send_result = tx.send(inner.0.saturating_add(1)).await;
                        report_tx
                            .send((started.elapsed(), send_result.is_ok()))
                            .await
                            .expect("probe report channel closed unexpectedly");
                    });
                }
            }
        }
    });

    let server_local = Client::<ProbeProtocol>::local(tx);
    let server_endpoint = iroh::Endpoint::bind().await?;
    let protocol = IrohProtocol::with_sender(
        server_local
            .as_local()
            .expect("local irpc sender missing for probe"),
    );
    let router = iroh::protocol::Router::builder(server_endpoint)
        .accept(IRPC_PROBE_ALPN, protocol)
        .spawn();
    let server_addr = router.endpoint().addr();

    let client_endpoint = iroh::Endpoint::bind().await?;
    let client = irpc_iroh::client::<ProbeProtocol>(client_endpoint, server_addr, IRPC_PROBE_ALPN);

    let call = tokio::spawn({
        let client = client.clone();
        async move { client.rpc(PingReq(123)).await }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    call.abort();
    let join_err = call.await.expect_err("probe task should have been aborted");
    if !join_err.is_cancelled() {
        eyre::bail!("expected cancelled join error after abort, got: {join_err}");
    }

    let (elapsed, send_ok) = report_rx
        .recv()
        .await
        .ok_or_eyre("probe report missing after client cancellation")?;
    println!(
        "irpc client cancel probe: responder send_ok={send_ok} after {:?}",
        elapsed
    );
    if send_ok {
        eyre::bail!("expected responder send failure after client cancellation");
    }

    router.shutdown().await.wrap_err("router shutdown failed")?;
    Ok(())
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

fn automerge08_minimal_repro() -> Res<()> {
    use automerge08::transaction::Transactable;
    for round in 0..40u32 {
        let mut main = automerge08::Automerge::new();

        {
            let mut tx = main.transaction();
            tx.put(automerge08::ROOT, "note", "init")?;
            tx.commit();
        }
        let h0 = main.get_heads();

        let mut branch_a = main.fork_at(&h0)?;
        {
            let mut tx = branch_a.transaction();
            tx.put(automerge08::ROOT, "title", "A")?;
            tx.commit();
        }
        {
            let mut patch_log = automerge08::PatchLog::active();
            main.merge_and_log_patches(&mut branch_a, &mut patch_log)?;
            let _ = main.make_patches(&mut patch_log);
        }
        {
            // Candidate barrier: canonicalize internal graph via save/load
            let bytes = main.save();
            main = automerge08::Automerge::load(&bytes)?;
            let mut tx = main.transaction();
            tx.put(automerge08::ROOT, "a", "ok")?;
            tx.commit();
        }
        let h1 = main.get_heads();

        let mut branch_b = main.fork_at(&h1)?;
        {
            let mut tx = branch_b.transaction();
            tx.put(automerge08::ROOT, "note", "B")?;
            tx.commit();
        }
        {
            let mut patch_log = automerge08::PatchLog::active();
            main.merge_and_log_patches(&mut branch_b, &mut patch_log)?;
            let _ = main.make_patches(&mut patch_log);
        }
        {
            let bytes = main.save();
            main = automerge08::Automerge::load(&bytes)?;
            let mut tx = main.transaction();
            tx.put(automerge08::ROOT, "b", "ok")?;
            tx.commit();
        }

        let _stale = main.fork_at(&h1)?;
        if round % 10 == 0 {
            println!("ok round={round}");
        }
    }
    println!("PASS: save/load barrier survived 40 rounds");
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
    BuildPlugOci {
        #[arg(long)]
        plug_root: PathBuf,
        #[arg(long)]
        out_root: Option<PathBuf>,
    },
    IrpcDisconnectProbe {},
    IrpcInflightDisconnectProbe {},
    IrpcClientCancelProbe {},
    Automerge08MinimalRepro {},
}
