#[expect(unused)]
mod interlude {
    pub use std::future::Future;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;

    pub use utils_rs::prelude::*;
}

mod gen;
mod keyhive_demo;

use clap::builder::styling::AnsiColor;

use crate::interlude::*;

const OCI_PLUG_ARTIFACT_TYPE: &str = "application/vnd.daybook.plug.v1";
const OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE: &str = "application/vnd.daybook.plug.manifest.v1+json";

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
        Commands::KeyhiveDemo {} => {
            keyhive_demo::cli().await?;
        }
        Commands::BuildPlugOci {
            plug_root,
            out_root,
        } => {
            build_plug_oci(plug_root, out_root).await?;
        }
        Commands::Play {} => {
            /*
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
            */
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
        MediaType::Other(OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE.into()),
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
        .artifact_type(MediaType::Other(OCI_PLUG_ARTIFACT_TYPE.into()))
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
        .artifact_type(MediaType::Other(OCI_PLUG_ARTIFACT_TYPE.into()))
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
    KeyhiveDemo {},
    Play {},
    Gen {},
    BuildPlugOci {
        #[arg(long)]
        plug_root: PathBuf,
        #[arg(long)]
        out_root: Option<PathBuf>,
    },
}
