//! Plug OCI packaging: runs a manifest crate's native `manifest` binary, builds the
//! sibling routines component for `wasm32-wasip3`, and writes an OCI image layout
//! (Wasm layers plus a plug-manifest layer) under `target/oci/<plug id>`.

use crate::interlude::*;

#[derive(Debug, clap::Subcommand)]
pub enum PlugCmd {
    /// Build a plug's OCI artifact from its manifest crate.
    BuildOci {
        #[arg(long)]
        plug_root: PathBuf,
        #[arg(long)]
        out_root: Option<PathBuf>,
    },
}

pub async fn dispatch(cmd: PlugCmd) -> Res<()> {
    match cmd {
        PlugCmd::BuildOci {
            plug_root,
            out_root,
        } => build_oci(plug_root, out_root).await,
    }
}

const OCI_PLUG_ARTIFACT_TYPE: &str = "application/vnd.daybook.plug.v1";
const OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE: &str = "application/vnd.daybook.plug.manifest.v1+json";
const OCI_WASM_LAYER_MEDIA_TYPE: &str = "application/vnd.wasm.content.layer.v1+wasm";

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlugManifestHeader {
    namespace: String,
    name: String,
    #[serde(default)]
    wflow_bundles: HashMap<String, PlugWflowBundleHeader>,
}

impl PlugManifestHeader {
    fn id(&self) -> String {
        format!("@{}/{}", self.namespace, self.name)
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlugWflowBundleHeader {
    #[serde(default)]
    component_urls: Vec<url::Url>,
}

async fn read_plug_manifest(
    plug_root: &Path,
) -> Res<(PlugManifestHeader, serde_json::Value, PathBuf)> {
    #[derive(serde::Deserialize)]
    struct PlugPackage {
        package: PackageMetadata,
    }
    #[derive(serde::Deserialize)]
    struct PackageMetadata {
        metadata: PlugMetadata,
    }
    #[derive(serde::Deserialize)]
    struct PlugMetadata {
        townframe: PlugBuild,
    }
    #[derive(serde::Deserialize)]
    struct PlugBuild {
        routines: PathBuf,
    }

    let manifest_path = plug_root.join("Cargo.toml");
    eyre::ensure!(
        manifest_path.is_file(),
        "expected Cargo.toml at {}",
        manifest_path.display()
    );
    let package: PlugPackage = toml::from_str(&tokio::fs::read_to_string(&manifest_path).await?)?;
    let routines_root = plug_root.join(package.package.metadata.townframe.routines);

    let output = tokio::process::Command::new("cargo")
        .args(["run", "--locked", "--quiet", "--manifest-path"])
        .arg(&manifest_path)
        .args(["--bin", "manifest"])
        .output()
        .await?;
    eyre::ensure!(
        output.status.success(),
        "failed running plug manifest at '{}': {}",
        plug_root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest_json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .wrap_err("error parsing manifest bin stdout as JSON")?;
    let manifest: PlugManifestHeader = serde_json::from_value(manifest_json.clone())
        .wrap_err("error parsing PlugManifest JSON")?;
    Ok((manifest, manifest_json, routines_root))
}

async fn build_oci(plug_root: PathBuf, out_root: Option<PathBuf>) -> Res<()> {
    use oci_spec::image::Descriptor;
    use oci_spec::image::ImageIndexBuilder;
    use oci_spec::image::ImageManifestBuilder;
    use oci_spec::image::MediaType;
    use oci_spec::image::OciLayoutBuilder;
    use sha2::Digest as _;
    use sha2::Sha256;

    let cwd = std::env::current_dir()?;
    let plug_root = if plug_root.is_absolute() {
        plug_root
    } else {
        cwd.join(plug_root)
    };
    let (manifest, manifest_json, routines_root) = read_plug_manifest(&plug_root).await?;

    #[derive(serde::Deserialize)]
    struct CargoWorkspace {
        workspace_root: PathBuf,
        target_directory: PathBuf,
    }

    let output = tokio::process::Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(&routines_root)
        .output()
        .await?;
    eyre::ensure!(
        output.status.success(),
        "failed reading Cargo workspace metadata for '{}': {}",
        plug_root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let CargoWorkspace {
        workspace_root,
        target_directory,
    } = serde_json::from_slice(&output.stdout)?;
    build_plug_wasm_component(&workspace_root, &routines_root).await?;
    let repository_root = crate::workspace_root().await?;
    let out_root = out_root.unwrap_or_else(|| repository_root.join("target/oci"));
    let artifact_root = out_root.join(manifest.id());
    let blobs_sha_root = artifact_root.join("blobs/sha256");

    if artifact_root.exists() {
        tokio::fs::remove_dir_all(&artifact_root).await?;
    }
    tokio::fs::create_dir_all(&blobs_sha_root).await?;

    let mut component_url_to_digest_hex: HashMap<String, String> = HashMap::new();
    let mut layer_descriptors: Vec<Descriptor> = vec![];
    let mut seen_digests = HashSet::<String>::new();
    let wasm_target_dir = target_directory;
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
                "build" => read_build_component_bytes(component_url, &wasm_target_dir)
                    .await
                    .wrap_err_with(|| {
                        format!("error resolving build component URL '{component_url}'")
                    })?,
                scheme => {
                    eyre::bail!(
                        "`plug build-oci` only supports file://, and build:// component urls, got '{}' in '{}'",
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
                            Some("wasm") => MediaType::Other(OCI_WASM_LAYER_MEDIA_TYPE.into()),
                            _ => MediaType::Other("application/octet-stream".into()),
                        }
                    }
                    "build" if is_build_wasm_component(component_url) => {
                        MediaType::Other(OCI_WASM_LAYER_MEDIA_TYPE.into())
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

async fn build_plug_wasm_component(workspace_root: &Path, plug_root: &Path) -> Res<()> {
    let mut build = tokio::process::Command::new("cargo");
    build
        .args([
            "build",
            "--locked",
            "--lib",
            "--release",
            "--target",
            "wasm32-wasip3",
            "--manifest-path",
            &plug_root.join("Cargo.toml").display().to_string(),
        ])
        .current_dir(workspace_root);
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
        .join("wasm32-wasip3")
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

    fn sanitize_flags(flags: &[String]) -> Vec<String> {
        let mut result = Vec::new();
        let mut index = 0;
        while index < flags.len() {
            let flag = &flags[index];
            if flag == "-C"
                && index + 1 < flags.len()
                && flags[index + 1].starts_with("instrument-coverage")
            {
                index += 2;
                continue;
            }
            if flag == "--cfg"
                && index + 1 < flags.len()
                && (flags[index + 1] == "coverage" || flags[index + 1] == "coverage_nightly")
            {
                index += 2;
                continue;
            }
            if flag.starts_with("-Cinstrument-coverage")
                || flag.starts_with("--cfg=coverage")
                || flag.starts_with("--cfg=coverage_nightly")
            {
                index += 1;
                continue;
            }
            result.push(flag.clone());
            index += 1;
        }
        result
    }

    let raw_flags: Vec<String> = if let Ok(encoded) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        encoded
            .split('\x1f')
            .filter(|segment| !segment.is_empty())
            .map(|segment| segment.to_string())
            .collect()
    } else if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
        rustflags
            .split_whitespace()
            .map(|segment| segment.to_string())
            .collect()
    } else {
        Vec::new()
    };

    let mut sanitized = sanitize_flags(&raw_flags);
    let has_tokio_unstable = sanitized
        .iter()
        .any(|flag| flag == TOKIO_UNSTABLE_VALUE || flag == "--cfg=tokio_unstable");
    if !has_tokio_unstable {
        sanitized.push(TOKIO_UNSTABLE_FLAG.to_string());
        sanitized.push(TOKIO_UNSTABLE_VALUE.to_string());
    }

    cmd.env("CARGO_ENCODED_RUSTFLAGS", sanitized.join("\x1f"));
    cmd.env_remove("RUSTFLAGS");
}
