/*
Requirements:

mltools_local: local execution of ML tools.
mltools_cloud: client for cloud token providers.
mltools_server: mltools_local but for servers.
mltools_gateway: durable-streams based API for mltools_server or mltools_cloud.
mltools: routes to mltools_local, mltools_client or mltools_cloud depending on config.

ML tools support:
- OCR
    - Local
    - Cloud
    - Server
- Embedding
    - Local
    - Cloud
    - Server
- LLM
    - Cloud
    - Server
- STT
    - Cloud
    - Server

*/
// mod wit {
//     wit_bindgen::generate!({
//         world: "guest",
//         additional_derives: [serde::Serialize, serde::Deserialize],
//         with: {
//             // "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
//             // "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
//             // "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
//             // "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
//             // "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
//             // "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
//             // "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
//             "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
//             // "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,
//
//             // "townframe:api-utils/utils": api_utils_rs::wit::utils,
//
//             "townframe:mltools/types": generate,
//             "townframe:mltools/llm-chat": generate,
//         }
//     });
// }

mod interlude {
    pub use utils_rs::prelude::*;
}

use interlude::*;

pub struct Config {
    pub ocr: OcrConfig,
    pub embed: EmbedConfig,
}

pub struct OcrConfig {
    pub backends: Vec<OcrBackendConfig>,
}

pub enum OcrBackendConfig {
    LocalOnnx {
        text_recognition_onnx_path: PathBuf,
        text_detection_onnx_path: PathBuf,
        character_dict_txt_path: PathBuf,

        document_orientation_onnx_path: Option<PathBuf>,
        text_line_orientation_onnx_path: Option<PathBuf>,
        document_rectification_onnx_path: Option<PathBuf>,

        supported_languages_bcp47: Vec<String>,
    },
}

pub struct EmbedConfig {
    pub backends: Vec<EmbedBackendConfig>,
}

pub enum EmbedBackendConfig {
    LocalFastembedNomic { cache_dir: PathBuf },
}

pub struct Ctx {
    pub config: Config,
}

impl Ctx {
    pub async fn new(config: Config) -> Arc<Self> {
        Self { config }.into()
    }
}

pub struct EmbedResult {
    pub vector: Vec<f32>,
    pub dimensions: u32,
    pub model_id: String,
}

pub async fn embed_text(ctx: &Ctx, text: &str) -> Res<EmbedResult> {
    if text.trim().is_empty() {
        eyre::bail!("empty input text");
    }

    let cache_dir = ctx
        .config
        .embed
        .backends
        .iter()
        .map(|backend| {
            let EmbedBackendConfig::LocalFastembedNomic { cache_dir } = backend;
            cache_dir.clone()
        })
        .next()
        .ok_or_eyre("no embed backend configured")?;

    let input_text = text.to_string();
    tokio::task::spawn_blocking(move || -> Res<EmbedResult> {
        use fastembed::{
            InitOptionsUserDefined, QuantizationMode, TextEmbedding, TokenizerFiles,
            UserDefinedEmbeddingModel,
        };

        let model_root = cache_dir.join("models--nomic-ai--nomic-embed-text-v1.5");
        let refs_main_path = model_root.join("refs/main");
        let snapshot_id = std::fs::read_to_string(&refs_main_path)
            .wrap_err_with(|| format!("failed reading refs/main: {}", refs_main_path.display()))?;
        let snapshot_id = snapshot_id.trim();
        if snapshot_id.is_empty() {
            eyre::bail!("empty snapshot id in {}", refs_main_path.display());
        }

        let snapshot_root = model_root.join("snapshots").join(snapshot_id);
        let onnx_path = snapshot_root.join("onnx/model_quantized.onnx");
        let tokenizer_path = snapshot_root.join("tokenizer.json");
        let config_path = snapshot_root.join("config.json");
        let special_tokens_map_path = snapshot_root.join("special_tokens_map.json");
        let tokenizer_config_path = snapshot_root.join("tokenizer_config.json");

        let user_model = UserDefinedEmbeddingModel::new(
            std::fs::read(&onnx_path)
                .wrap_err_with(|| format!("failed reading {}", onnx_path.display()))?,
            TokenizerFiles {
                tokenizer_file: std::fs::read(&tokenizer_path)
                    .wrap_err_with(|| format!("failed reading {}", tokenizer_path.display()))?,
                config_file: std::fs::read(&config_path)
                    .wrap_err_with(|| format!("failed reading {}", config_path.display()))?,
                special_tokens_map_file: std::fs::read(&special_tokens_map_path).wrap_err_with(
                    || format!("failed reading {}", special_tokens_map_path.display()),
                )?,
                tokenizer_config_file: std::fs::read(&tokenizer_config_path).wrap_err_with(
                    || format!("failed reading {}", tokenizer_config_path.display()),
                )?,
            },
        )
        .with_quantization(QuantizationMode::Dynamic);

        let mut embedder =
            TextEmbedding::try_new_from_user_defined(user_model, InitOptionsUserDefined::new())
                .map_err(|err| eyre::eyre!("failed to initialize embed model: {err}"))?;
        let mut vectors = embedder
            .embed(vec![input_text], None)
            .map_err(|err| eyre::eyre!("failed to embed text: {err}"))?;
        let Some(vector) = vectors.pop() else {
            eyre::bail!("embedding backend returned no vector");
        };
        let dimensions = vector.len() as u32;

        Ok(EmbedResult {
            vector,
            dimensions,
            model_id: "nomic-ai/nomic-embed-text-v1.5".to_string(),
        })
    })
    .await
    .wrap_err("embed task failed to join")?
}

pub struct OcrResult {
    pub text: String,
    pub regions: Vec<TextRegion>,
}
pub struct TextRegion {
    /// x,y pairs
    pub bounding_box: Vec<(f32, f32)>,
    pub text: Option<Arc<str>>,
    pub confidence: Option<f32>,
}

pub async fn ocr_image(ctx: &Ctx, images: &[PathBuf]) -> Res<Vec<OcrResult>> {
    if images.is_empty() {
        eyre::bail!("no images provided");
    }

    let backend_config = ctx
        .config
        .ocr
        .backends
        .iter()
        .map(|backend| {
            let OcrBackendConfig::LocalOnnx {
                text_recognition_onnx_path,
                text_detection_onnx_path,
                character_dict_txt_path,
                document_orientation_onnx_path,
                text_line_orientation_onnx_path,
                document_rectification_onnx_path,
                ..
            } = backend;

            (
                text_recognition_onnx_path,
                text_detection_onnx_path,
                character_dict_txt_path,
                document_orientation_onnx_path,
                text_line_orientation_onnx_path,
                document_rectification_onnx_path,
            )
        })
        .next()
        .ok_or_eyre("no ocr backend configured")?;

    use oar_ocr::oarocr::OAROCRBuilder;
    use oar_ocr::utils::load_image;

    let image_paths = images.to_vec();
    let (
        text_recognition_onnx_path,
        text_detection_onnx_path,
        character_dict_txt_path,
        document_orientation_onnx_path,
        text_line_orientation_onnx_path,
        document_rectification_onnx_path,
    ) = (
        backend_config.0.clone(),
        backend_config.1.clone(),
        backend_config.2.clone(),
        backend_config.3.clone(),
        backend_config.4.clone(),
        backend_config.5.clone(),
    );

    tokio::task::spawn_blocking(move || -> Res<Vec<OcrResult>> {
        let mut builder = OAROCRBuilder::new(
            &text_detection_onnx_path,
            &text_recognition_onnx_path,
            &character_dict_txt_path,
        );

        if let Some(path) = &document_orientation_onnx_path {
            builder = builder.with_document_image_orientation_classification(path);
        }
        if let Some(path) = &text_line_orientation_onnx_path {
            builder = builder.with_text_line_orientation_classification(path);
        }
        if let Some(path) = &document_rectification_onnx_path {
            builder = builder.with_document_image_rectification(path);
        }

        let ocr = builder.build()?;

        let mut loaded_images = Vec::with_capacity(image_paths.len());
        for image_path in &image_paths {
            loaded_images.push(load_image(image_path)?);
        }

        let raw_results = ocr.predict(loaded_images)?;
        let mut results = Vec::with_capacity(raw_results.len());

        for raw_result in raw_results {
            let mut regions = Vec::with_capacity(raw_result.text_regions.len());
            for raw_region in raw_result.text_regions {
                regions.push(TextRegion {
                    bounding_box: raw_region
                        .bounding_box
                        .points
                        .into_iter()
                        .map(|point| (point.x, point.y))
                        .collect(),
                    text: raw_region.text,
                    confidence: raw_region.confidence,
                });
            }

            let text = regions
                .iter()
                .filter_map(|region| region.text.as_deref())
                .collect::<Vec<_>>()
                .join("\n");

            results.push(OcrResult { text, regions });
        }

        Ok(results)
    })
    .await
    .wrap_err("ocr task failed to join")?
}

/// local execution of ML tools.
mod local {}
/// client for cloud token providers.
mod cloud {}
/// durable-streams based API for mltools_server or mltools_cloud.
mod gateway {}
/// mltools_local but for servers.
mod server {}
/// routes to mltools_local, mltools_client or mltools_cloud depending on config.
mod client {}
