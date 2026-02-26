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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub ocr: OcrConfig,
    pub embed: EmbedConfig,
    #[serde(default)]
    pub image_embed: ImageEmbedConfig,
    pub llm: LlmConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OcrConfig {
    pub backends: Vec<OcrBackendConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EmbedConfig {
    pub backends: Vec<EmbedBackendConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ImageEmbedConfig {
    pub backends: Vec<ImageEmbedBackendConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum EmbedBackendConfig {
    LocalFastembed {
        onnx_path: PathBuf,
        tokenizer_path: PathBuf,
        config_path: PathBuf,
        special_tokens_map_path: PathBuf,
        tokenizer_config_path: PathBuf,
        model_id: String,
    },
    CloudOllama {
        url: String,
        model: String,
        auth: Option<CloudAuth>,
    },
    CloudGemini {
        model: String,
        auth: Option<CloudAuth>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ImageEmbedBackendConfig {
    LocalFastembed {
        onnx_path: PathBuf,
        preprocessor_config_path: PathBuf,
        model_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LlmConfig {
    pub backends: Vec<LlmBackendConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum LlmBackendConfig {
    CloudOllama {
        url: String,
        model: String,
        auth: Option<CloudAuth>,
    },
    CloudGemini {
        model: String,
        auth: Option<CloudAuth>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum CloudAuth {
    Basic { username: String, password: String },
    ApiKey { key: String },
}

pub struct Ctx {
    pub config: Config,
}

impl Ctx {
    pub async fn new(config: Config) -> Arc<Self> {
        Self { config }.into()
    }
}

#[cfg(feature = "hf_hub")]
pub mod models;

pub struct EmbedResult {
    pub vector: Vec<f32>,
    pub dimensions: u32,
    pub model_id: String,
}

pub async fn embed_text(ctx: &Ctx, text: &str) -> Res<EmbedResult> {
    if text.trim().is_empty() {
        eyre::bail!("empty input text");
    }

    let Some(backend_config) = ctx.config.embed.backends.first() else {
        eyre::bail!("no embed backend configured");
    };
    match backend_config {
        EmbedBackendConfig::LocalFastembed { .. } => local::embed_text(backend_config, text).await,
        EmbedBackendConfig::CloudOllama { .. } | EmbedBackendConfig::CloudGemini { .. } => {
            cloud::embed_text(backend_config, text).await
        }
    }
}

pub async fn embed_image(ctx: &Ctx, image: &Path, mime: Option<&str>) -> Res<EmbedResult> {
    let Some(backend_config) = ctx.config.image_embed.backends.first() else {
        eyre::bail!("no image embed backend configured");
    };

    match backend_config {
        ImageEmbedBackendConfig::LocalFastembed { .. } => {
            local::embed_image(backend_config, image, mime).await
        }
    }
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
    let Some(backend_config) = ctx.config.ocr.backends.first() else {
        eyre::bail!("no ocr backend configured");
    };
    local::ocr_image(backend_config, images).await
}

pub struct LlmChatResult {
    pub text: String,
}

pub async fn llm_chat(ctx: &Ctx, text: &str) -> Res<LlmChatResult> {
    if text.trim().is_empty() {
        eyre::bail!("empty llm input text");
    }

    let Some(backend_config) = ctx.config.llm.backends.first() else {
        eyre::bail!("no llm backend configured");
    };

    match backend_config {
        LlmBackendConfig::CloudOllama { .. } | LlmBackendConfig::CloudGemini { .. } => {
            cloud::llm_chat(backend_config, text).await
        }
    }
}

/// local execution of ML tools.
mod local {
    use super::*;

    const NOMIC_EMBED_TEXT_V15_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";

    fn is_nomic_embed_text_v15(model_id: &str) -> bool {
        model_id.eq_ignore_ascii_case(NOMIC_EMBED_TEXT_V15_MODEL_ID)
    }

    fn l2_normalize_in_place(vector: &mut [f32]) {
        let mut data = nalgebra::DVector::<f32>::from_column_slice(vector);
        let norm = data.norm();
        if norm == 0.0 {
            return;
        }
        data /= norm;
        vector.copy_from_slice(data.as_slice());
    }

    fn layer_norm_in_place(vector: &mut [f32]) {
        if vector.is_empty() {
            return;
        }
        let len = vector.len() as f32;
        let mut data = nalgebra::DVector::<f32>::from_column_slice(vector);
        let mean = data.iter().copied().sum::<f32>() / len;
        for value in data.iter_mut() {
            *value -= mean;
        }
        let variance = data.norm_squared() / len;
        let inv_std = 1.0_f32 / (variance + 1e-5_f32).sqrt();
        data *= inv_std;
        vector.copy_from_slice(data.as_slice());
    }

    pub async fn embed_text(backend_config: &EmbedBackendConfig, text: &str) -> Res<EmbedResult> {
        let (
            onnx_path,
            tokenizer_path,
            config_path,
            special_tokens_map_path,
            tokenizer_config_path,
            model_id,
        ) = match backend_config {
            EmbedBackendConfig::LocalFastembed {
                onnx_path,
                tokenizer_path,
                config_path,
                special_tokens_map_path,
                tokenizer_config_path,
                model_id,
            } => (
                onnx_path.clone(),
                tokenizer_path.clone(),
                config_path.clone(),
                special_tokens_map_path.clone(),
                tokenizer_config_path.clone(),
                model_id.clone(),
            ),
            EmbedBackendConfig::CloudOllama { .. } | EmbedBackendConfig::CloudGemini { .. } => {
                eyre::bail!("cloud backend is not supported in local::embed_text")
            }
        };

        for required_path in [
            &onnx_path,
            &tokenizer_path,
            &config_path,
            &special_tokens_map_path,
            &tokenizer_config_path,
        ] {
            if !required_path.exists() {
                eyre::bail!("missing embedding model file: {}", required_path.display());
            }
        }

        let input_text = text.to_string();
        tokio::task::spawn_blocking(move || -> Res<EmbedResult> {
            use fastembed::{
                InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding, TokenizerFiles,
                UserDefinedEmbeddingModel,
            };

            let mut user_model = UserDefinedEmbeddingModel::new(
                std::fs::read(&onnx_path)
                    .wrap_err_with(|| format!("failed reading {}", onnx_path.display()))?,
                TokenizerFiles {
                    tokenizer_file: std::fs::read(&tokenizer_path)
                        .wrap_err_with(|| format!("failed reading {}", tokenizer_path.display()))?,
                    config_file: std::fs::read(&config_path)
                        .wrap_err_with(|| format!("failed reading {}", config_path.display()))?,
                    special_tokens_map_file: std::fs::read(&special_tokens_map_path)
                        .wrap_err_with(|| {
                            format!("failed reading {}", special_tokens_map_path.display())
                        })?,
                    tokenizer_config_file: std::fs::read(&tokenizer_config_path).wrap_err_with(
                        || format!("failed reading {}", tokenizer_config_path.display()),
                    )?,
                },
            )
            .with_quantization(QuantizationMode::Dynamic);
            if is_nomic_embed_text_v15(&model_id) {
                user_model = user_model.with_pooling(Pooling::Mean);
            }

            let mut embedder =
                TextEmbedding::try_new_from_user_defined(user_model, InitOptionsUserDefined::new())
                    .map_err(|err| eyre::eyre!("failed to initialize embed model: {err}"))?;
            let mut vectors = embedder
                .embed(vec![input_text], None)
                .map_err(|err| eyre::eyre!("failed to embed text: {err}"))?;
            let Some(mut vector) = vectors.pop() else {
                eyre::bail!("embedding backend returned no vector");
            };
            if is_nomic_embed_text_v15(&model_id) {
                // Reference Nomic text usage applies layer_norm then L2 normalization.
                layer_norm_in_place(&mut vector);
                l2_normalize_in_place(&mut vector);
            }
            let dimensions = vector.len() as u32;

            Ok(EmbedResult {
                vector,
                dimensions,
                model_id,
            })
        })
        .await
        .wrap_err("embed task failed to join")?
    }

    pub async fn ocr_image(
        backend_config: &OcrBackendConfig,
        images: &[PathBuf],
    ) -> Res<Vec<OcrResult>> {
        if images.is_empty() {
            eyre::bail!("no images provided");
        }

        let (
            text_recognition_onnx_path,
            text_detection_onnx_path,
            character_dict_txt_path,
            document_orientation_onnx_path,
            text_line_orientation_onnx_path,
            document_rectification_onnx_path,
        ) = match backend_config {
            OcrBackendConfig::LocalOnnx {
                text_recognition_onnx_path,
                text_detection_onnx_path,
                character_dict_txt_path,
                document_orientation_onnx_path,
                text_line_orientation_onnx_path,
                document_rectification_onnx_path,
                ..
            } => (
                text_recognition_onnx_path.clone(),
                text_detection_onnx_path.clone(),
                character_dict_txt_path.clone(),
                document_orientation_onnx_path.clone(),
                text_line_orientation_onnx_path.clone(),
                document_rectification_onnx_path.clone(),
            ),
        };

        use oar_ocr::oarocr::OAROCRBuilder;
        use oar_ocr::utils::load_image;

        let image_paths = images.to_vec();

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

    pub async fn embed_image(
        backend_config: &ImageEmbedBackendConfig,
        image: &Path,
        mime: Option<&str>,
    ) -> Res<EmbedResult> {
        let (onnx_path, preprocessor_config_path, model_id) = match backend_config {
            ImageEmbedBackendConfig::LocalFastembed {
                onnx_path,
                preprocessor_config_path,
                model_id,
            } => (
                onnx_path.clone(),
                preprocessor_config_path.clone(),
                model_id.clone(),
            ),
        };

        fn sniff_extension(bytes: &[u8]) -> Option<&'static str> {
            if bytes.len() >= 3 && bytes[..3] == [0xFF, 0xD8, 0xFF] {
                return Some("jpg");
            }
            if bytes.len() >= 8 && bytes[..8] == [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1A, b'\n']
            {
                return Some("png");
            }
            if bytes.len() >= 6 && (&bytes[..6] == b"GIF87a" || &bytes[..6] == b"GIF89a") {
                return Some("gif");
            }
            if bytes.len() >= 2 && &bytes[..2] == b"BM" {
                return Some("bmp");
            }
            if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
                return Some("webp");
            }
            None
        }

        fn extension_from_mime(mime: &str) -> Option<&'static str> {
            match mime.split(';').next().map(str::trim) {
                Some("image/jpeg" | "image/jpg") => Some("jpg"),
                Some("image/png") => Some("png"),
                Some("image/gif") => Some("gif"),
                Some("image/bmp") => Some("bmp"),
                Some("image/webp") => Some("webp"),
                _ => None,
            }
        }

        let image_path = image.to_path_buf();
        let image_mime = mime.map(str::to_string);
        if !image_path.exists() {
            eyre::bail!("missing image file: {}", image_path.display());
        }
        for required_path in [&onnx_path, &preprocessor_config_path] {
            if !required_path.exists() {
                eyre::bail!(
                    "missing image embedding model file: {}",
                    required_path.display()
                );
            }
        }

        tokio::task::spawn_blocking(move || -> Res<EmbedResult> {
            use fastembed::{
                ImageEmbedding, ImageInitOptionsUserDefined, UserDefinedImageEmbeddingModel,
            };

            let user_model = UserDefinedImageEmbeddingModel::new(
                std::fs::read(&onnx_path)
                    .wrap_err_with(|| format!("failed reading {}", onnx_path.display()))?,
                std::fs::read(&preprocessor_config_path).wrap_err_with(|| {
                    format!("failed reading {}", preprocessor_config_path.display())
                })?,
            );

            let mut embedder = ImageEmbedding::try_new_from_user_defined(
                user_model,
                ImageInitOptionsUserDefined::new(),
            )
            .map_err(|err| eyre::eyre!("failed to initialize image embed model: {err}"))?;

            // Blob store paths usually have no extension. fastembed's image loader relies on
            // extension-based format selection for some paths, so create a temporary suffixed copy.
            let mut temp_image_path: Option<std::path::PathBuf> = None;
            let image_path_for_embed = if image_path.extension().is_some() {
                image_path.clone()
            } else {
                let image_bytes = std::fs::read(&image_path).wrap_err_with(|| {
                    format!("failed reading image bytes {}", image_path.display())
                })?;
                let ext = image_mime
                    .as_deref()
                    .and_then(extension_from_mime)
                    .or_else(|| sniff_extension(&image_bytes))
                    .ok_or_else(|| {
                        eyre::eyre!("unsupported image format for {}", image_path.display())
                    })?;
                let now = jiff::Timestamp::now();
                let unique = format!("{}-{}", now.as_second(), now.subsec_nanosecond());
                let path = std::env::temp_dir().join(format!(
                    "mltools-fastembed-image-{}-{}.{}",
                    std::process::id(),
                    unique,
                    ext
                ));
                std::fs::write(&path, image_bytes)
                    .wrap_err_with(|| format!("failed writing temp image {}", path.display()))?;
                temp_image_path = Some(path.clone());
                path
            };

            let mut vectors = embedder
                .embed(vec![image_path_for_embed], None)
                .map_err(|err| eyre::eyre!("failed to embed image: {err}"))?;
            if let Some(path) = temp_image_path {
                let _ = std::fs::remove_file(path);
            }
            let Some(vector) = vectors.pop() else {
                eyre::bail!("image embedding backend returned no vector");
            };
            let dimensions = vector.len() as u32;

            Ok(EmbedResult {
                vector,
                dimensions,
                model_id,
            })
        })
        .await
        .wrap_err("image embed task failed to join")?
    }
}

/// client for cloud token providers.
mod cloud {
    use super::*;

    pub async fn embed_text(backend_config: &EmbedBackendConfig, text: &str) -> Res<EmbedResult> {
        let (model, auth, provider) = match backend_config {
            EmbedBackendConfig::CloudOllama { model, auth, .. } => {
                (model, auth, genai::adapter::AdapterKind::Ollama)
            }
            EmbedBackendConfig::CloudGemini { model, auth } => {
                (model, auth, genai::adapter::AdapterKind::Gemini)
            }
            _ => eyre::bail!("unsupported cloud embed backend"),
        };

        if let EmbedBackendConfig::CloudOllama { url, .. } = backend_config {
            std::env::set_var("OLLAMA_HOST", url);
        }

        let mut embed_options = genai::embed::EmbedOptions::default();
        if let Some(auth) = auth {
            match auth {
                CloudAuth::ApiKey { key } => {
                    std::env::set_var("GEMINI_API_KEY", key);
                }
                CloudAuth::Basic { username, password } => {
                    let mut token = "Basic ".to_string();
                    data_encoding::BASE64.encode_append(
                        format!("{username}:{password}").as_bytes(),
                        &mut token,
                    );
                    embed_options = embed_options.with_headers(genai::Headers::from([(
                        "Authorization",
                        token,
                    )]));
                }
            }
        }

        let client = genai::Client::default();
        let model_iden = genai::ModelIden::new(provider, model);

        let res = client
            .embed(&model_iden, text, Some(&embed_options))
            .await
            .map_err(|e| eyre::eyre!("genai embedding error: {e:?}"))?;

        let embedding = res
            .embeddings
            .into_iter()
            .next()
            .ok_or_eyre("no embeddings returned")?;

        Ok(EmbedResult {
            dimensions: embedding.dimensions as u32,
            vector: embedding.vector,
            model_id: model.to_string(),
        })
    }

    pub async fn llm_chat(backend_config: &LlmBackendConfig, text: &str) -> Res<LlmChatResult> {
        let (model, auth, provider) = match backend_config {
            LlmBackendConfig::CloudOllama { model, auth, .. } => {
                (model, auth, genai::adapter::AdapterKind::Ollama)
            }
            LlmBackendConfig::CloudGemini { model, auth } => {
                (model, auth, genai::adapter::AdapterKind::Gemini)
            }
        };

        if let LlmBackendConfig::CloudOllama { url, .. } = backend_config {
            std::env::set_var("OLLAMA_HOST", url);
        }

        let mut chat_options = genai::chat::ChatOptions::default();
        if let Some(auth) = auth {
            match auth {
                CloudAuth::ApiKey { key } => {
                    std::env::set_var("GEMINI_API_KEY", key);
                }
                CloudAuth::Basic { username, password } => {
                    let mut token = "Basic ".to_string();
                    data_encoding::BASE64.encode_append(
                        format!("{username}:{password}").as_bytes(),
                        &mut token,
                    );
                    chat_options = chat_options.with_extra_headers(genai::Headers::from([(
                        "Authorization",
                        token,
                    )]));
                }
            }
        }

        let client = genai::Client::default();

        let chat_req = genai::chat::ChatRequest::new(vec![genai::chat::ChatMessage::user(
            text.to_string(),
        )]);

        let model_iden = genai::ModelIden::new(provider, model);
        let response = client
            .exec_chat(&model_iden, chat_req, Some(&chat_options))
            .await
            .map_err(|e| eyre::eyre!("genai chat error: {e:?}"))?;

        Ok(LlmChatResult {
            text: response
                .first_text()
                .unwrap_or_default()
                .to_string(),
        })
    }
}

/// durable-streams based API for mltools_server or mltools_cloud.
mod gateway {}
/// mltools_local but for servers.
mod server {}
/// routes to mltools_local, mltools_client or mltools_cloud depending on config.
mod router {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ollama_url() -> String {
        std::env::var("OLLAMA_URL").expect("OLLAMA_URL must be set for cloud mltools tests")
    }

    #[cfg(feature = "hf_hub")]
    fn test_model_cache_dir() -> PathBuf {
        crate::models::test_cache_dir()
    }

    fn context_with(
        ocr_backends: Vec<OcrBackendConfig>,
        embed_backends: Vec<EmbedBackendConfig>,
        llm_backends: Vec<LlmBackendConfig>,
    ) -> Ctx {
        Ctx {
            config: Config {
                ocr: OcrConfig {
                    backends: ocr_backends,
                },
                embed: EmbedConfig {
                    backends: embed_backends,
                },
                image_embed: ImageEmbedConfig::default(),
                llm: LlmConfig {
                    backends: llm_backends,
                },
            },
        }
    }

    fn expect_error_message_contains(result: Res<impl Sized>, expected_fragment: &str) {
        let error = match result {
            Ok(_) => panic!("expected error"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains(expected_fragment),
            "expected error to contain '{expected_fragment}', got '{message}'",
        );
    }

    utils_rs::table_tests! {
        test_embed_text_api_contract,
        (context, text, expected_error_fragment),
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed building tokio runtime");
            let result = runtime.block_on(async { embed_text(&context, text).await });
            expect_error_message_contains(result, expected_error_fragment);
        }
    }

    test_embed_text_api_contract! {
        rejects_empty_input: (
            context_with(vec![], vec![], vec![]),
            "   ",
            "empty input text",
        ),
        rejects_missing_backend: (
            context_with(vec![], vec![], vec![]),
            "hello",
            "no embed backend configured",
        ),
    }

    utils_rs::table_tests! {
        test_llm_chat_api_contract,
        (context, text, expected_error_fragment),
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed building tokio runtime");
            let result = runtime.block_on(async { llm_chat(&context, text).await });
            expect_error_message_contains(result, expected_error_fragment);
        }
    }

    test_llm_chat_api_contract! {
        rejects_empty_input: (
            context_with(vec![], vec![], vec![]),
            "",
            "empty llm input text",
        ),
        rejects_missing_backend: (
            context_with(vec![], vec![], vec![]),
            "hello",
            "no llm backend configured",
        ),
    }

    utils_rs::table_tests! {
        test_ocr_image_api_contract,
        (context, image_paths, expected_error_fragment),
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed building tokio runtime");
            let result = runtime.block_on(async { ocr_image(&context, &image_paths).await });
            expect_error_message_contains(result, expected_error_fragment);
        }
    }

    test_ocr_image_api_contract! {
        rejects_missing_backend: (
            context_with(vec![], vec![], vec![]),
            vec![PathBuf::from("/tmp/does_not_matter.jpg")],
            "no ocr backend configured",
        ),
        rejects_empty_image_list: (
            context_with(
                vec![OcrBackendConfig::LocalOnnx {
                    text_recognition_onnx_path: PathBuf::from("unused"),
                    text_detection_onnx_path: PathBuf::from("unused"),
                    character_dict_txt_path: PathBuf::from("unused"),
                    document_orientation_onnx_path: None,
                    text_line_orientation_onnx_path: None,
                    document_rectification_onnx_path: None,
                    supported_languages_bcp47: vec!["en".to_string()],
                }],
                vec![],
                vec![],
            ),
            Vec::<PathBuf>::new(),
            "no images provided",
        ),
    }

    #[test]
    fn test_embed_image_api_contract_rejects_missing_backend() -> Res<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let context = context_with(vec![], vec![], vec![]);
        let image_path = PathBuf::from("/tmp/does_not_matter.jpg");
        let result = runtime.block_on(async { embed_image(&context, &image_path, None).await });
        expect_error_message_contains(result, "no image embed backend configured");
        Ok(())
    }

    #[test]
    fn test_embed_text_cloud_router_roundtrip() -> Res<()> {
        let embed_model_name =
            std::env::var("OLLAMA_EMBED_MODEL").unwrap_or_else(|_| "embeddinggemma".to_string());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let context = context_with(
                vec![],
                vec![EmbedBackendConfig::CloudOllama {
                    url: test_ollama_url(),
                    model: embed_model_name.clone(),
                    auth: Some(crate::CloudAuth::Basic {
                        username: crate::models::OLLAMA_USERNAME.to_string(),
                        password: crate::models::OLLAMA_PASSWORD.to_string(),
                    }),
                }],
                vec![],
            );
            let result = embed_text(&context, "cloud embedding smoke test").await?;
            assert!(!result.vector.is_empty());
            assert!(result.dimensions > 0);
            assert_eq!(result.dimensions as usize, result.vector.len());
            assert_eq!(result.model_id, embed_model_name);

            Ok(())
        })
    }

    #[test]
    fn test_llm_chat_cloud_router_roundtrip() -> Res<()> {
        let llm_model_name =
            std::env::var("OLLAMA_LLM_MODEL").unwrap_or_else(|_| "gemma3".to_string());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(async {
            let context = context_with(
                vec![],
                vec![],
                vec![LlmBackendConfig::CloudOllama {
                    url: test_ollama_url(),
                    model: llm_model_name,
                    auth: Some(crate::CloudAuth::Basic {
                        username: crate::models::OLLAMA_USERNAME.to_string(),
                        password: crate::models::OLLAMA_PASSWORD.to_string(),
                    }),
                }],
            );

            let result = llm_chat(&context, "reply with one short word").await?;
            assert!(!result.text.trim().is_empty());

            Ok(())
        })
    }

    #[test]
    #[ignore = "requires GEMINI_API_KEY"]
    fn test_embed_text_gemini_roundtrip() -> Res<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY must be set");
            let context = context_with(
                vec![],
                vec![EmbedBackendConfig::CloudGemini {
                    model: "gemini-embedding-001".to_string(),
                    auth: Some(crate::CloudAuth::ApiKey { key }),
                }],
                vec![],
            );
            let result = embed_text(&context, "gemini embedding smoke test").await?;
            assert!(!result.vector.is_empty());
            assert!(result.dimensions > 0);
            assert_eq!(result.dimensions as usize, result.vector.len());
            assert_eq!(result.model_id, "gemini-embedding-001");

            Ok(())
        })
    }

    #[test]
    #[ignore = "requires GEMINI_API_KEY"]
    fn test_llm_chat_gemini_roundtrip() -> Res<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(async {
            let key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY must be set");
            let context = context_with(
                vec![],
                vec![],
                vec![LlmBackendConfig::CloudGemini {
                    model: "gemini-flash-latest".to_string(),
                    auth: Some(crate::CloudAuth::ApiKey { key }),
                }],
            );

            let result = llm_chat(&context, "reply with one short word").await?;
            assert!(!result.text.trim().is_empty());

            Ok(())
        })
    }

    #[cfg(feature = "hf_hub")]
    #[test]
    #[ignore = "downloads model assets from remote registries"]
    fn test_mobile_default_provisions_models() -> Res<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let config = crate::models::mobile_default(test_model_cache_dir()).await?;

            assert_eq!(config.ocr.backends.len(), 1);
            assert_eq!(config.embed.backends.len(), 2);
            assert_eq!(config.image_embed.backends.len(), 1);
            assert_eq!(config.llm.backends.len(), 1);

            let EmbedBackendConfig::LocalFastembed {
                onnx_path,
                tokenizer_path,
                config_path,
                special_tokens_map_path,
                tokenizer_config_path,
                model_id,
            } = &config.embed.backends[0]
            else {
                panic!("expected local user-defined embed backend");
            };

            assert_eq!(model_id, "nomic-ai/nomic-embed-text-v1.5");
            assert!(onnx_path.exists());
            assert!(tokenizer_path.exists());
            assert!(config_path.exists());
            assert!(special_tokens_map_path.exists());
            assert!(tokenizer_config_path.exists());

            let ImageEmbedBackendConfig::LocalFastembed {
                onnx_path,
                preprocessor_config_path,
                model_id,
            } = &config.image_embed.backends[0];

            assert_eq!(model_id, "nomic-ai/nomic-embed-vision-v1.5");
            assert!(onnx_path.exists());
            assert!(preprocessor_config_path.exists());

            Ok(())
        })
    }
}
