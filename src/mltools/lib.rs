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
    CloudOllama { url: String, model: String },
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
        EmbedBackendConfig::CloudOllama { url, model } => {
            cloud::embed_text_ollama(url, model, text).await
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
        LlmBackendConfig::CloudOllama { url, model } => {
            cloud::llm_chat_ollama(url, model, text).await
        }
    }
}

/// local execution of ML tools.
mod local {
    use super::*;

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
            EmbedBackendConfig::CloudOllama { .. } => {
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
                InitOptionsUserDefined, QuantizationMode, TextEmbedding, TokenizerFiles,
                UserDefinedEmbeddingModel,
            };

            let user_model = UserDefinedEmbeddingModel::new(
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
}

/// client for cloud token providers.
mod cloud {
    use super::*;

    pub async fn embed_text_ollama(url: &str, model: &str, text: &str) -> Res<EmbedResult> {
        let parsed_url =
            url::Url::parse(url).wrap_err_with(|| format!("invalid Ollama url: {url}"))?;
        let host = parsed_url
            .host_str()
            .ok_or_eyre("Ollama url missing host")?;
        let scheme = parsed_url.scheme();
        let port = parsed_url.port().unwrap_or(11434);

        let ollama = ollama_rs::Ollama::new(format!("{scheme}://{host}"), port);
        use ollama_rs::generation::embeddings::request::GenerateEmbeddingsRequest;

        let request = GenerateEmbeddingsRequest::new(model.to_owned(), text.into());
        let mut response = ollama
            .generate_embeddings(request)
            .await
            .map_err(|error| eyre::eyre!("ollama embedding error: {error}"))?;
        if response.embeddings.is_empty() {
            eyre::bail!("ollama embedding response is empty");
        }
        let vector = response.embeddings.swap_remove(0);
        if vector.is_empty() {
            eyre::bail!("ollama embedding vector is empty");
        }

        Ok(EmbedResult {
            dimensions: vector.len() as u32,
            vector,
            model_id: model.to_string(),
        })
    }

    pub async fn llm_chat_ollama(url: &str, model: &str, text: &str) -> Res<LlmChatResult> {
        let parsed_url =
            url::Url::parse(url).wrap_err_with(|| format!("invalid Ollama url: {url}"))?;
        let host = parsed_url
            .host_str()
            .ok_or_eyre("Ollama url missing host")?;
        let scheme = parsed_url.scheme();
        let port = parsed_url.port().unwrap_or(11434);

        let ollama = ollama_rs::Ollama::new(format!("{scheme}://{host}"), port);
        use ollama_rs::generation::completion::request::GenerationRequest;

        let generation_request = GenerationRequest::new(model.to_owned(), text.to_owned());
        let response = ollama
            .generate(generation_request)
            .await
            .map_err(|error| eyre::eyre!("ollama error: {error}"))?;

        Ok(LlmChatResult {
            text: response.response,
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

            Ok(())
        })
    }
}
