use crate::{
    Config, EmbedBackendConfig, EmbedConfig, LlmBackendConfig, LlmConfig, OcrBackendConfig,
    OcrConfig,
};
use fs4::fs_std::FileExt;
use hf_hub::{api::tokio::ApiBuilder, Cache};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use utils_rs::prelude::*;

const OLLAMA_URL_DEFAULT: &str = "http://localhost:11434";
const OLLAMA_EMBED_MODEL_DEFAULT: &str = "embeddinggemma";
const OLLAMA_LLM_MODEL_DEFAULT: &str = "gemma3";
const NOMIC_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const OAR_RELEASE_BASE_URL: &str = "https://github.com/GreatV/oar-ocr/releases/download/v0.3.0";

struct DownloadLockGuard {
    file: std::fs::File,
}

impl Drop for DownloadLockGuard {
    fn drop(&mut self) {
        self.file.unlock().unwrap_or_log();
    }
}

fn acquire_download_lock(download_dir: &Path) -> Res<DownloadLockGuard> {
    let lock_path = download_dir.join(".download.lock");
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .wrap_err_with(|| format!("error opening lock file {}", lock_path.display()))?;

    lock_file
        .lock_exclusive()
        .wrap_err_with(|| format!("error acquiring download lock for {}", lock_path.display()))?;

    Ok(DownloadLockGuard { file: lock_file })
}

async fn download_url_to_path(url: &str, output_path: &Path) -> Res<()> {
    if output_path.exists() {
        return Ok(());
    }

    let output_path = output_path.to_path_buf();
    let url = url.to_string();
    tokio::task::spawn_blocking(move || -> Res<()> {
        let response = ureq::get(&url)
            .call()
            .map_err(|error| eyre::eyre!("error downloading {url}: {error}"))?;

        let mut response = response.into_body();
        let mut bytes = response
            .read_to_vec()
            .map_err(|error| eyre::eyre!("error reading response body from {url}: {error}"))?;

        let parent_dir = output_path
            .parent()
            .ok_or_else(|| eyre::eyre!("output path has no parent: {}", output_path.display()))?;
        std::fs::create_dir_all(parent_dir)
            .wrap_err_with(|| format!("error creating directory {}", parent_dir.display()))?;

        let tmp_path = output_path.with_extension("part");
        {
            let mut tmp_file = std::fs::File::create(&tmp_path)
                .wrap_err_with(|| format!("error creating {}", tmp_path.display()))?;
            tmp_file
                .write_all(&bytes)
                .wrap_err_with(|| format!("error writing {}", tmp_path.display()))?;
            tmp_file
                .flush()
                .wrap_err_with(|| format!("error flushing {}", tmp_path.display()))?;
        }
        std::fs::rename(&tmp_path, &output_path).wrap_err_with(|| {
            format!(
                "error renaming {} to {}",
                tmp_path.display(),
                output_path.display()
            )
        })?;

        bytes.clear();
        Ok(())
    })
    .await
    .wrap_err("url download task failed to join")??;

    Ok(())
}

/// Downloads mobile-friendly OCR + embedding model artifacts and returns a ready-to-use config.
pub async fn mobile_default(download_dir: impl AsRef<Path>) -> Res<Config> {
    let download_dir = download_dir.as_ref().to_path_buf();
    tokio::fs::create_dir_all(&download_dir)
        .await
        .wrap_err_with(|| format!("error creating {}", download_dir.display()))?;

    let _download_lock = acquire_download_lock(&download_dir)?;

    let ocr_dir = download_dir.join("oar-ocr/v0.3.0/mobile");
    tokio::fs::create_dir_all(&ocr_dir)
        .await
        .wrap_err_with(|| format!("error creating {}", ocr_dir.display()))?;

    let det_model_path = ocr_dir.join("pp-ocrv5_mobile_det.onnx");
    let rec_model_path = ocr_dir.join("pp-ocrv5_mobile_rec.onnx");
    let dict_path = ocr_dir.join("ppocrv5_dict.txt");

    download_url_to_path(
        &format!("{OAR_RELEASE_BASE_URL}/pp-ocrv5_mobile_det.onnx"),
        &det_model_path,
    )
    .await?;
    download_url_to_path(
        &format!("{OAR_RELEASE_BASE_URL}/pp-ocrv5_mobile_rec.onnx"),
        &rec_model_path,
    )
    .await?;
    download_url_to_path(
        &format!("{OAR_RELEASE_BASE_URL}/ppocrv5_dict.txt"),
        &dict_path,
    )
    .await?;

    let hf_cache_dir = download_dir.join("hf");
    tokio::fs::create_dir_all(&hf_cache_dir)
        .await
        .wrap_err_with(|| format!("error creating {}", hf_cache_dir.display()))?;
    let api = ApiBuilder::from_cache(Cache::new(hf_cache_dir))
        .with_progress(true)
        .build()?;
    let model_repo = api.model(NOMIC_MODEL_ID.to_string());

    let onnx_path = model_repo.get("onnx/model_quantized.onnx").await?;
    let tokenizer_path = model_repo.get("tokenizer.json").await?;
    let config_path = model_repo.get("config.json").await?;
    let special_tokens_map_path = model_repo.get("special_tokens_map.json").await?;
    let tokenizer_config_path = model_repo.get("tokenizer_config.json").await?;

    Ok(Config {
        ocr: OcrConfig {
            backends: vec![OcrBackendConfig::LocalOnnx {
                text_recognition_onnx_path: rec_model_path,
                text_detection_onnx_path: det_model_path,
                character_dict_txt_path: dict_path,
                document_orientation_onnx_path: None,
                text_line_orientation_onnx_path: None,
                document_rectification_onnx_path: None,
                supported_languages_bcp47: vec!["en".to_string()],
            }],
        },
        embed: EmbedConfig {
            backends: vec![
                EmbedBackendConfig::LocalFastembed {
                    onnx_path,
                    tokenizer_path,
                    config_path,
                    special_tokens_map_path,
                    tokenizer_config_path,
                    model_id: NOMIC_MODEL_ID.to_string(),
                },
                EmbedBackendConfig::CloudOllama {
                    url: OLLAMA_URL_DEFAULT.to_string(),
                    model: OLLAMA_EMBED_MODEL_DEFAULT.to_string(),
                },
            ],
        },
        llm: LlmConfig {
            backends: vec![LlmBackendConfig::CloudOllama {
                url: OLLAMA_URL_DEFAULT.to_string(),
                model: OLLAMA_LLM_MODEL_DEFAULT.to_string(),
            }],
        },
    })
}

#[cfg(any(test, feature = "tests"))]
pub fn test_cache_dir() -> PathBuf {
    if let Some(home_dir) = dirs::home_dir() {
        let preferred_path = home_dir.join(".cache/daybook-tests/mltools/mobile_default");
        if std::fs::create_dir_all(&preferred_path).is_ok() {
            return preferred_path;
        }
    }

    let fallback_path = PathBuf::from("/tmp/daybook-tests/mltools/mobile_default");
    std::fs::create_dir_all(&fallback_path).unwrap_or_log();
    fallback_path
}
