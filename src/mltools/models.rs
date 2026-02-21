use crate::{
    Config, EmbedBackendConfig, EmbedConfig, LlmBackendConfig, LlmConfig, OcrBackendConfig,
    OcrConfig,
};
use fs4::fs_std::FileExt;
use hf_hub::{api::tokio::ApiBuilder, Cache};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use utils_rs::downloader::Downloader;
use utils_rs::prelude::*;

const OLLAMA_URL_DEFAULT: &str = env!("OLLAMA_URL");
const OLLAMA_USERNAME: &str = env!("OLLAMA_USERNAME");
const OLLAMA_PASSWORD: &str = env!("OLLAMA_PASSWORD");
const OLLAMA_EMBED_MODEL_DEFAULT: &str = "embeddinggemma";
const OLLAMA_LLM_MODEL_DEFAULT: &str = "gemma3";
const NOMIC_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const OAR_RELEASE_BASE_URL: &str = "https://github.com/GreatV/oar-ocr/releases/download/v0.3.0";

#[derive(Debug, Clone)]
pub enum MobileDefaultEvent {
    DownloadStarted {
        source: String,
        file: String,
    },
    DownloadProgress {
        source: String,
        file: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    DownloadCompleted {
        source: String,
        file: String,
    },
    DownloadFailed {
        source: String,
        file: String,
        message: String,
    },
}

#[derive(Clone)]
pub struct MobileDefaultObserver {
    on_event: Arc<dyn Fn(MobileDefaultEvent) + Send + Sync>,
}

impl MobileDefaultObserver {
    pub fn new<F>(on_event: F) -> Self
    where
        F: Fn(MobileDefaultEvent) + Send + Sync + 'static,
    {
        Self {
            on_event: Arc::new(on_event),
        }
    }

    fn emit(&self, event: MobileDefaultEvent) {
        (self.on_event)(event);
    }
}

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

async fn download_url_to_path_with_observer(
    url: &str,
    output_path: &Path,
    observer: Option<&MobileDefaultObserver>,
) -> Res<()> {
    let source = "oar".to_string();
    let file = output_path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| output_path.display().to_string());

    if output_path.exists() {
        if let Some(observer) = observer {
            observer.emit(MobileDefaultEvent::DownloadCompleted { source, file });
        }
        return Ok(());
    }

    let downloader = utils_rs::downloader::ReqwestRangeDownloader::new()?;
    let download_observer = observer.map(|observer| {
        let observer = observer.clone();
        let source = source.clone();
        let file = file.clone();
        utils_rs::downloader::DownloadObserver::new(move |event| match event {
            utils_rs::downloader::DownloadEvent::Started => {
                observer.emit(MobileDefaultEvent::DownloadStarted {
                    source: source.clone(),
                    file: file.clone(),
                });
            }
            utils_rs::downloader::DownloadEvent::Progress(progress) => {
                observer.emit(MobileDefaultEvent::DownloadProgress {
                    source: source.clone(),
                    file: file.clone(),
                    downloaded_bytes: progress.downloaded_bytes,
                    total_bytes: progress.total_bytes,
                });
            }
            utils_rs::downloader::DownloadEvent::Completed => {
                observer.emit(MobileDefaultEvent::DownloadCompleted {
                    source: source.clone(),
                    file: file.clone(),
                });
            }
            utils_rs::downloader::DownloadEvent::Failed { message } => {
                observer.emit(MobileDefaultEvent::DownloadFailed {
                    source: source.clone(),
                    file: file.clone(),
                    message,
                });
            }
        })
    });
    downloader
        .download_to_path(
            &utils_rs::downloader::DownloadRequest {
                url: url.to_string(),
                output_path: output_path.to_path_buf(),
            },
            download_observer.as_ref(),
        )
        .await?;

    Ok(())
}

/// Downloads mobile-friendly OCR + embedding model artifacts and returns a ready-to-use config.
pub async fn mobile_default(download_dir: impl AsRef<Path>) -> Res<Config> {
    mobile_default_with_observer(download_dir, None).await
}

#[derive(Clone)]
struct HfHubProgress {
    observer: Option<MobileDefaultObserver>,
    source: String,
    file: String,
    downloaded_bytes: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
}

impl HfHubProgress {
    fn new(observer: Option<MobileDefaultObserver>, file: impl Into<String>) -> Self {
        Self {
            observer,
            source: "hf-hub".to_string(),
            file: file.into(),
            downloaded_bytes: Arc::new(AtomicU64::new(0)),
            total_bytes: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl hf_hub::api::tokio::Progress for HfHubProgress {
    async fn init(&mut self, size: usize, _filename: &str) {
        self.downloaded_bytes.store(0, Ordering::Relaxed);
        self.total_bytes.store(size as u64, Ordering::Relaxed);
        if let Some(observer) = &self.observer {
            observer.emit(MobileDefaultEvent::DownloadStarted {
                source: self.source.clone(),
                file: self.file.clone(),
            });
            observer.emit(MobileDefaultEvent::DownloadProgress {
                source: self.source.clone(),
                file: self.file.clone(),
                downloaded_bytes: 0,
                total_bytes: Some(size as u64),
            });
        }
    }

    async fn update(&mut self, size: usize) {
        let downloaded = self
            .downloaded_bytes
            .fetch_add(size as u64, Ordering::Relaxed)
            + size as u64;
        let total = self.total_bytes.load(Ordering::Relaxed);
        if let Some(observer) = &self.observer {
            observer.emit(MobileDefaultEvent::DownloadProgress {
                source: self.source.clone(),
                file: self.file.clone(),
                downloaded_bytes: downloaded,
                total_bytes: Some(total),
            });
        }
    }

    async fn finish(&mut self) {
        if let Some(observer) = &self.observer {
            observer.emit(MobileDefaultEvent::DownloadCompleted {
                source: self.source.clone(),
                file: self.file.clone(),
            });
        }
    }
}

async fn hf_download_with_progress(
    model_repo: &hf_hub::api::tokio::ApiRepo,
    file: &str,
    observer: Option<&MobileDefaultObserver>,
) -> Res<PathBuf> {
    let progress = HfHubProgress::new(observer.cloned(), file.to_string());
    let result = model_repo
        .download_with_progress(file, progress)
        .await
        .wrap_err_with(|| format!("error downloading {file} from {NOMIC_MODEL_ID}"));
    if let Err(err) = &result {
        if let Some(observer) = observer {
            observer.emit(MobileDefaultEvent::DownloadFailed {
                source: "hf-hub".to_string(),
                file: file.to_string(),
                message: format!("{err:?}"),
            });
        }
    }
    result
}

/// Downloads mobile-friendly OCR + embedding model artifacts and returns a ready-to-use config.
pub async fn mobile_default_with_observer(
    download_dir: impl AsRef<Path>,
    observer: Option<&MobileDefaultObserver>,
) -> Res<Config> {
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

    download_url_to_path_with_observer(
        &format!("{OAR_RELEASE_BASE_URL}/pp-ocrv5_mobile_det.onnx"),
        &det_model_path,
        observer,
    )
    .await?;
    download_url_to_path_with_observer(
        &format!("{OAR_RELEASE_BASE_URL}/pp-ocrv5_mobile_rec.onnx"),
        &rec_model_path,
        observer,
    )
    .await?;
    download_url_to_path_with_observer(
        &format!("{OAR_RELEASE_BASE_URL}/ppocrv5_dict.txt"),
        &dict_path,
        observer,
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

    let onnx_path =
        hf_download_with_progress(&model_repo, "onnx/model_quantized.onnx", observer).await?;
    let tokenizer_path = hf_download_with_progress(&model_repo, "tokenizer.json", observer).await?;
    let config_path = hf_download_with_progress(&model_repo, "config.json", observer).await?;
    let special_tokens_map_path =
        hf_download_with_progress(&model_repo, "special_tokens_map.json", observer).await?;
    let tokenizer_config_path =
        hf_download_with_progress(&model_repo, "tokenizer_config.json", observer).await?;

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
                auth: Some(crate::CloudAuth::Basic {
                    username: OLLAMA_USERNAME.to_string(),
                    password: OLLAMA_PASSWORD.to_string(),
                }),
            }],
        },
    })
}

#[cfg(any(test, feature = "tests"))]
pub fn test_cache_dir() -> PathBuf {
    fn is_writable_dir(path: &Path) -> bool {
        if std::fs::create_dir_all(path).is_err() {
            return false;
        }
        let probe_path = path.join(".write_probe");
        let create_res = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&probe_path);
        match create_res {
            Ok(_) => {
                let _ = std::fs::remove_file(probe_path);
                true
            }
            Err(_) => false,
        }
    }

    if let Some(base_dirs) = directories::BaseDirs::new() {
        let preferred_path = base_dirs
            .cache_dir()
            .join("daybook-tests/mltools/mobile_default");
        if is_writable_dir(&preferred_path) {
            return preferred_path;
        }
    }
    let fallback_path = std::env::temp_dir().join("daybook-tests/mltools/mobile_default");
    assert!(
        is_writable_dir(&fallback_path),
        "failed to create writable fallback mltools test cache at {}",
        fallback_path.display()
    );
    fallback_path
}
