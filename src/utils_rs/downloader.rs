use crate::interlude::*;
use futures::TryStreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, RANGE};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum DownloadEvent {
    Started,
    Progress(DownloadProgress),
    Completed,
    Failed { message: String },
}

#[derive(Clone)]
pub struct DownloadObserver {
    on_event: Arc<dyn Fn(DownloadEvent) + Send + Sync>,
}

impl DownloadObserver {
    pub fn new<F>(on_event: F) -> Self
    where
        F: Fn(DownloadEvent) + Send + Sync + 'static,
    {
        Self {
            on_event: Arc::new(on_event),
        }
    }

    fn emit(&self, event: DownloadEvent) {
        (self.on_event)(event);
    }
}

#[async_trait]
pub trait Downloader: Send + Sync {
    async fn download_to_path(
        &self,
        request: &DownloadRequest,
        observer: Option<&DownloadObserver>,
    ) -> Res<()>;
}

pub struct ReqwestRangeDownloader {
    client: reqwest::Client,
}

impl ReqwestRangeDownloader {
    pub fn new() -> Res<Self> {
        let client = reqwest::Client::builder().build()?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Downloader for ReqwestRangeDownloader {
    async fn download_to_path(
        &self,
        request: &DownloadRequest,
        observer: Option<&DownloadObserver>,
    ) -> Res<()> {
        async fn emit_failed_and_err(
            observer: Option<&DownloadObserver>,
            err: eyre::Report,
        ) -> Res<()> {
            if let Some(observer) = observer {
                observer.emit(DownloadEvent::Failed {
                    message: format!("{err:#}"),
                });
            }
            Err(err)
        }

        if request.output_path.exists() {
            return Ok(());
        }

        let parent_dir = request.output_path.parent().ok_or_else(|| {
            eyre::eyre!(
                "output path has no parent: {}",
                request.output_path.display()
            )
        })?;
        if let Err(err) = tokio::fs::create_dir_all(parent_dir)
            .await
            .wrap_err_with(|| format!("error creating {}", parent_dir.display()))
        {
            return emit_failed_and_err(observer, err).await;
        }

        let part_path = request.output_path.with_extension("part");
        let mut resume_from = match tokio::fs::metadata(&part_path).await {
            Ok(meta) => meta.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
            Err(err) => return emit_failed_and_err(observer, err.into()).await,
        };

        if let Some(observer) = observer {
            observer.emit(DownloadEvent::Started);
        }

        let mut req = self.client.get(&request.url);
        if resume_from > 0 {
            let range_header = format!("bytes={resume_from}-");
            let header_value = match HeaderValue::from_str(&range_header)
                .wrap_err_with(|| format!("invalid range header value: {range_header}"))
            {
                Ok(value) => value,
                Err(err) => return emit_failed_and_err(observer, err).await,
            };
            req = req.header(RANGE, header_value);
        }

        let response = match req
            .send()
            .await
            .wrap_err_with(|| format!("error downloading {}", request.url))
        {
            Ok(response) => response,
            Err(err) => return emit_failed_and_err(observer, err).await,
        };

        let status = response.status();
        if !(status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT) {
            return emit_failed_and_err(
                observer,
                eyre::eyre!(
                    "download request failed with status {status} for {}",
                    request.url
                ),
            )
            .await;
        }

        let is_partial = status == reqwest::StatusCode::PARTIAL_CONTENT;
        if !is_partial && resume_from > 0 {
            resume_from = 0;
        }

        let total_bytes = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(|len| if is_partial { len + resume_from } else { len });

        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(!is_partial)
            .append(is_partial)
            .open(&part_path)
            .await
            .wrap_err_with(|| format!("error opening {}", part_path.display()))
        {
            Ok(file) => file,
            Err(err) => return emit_failed_and_err(observer, err).await,
        };

        let mut downloaded = resume_from;
        let mut body_stream = response.bytes_stream();
        while let Some(chunk) = match body_stream.try_next().await {
            Ok(chunk) => chunk,
            Err(err) => return emit_failed_and_err(observer, err.into()).await,
        } {
            if let Err(err) = file
                .write_all(&chunk)
                .await
                .wrap_err_with(|| format!("error writing {}", part_path.display()))
            {
                return emit_failed_and_err(observer, err).await;
            }
            downloaded += chunk.len() as u64;
            if let Some(observer) = observer {
                observer.emit(DownloadEvent::Progress(DownloadProgress {
                    downloaded_bytes: downloaded,
                    total_bytes,
                }));
            }
        }

        if let Err(err) = file
            .flush()
            .await
            .wrap_err_with(|| format!("error flushing {}", part_path.display()))
        {
            return emit_failed_and_err(observer, err).await;
        }
        drop(file);

        if let Err(err) = tokio::fs::rename(&part_path, &request.output_path)
            .await
            .wrap_err_with(|| {
                format!(
                    "error renaming {} to {}",
                    part_path.display(),
                    request.output_path.display()
                )
            })
        {
            return emit_failed_and_err(observer, err).await;
        }

        if let Some(observer) = observer {
            observer.emit(DownloadEvent::Completed);
        }
        Ok(())
    }
}
