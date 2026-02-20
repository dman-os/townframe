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
        if request.output_path.exists() {
            return Ok(());
        }

        let parent_dir = request.output_path.parent().ok_or_else(|| {
            eyre::eyre!(
                "output path has no parent: {}",
                request.output_path.display()
            )
        })?;
        tokio::fs::create_dir_all(parent_dir)
            .await
            .wrap_err_with(|| format!("error creating {}", parent_dir.display()))?;

        let part_path = request.output_path.with_extension("part");
        let mut resume_from = match tokio::fs::metadata(&part_path).await {
            Ok(meta) => meta.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
            Err(err) => return Err(err.into()),
        };

        if let Some(observer) = observer {
            observer.emit(DownloadEvent::Started);
        }

        let mut req = self.client.get(&request.url);
        if resume_from > 0 {
            let range_header = format!("bytes={resume_from}-");
            req = req.header(
                RANGE,
                HeaderValue::from_str(&range_header)
                    .wrap_err_with(|| format!("invalid range header value: {range_header}"))?,
            );
        }

        let response = req
            .send()
            .await
            .wrap_err_with(|| format!("error downloading {}", request.url))?;

        let status = response.status();
        if !(status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT) {
            eyre::bail!(
                "download request failed with status {status} for {}",
                request.url
            );
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

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(!is_partial)
            .append(is_partial)
            .open(&part_path)
            .await
            .wrap_err_with(|| format!("error opening {}", part_path.display()))?;

        let mut downloaded = resume_from;
        let mut body_stream = response.bytes_stream();
        while let Some(chunk) = body_stream.try_next().await? {
            file.write_all(&chunk)
                .await
                .wrap_err_with(|| format!("error writing {}", part_path.display()))?;
            downloaded += chunk.len() as u64;
            if let Some(observer) = observer {
                observer.emit(DownloadEvent::Progress(DownloadProgress {
                    downloaded_bytes: downloaded,
                    total_bytes,
                }));
            }
        }

        file.flush()
            .await
            .wrap_err_with(|| format!("error flushing {}", part_path.display()))?;
        drop(file);

        tokio::fs::rename(&part_path, &request.output_path)
            .await
            .wrap_err_with(|| {
                format!(
                    "error renaming {} to {}",
                    part_path.display(),
                    request.output_path.display()
                )
            })?;

        if let Some(observer) = observer {
            observer.emit(DownloadEvent::Completed);
        }
        Ok(())
    }
}
