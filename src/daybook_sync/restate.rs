use crate::interlude::*;

pub struct RestateCtx {
    http: reqwest::Client,
}

impl RestateCtx {
    pub fn new() -> Res<Self> {
        Ok(Self {
            http: reqwest::Client::builder().build()?,
        })
    }
}
const DOCS_PIPELINE_PATH: &str = "DocsPipeline";

#[derive(Debug, displaydoc::Display, thiserror::Error)]
pub enum RestateError {
    /// http error {0}
    HttpError(#[from] reqwest::Error),
    /// request error {status} {body:?}
    RequestError {
        status: reqwest::StatusCode,
        headers: reqwest::header::HeaderMap,
        body: Option<String>,
    },
}

pub async fn start_doc_pipeline(
    cx: &Ctx,
    ev: &crate::gen::doc::DocAddedEvent,
) -> Result<(), RestateError> {
    let mut res = cx
        .rcx
        .http
        .post(
            cx.config
                .restate_base_url
                .join(&format!("/{DOCS_PIPELINE_PATH}/{id}/run", id = &ev.id))
                .expect("url error"),
        )
        .json(&ev)
        .send()
        .await
        .map_err(RestateError::HttpError)?;

    let status = res.status();
    if !status.is_success() {
        let headers = std::mem::take(res.headers_mut());
        let body = res.text().await.ok();
        return Err(RestateError::RequestError {
            status,
            headers,
            body,
        });
    }
    Ok(())
}
