use crate::interlude::*;

use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::DaybookPlugin;
use super::binds_guest;
use super::capabilities;
use super::mltools_embed;
use super::mltools_image_tools;
use super::mltools_llm_chat;
use super::mltools_ocr;

async fn mltools_ctx_from_config_repo(plugin: &DaybookPlugin) -> mltools::Ctx {
    mltools::Ctx {
        config: plugin.config_repo.get_mltools_config().await,
    }
}

impl mltools_ocr::Host for SharedWashCtx {}

impl<T: 'static> mltools_ocr::HostWithStore<T> for wasmtime::component::HasSelf<SharedWashCtx> {
    fn ocr_image(
        accessor: &wasmtime::component::Accessor<T, Self>,
        blob_facet: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> impl std::future::Future<Output = wasmtime::Result<Result<mltools_ocr::OcrResult, String>>> + Send
    {
        async move {
            let (plugin, token) = super::caps::with_host_ctx(accessor, |ctx| {
                let token = ctx
                    .table
                    .get(&blob_facet)
                    .map_err(|err| {
                        wasmtime::Error::msg(format!("error locating facet token: {err}"))
                    })?
                    .clone();
                Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
            })?;
            let blob = match super::caps::get_blob_facet_from_owned_token(&plugin, &token).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            let image_path =
                match super::caps::resolve_blob_path_from_blob_facet(&plugin, &blob).await {
                    Ok(value) => value,
                    Err(err) => return Ok(Err(err)),
                };
            let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;
            let mut results = match mltools::ocr_image(&mltools_ctx, &[image_path]).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            let Some(result) = results.pop() else {
                return Ok(Err("ocr returned no results".to_string()));
            };
            Ok(Ok(mltools_ocr::OcrResult {
                text: result.text,
                regions: result
                    .regions
                    .into_iter()
                    .map(|region| binds_guest::townframe::mltools::ocr::TextRegion {
                        bounding_box: region.bounding_box,
                        text: region.text.map(|text| text.to_string()),
                        confidence: region.confidence,
                    })
                    .collect(),
            }))
        }
    }
}

impl mltools_embed::Host for SharedWashCtx {}

impl<T: 'static> mltools_embed::HostWithStore<T> for wasmtime::component::HasSelf<SharedWashCtx> {
    fn embed_text(
        accessor: &wasmtime::component::Accessor<T, Self>,
        text: String,
    ) -> impl std::future::Future<
        Output = wasmtime::Result<Result<mltools_embed::EmbedResult, String>>,
    > + Send {
        async move {
            let plugin = super::caps::with_host_ctx(accessor, |ctx| DaybookPlugin::from_ctx(ctx));
            let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;
            let result = match mltools::embed_text(&mltools_ctx, &text).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            Ok(Ok(mltools_embed::EmbedResult {
                vector: result.vector,
                dimensions: result.dimensions,
                model_id: result.model_id,
            }))
        }
    }

    fn embed_image(
        accessor: &wasmtime::component::Accessor<T, Self>,
        blob_facet: wasmtime::component::Resource<capabilities::FacetToken>,
    ) -> impl std::future::Future<
        Output = wasmtime::Result<Result<mltools_embed::EmbedResult, String>>,
    > + Send {
        async move {
            let (plugin, token) = super::caps::with_host_ctx(accessor, |ctx| {
                let token = ctx
                    .table
                    .get(&blob_facet)
                    .map_err(|err| {
                        wasmtime::Error::msg(format!("error locating facet token: {err}"))
                    })?
                    .clone();
                Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
            })?;
            let blob = match super::caps::get_blob_facet_from_owned_token(&plugin, &token).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            let blob_hash = match super::caps::blob_hash_from_blob_facet(&blob) {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            let blob_id = match blob_hash.parse::<crate::blobs::BlobId>() {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            let requested_ext = match extension_for_blob_mime(blob.mime.as_str()) {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            let image_path = match plugin
                .blobs_repo
                .materialize(
                    blob_id,
                    crate::blobs::BlobMaterializeRequest::Extension(requested_ext.to_string()),
                )
                .await
            {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;
            let result =
                match mltools::embed_image(&mltools_ctx, &image_path, blob.mime.as_str()).await {
                    Ok(value) => value,
                    Err(err) => return Ok(Err(err.to_string())),
                };
            Ok(Ok(mltools_embed::EmbedResult {
                vector: result.vector,
                dimensions: result.dimensions,
                model_id: result.model_id,
            }))
        }
    }
}

fn extension_for_blob_mime(mime: &str) -> Result<&'static str, String> {
    let token = mime
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if token.is_empty() {
        return Err("empty image mime".to_string());
    }
    match token.as_str() {
        "image/jpeg" | "image/jpg" => Ok("jpg"),
        "image/png" => Ok("png"),
        "image/gif" => Ok("gif"),
        "image/bmp" => Ok("bmp"),
        "image/webp" => Ok("webp"),
        value => Err(format!(
            "unsupported image mime for embedding materialization: {value}"
        )),
    }
}

impl mltools_llm_chat::Host for SharedWashCtx {}

impl<T: 'static> mltools_llm_chat::HostWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    fn llm_chat(
        accessor: &wasmtime::component::Accessor<T, Self>,
        text: String,
    ) -> impl std::future::Future<Output = wasmtime::Result<Result<String, String>>> + Send {
        async move {
            let plugin = super::caps::with_host_ctx(accessor, |ctx| DaybookPlugin::from_ctx(ctx));
            let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;
            let result = match mltools::llm_chat(&mltools_ctx, &text).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            Ok(Ok(result.text))
        }
    }

    fn llm_chat_multimodal(
        accessor: &wasmtime::component::Accessor<T, Self>,
        prompt: String,
        image_bytes: Vec<u8>,
        image_mime: String,
    ) -> impl std::future::Future<Output = wasmtime::Result<Result<String, String>>> + Send {
        async move {
            let plugin = super::caps::with_host_ctx(accessor, |ctx| DaybookPlugin::from_ctx(ctx));
            let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;
            let result = match mltools::llm_chat_multimodal(
                &mltools_ctx,
                &prompt,
                &image_bytes,
                &image_mime,
            )
            .await
            {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            Ok(Ok(result.text))
        }
    }
}

impl mltools_image_tools::Host for SharedWashCtx {}

impl<T: 'static> mltools_image_tools::HostWithStore<T>
    for wasmtime::component::HasSelf<SharedWashCtx>
{
    fn downsize_image_from_blob(
        accessor: &wasmtime::component::Accessor<T, Self>,
        blob_facet: wasmtime::component::Resource<capabilities::FacetToken>,
        max_side: u32,
        jpeg_quality: u8,
    ) -> impl std::future::Future<
        Output = wasmtime::Result<Result<mltools_image_tools::ImageBytesResult, String>>,
    > + Send {
        async move {
            let (plugin, token) = super::caps::with_host_ctx(accessor, |ctx| {
                let token = ctx
                    .table
                    .get(&blob_facet)
                    .map_err(|err| {
                        wasmtime::Error::msg(format!("error locating facet token: {err}"))
                    })?
                    .clone();
                Ok::<_, wasmtime::Error>((DaybookPlugin::from_ctx(ctx), token))
            })?;
            let blob = match super::caps::get_blob_facet_from_owned_token(&plugin, &token).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(err)),
            };
            if !blob.mime.starts_with("image/") {
                return Ok(Err(format!("blob mime is not image/*: {}", blob.mime)));
            }
            tracing::Span::current().record("obj_id", tracing::field::display(&blob.digest));
            let image_path =
                match super::caps::resolve_blob_path_from_blob_facet(&plugin, &blob).await {
                    Ok(value) => value,
                    Err(err) => return Ok(Err(err)),
                };
            let image_bytes = match tokio::fs::read(&image_path).await {
                Ok(value) => value,
                Err(err) => return Ok(Err(format!("error reading blob bytes: {err}"))),
            };
            let span = tracing::Span::current();
            let downsized = match tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    crate::imgtools::downsize_image_jpeg(&image_bytes, max_side, jpeg_quality)
                })
            })
            .await
            .expect(ERROR_TOKIO)
            {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
            Ok(Ok(mltools_image_tools::ImageBytesResult {
                bytes: downsized.bytes,
                mime: downsized.mime,
                width: downsized.width,
                height: downsized.height,
            }))
        }
    }
}
