use crate::interlude::*;

use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::{
    binds_guest, capabilities, mltools_embed, mltools_llm_chat, mltools_ocr, DaybookPlugin,
};

async fn mltools_ctx_from_config_repo(plugin: &DaybookPlugin) -> mltools::Ctx {
    mltools::Ctx {
        config: plugin.config_repo.get_mltools_config().await,
    }
}

async fn resolve_blob_image_path_from_token_ro(
    ctx: &mut SharedWashCtx,
    blob_facet: &wasmtime::component::Resource<capabilities::FacetTokenRo>,
) -> wasmtime::Result<Result<std::path::PathBuf, String>> {
    let plugin = DaybookPlugin::from_ctx(ctx);

    let (doc_id, heads, facet_key) = {
        let token = ctx
            .table
            .get(blob_facet)
            .context("error locating blob facet token")
            .to_anyhow()?;
        (
            token.doc_id.clone(),
            token.heads.clone(),
            token.facet_key.clone(),
        )
    };

    let Some(doc) = plugin.get_doc(&doc_id, &heads).await.to_anyhow()? else {
        return Ok(Err(format!("doc not found: {doc_id}")));
    };

    let Some(blob_facet_raw) = doc.facets.get(&facet_key) else {
        return Ok(Err(format!("blob facet not found: {}", facet_key)));
    };

    let blob_facet_value = match daybook_types::doc::WellKnownFacet::from_json(
        blob_facet_raw.clone(),
        daybook_types::doc::WellKnownFacetTag::Blob,
    ) {
        Ok(value) => value,
        Err(err) => return Ok(Err(err.to_string())),
    };

    let daybook_types::doc::WellKnownFacet::Blob(blob) = blob_facet_value else {
        return Ok(Err("facet is not a blob".to_string()));
    };

    let Some(urls) = blob.urls.as_ref() else {
        return Ok(Err("blob facet is missing urls".to_string()));
    };
    let Some(first_url) = urls.first() else {
        return Ok(Err("blob facet urls is empty".to_string()));
    };

    let parsed_url = match url::Url::parse(first_url) {
        Ok(value) => value,
        Err(err) => return Ok(Err(err.to_string())),
    };
    if parsed_url.scheme() != crate::blobs::BLOB_SCHEME {
        return Ok(Err(format!(
            "unsupported blob url scheme '{}'",
            parsed_url.scheme()
        )));
    }
    if parsed_url.host_str().is_some() {
        return Ok(Err("blob url authority must be empty".to_string()));
    }

    let hash = parsed_url.path().trim_start_matches('/');
    if hash.is_empty() {
        return Ok(Err("blob url path is missing hash".to_string()));
    }

    match plugin.blobs_repo.get_path(hash).await {
        Ok(path) => Ok(Ok(path)),
        Err(err) => Ok(Err(err.to_string())),
    }
}

impl mltools_ocr::Host for SharedWashCtx {
    async fn ocr_image(
        &mut self,
        blob_facet: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<Result<mltools_ocr::OcrResult, String>> {
        let image_path = match resolve_blob_image_path_from_token_ro(self, &blob_facet).await? {
            Ok(path) => path,
            Err(err) => return Ok(Err(err)),
        };
        let plugin = DaybookPlugin::from_ctx(self);

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

impl mltools_embed::Host for SharedWashCtx {
    async fn embed_text(
        &mut self,
        text: String,
    ) -> wasmtime::Result<Result<mltools_embed::EmbedResult, String>> {
        let plugin = DaybookPlugin::from_ctx(self);
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

    async fn embed_image(
        &mut self,
        blob_facet: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<Result<mltools_embed::EmbedResult, String>> {
        let image_path = match resolve_blob_image_path_from_token_ro(self, &blob_facet).await? {
            Ok(path) => path,
            Err(err) => return Ok(Err(err)),
        };
        let plugin = DaybookPlugin::from_ctx(self);
        let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;

        let result = match mltools::embed_image(&mltools_ctx, &image_path).await {
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

impl mltools_llm_chat::Host for SharedWashCtx {
    async fn llm_chat(&mut self, text: String) -> wasmtime::Result<Result<String, String>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;

        let result = match mltools::llm_chat(&mltools_ctx, &text).await {
            Ok(value) => value,
            Err(err) => return Ok(Err(err.to_string())),
        };
        Ok(Ok(result.text))
    }
}
