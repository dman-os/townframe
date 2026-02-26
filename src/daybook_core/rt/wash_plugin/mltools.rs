use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::{
    binds_guest, capabilities, mltools_embed, mltools_image_tools, mltools_llm_chat, mltools_ocr,
    DaybookPlugin,
};

async fn mltools_ctx_from_config_repo(plugin: &DaybookPlugin) -> mltools::Ctx {
    mltools::Ctx {
        config: plugin.config_repo.get_mltools_config().await,
    }
}

impl mltools_ocr::Host for SharedWashCtx {
    async fn ocr_image(
        &mut self,
        blob_facet: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<Result<mltools_ocr::OcrResult, String>> {
        let blob = match super::caps::get_blob_facet_from_token_ro(self, &blob_facet).await? {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        let plugin = DaybookPlugin::from_ctx(self);
        let image_path = match super::caps::resolve_blob_path_from_blob_facet(&plugin, &blob).await
        {
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
        let blob = match super::caps::get_blob_facet_from_token_ro(self, &blob_facet).await? {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        let plugin = DaybookPlugin::from_ctx(self);
        let image_path = match super::caps::resolve_blob_path_from_blob_facet(&plugin, &blob).await
        {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;

        let result =
            match mltools::embed_image(&mltools_ctx, &image_path, Some(blob.mime.as_str())).await {
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

    async fn llm_chat_multimodal(
        &mut self,
        prompt: String,
        image_bytes: Vec<u8>,
        image_mime: String,
    ) -> wasmtime::Result<Result<String, String>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let mltools_ctx = mltools_ctx_from_config_repo(&plugin).await;

        let result =
            match mltools::llm_chat_multimodal(&mltools_ctx, &prompt, &image_bytes, &image_mime)
                .await
            {
                Ok(value) => value,
                Err(err) => return Ok(Err(err.to_string())),
            };
        Ok(Ok(result.text))
    }
}

impl mltools_image_tools::Host for SharedWashCtx {
    async fn downsize_image_from_blob(
        &mut self,
        blob_facet: wasmtime::component::Resource<capabilities::FacetTokenRo>,
        max_side: u32,
        jpeg_quality: u8,
    ) -> wasmtime::Result<Result<mltools_image_tools::ImageBytesResult, String>> {
        let blob = match super::caps::get_blob_facet_from_token_ro(self, &blob_facet).await? {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        if !blob.mime.starts_with("image/") {
            return Ok(Err(format!("blob mime is not image/*: {}", blob.mime)));
        }
        let plugin = DaybookPlugin::from_ctx(self);
        let image_path = match super::caps::resolve_blob_path_from_blob_facet(&plugin, &blob).await
        {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        let image_bytes = match std::fs::read(&image_path) {
            Ok(value) => value,
            Err(err) => return Ok(Err(format!("error reading blob bytes: {err}"))),
        };
        let downsized =
            match crate::imgtools::downsize_image_jpeg(&image_bytes, max_side, jpeg_quality) {
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
