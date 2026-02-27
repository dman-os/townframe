use crate::interlude::*;

use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;

use super::{bindgen_doc, binds_guest, capabilities, drawer, root_doc, wit_doc, DaybookPlugin};

pub struct DocTokenRo {
    pub doc_id: DocId,
    pub heads: ChangeHashSet,
}

impl capabilities::HostDocTokenRo for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<DocTokenRo>,
    ) -> wasmtime::Result<bindgen_doc::Doc> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        match plugin
            .get_doc(&token.doc_id, &token.heads)
            .await
            .to_anyhow()?
        {
            Some(doc) => {
                let bind_doc: bindgen_doc::Doc = binds_guest::townframe::daybook_types::doc::Doc {
                    id: doc.id.clone(),
                    facets: doc
                        .facets
                        .iter()
                        .map(|(facet_key, facet_value)| {
                            (facet_key.to_string(), wit_doc::facet_from(facet_value))
                        })
                        .collect(),
                };
                Ok(bind_doc)
            }
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            None => todo!(),
        }
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::DocTokenRo>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub struct DocTokenRw {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
}

impl capabilities::HostDocTokenRw for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocTokenRw>,
    ) -> wasmtime::Result<bindgen_doc::Doc> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        match plugin
            .get_doc(&token.doc_id, &token.heads)
            .await
            .to_anyhow()?
        {
            Some(doc) => {
                let bind_doc: bindgen_doc::Doc = binds_guest::townframe::daybook_types::doc::Doc {
                    id: doc.id.clone(),
                    facets: doc
                        .facets
                        .iter()
                        .map(|(facet_key, facet_value)| {
                            (facet_key.to_string(), wit_doc::facet_from(facet_value))
                        })
                        .collect(),
                };
                Ok(bind_doc)
            }
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            None => todo!(),
        }
    }

    async fn update(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocTokenRw>,
        patch: bindgen_doc::DocPatch,
    ) -> wasmtime::Result<Result<(), capabilities::UpdateDocError>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        let patch = wit_doc::DocPatch {
            id: patch.id,
            facets_set: patch.facets_set.into_iter().collect(),
            facets_remove: patch.facets_remove,
            user_path: None,
        };
        let patch: daybook_types::doc::DocPatch =
            patch.try_into().map_err(|err: serde_json::Error| {
                drawer::UpdateDocError::InvalidPatch(err.to_string())
            })?;
        match plugin
            .patch_doc(token.branch_path.clone(), Some(token.heads.clone()), patch)
            .await
        {
            Ok(_) => Ok(Ok(())),
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => todo!(),
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => todo!(),
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(anyhow::anyhow!("unexepcted error: {inner}"))
            }
        }
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::DocTokenRw>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub struct FacetTokenRo {
    pub doc_id: DocId,
    pub heads: ChangeHashSet,
    pub facet_key: daybook_types::doc::FacetKey,
}

impl capabilities::HostFacetTokenRo for SharedWashCtx {
    async fn exists(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<bool> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.heads)
            .await
            .to_anyhow()?
        else {
            return Ok(false);
        };
        Ok(doc.facets.contains_key(&token.facet_key))
    }

    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<String> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        match plugin
            .get_doc(&token.doc_id, &token.heads)
            .await
            .to_anyhow()?
        {
            Some(doc) => {
                let Some(facet) = doc.facets.get(&token.facet_key) else {
                    // FIXME: either the context should terminal error this
                    // or communicate with the wflow engine
                    todo!("")
                };
                let facet = wit_doc::facet_from(facet);
                Ok(facet)
            }
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            None => todo!(),
        }
    }

    async fn heads(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<Vec<String>> {
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        Ok(am_utils_rs::serialize_commit_heads(token.heads.as_ref()))
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub(super) async fn get_facet_raw_from_token_ro(
    ctx: &mut SharedWashCtx,
    handle: &wasmtime::component::Resource<capabilities::FacetTokenRo>,
) -> wasmtime::Result<Result<(daybook_types::doc::FacetKey, daybook_types::doc::FacetRaw), String>>
{
    let plugin = DaybookPlugin::from_ctx(ctx);
    let (doc_id, heads, facet_key) = {
        let token = ctx
            .table
            .get(handle)
            .context("error locating facet token")
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
    let Some(facet_raw) = doc.facets.get(&facet_key) else {
        return Ok(Err(format!("facet not found: {}", facet_key)));
    };
    Ok(Ok((facet_key, facet_raw.clone())))
}

pub(super) async fn get_blob_facet_from_token_ro(
    ctx: &mut SharedWashCtx,
    handle: &wasmtime::component::Resource<capabilities::FacetTokenRo>,
) -> wasmtime::Result<Result<daybook_types::doc::Blob, String>> {
    let (_facet_key, facet_raw) = match get_facet_raw_from_token_ro(ctx, handle).await? {
        Ok(value) => value,
        Err(err) => return Ok(Err(err)),
    };
    let blob_facet_value = match daybook_types::doc::WellKnownFacet::from_json(
        facet_raw,
        daybook_types::doc::WellKnownFacetTag::Blob,
    ) {
        Ok(value) => value,
        Err(err) => return Ok(Err(err.to_string())),
    };
    let daybook_types::doc::WellKnownFacet::Blob(blob) = blob_facet_value else {
        return Ok(Err("facet is not a blob".to_string()));
    };
    Ok(Ok(blob))
}

pub(super) async fn resolve_blob_path_from_blob_facet(
    plugin: &DaybookPlugin,
    blob: &daybook_types::doc::Blob,
) -> Result<std::path::PathBuf, String> {
    let Some(urls) = blob.urls.as_ref() else {
        return Err("blob facet is missing urls".to_string());
    };
    let Some(first_url) = urls.first() else {
        return Err("blob facet urls is empty".to_string());
    };

    let parsed_url = url::Url::parse(first_url).map_err(|err| err.to_string())?;
    if parsed_url.scheme() != crate::blobs::BLOB_SCHEME {
        return Err(format!(
            "unsupported blob url scheme '{}'",
            parsed_url.scheme()
        ));
    }
    if parsed_url.host_str().is_some() {
        return Err("blob url authority must be empty".to_string());
    }

    let hash = parsed_url.path().trim_start_matches('/');
    if hash.is_empty() {
        return Err("blob url path is missing hash".to_string());
    }

    plugin
        .blobs_repo
        .get_path(hash)
        .await
        .map_err(|err| err.to_string())
}

pub struct FacetTokenRw {
    pub doc_id: DocId,
    pub branch_path: daybook_types::doc::BranchPath,
    #[allow(dead_code)]
    pub target_branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub facet_key: daybook_types::doc::FacetKey,
    #[allow(dead_code)]
    pub facet_acl: Vec<crate::plugs::manifest::RoutineFacetAccess>,
}

impl capabilities::HostFacetTokenRw for SharedWashCtx {
    async fn exists(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRw>,
    ) -> wasmtime::Result<bool> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        let Some(doc) = plugin
            .get_doc(&token.doc_id, &token.heads)
            .await
            .to_anyhow()?
        else {
            return Ok(false);
        };
        Ok(doc.facets.contains_key(&token.facet_key))
    }

    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRw>,
    ) -> wasmtime::Result<String> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        match plugin
            .get_doc(&token.doc_id, &token.heads)
            .await
            .to_anyhow()?
        {
            Some(doc) => {
                let Some(facet) = doc.facets.get(&token.facet_key) else {
                    // FIXME: either the context should terminal error this
                    // or communicate with the wflow engine
                    todo!("")
                };
                let facet = wit_doc::facet_from(facet);
                Ok(facet)
            }
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            None => todo!(),
        }
    }

    async fn heads(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRw>,
    ) -> wasmtime::Result<Vec<String>> {
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        Ok(am_utils_rs::serialize_commit_heads(token.heads.as_ref()))
    }

    async fn update(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::FacetTokenRw>,
        facet_json: String,
    ) -> wasmtime::Result<Result<(), capabilities::UpdateDocError>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        let facet: daybook_types::doc::FacetRaw = wit_doc::facet_into(&facet_json)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    facets_set: HashMap::from([(token.facet_key.clone(), facet)]),
                    facets_remove: default(),
                    user_path: None,
                },
                token.branch_path.clone(),
                Some(token.heads.clone()),
            )
            .await
        {
            Ok(_) => Ok(Ok(())),
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => todo!(),
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => todo!(),
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(anyhow::anyhow!("unexepcted error: {inner}"))
            }
        }
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetTokenRw>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl capabilities::Host for SharedWashCtx {}
