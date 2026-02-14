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

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
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
