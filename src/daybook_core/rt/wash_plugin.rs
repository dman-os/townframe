use crate::interlude::*;
use daybook_types::doc::{self as root_doc};

mod binds_guest {
    use crate::interlude::*;

    use daybook_types::doc::{self as root_doc};
    use daybook_types::wit::doc as wit_doc;

    wash_runtime::wasmtime::component::bindgen!({
        world: "all-guest",

        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        with: {
            "townframe:daybook/capabilities.doc-token-ro": super::DocTokenRo,
            "townframe:daybook/capabilities.doc-token-rw": super::DocTokenRw,
            "townframe:daybook/capabilities.facet-token-ro": super::FacetTokenRo,
            "townframe:daybook/capabilities.facet-token-rw": super::FacetTokenRw,
        }
    });

    #[allow(dead_code)]
    pub fn well_known_facet_to_wit(value: root_doc::WellKnownFacet) -> wit_doc::WellKnownFacet {
        match value {
            root_doc::WellKnownFacet::RefGeneric(val) => wit_doc::WellKnownFacet::RefGeneric(val),
            root_doc::WellKnownFacet::LabelGeneric(val) => {
                wit_doc::WellKnownFacet::LabelGeneric(val)
            }
            root_doc::WellKnownFacet::PseudoLabel(val) => wit_doc::WellKnownFacet::PseudoLabel(val),
            root_doc::WellKnownFacet::TitleGeneric(val) => {
                wit_doc::WellKnownFacet::TitleGeneric(val)
            }
            root_doc::WellKnownFacet::PathGeneric(val) => {
                wit_doc::WellKnownFacet::PathGeneric(val.to_string_lossy().into_owned())
            }
            root_doc::WellKnownFacet::ImageMetadata(val) => {
                wit_doc::WellKnownFacet::ImageMetadata(wit_doc::ImageMetadata {
                    facet_ref: val.facet_ref.to_string(),
                    ref_heads: utils_rs::am::serialize_commit_heads(&val.ref_heads.0),
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                })
            }
            root_doc::WellKnownFacet::OcrResult(val) => {
                wit_doc::WellKnownFacet::OcrResult(wit_doc::OcrResult {
                    facet_ref: val.facet_ref.to_string(),
                    ref_heads: utils_rs::am::serialize_commit_heads(&val.ref_heads.0),
                    model_tag: val.model_tag,
                    text: val.text,
                    text_regions: val.text_regions.map(|regions| {
                        regions
                            .into_iter()
                            .map(|region| wit_doc::OcrTextRegion {
                                bounding_box: region
                                    .bounding_box
                                    .into_iter()
                                    .map(|point| wit_doc::Point {
                                        x: point.x,
                                        y: point.y,
                                    })
                                    .collect(),
                                text: region.text,
                                confidence_score: region.confidence_score,
                            })
                            .collect()
                    }),
                })
            }
            root_doc::WellKnownFacet::Embedding(val) => {
                wit_doc::WellKnownFacet::Embedding(wit_doc::Embedding {
                    facet_ref: val.facet_ref.to_string(),
                    ref_heads: utils_rs::am::serialize_commit_heads(&val.ref_heads.0),
                    model_tag: val.model_tag,
                    vector: val.vector,
                    dim: val.dim,
                    dtype: match val.dtype {
                        root_doc::EmbeddingDtype::F32 => wit_doc::EmbeddingDtype::F32,
                        root_doc::EmbeddingDtype::F16 => wit_doc::EmbeddingDtype::F16,
                        root_doc::EmbeddingDtype::I8 => wit_doc::EmbeddingDtype::I8,
                        root_doc::EmbeddingDtype::Binary => wit_doc::EmbeddingDtype::Binary,
                    },
                    compression: val.compression.map(|compression| match compression {
                        root_doc::EmbeddingCompression::Zstd => wit_doc::EmbeddingCompression::Zstd,
                    }),
                })
            }
            root_doc::WellKnownFacet::Note(val) => wit_doc::WellKnownFacet::Note(root_doc::Note {
                mime: val.mime,
                content: val.content,
            }),
            root_doc::WellKnownFacet::Blob(val) => wit_doc::WellKnownFacet::Blob(root_doc::Blob {
                mime: val.mime,
                length_octets: val.length_octets,
                digest: val.digest,
                inline: val.inline,
                urls: val.urls,
            }),
            root_doc::WellKnownFacet::Pending(pending) => {
                wit_doc::WellKnownFacet::Pending(wit_doc::Pending {
                    key: pending.key.to_string(),
                })
            }
            root_doc::WellKnownFacet::Dmeta(dmeta) => {
                wit_doc::WellKnownFacet::Dmeta(wit_doc::Dmeta {
                    id: dmeta.id,
                    created_at: dmeta.created_at.into(),
                    updated_at: dmeta.updated_at.into_iter().map(Into::into).collect(),
                    facet_uuids: dmeta
                        .facet_uuids
                        .into_iter()
                        .map(|(uuid, key)| (uuid.to_string(), key.to_string()))
                        .collect(),
                    facets: dmeta
                        .facets
                        .into_iter()
                        .map(|(key, meta)| {
                            (
                                key.to_string(),
                                wit_doc::FacetMeta {
                                    created_at: meta.created_at.into(),
                                    updated_at: meta
                                        .updated_at
                                        .into_iter()
                                        .map(Into::into)
                                        .collect(),
                                    uuid: meta
                                        .uuid
                                        .into_iter()
                                        .map(|facet_uuid| facet_uuid.to_string())
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
            }
        }
    }

    #[allow(dead_code)]
    pub fn wit_to_well_known_facet(value: wit_doc::WellKnownFacet) -> root_doc::WellKnownFacet {
        match value {
            wit_doc::WellKnownFacet::RefGeneric(val) => root_doc::WellKnownFacet::RefGeneric(val),
            wit_doc::WellKnownFacet::LabelGeneric(val) => {
                root_doc::WellKnownFacet::LabelGeneric(val)
            }
            wit_doc::WellKnownFacet::PseudoLabel(val) => root_doc::WellKnownFacet::PseudoLabel(val),
            wit_doc::WellKnownFacet::TitleGeneric(val) => {
                root_doc::WellKnownFacet::TitleGeneric(val)
            }
            wit_doc::WellKnownFacet::PathGeneric(val) => {
                root_doc::WellKnownFacet::PathGeneric(val.into())
            }
            wit_doc::WellKnownFacet::ImageMetadata(val) => {
                root_doc::WellKnownFacet::ImageMetadata(root_doc::ImageMetadata {
                    facet_ref: val.facet_ref.parse().unwrap(),
                    ref_heads: root_doc::ChangeHashSet(
                        utils_rs::am::parse_commit_heads(&val.ref_heads).unwrap(),
                    ),
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                })
            }
            wit_doc::WellKnownFacet::OcrResult(val) => {
                root_doc::WellKnownFacet::OcrResult(root_doc::OcrResult {
                    facet_ref: val.facet_ref.parse().unwrap(),
                    ref_heads: root_doc::ChangeHashSet(
                        utils_rs::am::parse_commit_heads(&val.ref_heads).unwrap(),
                    ),
                    model_tag: val.model_tag,
                    text: val.text,
                    text_regions: val.text_regions.map(|regions| {
                        regions
                            .into_iter()
                            .map(|region| root_doc::OcrTextRegion {
                                bounding_box: region
                                    .bounding_box
                                    .into_iter()
                                    .map(|point| root_doc::Point {
                                        x: point.x,
                                        y: point.y,
                                    })
                                    .collect(),
                                text: region.text,
                                confidence_score: region.confidence_score,
                            })
                            .collect()
                    }),
                })
            }
            wit_doc::WellKnownFacet::Embedding(val) => {
                root_doc::WellKnownFacet::Embedding(root_doc::Embedding {
                    facet_ref: val.facet_ref.parse().unwrap(),
                    ref_heads: root_doc::ChangeHashSet(
                        utils_rs::am::parse_commit_heads(&val.ref_heads).unwrap(),
                    ),
                    model_tag: val.model_tag,
                    vector: val.vector,
                    dim: val.dim,
                    dtype: match val.dtype {
                        wit_doc::EmbeddingDtype::F32 => root_doc::EmbeddingDtype::F32,
                        wit_doc::EmbeddingDtype::F16 => root_doc::EmbeddingDtype::F16,
                        wit_doc::EmbeddingDtype::I8 => root_doc::EmbeddingDtype::I8,
                        wit_doc::EmbeddingDtype::Binary => root_doc::EmbeddingDtype::Binary,
                    },
                    compression: val.compression.map(|compression| match compression {
                        wit_doc::EmbeddingCompression::Zstd => root_doc::EmbeddingCompression::Zstd,
                    }),
                })
            }
            wit_doc::WellKnownFacet::Pending(pending) => {
                root_doc::WellKnownFacet::Pending(root_doc::Pending {
                    key: root_doc::FacetKey::from(pending.key),
                })
            }
            wit_doc::WellKnownFacet::Dmeta(dmeta) => {
                root_doc::WellKnownFacet::Dmeta(root_doc::Dmeta {
                    id: dmeta.id,
                    created_at: Timestamp::from_second(dmeta.created_at.seconds as i64).unwrap(),
                    updated_at: dmeta
                        .updated_at
                        .into_iter()
                        .map(|dt| Timestamp::from_second(dt.seconds as i64).unwrap())
                        .collect(),
                    facet_uuids: dmeta
                        .facet_uuids
                        .into_iter()
                        .map(|(facet_uuid_str, facet_key_str)| {
                            (
                                Uuid::parse_str(&facet_uuid_str).unwrap(),
                                root_doc::FacetKey::from(facet_key_str),
                            )
                        })
                        .collect(),
                    facets: dmeta
                        .facets
                        .into_iter()
                        .map(|(facet_key_str, facet_meta)| {
                            (
                                root_doc::FacetKey::from(facet_key_str),
                                root_doc::FacetMeta {
                                    created_at: Timestamp::from_second(
                                        facet_meta.created_at.seconds as i64,
                                    )
                                    .unwrap(),
                                    updated_at: facet_meta
                                        .updated_at
                                        .into_iter()
                                        .map(|dt| {
                                            Timestamp::from_second(dt.seconds as i64).unwrap()
                                        })
                                        .collect(),
                                    uuid: facet_meta
                                        .uuid
                                        .into_iter()
                                        .map(|facet_uuid_str| {
                                            Uuid::parse_str(&facet_uuid_str).unwrap()
                                        })
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
            }
            wit_doc::WellKnownFacet::Note(note) => root_doc::WellKnownFacet::Note(root_doc::Note {
                mime: note.mime,
                content: note.content,
            }),
            wit_doc::WellKnownFacet::Blob(blob) => root_doc::WellKnownFacet::Blob(root_doc::Blob {
                mime: blob.mime,
                length_octets: blob.length_octets,
                digest: blob.digest,
                inline: blob.inline,
                urls: blob.urls,
            }),
        }
    }

    #[allow(dead_code)]
    pub fn wit_to_root_doc(value: wit_doc::Doc) -> root_doc::Doc {
        root_doc::Doc {
            id: value.id,
            facets: value
                .facets
                .into_iter()
                .map(|(key, val)| {
                    (
                        root_doc::FacetKey::from(&key),
                        root_doc::FacetRaw::from(val),
                    )
                })
                .collect(),
        }
    }
}

pub use binds_guest::townframe::daybook::capabilities;
pub use binds_guest::townframe::daybook::drawer;
pub use binds_guest::townframe::daybook::facet_routine;
pub use binds_guest::townframe::daybook::index_vector;
pub use binds_guest::townframe::daybook::mltools_embed;
pub use binds_guest::townframe::daybook::mltools_llm_chat;
pub use binds_guest::townframe::daybook::mltools_ocr;
use binds_guest::townframe::daybook_types::doc as bindgen_doc;

use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use daybook_types::wit::doc as wit_doc;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

pub struct DaybookPlugin {
    drawer_repo: Arc<crate::drawer::DrawerRepo>,
    dispatch_repo: Arc<crate::rt::DispatchRepo>,
    blobs_repo: Arc<crate::blobs::BlobsRepo>,
    doc_embedding_index_repo: Arc<crate::index::DocEmbeddingIndexRepo>,
}

impl DaybookPlugin {
    pub fn new(
        drawer_repo: Arc<crate::drawer::DrawerRepo>,
        dispatch_repo: Arc<crate::rt::DispatchRepo>,
        blobs_repo: Arc<crate::blobs::BlobsRepo>,
        doc_embedding_index_repo: Arc<crate::index::DocEmbeddingIndexRepo>,
    ) -> Self {
        Self {
            drawer_repo,
            dispatch_repo,
            blobs_repo,
            doc_embedding_index_repo,
        }
    }

    pub const ID: &str = "townframe:daybook";

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        let Some(this) = wcx.active_ctx.get_plugin::<Self>(Self::ID) else {
            panic!("plugin not on ctx");
        };
        this
    }

    async fn get_doc(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<Arc<daybook_types::doc::Doc>>> {
        self.drawer_repo
            .get_doc_with_facets_at_heads(doc_id, heads, None)
            .await
    }

    async fn patch_doc(
        &self,
        branch_path: daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
        patch: root_doc::DocPatch,
    ) -> Result<(), crate::drawer::types::DrawerError> {
        self.drawer_repo
            .update_at_heads(patch, branch_path, heads)
            .await
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for DaybookPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:daybook/drawer,capabilities,facet-routine,mltools-ocr,mltools-embed,mltools-llm-chat,index-vector",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe" && iface.package == "daybook" {
                if iface.interfaces.contains("drawer") {
                    drawer::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("capabilities") {
                    capabilities::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("facet-routine") {
                    facet_routine::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("mltools-ocr") {
                    mltools_ocr::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("mltools-embed") {
                    mltools_embed::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("mltools-llm-chat") {
                    mltools_llm_chat::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<SharedWashCtx>,
                    >(item.linker(), |ctx| ctx)?;
                }
                if iface.interfaces.contains("index-vector") {
                    index_vector::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        _resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl drawer::Host for SharedWashCtx {
    async fn get_doc_at_heads(
        &mut self,
        doc_id: drawer::DocId,
        heads: drawer::Heads,
    ) -> wasmtime::Result<Result<drawer::Doc, drawer::GetDocError>> {
        let heads = match utils_rs::am::parse_commit_heads(&heads) {
            Ok(val) => val,
            Err(err) => return Ok(Err(drawer::GetDocError::InvalidHeads(format!("{err:?}")))),
        };
        let heads = ChangeHashSet(heads);

        let plugin = DaybookPlugin::from_ctx(self);

        match plugin.get_doc(&doc_id, &heads).await.to_anyhow()? {
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
                Ok(Ok(bind_doc))
            }
            None => Ok(Err(drawer::GetDocError::DocNotFound)),
        }
    }

    async fn update_doc_at_heads(
        &mut self,
        branch_path: String,
        heads: Option<drawer::Heads>,
        patch: drawer::DocPatch,
    ) -> wasmtime::Result<Result<(), drawer::UpdateDocError>> {
        let heads = match heads {
            Some(heads) => match utils_rs::am::parse_commit_heads(&heads) {
                Ok(val) => Some(ChangeHashSet(val)),
                Err(err) => {
                    return Ok(Err(drawer::UpdateDocError::InvalidHeads(format!(
                        "{err:?}"
                    ))))
                }
            },
            None => None,
        };
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

        let plugin = DaybookPlugin::from_ctx(self);
        match plugin
            .patch_doc(
                daybook_types::doc::BranchPath::from(branch_path),
                heads,
                patch,
            )
            .await
        {
            Ok(_) => Ok(Ok(())),
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::DocNotFound))
            }
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::BranchNotFound))
            }
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(drawer::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(anyhow::anyhow!("unexepcted error: {inner}"))
            }
        }
    }
}

pub struct DocTokenRo {
    doc_id: DocId,
    heads: ChangeHashSet,
}

impl capabilities::HostDocTokenRo for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::DocTokenRo>,
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
    doc_id: DocId,
    branch_path: daybook_types::doc::BranchPath,
    heads: ChangeHashSet,
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
    doc_id: DocId,
    heads: ChangeHashSet,
    facet_key: daybook_types::doc::FacetKey,
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
    doc_id: DocId,
    branch_path: daybook_types::doc::BranchPath,
    #[allow(dead_code)]
    target_branch_path: daybook_types::doc::BranchPath,
    heads: ChangeHashSet,
    facet_key: daybook_types::doc::FacetKey,
    #[allow(dead_code)]
    facet_acl: Vec<crate::plugs::manifest::RoutineFacetAccess>,
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

impl mltools_ocr::Host for SharedWashCtx {
    async fn ocr_image(
        &mut self,
        blob_facet: wasmtime::component::Resource<capabilities::FacetTokenRo>,
    ) -> wasmtime::Result<Result<mltools_ocr::OcrResult, String>> {
        let plugin = DaybookPlugin::from_ctx(self);

        let (doc_id, heads, facet_key) = {
            let token = self
                .table
                .get(&blob_facet)
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

        let image_path = match plugin.blobs_repo.get_path(hash).await {
            Ok(value) => value,
            Err(err) => return Ok(Err(err.to_string())),
        };

        let model_path = |suffix: &str| -> Result<PathBuf, String> {
            let candidate = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(suffix);
            std::path::absolute(candidate)
                .map_err(|err| format!("failed to resolve absolute model path: {err}"))
        };

        let det_model_path = match model_path("target/models/detection/v5/det.onnx") {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        let rec_model_path = match model_path("target/models/languages/latin/rec.onnx") {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };
        let dict_path = match model_path("target/models/languages/latin/dict.txt") {
            Ok(value) => value,
            Err(err) => return Ok(Err(err)),
        };

        let mltools_ctx = mltools::Ctx {
            config: mltools::Config {
                ocr: mltools::OcrConfig {
                    backends: vec![mltools::OcrBackendConfig::LocalOnnx {
                        text_recognition_onnx_path: rec_model_path,
                        text_detection_onnx_path: det_model_path,
                        character_dict_txt_path: dict_path,
                        document_orientation_onnx_path: None,
                        text_line_orientation_onnx_path: None,
                        document_rectification_onnx_path: None,
                        supported_languages_bcp47: vec!["en".to_string()],
                    }],
                },
                embed: mltools::EmbedConfig { backends: vec![] },
                llm: mltools::LlmConfig { backends: vec![] },
            },
        };

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
        let cache_dir = std::path::absolute(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target/models/.fastembed_cache"),
        )
        .map_err(|err| anyhow::anyhow!("failed to resolve embedding cache path: {err}"))?;

        let mltools_ctx = mltools::Ctx {
            config: mltools::Config {
                ocr: mltools::OcrConfig { backends: vec![] },
                embed: mltools::EmbedConfig {
                    backends: vec![mltools::EmbedBackendConfig::LocalFastembedNomic { cache_dir }],
                },
                llm: mltools::LlmConfig { backends: vec![] },
            },
        };

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

impl mltools_llm_chat::Host for SharedWashCtx {
    async fn llm_chat(&mut self, text: String) -> wasmtime::Result<Result<String, String>> {
        let ollama_url = match utils_rs::get_env_var("OLLAMA_URL") {
            Ok(value) => value,
            Err(err) => return Ok(Err(err.to_string())),
        };
        let ollama_model = match utils_rs::get_env_var("OLLAMA_MODEL") {
            Ok(value) => value,
            Err(err) => return Ok(Err(err.to_string())),
        };

        let mltools_ctx = mltools::Ctx {
            config: mltools::Config {
                ocr: mltools::OcrConfig { backends: vec![] },
                embed: mltools::EmbedConfig { backends: vec![] },
                llm: mltools::LlmConfig {
                    backends: vec![mltools::LlmBackendConfig::CloudOllama {
                        url: ollama_url,
                        model: ollama_model,
                    }],
                },
            },
        };

        let result = match mltools::llm_chat(&mltools_ctx, &text).await {
            Ok(value) => value,
            Err(err) => return Ok(Err(err.to_string())),
        };
        Ok(Ok(result.text))
    }
}

impl index_vector::Host for SharedWashCtx {
    async fn query_text(
        &mut self,
        text: String,
        num_neighbors: u32,
    ) -> wasmtime::Result<Result<Vec<index_vector::VectorHit>, String>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let hits = match plugin
            .doc_embedding_index_repo
            .query_text(&text, num_neighbors)
            .await
        {
            Ok(value) => value,
            Err(err) => return Ok(Err(err.to_string())),
        };
        Ok(Ok(hits
            .into_iter()
            .map(|hit| index_vector::VectorHit {
                doc_id: hit.doc_id,
                facet_key: hit.facet_key,
                heads: utils_rs::am::serialize_commit_heads(&hit.heads.0),
                distance: hit.distance,
            })
            .collect()))
    }
}

impl capabilities::Host for SharedWashCtx {}

impl facet_routine::Host for SharedWashCtx {
    async fn get_args(&mut self) -> wasmtime::Result<facet_routine::FacetRoutineArgs> {
        use crate::rt::*;
        use anyhow::Context;
        use daybook_types::doc::FacetKey;

        let wflow_plugin = wflow::wash_plugin_wflow::WflowPlugin::try_from_ctx(self)
            .context("only wflows are supported as facet-routine")?;
        let dayook_plugin = DaybookPlugin::from_ctx(self);
        let job_id = wflow_plugin
            .job_id_of_ctx(self)
            .expect("there should be a job??");
        let Some(dispatch) = dayook_plugin
            .dispatch_repo
            .get_by_wflow_job(&job_id[..])
            .await
        else {
            anyhow::bail!("no active dispatch found for job: {job_id}");
        };
        let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            doc_id,
            heads,
            facet_key,
            branch_path: target_branch_path,
            staging_branch_path,
            facet_acl,
        }) = &dispatch.args;

        // Use staging branch path from dispatch (already set when job was created)
        let staging_branch_path = staging_branch_path.clone();

        // Create tokens based on ACL
        let mut rw_facet_tokens: Vec<(
            String,
            wasmtime::component::Resource<capabilities::FacetTokenRw>,
        )> = Vec::new();
        let mut ro_facet_tokens: Vec<(
            String,
            wasmtime::component::Resource<capabilities::FacetTokenRo>,
        )> = Vec::new();

        for access in facet_acl {
            let facet_key = access
                .key_id
                .as_ref()
                .map(|id| daybook_types::doc::FacetKey {
                    tag: daybook_types::doc::FacetTag::from(access.tag.0.as_str()),
                    id: id.clone(),
                })
                .unwrap_or_else(|| FacetKey::from(access.tag.0.as_str()));
            let facet_key_str = facet_key.to_string();

            if access.write {
                let token = self.table.push(FacetTokenRw {
                    doc_id: doc_id.clone(),
                    heads: heads.clone(),
                    branch_path: staging_branch_path.clone(),
                    target_branch_path: target_branch_path.clone(),
                    facet_key: facet_key.clone(),
                    facet_acl: facet_acl.clone(),
                })?;
                rw_facet_tokens.push((facet_key_str, token));
            } else if access.read {
                let token = self.table.push(FacetTokenRo {
                    doc_id: doc_id.clone(),
                    heads: heads.clone(),
                    facet_key: facet_key.clone(),
                })?;
                ro_facet_tokens.push((facet_key_str, token));
            }
        }

        Ok(facet_routine::FacetRoutineArgs {
            doc_id: doc_id.clone(),
            heads: utils_rs::am::serialize_commit_heads(heads.as_ref()),
            facet_key: facet_key.clone(),
            rw_facet_tokens,
            ro_facet_tokens,
        })
    }
}
