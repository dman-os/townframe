use crate::interlude::*;
use crate::rt::dispatch;
use daybook_types::doc::BranchPath;
use daybook_types::doc::{self as root_doc};
use lettre::AsyncTransport;

fn wasmtime_err(msg: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(msg.to_string())
}

mod binds_guest {
    use crate::interlude::*;

    use daybook_types::doc::{self as root_doc};
    use daybook_types::wit::doc as wit_doc;

    wash_runtime::wasmtime::component::bindgen!({
        world: "all-guest",

        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        with: {
            "townframe:daybook/capabilities.doc-token": super::caps::DocToken,
            "townframe:daybook/capabilities.facet-token": super::caps::FacetToken,
            "townframe:daybook/capabilities.facet-create-token": super::caps::FacetCreateToken,
            "townframe:daybook/capabilities.facet-tag-token": super::caps::FacetTagToken,
            "townframe:daybook/capabilities.command-invoke-token": super::caps::CommandInvokeToken,
            "townframe:sqlite/sqlite-connection.connection": wash_plugin_sqlite::SqliteConnectionToken,
            "townframe:sqlite/sqlite-connection.transaction": wash_plugin_sqlite::SqliteTransactionToken,
        }
    });

    #[expect(dead_code)]
    pub fn well_known_facet_to_wit(value: root_doc::WellKnownFacet) -> wit_doc::WellKnownFacet {
        match value {
            root_doc::WellKnownFacet::RefGeneric(val) => wit_doc::WellKnownFacet::RefGeneric(val),
            root_doc::WellKnownFacet::LabelGeneric(val) => {
                wit_doc::WellKnownFacet::LabelGeneric(val)
            }
            root_doc::WellKnownFacet::TitleGeneric(val) => {
                wit_doc::WellKnownFacet::TitleGeneric(val)
            }
            root_doc::WellKnownFacet::PathGeneric(val) => wit_doc::WellKnownFacet::PathGeneric(val),
            root_doc::WellKnownFacet::ImageMetadata(val) => {
                wit_doc::WellKnownFacet::ImageMetadata(wit_doc::ImageMetadata {
                    facet_ref: val.facet_ref.to_string(),
                    ref_heads: am_utils_rs::serialize_commit_heads(&val.ref_heads.0),
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                })
            }
            root_doc::WellKnownFacet::OcrResult(val) => {
                wit_doc::WellKnownFacet::OcrResult(wit_doc::OcrResult {
                    facet_ref: val.facet_ref.to_string(),
                    ref_heads: am_utils_rs::serialize_commit_heads(&val.ref_heads.0),
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
                    ref_heads: am_utils_rs::serialize_commit_heads(&val.ref_heads.0),
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
            root_doc::WellKnownFacet::Body(body) => wit_doc::WellKnownFacet::Body(wit_doc::Body {
                order: body.order.into_iter().map(|url| url.to_string()).collect(),
            }),
            root_doc::WellKnownFacet::Dmeta(dmeta) => {
                wit_doc::WellKnownFacet::Dmeta(wit_doc::Dmeta {
                    id: dmeta.id,
                    created_at: dmeta.created_at.into(),
                    updated_at: dmeta.updated_at.into_iter().map(Into::into).collect(),
                    actors: serde_json::to_string(&dmeta.actors).expect(ERROR_JSON),
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
                                    deleted_at: meta
                                        .deleted_at
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

    #[expect(dead_code)]
    pub fn wit_to_well_known_facet(
        value: wit_doc::WellKnownFacet,
    ) -> Res<root_doc::WellKnownFacet> {
        Ok(match value {
            wit_doc::WellKnownFacet::RefGeneric(val) => root_doc::WellKnownFacet::RefGeneric(val),
            wit_doc::WellKnownFacet::LabelGeneric(val) => {
                root_doc::WellKnownFacet::LabelGeneric(val)
            }
            wit_doc::WellKnownFacet::TitleGeneric(val) => {
                root_doc::WellKnownFacet::TitleGeneric(val)
            }
            wit_doc::WellKnownFacet::PathGeneric(val) => root_doc::WellKnownFacet::PathGeneric(val),
            wit_doc::WellKnownFacet::ImageMetadata(val) => {
                root_doc::WellKnownFacet::ImageMetadata(root_doc::ImageMetadata {
                    facet_ref: val.facet_ref.parse()?,
                    ref_heads: root_doc::ChangeHashSet(am_utils_rs::parse_commit_heads(
                        &val.ref_heads,
                    )?),
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                })
            }
            wit_doc::WellKnownFacet::OcrResult(val) => {
                root_doc::WellKnownFacet::OcrResult(root_doc::OcrResult {
                    facet_ref: val.facet_ref.parse()?,
                    ref_heads: root_doc::ChangeHashSet(am_utils_rs::parse_commit_heads(
                        &val.ref_heads,
                    )?),
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
                    facet_ref: val.facet_ref.parse()?,
                    ref_heads: root_doc::ChangeHashSet(am_utils_rs::parse_commit_heads(
                        &val.ref_heads,
                    )?),
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
            wit_doc::WellKnownFacet::Body(body) => root_doc::WellKnownFacet::Body(root_doc::Body {
                order: body
                    .order
                    .into_iter()
                    .map(|url| {
                        url.parse().wrap_err_with(|| {
                            format!("invalid Body.order facet reference URL from guest: {url}")
                        })
                    })
                    .collect::<Res<Vec<_>>>()?,
            }),
            wit_doc::WellKnownFacet::Dmeta(dmeta) => {
                root_doc::WellKnownFacet::Dmeta(root_doc::Dmeta {
                    id: dmeta.id,
                    created_at: Timestamp::from_second(dmeta.created_at.seconds as i64)?,
                    updated_at: dmeta
                        .updated_at
                        .into_iter()
                        .map(|dt| Timestamp::from_second(dt.seconds as i64))
                        .collect::<Result<_, _>>()?,
                    actors: serde_json::from_str(&dmeta.actors)?,
                    facet_uuids: dmeta
                        .facet_uuids
                        .into_iter()
                        .map(|(facet_uuid_str, facet_key_str)| {
                            eyre::Ok((
                                Uuid::parse_str(&facet_uuid_str)?,
                                root_doc::FacetKey::from(facet_key_str),
                            ))
                        })
                        .collect::<Result<_, _>>()?,
                    facets: dmeta
                        .facets
                        .into_iter()
                        .map(|(facet_key_str, facet_meta)| {
                            eyre::Ok((
                                root_doc::FacetKey::from(facet_key_str),
                                root_doc::FacetMeta {
                                    created_at: Timestamp::from_second(
                                        facet_meta.created_at.seconds as i64,
                                    )?,
                                    updated_at: facet_meta
                                        .updated_at
                                        .into_iter()
                                        .map(|dt| Timestamp::from_second(dt.seconds as i64))
                                        .collect::<Result<_, _>>()?,
                                    deleted_at: facet_meta
                                        .deleted_at
                                        .into_iter()
                                        .map(|dt| Timestamp::from_second(dt.seconds as i64))
                                        .collect::<Result<_, _>>()?,
                                    uuid: facet_meta
                                        .uuid
                                        .into_iter()
                                        .map(|facet_uuid_str| Uuid::parse_str(&facet_uuid_str))
                                        .collect::<Result<_, _>>()?,
                                },
                            ))
                        })
                        .collect::<Result<_, _>>()?,
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
        })
    }

    #[expect(dead_code)]
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

mod caps;
mod mltools;
mod stateless_view_host;

pub(crate) use binds_guest::AllGuestPre;
pub(crate) use binds_guest::exports::townframe::daybook::stateless_view;
pub use binds_guest::townframe::api_utils::http_service;
pub use binds_guest::townframe::api_utils::mail;
pub use binds_guest::townframe::daybook::capabilities;
pub use binds_guest::townframe::daybook::drawer;
pub use binds_guest::townframe::daybook::facet_routine;
pub use binds_guest::townframe::daybook::mltools_embed;
pub use binds_guest::townframe::daybook::mltools_image_tools;
pub use binds_guest::townframe::daybook::mltools_llm_chat;
pub use binds_guest::townframe::daybook::mltools_ocr;
use binds_guest::townframe::daybook_types::doc as bindgen_doc;
pub use binds_guest::townframe::sqlite::sqlite_connection;
pub(crate) use stateless_view_host::StatelessViewPlugin;

use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use daybook_types::wit::doc as wit_doc;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::plugin::WitInterfaces;
use wash_runtime::wit::{WitInterface, WitWorld};

pub struct DaybookPlugin {
    drawer_repo: Arc<crate::drawer::DrawerRepo>,
    dispatch_repo: Arc<crate::rt::DispatchRepo>,
    blobs_repo: Arc<crate::blobs::BlobsRepo>,
    sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
    config_repo: Arc<crate::config::ConfigRepo>,
    plugs_repo: Arc<crate::plugs::PlugsRepo>,
    rt: RwLock<Option<std::sync::Weak<crate::rt::Rt>>>,
}

impl DaybookPlugin {
    pub fn new(
        drawer_repo: Arc<crate::drawer::DrawerRepo>,
        dispatch_repo: Arc<crate::rt::DispatchRepo>,
        blobs_repo: Arc<crate::blobs::BlobsRepo>,
        sqlite_local_state_repo: Arc<crate::local_state::SqliteLocalStateRepo>,
        config_repo: Arc<crate::config::ConfigRepo>,
        plugs_repo: Arc<crate::plugs::PlugsRepo>,
    ) -> Self {
        Self {
            drawer_repo,
            dispatch_repo,
            blobs_repo,
            sqlite_local_state_repo,
            config_repo,
            plugs_repo,
            rt: default(),
        }
    }

    pub const ID: &str = "townframe:daybook";

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    async fn get_doc(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPathBuf,
        heads: &ChangeHashSet,
    ) -> Res<Option<Arc<daybook_types::doc::Doc>>> {
        self.drawer_repo
            .get_doc_with_facets_at_branch_heads(doc_id, branch_path, heads, None)
            .await
    }

    async fn patch_doc(
        &self,
        branch_path: &BranchPath,
        heads: Option<ChangeHashSet>,
        patch: root_doc::DocPatch,
    ) -> Result<(), crate::drawer::types::DrawerError> {
        self.drawer_repo
            .update_at_heads(patch, branch_path, heads)
            .await
    }

    pub fn attach_rt(&self, rt: std::sync::Weak<crate::rt::Rt>) {
        let mut rt_slot = self.rt.write().expect(ERROR_MUTEX);
        *rt_slot = Some(rt);
    }

    fn rt(&self) -> Res<Arc<crate::rt::Rt>> {
        let weak = self
            .rt
            .read()
            .expect(ERROR_MUTEX)
            .as_ref()
            .cloned()
            .ok_or_else(|| ferr!("daybook runtime not attached to plugin"))?;
        weak.upgrade()
            .ok_or_else(|| ferr!("daybook runtime is no longer available"))
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
            imports: std::collections::HashSet::from([
                WitInterface::from("townframe:utils/types"),
                WitInterface::from("townframe:api-utils/utils"),
                WitInterface::from(
                    "townframe:daybook/drawer,capabilities,facet-routine,mltools-ocr,mltools-embed,mltools-image-tools,mltools-llm-chat",
                ),
            ]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
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
                if iface.interfaces.contains("mltools-image-tools") {
                    mltools_image_tools::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<SharedWashCtx>,
                    >(item.linker(), |ctx| ctx)?;
                }
                if iface.interfaces.contains("mltools-llm-chat") {
                    mltools_llm_chat::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<SharedWashCtx>,
                    >(item.linker(), |ctx| ctx)?;
                }
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let _resolved = (resolved, component_id);
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let _workload_id = workload_id;
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
        use crate::rt::dispatch::{ActiveDispatchArgs, FacetRoutineArgs};

        let heads = match am_utils_rs::parse_commit_heads(&heads) {
            Ok(val) => val,
            Err(err) => return Ok(Err(drawer::GetDocError::InvalidHeads(format!("{err:?}")))),
        };
        let heads = ChangeHashSet(heads);

        let plugin = DaybookPlugin::from_ctx(self);
        let wflow_plugin = wflow::wash_plugin_wflow::WflowPlugin::try_from_ctx(self)
            .ok_or_else(|| wasmtime_err("only wflows are supported as drawer host"))?;
        let job_id = wflow_plugin
            .job_id_of_ctx(self)
            .expect("there should be a job??");
        let Some(dispatch) = plugin.dispatch_repo.get_by_wflow_job(&job_id[..]).await else {
            return Err(wasmtime_err(format!(
                "no active dispatch found for job: {job_id}"
            )));
        };
        let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            branch_path,
            doc_id: dispatch_doc_id,
            ..
        }) = &dispatch.args;
        if *dispatch_doc_id != doc_id {
            return Err(wasmtime_err(format!(
                "doc_id mismatch for get_doc_at_heads: requested={} dispatch={}",
                doc_id, dispatch_doc_id
            )));
        }

        match plugin
            .get_doc(&doc_id, branch_path, &heads)
            .await
            .map_err(wasmtime_err)?
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
            Some(heads) => match am_utils_rs::parse_commit_heads(&heads) {
                Ok(val) => Some(ChangeHashSet(val)),
                Err(err) => {
                    return Ok(Err(drawer::UpdateDocError::InvalidHeads(format!(
                        "{err:?}"
                    ))));
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
            .patch_doc(BranchPath::new(&branch_path), heads, patch)
            .await
        {
            Ok(_) => Ok(Ok(())),
            Err(crate::drawer::types::DrawerError::DocNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::DocNotFound))
            }
            Err(crate::drawer::types::DrawerError::BranchNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::BranchNotFound))
            }
            Err(crate::drawer::types::DrawerError::BranchAlreadyExists { .. }) => Err(
                wasmtime_err("unexpected branch already exists on patch_doc"),
            ),
            Err(crate::drawer::types::DrawerError::InvalidKey {
                inner: root_doc::FacetTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(drawer::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::types::DrawerError::Other { inner }) => {
                Err(wasmtime_err(format!("unexepcted error: {inner}")))
            }
        }
    }
}

pub(crate) async fn build_doc_facet_tokens(
    ctx: &mut SharedWashCtx,
    plugin: &Arc<DaybookPlugin>,
    doc_tokens: &dispatch::DocFacetTokens,
) -> wasmtime::Result<facet_routine::DocFacetTokens> {
    let doc_rights = caps::doc_rights_from_facet_acl(&doc_tokens.facet_acl);
    let doc_token = ctx.table.push(caps::DocToken {
        doc_id: doc_tokens.doc_id.clone(),
        branch_path: doc_tokens.branch_path.clone(),
        staging_branch_path: doc_tokens.staging_branch_path.clone(),
        heads: doc_tokens.heads.clone(),
        rights: doc_rights,
        facet_acl: doc_tokens.facet_acl.clone(),
    })?;

    let doc = match plugin
        .get_doc(
            &doc_tokens.doc_id,
            &doc_tokens.branch_path,
            &doc_tokens.heads,
        )
        .await
        .map_err(wasmtime_err)?
    {
        Some(doc) => doc,
        None => {
            return Ok(facet_routine::DocFacetTokens {
                doc: doc_token,
                facets: vec![],
                tags: vec![],
            });
        }
    };

    // Build facet tokens: for every existing facet that matches any ACL entry,
    // aggregate rights from all matching entries (tag-wide + key-specific).
    let mut facet_tokens: Vec<wasmtime::component::Resource<capabilities::FacetToken>> = Vec::new();
    for facet_key in doc.facets.keys() {
        let mut rights = capabilities::FacetRights::empty();
        for access in &doc_tokens.facet_acl {
            if access.tag.0 != facet_key.tag.to_string() {
                continue;
            }
            if let Some(id) = &access.key_id
                && id != &facet_key.id
            {
                continue;
            }
            rights |= caps::facet_rights_from_access(access);
        }
        if rights == capabilities::FacetRights::empty() {
            continue;
        }
        let ftoken = ctx.table.push(caps::FacetToken {
            doc_id: doc_tokens.doc_id.clone(),
            branch_path: doc_tokens.branch_path.clone(),
            staging_branch_path: doc_tokens.staging_branch_path.clone(),
            heads: doc_tokens.heads.clone(),
            facet_key: facet_key.clone(),
            rights,
        })?;
        facet_tokens.push(ftoken);
    }

    // Build tag tokens: one per tag that has at least one tag-wide ACL entry.
    // Aggregate rights from all tag-wide entries for that tag.
    let mut tag_tokens: Vec<wasmtime::component::Resource<capabilities::FacetTagToken>> =
        Vec::new();
    let mut tag_rights_map: std::collections::HashMap<String, capabilities::FacetRights> =
        std::collections::HashMap::new();
    for access in &doc_tokens.facet_acl {
        let tag_str = access.tag.0.clone();
        if access.key_id.is_some() {
            // Key-specific entries: only contribute CREATE to the tag token
            // (facet-scoped READ/UPDATE/DELETE are handled by the facet loop above).
            // This ensures routines can call tag_token.create(key_id, data) for
            // key-specific create ACLs even when the facet doesn't exist yet.
            if !access.create {
                continue;
            }
            tag_rights_map
                .entry(tag_str)
                .and_modify(|rights| *rights |= capabilities::FacetRights::CREATE)
                .or_insert(capabilities::FacetRights::CREATE);
        } else {
            let entry_rights = caps::facet_rights_from_access(access);
            tag_rights_map
                .entry(tag_str)
                .and_modify(|rights| *rights |= entry_rights)
                .or_insert(entry_rights);
        }
    }
    for (tag_str, rights) in tag_rights_map {
        let ttoken = ctx.table.push(caps::FacetTagToken {
            doc_id: doc_tokens.doc_id.clone(),
            branch_path: doc_tokens.branch_path.clone(),
            staging_branch_path: doc_tokens.staging_branch_path.clone(),
            heads: doc_tokens.heads.clone(),
            tag: tag_str.clone(),
            rights,
            facet_acl: doc_tokens.facet_acl.clone(),
        })?;
        tag_tokens.push(ttoken);
    }

    Ok(facet_routine::DocFacetTokens {
        doc: doc_token,
        facets: facet_tokens,
        tags: tag_tokens,
    })
}

impl facet_routine::Host for SharedWashCtx {
    async fn get_args(&mut self) -> wasmtime::Result<facet_routine::FacetRoutineArgs> {
        use crate::rt::*;

        let wflow_plugin = wflow::wash_plugin_wflow::WflowPlugin::try_from_ctx(self)
            .ok_or_else(|| wasmtime_err("only wflows are supported as facet-routine"))?;
        let dayook_plugin = DaybookPlugin::from_ctx(self);
        let job_id = wflow_plugin
            .job_id_of_ctx(self)
            .expect("there should be a job??");
        let Some(dispatch) = dayook_plugin
            .dispatch_repo
            .get_by_wflow_job(&job_id[..])
            .await
        else {
            return Err(wasmtime_err(format!(
                "no active dispatch found for job: {job_id}"
            )));
        };
        let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            doc_id,
            branch_path: _,
            staging_branch_path: _,
            heads,
            invocation,
            primary_doc,
            config_docs,
            local_state_acl,
            command_invoke_acl_snapshot,
            wflow_args_json: _,
        }) = &dispatch.args;
        let ActiveDispatchDeets::Wflow { plug_id, .. } = &dispatch.deets;

        let primary_doc_tokens = build_doc_facet_tokens(self, &dayook_plugin, primary_doc).await?;

        let mut config_doc_tokens: Vec<facet_routine::DocFacetTokens> = Vec::new();
        if !config_docs.is_empty() {
            let mut owner_config_docs: HashMap<String, (String, ChangeHashSet)> = HashMap::new();
            for config_doc_meta in config_docs {
                let owner_plug_id =
                    config_doc_owner_plug_id(&config_doc_meta.facet_acl, plug_id.as_str())?;
                let (config_doc_id, config_heads) =
                    if let Some(found) = owner_config_docs.get(&owner_plug_id) {
                        found.clone()
                    } else {
                        let config_doc_id = dayook_plugin
                        .plugs_repo
                        .get_or_init_plug_config_doc_id(&owner_plug_id, &dayook_plugin.drawer_repo)
                        .await
                        .map_err(|err| {
                            wasmtime_err(format!(
                            "error getting/initializing config doc for plug {owner_plug_id}: {err}"
                        ))
                        })?;
                        let config_heads = dayook_plugin
                            .drawer_repo
                            .get_doc_branches(&config_doc_id)
                            .await
                            .map_err(|err| {
                                wasmtime_err(format!("error getting config doc branches: {err}"))
                            })?
                            .and_then(|doc| doc.branches.get("main").cloned())
                            .ok_or_else(|| {
                                wasmtime_err(format!(
                                    "config doc missing main branch for plug {owner_plug_id}"
                                ))
                            })?;
                        owner_config_docs.insert(
                            owner_plug_id.clone(),
                            (config_doc_id.clone(), config_heads.clone()),
                        );
                        (config_doc_id, config_heads)
                    };
                let config_doc_tokens_meta = dispatch::DocFacetTokens {
                    doc_id: config_doc_id,
                    branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                    staging_branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                    heads: config_heads,
                    facet_acl: config_doc_meta.facet_acl.clone(),
                };
                let tokens =
                    build_doc_facet_tokens(self, &dayook_plugin, &config_doc_tokens_meta).await?;
                config_doc_tokens.push(tokens);
            }
        }

        let mut sqlite_connections: Vec<(
            String,
            wasmtime::component::Resource<sqlite_connection::Connection>,
        )> = Vec::new();
        for local_state_access in local_state_acl {
            let local_state_id = crate::local_state::SqliteLocalStateRepo::local_state_id(
                &local_state_access.plug_id,
                &local_state_access.local_state_key.0,
            );
            // Eagerly resolve the sqlite ctx + file path here (the orchestrator),
            // then hand a fully-resolved token to the sqlite plugin which only
            // manages the wasm resource table + query execution.
            let sqlite_file_path = dayook_plugin
                .sqlite_local_state_repo
                .get_sqlite_file_path(&local_state_id)
                .await
                .map_err(|err| {
                    wasmtime_err(format!(
                        "error resolving sqlite file path for {local_state_id}: {err}"
                    ))
                })?;
            let sql = dayook_plugin
                .sqlite_local_state_repo
                .ensure_sqlite_ctx(&local_state_id)
                .await
                .map_err(|err| {
                    wasmtime_err(format!(
                        "error initializing sqlite ctx for {local_state_id}: {err}"
                    ))
                })?;
            let handle = wash_plugin_sqlite::SqlPlugin::create_connection(
                self,
                wash_plugin_sqlite::SqliteConnectionToken {
                    sqlite_file_path: sqlite_file_path.to_string_lossy().to_string(),
                    sql,
                },
            )?;
            sqlite_connections.push((
                format!(
                    "{}/{}",
                    local_state_access.plug_id, local_state_access.local_state_key.0
                ),
                handle,
            ));
        }

        let mut command_invoke_tokens: Vec<(
            String,
            wasmtime::component::Resource<capabilities::CommandInvokeToken>,
        )> = Vec::new();
        for target_url in command_invoke_acl_snapshot {
            let token = self.table.push(caps::CommandInvokeToken {
                parent_wflow_job_id: Arc::clone(&job_id),
                target_url: target_url.to_string(),
            })?;
            command_invoke_tokens.push((target_url.to_string(), token));
        }

        let wit_invocation = match invocation {
            dispatch::RoutineInvocation::Processor(proc) => {
                facet_routine::RoutineInvocation::Processor(facet_routine::ProcessorInvocation {
                    trigger_doc_id: proc.trigger_doc_id.clone(),
                    changed_facet_keys: proc.changed_facet_keys.clone(),
                })
            }
            dispatch::RoutineInvocation::Command => facet_routine::RoutineInvocation::Command,
        };

        Ok(facet_routine::FacetRoutineArgs {
            doc_id: doc_id.clone(),
            heads: am_utils_rs::serialize_commit_heads(heads.as_ref()),
            invocation: wit_invocation,
            primary_doc: primary_doc_tokens,
            config_docs: config_doc_tokens,
            command_invoke_tokens,
            sqlite_connections,
        })
    }
}

fn config_doc_owner_plug_id(
    facet_acl: &[daybook_types::manifest::RoutineFacetAccess],
    default_owner_plug_id: &str,
) -> wasmtime::Result<String> {
    let mut owner_plug_id: Option<String> = None;
    for access in facet_acl {
        let access_owner = access
            .owner_plug_id
            .as_deref()
            .unwrap_or(default_owner_plug_id);
        match owner_plug_id.as_deref() {
            None => owner_plug_id = Some(access_owner.to_string()),
            Some(existing) if existing == access_owner => {}
            Some(existing) => {
                return Err(wasmtime_err(format!(
                    "config doc facet ACL mixes owner_plug_id values: expected {existing}, found {access_owner}"
                )));
            }
        }
    }

    Ok(owner_plug_id.unwrap_or_else(|| default_owner_plug_id.to_string()))
}

/// Host plugin for `townframe:api-utils/http-service`.
///
/// Playground implementation, not config-driven yet: resolves a single sqlite
/// connection from a hardcoded file path and hands it to the guest via
/// `get-args`, mirroring how [`DaybookPlugin`] provides `facet-routine` args.
pub struct ServicePlugin {
    sqlite_file_path: PathBuf,
    sql: tokio::sync::RwLock<Option<SqlCtx>>,
}

impl Default for ServicePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl ServicePlugin {
    pub const ID: &str = "townframe:api-utils/http-service";

    pub fn new() -> Self {
        Self {
            sqlite_file_path: std::env::temp_dir().join("townframe-auth.sqlite"),
            sql: default(),
        }
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    async fn sql_ctx(&self) -> Res<SqlCtx> {
        if let Some(sql) = self.sql.read().await.clone() {
            return Ok(sql);
        }
        let sqlite_url = format!("sqlite://{}", self.sqlite_file_path.display());
        sqlx_utils_rs::init_sqlite_vec();
        let sql = sqlx_utils_rs::SqlCtx::url(&sqlite_url)
            .await
            .wrap_err("error initializing auth sqlite ctx")?;
        let mut slot = self.sql.write().await;
        *slot = Some(sql.clone());
        Ok(sql)
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for ServicePlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:api-utils/http-service",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "api-utils"
                && iface.interfaces.contains("http-service")
            {
                http_service::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                    item.linker(),
                    |ctx| ctx,
                )?;
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
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl http_service::Host for SharedWashCtx {
    async fn get_args(&mut self) -> wasmtime::Result<http_service::ServiceArgs> {
        let plugin = ServicePlugin::from_ctx(self);
        let sql = plugin.sql_ctx().await.map_err(wasmtime_err)?;
        let handle = wash_plugin_sqlite::SqlPlugin::create_connection(
            self,
            wash_plugin_sqlite::SqliteConnectionToken {
                sqlite_file_path: plugin.sqlite_file_path.to_string_lossy().to_string(),
                sql,
            },
        )?;
        Ok(http_service::ServiceArgs {
            sqlite_connections: vec![("auth-db".to_string(), handle)],
        })
    }
}

/// Host plugin for `townframe:api-utils/mail`.
///
/// Native SMTP transport (lettre) for the btress auth service. Config comes
/// from env vars (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`,
/// `SMTP_TLS`, `MAIL_FROM`); the transport is built lazily and cached.
pub struct MailPlugin {
    smtp: tokio::sync::RwLock<Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>>,
    from: String,
}

impl Default for MailPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl MailPlugin {
    pub const ID: &str = "townframe:api-utils/mail";

    pub fn new() -> Self {
        Self {
            smtp: default(),
            from: std::env::var("MAIL_FROM").unwrap_or_else(|_| "btress@localhost".into()),
        }
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    async fn transport(&self) -> Res<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>> {
        if let Some(smtp) = self.smtp.read().await.clone() {
            return Ok(smtp);
        }
        let host = std::env::var("SMTP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("SMTP_PORT")
            .ok()
            .and_then(|port| port.parse().ok())
            .unwrap_or(2500);
        let user = std::env::var("SMTP_USER").unwrap_or_default();
        let pass = std::env::var("SMTP_PASS").unwrap_or_default();
        let tls = std::env::var("SMTP_TLS").unwrap_or_else(|_| "plain".into());

        let mut builder = match tls.as_str() {
            "starttls" => {
                lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::starttls_relay(&host)?
            }
            "tls" => lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(&host)?,
            _ => lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::builder_dangerous(&host),
        };
        builder = builder.port(port);
        if !user.is_empty() {
            builder = builder.credentials(
                lettre::transport::smtp::authentication::Credentials::new(user, pass),
            );
        }
        let smtp = builder.build();
        let mut slot = self.smtp.write().await;
        *slot = Some(smtp.clone());
        Ok(smtp)
    }

    async fn send(&self, message: mail::EmailMessage) -> Res<()> {
        let transport = self.transport().await?;
        let from = message
            .from_address
            .clone()
            .unwrap_or_else(|| self.from.clone());
        let mut builder = lettre::Message::builder()
            .from(from.parse()?)
            .to(message.to.parse()?)
            .subject(message.subject.clone())
            .header(lettre::message::header::ContentType::TEXT_HTML);
        if let Some(reply_to) = &message.reply_to {
            builder = builder.reply_to(reply_to.parse()?);
        }
        let email = builder.body(message.html)?;
        transport.send(email).await?;
        Ok(())
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for MailPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:api-utils/mail",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "api-utils"
                && iface.interfaces.contains("mail")
            {
                mail::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                    item.linker(),
                    |ctx| ctx,
                )?;
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
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl mail::Host for SharedWashCtx {
    async fn send(
        &mut self,
        message: mail::EmailMessage,
    ) -> wasmtime::Result<Result<(), mail::MailError>> {
        let plugin = MailPlugin::from_ctx(self);
        match plugin.send(message).await {
            Ok(()) => Ok(Ok(())),
            Err(err) => Ok(Err(mail::MailError::SendFailed(err.to_string()))),
        }
    }
}
