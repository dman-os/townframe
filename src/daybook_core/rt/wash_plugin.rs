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
            "townframe:daybook/capabilities/doc-token-ro": super::DocTokenRo,
            "townframe:daybook/capabilities/doc-token-rw": super::DocTokenRw,
            "townframe:daybook/capabilities/prop-token-ro": super::PropTokenRo,
            "townframe:daybook/capabilities/prop-token-rw": super::PropTokenRw,
        }
    });

    #[allow(dead_code)]
    pub fn well_known_facet_to_wit(value: root_doc::WellKnownFacet) -> wit_doc::WellKnownProp {
        match value {
            root_doc::WellKnownFacet::RefGeneric(val) => wit_doc::WellKnownProp::RefGeneric(val),
            root_doc::WellKnownFacet::LabelGeneric(val) => {
                wit_doc::WellKnownProp::LabelGeneric(val)
            }
            root_doc::WellKnownFacet::PseudoLabel(val) => wit_doc::WellKnownProp::PseudoLabel(val),
            root_doc::WellKnownFacet::TitleGeneric(val) => {
                wit_doc::WellKnownProp::TitleGeneric(val)
            }
            root_doc::WellKnownFacet::PathGeneric(val) => {
                wit_doc::WellKnownProp::PathGeneric(val.to_string_lossy().into_owned())
            }
            root_doc::WellKnownFacet::ImageMetadata(val) => {
                wit_doc::WellKnownProp::ImageMetadata(root_doc::ImageMetadata {
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                })
            }
            root_doc::WellKnownFacet::Note(val) => wit_doc::WellKnownProp::Note(root_doc::Note {
                mime: val.mime,
                content: val.content,
            }),
            root_doc::WellKnownFacet::Blob(val) => wit_doc::WellKnownProp::Blob(root_doc::Blob {
                length_octets: val.length_octets,
                hash: val.hash,
            }),
            root_doc::WellKnownFacet::Pending(pending) => {
                wit_doc::WellKnownProp::Pending(wit_doc::Pending {
                    key: pending.key.to_string(),
                })
            }
            root_doc::WellKnownFacet::Dmeta(dmeta) => {
                wit_doc::WellKnownProp::Dmeta(wit_doc::Dmeta {
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
    pub fn wit_to_well_known_facet(value: wit_doc::WellKnownProp) -> root_doc::WellKnownFacet {
        match value {
            wit_doc::WellKnownProp::RefGeneric(val) => root_doc::WellKnownFacet::RefGeneric(val),
            wit_doc::WellKnownProp::LabelGeneric(val) => {
                root_doc::WellKnownFacet::LabelGeneric(val)
            }
            wit_doc::WellKnownProp::PseudoLabel(val) => root_doc::WellKnownFacet::PseudoLabel(val),
            wit_doc::WellKnownProp::TitleGeneric(val) => {
                root_doc::WellKnownFacet::TitleGeneric(val)
            }
            wit_doc::WellKnownProp::PathGeneric(val) => {
                root_doc::WellKnownFacet::PathGeneric(val.into())
            }
            wit_doc::WellKnownProp::ImageMetadata(val) => {
                root_doc::WellKnownFacet::ImageMetadata(root_doc::ImageMetadata {
                    mime: val.mime,
                    width_px: val.width_px,
                    height_px: val.height_px,
                })
            }
            wit_doc::WellKnownProp::Pending(pending) => {
                root_doc::WellKnownFacet::Pending(root_doc::Pending {
                    key: root_doc::FacetKey::from(pending.key),
                })
            }
            wit_doc::WellKnownProp::Dmeta(dmeta) => {
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
            wit_doc::WellKnownProp::Note(note) => root_doc::WellKnownFacet::Note(root_doc::Note {
                mime: note.mime,
                content: note.content,
            }),
            wit_doc::WellKnownProp::Blob(blob) => root_doc::WellKnownFacet::Blob(root_doc::Blob {
                length_octets: blob.length_octets,
                hash: blob.hash,
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
pub use binds_guest::townframe::daybook::prop_routine;
use binds_guest::townframe::daybook_types::doc as bindgen_doc;

use daybook_types::doc::ChangeHashSet;
use daybook_types::doc::DocId;
use daybook_types::wit::doc as wit_doc;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

pub struct DaybookPlugin {
    drawer_repo: Arc<crate::drawer::DrawerRepo>,
    dispatch_repo: Arc<crate::rt::DispatchRepo>,
}

impl DaybookPlugin {
    pub fn new(
        drawer_repo: Arc<crate::drawer::DrawerRepo>,
        dispatch_repo: Arc<crate::rt::DispatchRepo>,
    ) -> Self {
        Self {
            drawer_repo,
            dispatch_repo,
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
                "townframe:daybook/drawer,capabilities,prop-routine",
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
                if iface.interfaces.contains("prop-routine") {
                    prop_routine::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
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
                    props: doc
                        .facets
                        .iter()
                        .map(|(facet_key, facet_value)| {
                            (facet_key.to_string(), wit_doc::doc_prop_from(facet_value))
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
            facets_set: patch.props_set.into_iter().collect(),
            facets_remove: patch.props_remove,
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
                    props: doc
                        .facets
                        .iter()
                        .map(|(facet_key, facet_value)| {
                            (facet_key.to_string(), wit_doc::doc_prop_from(facet_value))
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
                    props: doc
                        .facets
                        .iter()
                        .map(|(facet_key, facet_value)| {
                            (facet_key.to_string(), wit_doc::doc_prop_from(facet_value))
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
            facets_set: patch.props_set.into_iter().collect(),
            facets_remove: patch.props_remove,
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

pub struct PropTokenRo {
    doc_id: DocId,
    heads: ChangeHashSet,
    prop_key: daybook_types::doc::FacetKey,
}

impl capabilities::HostPropTokenRo for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::PropTokenRo>,
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
                let Some(prop) = doc.facets.get(&token.prop_key) else {
                    // FIXME: either the context should terminal error this
                    // or communicate with the wflow engine
                    todo!("")
                };
                let prop = wit_doc::doc_prop_from(prop);
                Ok(prop)
            }
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            None => todo!(),
        }
    }

    async fn drop(
        &mut self,
        rep: wasmtime::component::Resource<capabilities::PropTokenRo>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

pub struct PropTokenRw {
    doc_id: DocId,
    branch_path: daybook_types::doc::BranchPath,
    #[allow(dead_code)]
    target_branch_path: daybook_types::doc::BranchPath,
    heads: ChangeHashSet,
    prop_key: daybook_types::doc::FacetKey,
    #[allow(dead_code)]
    prop_acl: Vec<crate::plugs::manifest::RoutinePropAccess>,
}

impl capabilities::HostPropTokenRw for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::PropTokenRw>,
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
                let Some(prop) = doc.facets.get(&token.prop_key) else {
                    // FIXME: either the context should terminal error this
                    // or communicate with the wflow engine
                    todo!("")
                };
                let prop = wit_doc::doc_prop_from(prop);
                Ok(prop)
            }
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            None => todo!(),
        }
    }

    async fn update(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::PropTokenRw>,
        prop: String,
    ) -> wasmtime::Result<Result<(), capabilities::UpdateDocError>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        let prop: daybook_types::doc::FacetRaw = wit_doc::doc_prop_into(&prop)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    facets_set: HashMap::from([(token.prop_key.clone(), prop)]),
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
        rep: wasmtime::component::Resource<capabilities::PropTokenRw>,
    ) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

impl capabilities::Host for SharedWashCtx {}

impl prop_routine::Host for SharedWashCtx {
    async fn get_args(&mut self) -> wasmtime::Result<prop_routine::PropRoutineArgs> {
        use crate::rt::*;
        use anyhow::Context;
        use daybook_types::doc::FacetKey;

        let wflow_plugin = wflow::wash_plugin_wflow::WflowPlugin::try_from_ctx(self)
            .context("only wflows are supported as prop-routine")?;
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
        let ActiveDispatchArgs::PropRoutine(PropRoutineArgs {
            doc_id,
            heads,
            prop_key,
            branch_path: target_branch_path,
            staging_branch_path,
            prop_acl,
        }) = &dispatch.args;

        // Use staging branch path from dispatch (already set when job was created)
        let staging_branch_path = staging_branch_path.clone();

        // Create tokens based on ACL
        let mut rw_prop_tokens: Vec<(
            String,
            wasmtime::component::Resource<capabilities::PropTokenRw>,
        )> = Vec::new();
        let mut ro_prop_tokens: Vec<(
            String,
            wasmtime::component::Resource<capabilities::PropTokenRo>,
        )> = Vec::new();

        for access in prop_acl {
            let prop_key = FacetKey::from(daybook_types::doc::FacetTag::from(access.tag.0.clone()));
            let prop_key_str = prop_key.to_string();

            if access.write {
                let token = self.table.push(PropTokenRw {
                    doc_id: doc_id.clone(),
                    heads: heads.clone(),
                    branch_path: staging_branch_path.clone(),
                    target_branch_path: target_branch_path.clone(),
                    prop_key: prop_key.clone(),
                    prop_acl: prop_acl.clone(),
                })?;
                rw_prop_tokens.push((prop_key_str, token));
            } else if access.read {
                let token = self.table.push(PropTokenRo {
                    doc_id: doc_id.clone(),
                    heads: heads.clone(),
                    prop_key: prop_key.clone(),
                })?;
                ro_prop_tokens.push((prop_key_str, token));
            }
        }

        Ok(prop_routine::PropRoutineArgs {
            doc_id: doc_id.clone(),
            heads: utils_rs::am::serialize_commit_heads(heads.as_ref()),
            prop_key: prop_key.clone(),
            rw_prop_tokens,
            ro_prop_tokens,
        })
    }
}
