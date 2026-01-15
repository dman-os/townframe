use crate::interlude::*;
use daybook_types::doc::{self as root_doc};

mod binds_guest {
    use daybook_types::doc::{self as root_doc};
    use daybook_types::wit::doc as wit_doc;

    use townframe::daybook_types::doc as binds_doc;

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

    impl From<api_utils_rs::wit::townframe::api_utils::utils::Datetime>
        for townframe::api_utils::utils::Datetime
    {
        fn from(value: api_utils_rs::wit::townframe::api_utils::utils::Datetime) -> Self {
            Self {
                seconds: value.seconds,
                nanoseconds: value.nanoseconds,
            }
        }
    }
    impl From<binds_doc::WellKnownProp> for wit_doc::WellKnownProp {
        fn from(value: binds_doc::WellKnownProp) -> Self {
            match value {
                binds_doc::WellKnownProp::RefGeneric(val) => {
                    wit_doc::WellKnownProp::RefGeneric(val)
                }
                binds_doc::WellKnownProp::LabelGeneric(val) => {
                    wit_doc::WellKnownProp::LabelGeneric(val)
                }
                binds_doc::WellKnownProp::PseudoLabel(val) => {
                    wit_doc::WellKnownProp::PseudoLabel(val)
                }
                binds_doc::WellKnownProp::TitleGeneric(val) => {
                    wit_doc::WellKnownProp::TitleGeneric(val)
                }
                binds_doc::WellKnownProp::PathGeneric(val) => {
                    wit_doc::WellKnownProp::PathGeneric(val)
                }
                binds_doc::WellKnownProp::ImageMetadata(val) => {
                    wit_doc::WellKnownProp::ImageMetadata(root_doc::ImageMetadata {
                        mime: val.mime,
                        width_px: val.width_px,
                        height_px: val.height_px,
                    })
                }
                binds_doc::WellKnownProp::Content(val) => {
                    wit_doc::WellKnownProp::Content(match val {
                        binds_doc::DocContent::Text(val) => root_doc::DocContent::Text(val),
                        binds_doc::DocContent::Blob(val) => {
                            root_doc::DocContent::Blob(root_doc::Blob {
                                length_octets: val.length_octets,
                                hash: val.hash,
                            })
                        }
                    })
                }
                binds_doc::WellKnownProp::Pending(pending) => {
                    wit_doc::WellKnownProp::Pending(wit_doc::Pending { key: pending.key })
                }
            }
        }
    }
    impl From<wit_doc::WellKnownProp> for binds_doc::WellKnownProp {
        fn from(value: wit_doc::WellKnownProp) -> Self {
            match value {
                wit_doc::WellKnownProp::RefGeneric(val) => {
                    binds_doc::WellKnownProp::RefGeneric(val)
                }
                wit_doc::WellKnownProp::LabelGeneric(val) => {
                    binds_doc::WellKnownProp::LabelGeneric(val)
                }
                wit_doc::WellKnownProp::PseudoLabel(val) => {
                    binds_doc::WellKnownProp::PseudoLabel(val)
                }
                wit_doc::WellKnownProp::TitleGeneric(val) => {
                    binds_doc::WellKnownProp::TitleGeneric(val)
                }
                wit_doc::WellKnownProp::PathGeneric(val) => {
                    binds_doc::WellKnownProp::PathGeneric(val)
                }
                wit_doc::WellKnownProp::ImageMetadata(val) => {
                    binds_doc::WellKnownProp::ImageMetadata(binds_doc::ImageMetadata {
                        mime: val.mime,
                        width_px: val.width_px,
                        height_px: val.height_px,
                    })
                }
                wit_doc::WellKnownProp::Content(val) => {
                    binds_doc::WellKnownProp::Content(match val {
                        root_doc::DocContent::Text(val) => binds_doc::DocContent::Text(val),
                        root_doc::DocContent::Blob(val) => {
                            binds_doc::DocContent::Blob(binds_doc::Blob {
                                length_octets: val.length_octets,
                                hash: val.hash,
                            })
                        }
                    })
                }
                wit_doc::WellKnownProp::Pending(val) => {
                    binds_doc::WellKnownProp::Pending(binds_doc::Pending { key: val.key })
                } // wit_doc::WellKnownProp::CreatedAt(datetime) => binds_doc::WellKnownProp::CreaedAt(),
                  // wit_doc::WellKnownProp::UpdatedAt(datetime) => todo!(),
            }
        }
    }
    impl From<wit_doc::Doc> for binds_doc::Doc {
        fn from(value: wit_doc::Doc) -> Self {
            Self {
                id: value.id,
                created_at: value.created_at.into(),
                updated_at: value.updated_at.into(),
                props: value.props.into_iter().collect(),
            }
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
        let Some(doc) = self.drawer_repo.get_at_heads(doc_id, heads).await? else {
            return Ok(None);
        };
        Ok(Some(doc))
    }

    async fn patch_doc(
        &self,
        branch_path: daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
        patch: root_doc::DocPatch,
    ) -> Result<(), crate::drawer::UpdateDocErr> {
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

    async fn on_component_bind(
        &self,
        component: &mut wash_runtime::engine::workload::WorkloadComponent,
        _interface_configs: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let world = component.world();
        for iface in world.imports {
            if iface.namespace == "townframe" && iface.package == "daybook" {
                if iface.interfaces.contains("drawer") {
                    drawer::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        component.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("capabilities") {
                    capabilities::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        component.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("prop-routine") {
                    prop_routine::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        component.linker(),
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
                let wit_doc: wit_doc::Doc = (*doc).clone().into();
                let bind_doc: bindgen_doc::Doc = wit_doc.into();
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
            props_set: patch.props_set.into_iter().collect(),
            props_remove: patch.props_remove,
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
            Err(crate::drawer::UpdateDocErr::DocNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::DocNotFound))
            }
            Err(crate::drawer::UpdateDocErr::BranchNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::BranchNotFound))
            }
            Err(crate::drawer::UpdateDocErr::InvalidKey {
                inner: root_doc::DocPropTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(drawer::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::UpdateDocErr::Other { inner }) => {
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
                let wit_doc: wit_doc::Doc = (*doc).clone().into();
                let bind_doc: bindgen_doc::Doc = wit_doc.into();
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
                let wit_doc: wit_doc::Doc = (*doc).clone().into();
                let bind_doc: bindgen_doc::Doc = wit_doc.into();
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
            props_set: patch.props_set.into_iter().collect(),
            props_remove: patch.props_remove,
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
            Err(crate::drawer::UpdateDocErr::DocNotFound { .. }) => todo!(),
            Err(crate::drawer::UpdateDocErr::BranchNotFound { .. }) => todo!(),
            Err(crate::drawer::UpdateDocErr::InvalidKey {
                inner: root_doc::DocPropTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::UpdateDocErr::Other { inner }) => {
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
    prop_key: daybook_types::doc::DocPropKey,
}

impl capabilities::HostPropTokenRo for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::PropTokenRo>,
    ) -> wasmtime::Result<bindgen_doc::DocProp> {
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
                let Some(prop) = doc.props.get(&token.prop_key) else {
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
    prop_key: daybook_types::doc::DocPropKey,
    #[allow(dead_code)]
    prop_acl: Vec<crate::plugs::manifest::RoutinePropAccess>,
}

impl capabilities::HostPropTokenRw for SharedWashCtx {
    async fn get(
        &mut self,
        handle: wasmtime::component::Resource<capabilities::PropTokenRw>,
    ) -> wasmtime::Result<bindgen_doc::DocProp> {
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
                let Some(prop) = doc.props.get(&token.prop_key) else {
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
        prop: bindgen_doc::DocProp,
    ) -> wasmtime::Result<Result<(), capabilities::UpdateDocError>> {
        let plugin = DaybookPlugin::from_ctx(self);
        let token = self
            .table
            .get(&handle)
            .context("error locating token")
            .to_anyhow()?;
        let prop: daybook_types::doc::DocProp = wit_doc::doc_prop_into(&prop)
            .map_err(|err| capabilities::UpdateDocError::InvalidPatch(err.to_string()))?;
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    props_set: HashMap::from([(token.prop_key.clone(), prop)]),
                    props_remove: default(),
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
            Err(crate::drawer::UpdateDocErr::DocNotFound { .. }) => todo!(),
            Err(crate::drawer::UpdateDocErr::BranchNotFound { .. }) => todo!(),
            Err(crate::drawer::UpdateDocErr::InvalidKey {
                inner: root_doc::DocPropTagParseError::NotDomainName { _tag: tag },
            }) => Ok(Err(capabilities::UpdateDocError::InvalidKey(tag))),
            Err(crate::drawer::UpdateDocErr::Other { inner }) => {
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
        use daybook_types::doc::DocPropKey;

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
            let prop_key =
                DocPropKey::Tag(daybook_types::doc::DocPropTag::from(access.tag.0.clone()));
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
