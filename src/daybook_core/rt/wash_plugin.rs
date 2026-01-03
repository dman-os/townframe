use crate::interlude::*;
use daybook_types::doc::{self as root_doc, DocPropKey};

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
    impl From<binds_doc::DocProp> for wit_doc::DocProp {
        fn from(value: binds_doc::DocProp) -> Self {
            match value {
                binds_doc::DocProp::WellKnown(val) => Self::WellKnown(match val {
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
                }),
                binds_doc::DocProp::Any(val) => Self::Any(val),
            }
        }
    }
    impl From<wit_doc::DocProp> for binds_doc::DocProp {
        fn from(value: wit_doc::DocProp) -> Self {
            match value {
                wit_doc::DocProp::WellKnown(val) => Self::WellKnown(match val {
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
                }),
                wit_doc::DocProp::Any(val) => Self::Any(val),
            }
        }
    }
    impl From<wit_doc::Doc> for binds_doc::Doc {
        fn from(value: wit_doc::Doc) -> Self {
            Self {
                id: value.id,
                created_at: value.created_at.into(),
                updated_at: value.updated_at.into(),
                props: value
                    .props
                    .into_iter()
                    .map(|(key, val)| (key, val.into()))
                    .collect(),
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
use wash_runtime::engine::ctx::Ctx as WashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

pub struct DaybookPlugin {
    drawer_repo: Arc<crate::drawer::DrawerRepo>,
    dispatcher_repo: Arc<crate::rt::DispatcherRepo>,
}

impl DaybookPlugin {
    pub fn new(
        drawer_repo: Arc<crate::drawer::DrawerRepo>,
        dispatcher_repo: Arc<crate::rt::DispatcherRepo>,
    ) -> Self {
        Self {
            drawer_repo,
            dispatcher_repo,
        }
    }

    pub const ID: &str = "townframe:daybook";

    fn from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
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
        heads: &ChangeHashSet,
        patch: bindgen_doc::DocPatch,
    ) -> Result<(), crate::drawer::UpdateDocErr> {
        let patch = wit_doc::DocPatch {
            id: patch.id,
            props_set: patch
                .props_set
                .into_iter()
                .map(|(key, prop)| (key, prop.into()))
                .collect(),
            props_remove: patch.props_remove,
        };
        let doc_patch: daybook_types::doc::DocPatch = patch
            .try_into()
            .map_err(|err| crate::drawer::UpdateDocErr::InvalidKey { inner: err })?;
        self.drawer_repo.update_at_heads(doc_patch, heads).await
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
                "townframe:daybook/drawer",
            )]),
            ..default()
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
                    drawer::add_to_linker::<_, wasmtime::component::HasSelf<WashCtx>>(
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

impl drawer::Host for WashCtx {
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
        heads: drawer::Heads,
        patch: drawer::DocPatch,
    ) -> wasmtime::Result<Result<(), drawer::UpdateDocError>> {
        let heads = match utils_rs::am::parse_commit_heads(&heads) {
            Ok(val) => val,
            Err(err) => {
                return Ok(Err(drawer::UpdateDocError::InvalidHeads(format!(
                    "{err:?}"
                ))))
            }
        };
        let heads = ChangeHashSet(heads);

        let plugin = DaybookPlugin::from_ctx(self);
        match plugin.patch_doc(&heads, patch).await {
            Ok(_) => Ok(Ok(())),
            Err(crate::drawer::UpdateDocErr::DocNotFound { .. }) => {
                Ok(Err(drawer::UpdateDocError::DocNotFound))
            }
            Err(crate::drawer::UpdateDocErr::InvalidKey {
                inner: root_doc::DocPropTagParseError::NotDomainName { tag },
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

impl capabilities::HostDocTokenRo for WashCtx {
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
    heads: ChangeHashSet,
}

impl capabilities::HostDocTokenRw for WashCtx {
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
        match plugin.patch_doc(&token.heads, patch).await {
            Ok(_) => Ok(Ok(())),
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            Err(crate::drawer::UpdateDocErr::DocNotFound { .. }) => todo!(),
            Err(crate::drawer::UpdateDocErr::InvalidKey {
                inner: root_doc::DocPropTagParseError::NotDomainName { tag },
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

pub struct PropTokenRw {
    doc_id: DocId,
    heads: ChangeHashSet,
    prop_key: daybook_types::doc::DocPropKey,
}

impl capabilities::HostPropTokenRw for WashCtx {
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
                let prop: wit_doc::DocProp = prop.clone().into();
                let prop: bindgen_doc::DocProp = prop.into();
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
        let prop: wit_doc::DocProp = prop.into();
        let prop: daybook_types::doc::DocProp = prop.try_into().map_err(|err| {
            let root_doc::DocPropTagParseError::NotDomainName { tag } = err;
            capabilities::UpdateDocError::InvalidKey(tag)
        })?;
        match plugin
            .drawer_repo
            .update_at_heads(
                daybook_types::doc::DocPatch {
                    id: token.doc_id.clone(),
                    props_set: HashMap::from([(token.prop_key.clone(), prop)]),
                    props_remove: default(),
                },
                &token.heads,
            )
            .await
        {
            Ok(_) => Ok(Ok(())),
            // FIXME: either the context should terminal error this
            // or communicate with the wflow engine
            Err(crate::drawer::UpdateDocErr::DocNotFound { .. }) => todo!(),
            Err(crate::drawer::UpdateDocErr::InvalidKey {
                inner: root_doc::DocPropTagParseError::NotDomainName { tag },
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

impl capabilities::Host for WashCtx {}

impl prop_routine::Host for WashCtx {
    async fn get_args(&mut self) -> wasmtime::Result<prop_routine::PropRoutineArgs> {
        use crate::rt::*;
        use anyhow::Context;

        let wflow_plugin = wflow::WflowPlugin::try_from_ctx(self)
            .context("only wflows are supported as prop-routines")?;
        let dayook_plugin = DaybookPlugin::from_ctx(self);
        let job_id = wflow_plugin
            .job_id_of_ctx(self)
            .expect("there should be a job??");
        let Some(dispatch) = dayook_plugin.dispatcher_repo.get(&job_id[..]).await else {
            anyhow::bail!("no active dispatch found for job: {job_id}");
        };
        let ActiveDispatchDeets::PropRoutine(PropRoutineArgs {
            doc_id,
            heads,
            prop_key,
        }) = &dispatch.deets;
        // else {
        //     anyhow::bail!("job is not a prop routine: {job_id}");
        // };
        Ok(prop_routine::PropRoutineArgs {
            doc_id: doc_id.clone(),
            heads: utils_rs::am::serialize_commit_heads(heads.as_ref()),
            prop_key: prop_key.clone(),

            doc_token: self.table.push(DocTokenRo {
                doc_id: doc_id.clone(),
                heads: heads.clone(),
            })?,
            prop_token: self.table.push(PropTokenRw {
                doc_id: doc_id.clone(),
                heads: heads.clone(),
                prop_key: DocPropKey::from(prop_key.as_str()),
            })?,
        })
    }
}
