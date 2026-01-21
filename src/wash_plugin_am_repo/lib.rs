mod interlude {
    pub use std::sync::Arc;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

mod binds_guest {
    wash_runtime::wasmtime::component::bindgen!({
        world: "guest",

        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
    });
}

use wash_runtime::engine::ctx::{Ctx as WashCtx, SharedCtx as SharedWashCtx};
use wash_runtime::wit::{WitInterface, WitWorld};

// The bindgen macro generates types based on the WIT package structure
// For package "townframe:am-repo" with interface "repo",
// the structure follows: binds_guest::townframe::am_repo::repo
// The Host trait is generated for implementing the host side
use binds_guest::townframe::am_repo::repo;

pub struct AmRepoPlugin {
    am_ctx: Arc<utils_rs::am::AmCtx>,
}

impl AmRepoPlugin {
    pub fn new(am_ctx: Arc<utils_rs::am::AmCtx>) -> Self {
        Self { am_ctx }
    }

    const ID: &str = "townframe:am-repo";

    fn from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
            panic!("plugin not on ctx");
        };
        this
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for AmRepoPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:am-repo/repo",
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
            if iface.namespace == "townframe"
                && iface.package == "am-repo"
                && iface.interfaces.contains("repo")
            {
                repo::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
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
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl repo::Host for SharedWashCtx {
    async fn reconcile_path(
        &mut self,
        doc_id: repo::DocId,
        obj_id: repo::ObjId,
        path: Vec<repo::PathProp>,
        json: repo::Json,
    ) -> wasmtime::Result<Result<(), repo::ReconcileError>> {
        let plugin = AmRepoPlugin::from_ctx(&self.active_ctx);

        // Convert WIT types to Rust types
        let doc_id_rust: samod::DocumentId = doc_id
            .parse()
            .map_err(|err| wasmtime::Error::msg(format!("invalid doc-id: {err}")))?;

        let obj_id_rust = match obj_id {
            repo::ObjId::Root => automerge::ObjId::Root,
            repo::ObjId::Id((counter, actor_id, op_id)) => {
                automerge::ObjId::Id(counter, actor_id.into(), op_id as usize)
            }
        };

        let path_rust: Vec<autosurgeon::Prop<'static>> = path
            .into_iter()
            .map(|prop| match prop {
                repo::PathProp::Key(key) => autosurgeon::Prop::Key(key.into()),
                repo::PathProp::Index(idx) => autosurgeon::Prop::Index(idx as u32),
            })
            .collect();

        let json_value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|err| wasmtime::Error::msg(format!("invalid json: {err}")))?;

        plugin
            .am_ctx
            .reconcile_path(
                &doc_id_rust,
                obj_id_rust,
                path_rust,
                &ThroughJson(json_value),
            )
            .await
            .wrap_err("error on reconcile")
            .to_anyhow()?;

        Ok(Ok(()))
    }

    async fn reconcile_path_at_head(
        &mut self,
        doc_id: repo::DocId,
        heads: repo::Heads,
        obj_id: repo::ObjId,
        path: Vec<repo::PathProp>,
        json: repo::Json,
    ) -> wasmtime::Result<Result<(), repo::ReconcileError>> {
        let plugin = AmRepoPlugin::from_ctx(&self.active_ctx);

        // Convert WIT types to Rust types
        let doc_id_rust: samod::DocumentId = doc_id
            .parse()
            .map_err(|err| wasmtime::Error::msg(format!("invalid doc-id: {err}")))?;

        let heads = match utils_rs::am::parse_commit_heads(&heads) {
            Ok(val) => val,
            Err(err) => return Ok(Err(repo::ReconcileError::InvalidHeads(format!("{err:?}")))),
        };

        let obj_id_rust = match obj_id {
            repo::ObjId::Root => automerge::ObjId::Root,
            repo::ObjId::Id((counter, actor_id, op_id)) => {
                automerge::ObjId::Id(counter, actor_id.into(), op_id as usize)
            }
        };

        let path_rust: Vec<autosurgeon::Prop<'static>> = path
            .into_iter()
            .map(|prop| match prop {
                repo::PathProp::Key(key) => autosurgeon::Prop::Key(key.into()),
                repo::PathProp::Index(idx) => autosurgeon::Prop::Index(idx as u32),
            })
            .collect();

        let json_value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|err| wasmtime::Error::msg(format!("invalid json: {err}")))?;

        plugin
            .am_ctx
            .reconcile_path_at_heads(
                &doc_id_rust,
                &heads,
                obj_id_rust,
                path_rust,
                &ThroughJson(json_value),
            )
            .await
            .wrap_err("error on reconcile")
            .to_anyhow()?;

        Ok(Ok(()))
    }

    async fn hydrate_path_at_head(
        &mut self,
        doc_id: repo::DocId,
        heads: repo::Heads,
        obj_id: repo::ObjId,
        path: Vec<repo::PathProp>,
    ) -> wasmtime::Result<Result<repo::Json, repo::HydrateAtHeadError>> {
        let plugin = AmRepoPlugin::from_ctx(&self.active_ctx);

        // Convert WIT types to Rust types
        let doc_id_rust: samod::DocumentId = doc_id
            .parse()
            .map_err(|err| wasmtime::Error::msg(format!("invalid doc-id: {err}")))?;

        let heads = match utils_rs::am::parse_commit_heads(&heads) {
            Ok(val) => val,
            Err(err) => {
                return Ok(Err(repo::HydrateAtHeadError::InvalidHeads(format!(
                    "{err:?}"
                ))))
            }
        };

        // Convert obj-id to automerge::ObjId
        let obj_id_rust = match obj_id {
            repo::ObjId::Root => automerge::ObjId::Root,
            repo::ObjId::Id((counter, actor_id, op_id)) => {
                automerge::ObjId::Id(counter, actor_id.into(), op_id as usize)
            }
        };

        // Convert path from Vec<PathProp> to Vec<autosurgeon::Prop>
        let path_rust: Vec<autosurgeon::Prop<'static>> = path
            .into_iter()
            .map(|prop| match prop {
                repo::PathProp::Key(key) => autosurgeon::Prop::Key(key.into()),
                repo::PathProp::Index(idx) => autosurgeon::Prop::Index(idx as u32),
            })
            .collect();

        match plugin
            .am_ctx
            .hydrate_path_at_heads::<ThroughJson<serde_json::Value>>(
                &doc_id_rust,
                &heads,
                obj_id_rust,
                path_rust,
            )
            .await
        {
            Ok(Some(json_wrapper)) => {
                let json_str = serde_json::to_string(&json_wrapper.0).map_err(|err| {
                    wasmtime::Error::msg(format!("error serializing to json: {err}"))
                })?;
                Ok(Ok(json_str))
            }
            Ok(None) => Ok(Err(repo::HydrateAtHeadError::PathNotFound)),
            Err(utils_rs::am::HydrateAtHeadError::HashNotFound(hash)) => Ok(Err(
                repo::HydrateAtHeadError::HashNotFound(format!("{hash:?}")),
            )),
            Err(utils_rs::am::HydrateAtHeadError::Other(err)) => {
                Err(anyhow::anyhow!("error on hydrate: {err:?}"))
            }
        }
    }
}
