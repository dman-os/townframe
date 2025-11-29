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

use wash_runtime::engine::ctx::Ctx as WashCtx;
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
            if iface.namespace == "townframe" && iface.package == "am-repo" {
                if iface.interfaces.contains("repo") {
                    repo::add_to_linker::<_, wasmtime::component::HasSelf<WashCtx>>(
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

impl repo::Host for WashCtx {
    async fn reconcile_path(
        &mut self,
        doc_id: repo::DocId,
        obj_id: repo::ObjId,
        path: Vec<repo::PathProp>,
        json: repo::Json,
    ) -> wasmtime::Result<Result<(), repo::ReconcileError>> {
        let plugin = AmRepoPlugin::from_ctx(self);

        // Convert WIT types to Rust types
        let doc_id_rust: samod::DocumentId = doc_id
            .parse()
            .map_err(|e| wasmtime::Error::msg(format!("invalid doc-id: {e}")))?;

        let obj_id_rust = match obj_id {
            repo::ObjId::Root => automerge::ObjId::Root,
            repo::ObjId::Id((counter, actor_id, op_id)) => {
                automerge::ObjId::Id(counter, actor_id.into(), op_id as usize)
            }
        };

        let path_rust: Vec<autosurgeon::Prop<'static>> = path
            .into_iter()
            .map(|p| match p {
                repo::PathProp::Key(key) => autosurgeon::Prop::Key(key.into()),
                repo::PathProp::Index(idx) => autosurgeon::Prop::Index(idx as u32),
            })
            .collect();

        let json_value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|e| wasmtime::Error::msg(format!("invalid json: {e}")))?;

        // Convert JSON to AutosurgeonJson for reconciliation
        let autosurgeon_json = utils_rs::am::AutosurgeonJson(json_value);

        match plugin
            .am_ctx
            .reconcile_path(&doc_id_rust, obj_id_rust, path_rust, &autosurgeon_json)
            .await
        {
            Ok(()) => Ok(Ok(())),
            Err(e) => {
                if e.to_string().contains("doc not found") {
                    Ok(Err(repo::ReconcileError::DocNotFound))
                } else if e.to_string().contains("invalid json") {
                    Ok(Err(repo::ReconcileError::InvalidJson(e.to_string())))
                } else {
                    Ok(Err(repo::ReconcileError::Other(e.to_string())))
                }
            }
        }
    }

    async fn reconcile_path_at_head(
        &mut self,
        doc_id: repo::DocId,
        heads: repo::Heads,
        obj_id: repo::ObjId,
        path: Vec<repo::PathProp>,
        json: repo::Json,
    ) -> wasmtime::Result<Result<(), repo::ReconcileError>> {
        let plugin = AmRepoPlugin::from_ctx(self);

        // Convert WIT types to Rust types
        let doc_id_rust: samod::DocumentId = doc_id
            .parse()
            .map_err(|e| wasmtime::Error::msg(format!("invalid doc-id: {e}")))?;

        // Parse heads from base32 strings to ChangeHash
        let heads_rust: Result<Vec<automerge::ChangeHash>, _> = heads
            .iter()
            .map(|head_str| {
                utils_rs::hash::decode_base32_multibase(head_str).and_then(|bytes| {
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| ferr!("invalid change hash length"))
                })
            })
            .collect();

        let heads_rust =
            heads_rust.map_err(|e| wasmtime::Error::msg(format!("error parsing heads: {e}")))?;

        let obj_id_rust = match obj_id {
            repo::ObjId::Root => automerge::ObjId::Root,
            repo::ObjId::Id((counter, actor_id, op_id)) => {
                automerge::ObjId::Id(counter, actor_id.into(), op_id as usize)
            }
        };

        let path_rust: Vec<autosurgeon::Prop<'static>> = path
            .into_iter()
            .map(|p| match p {
                repo::PathProp::Key(key) => autosurgeon::Prop::Key(key.into()),
                repo::PathProp::Index(idx) => autosurgeon::Prop::Index(idx as u32),
            })
            .collect();

        let json_value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|e| wasmtime::Error::msg(format!("invalid json: {e}")))?;

        // Convert JSON to AutosurgeonJson for reconciliation
        let autosurgeon_json = utils_rs::am::AutosurgeonJson(json_value);

        match plugin
            .am_ctx
            .reconcile_path_at_heads(&doc_id_rust, &heads_rust, obj_id_rust, path_rust, &autosurgeon_json)
            .await
        {
            Ok(()) => Ok(Ok(())),
            Err(e) => {
                if e.to_string().contains("doc not found") {
                    Ok(Err(repo::ReconcileError::DocNotFound))
                } else if e.to_string().contains("invalid json") {
                    Ok(Err(repo::ReconcileError::InvalidJson(e.to_string())))
                } else {
                    Ok(Err(repo::ReconcileError::Other(e.to_string())))
                }
            }
        }
    }

    async fn hydrate_path_at_head(
        &mut self,
        doc_id: repo::DocId,
        heads: repo::Heads,
        obj_id: repo::ObjId,
        path: Vec<repo::PathProp>,
    ) -> wasmtime::Result<Result<repo::Json, repo::HydrateAtHeadError>> {
        let plugin = AmRepoPlugin::from_ctx(self);

        // Convert WIT types to Rust types
        let doc_id_rust: samod::DocumentId = doc_id
            .parse()
            .map_err(|e| wasmtime::Error::msg(format!("invalid doc-id: {e}")))?;

        // Parse heads from base32 strings to ChangeHash
        let heads_rust: Result<Vec<automerge::ChangeHash>, _> = heads
            .iter()
            .map(|head_str| {
                utils_rs::hash::decode_base32_multibase(head_str).and_then(|bytes| {
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| ferr!("invalid change hash length"))
                })
            })
            .collect();

        let heads_rust =
            heads_rust.map_err(|e| wasmtime::Error::msg(format!("error parsing heads: {e}")))?;

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
            .map(|p| match p {
                repo::PathProp::Key(key) => autosurgeon::Prop::Key(key.into()),
                repo::PathProp::Index(idx) => autosurgeon::Prop::Index(idx as u32),
            })
            .collect();

        // Use hydrate_path_at_head with AutosurgeonJson
        let result = plugin
            .am_ctx
            .hydrate_path_at_heads::<utils_rs::am::AutosurgeonJson>(
                &doc_id_rust,
                &heads_rust,
                obj_id_rust,
                path_rust,
            )
            .await;

        match result {
            Ok(Some(json_wrapper)) => {
                let json_str = serde_json::to_string(&json_wrapper.0)
                    .map_err(|e| wasmtime::Error::msg(format!("error serializing to json: {e}")))?;
                Ok(Ok(json_str))
            }
            Ok(None) => Ok(Err(repo::HydrateAtHeadError::PathNotFound)),
            Err(utils_rs::am::HydrateAtHeadError::HashNotFound(hash)) => Ok(Err(
                repo::HydrateAtHeadError::HashNotFound(format!("{:?}", hash)),
            )),
            Err(utils_rs::am::HydrateAtHeadError::Other(e)) => {
                // Check if it's a doc-not-found error
                if e.to_string().contains("doc not found") {
                    Ok(Err(repo::HydrateAtHeadError::DocNotFound))
                } else if e.to_string().contains("obj not found") {
                    Ok(Err(repo::HydrateAtHeadError::ObjNotFound))
                } else {
                    Err(wasmtime::Error::msg(format!("error hydrating: {e}")))
                }
            }
        }
    }
}
