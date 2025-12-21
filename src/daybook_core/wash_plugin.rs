use crate::interlude::*;

mod binds_guest {
    wash_runtime::wasmtime::component::bindgen!({
        world: "guest",
        path: "wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
    });
}

pub use binds_guest::townframe::daybook::drawer;

use wash_runtime::engine::ctx::Ctx as WashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

pub struct DaybookPlugin {
    drawer_repo: Arc<crate::drawer::DrawerRepo>,
}

impl DaybookPlugin {
    pub fn new(drawer_repo: Arc<crate::drawer::DrawerRepo>) -> Self {
        Self { drawer_repo }
    }

    pub const ID: &str = "townframe:daybook";

    fn from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
            panic!("plugin not on ctx");
        };
        this
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
    ) -> wasmtime::Result<Result<Option<drawer::Json>, drawer::GetDocError>> {
        let plugin = DaybookPlugin::from_ctx(self);

        // Parse heads from base32 strings to ChangeHashSet
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

        let change_hash_set = crate::ChangeHashSet(Arc::from(heads_rust));

        match plugin
            .drawer_repo
            .get_at_heads(&doc_id, &change_hash_set)
            .await
        {
            Ok(Some(doc)) => {
                let json = serde_json::to_string(&doc)
                    .map_err(|e| wasmtime::Error::msg(format!("error serializing doc: {e}")))?;
                Ok(Ok(Some(json)))
            }
            Ok(None) => Ok(Ok(None)),
            Err(e) => {
                if e.to_string().contains("doc not found") {
                    Ok(Err(drawer::GetDocError::DocNotFound))
                } else if e.to_string().contains("invalid heads") || e.to_string().contains("hash")
                {
                    Ok(Err(drawer::GetDocError::InvalidHeads(e.to_string())))
                } else {
                    Ok(Err(drawer::GetDocError::Other(e.to_string())))
                }
            }
        }
    }

    async fn update_doc_at_heads(
        &mut self,
        doc_id: drawer::DocId,
        heads: drawer::Heads,
        patch: drawer::Json,
    ) -> wasmtime::Result<Result<(), drawer::UpdateDocError>> {
        let plugin = DaybookPlugin::from_ctx(self);

        // Parse heads from base32 strings to ChangeHashSet
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

        let change_hash_set = crate::ChangeHashSet(Arc::from(heads_rust));

        // Parse patch from JSON - DocPatch doesn't implement Deserialize, so we construct it manually
        let patch_value: serde_json::Value = serde_json::from_str(&patch)
            .map_err(|e| wasmtime::Error::msg(format!("invalid json: {e}")))?;

        // Construct DocPatch manually from JSON value
        use std::default::Default;
        let mut doc_patch = daybook_types::doc::DocPatch::default();

        if let Some(id) = patch_value.get("id").and_then(|v| v.as_str()) {
            doc_patch.id = Some(id.to_string());
        }
        if let Some(created_at) = patch_value.get("created_at") {
            doc_patch.created_at = serde_json::from_value(created_at.clone()).map_err(|e| {
                wasmtime::Error::msg(format!("error deserializing created_at: {e}"))
            })?;
        }
        if let Some(updated_at) = patch_value.get("updated_at") {
            doc_patch.updated_at = serde_json::from_value(updated_at.clone()).map_err(|e| {
                wasmtime::Error::msg(format!("error deserializing updated_at: {e}"))
            })?;
        }
        if let Some(content) = patch_value.get("content") {
            doc_patch.content = serde_json::from_value(content.clone())
                .map_err(|e| wasmtime::Error::msg(format!("error deserializing content: {e}")))?;
        }
        if let Some(props) = patch_value.get("props") {
            doc_patch.props = serde_json::from_value(props.clone())
                .map_err(|e| wasmtime::Error::msg(format!("error deserializing props: {e}")))?;
        }

        // Set the doc_id in the patch
        doc_patch.id = Some(doc_id.clone());

        // Get the document at the specified heads to ensure we're updating from the right version
        let doc = plugin
            .drawer_repo
            .get_at_heads(&doc_id, &change_hash_set)
            .await
            .map_err(|e| {
                if e.to_string().contains("doc not found") {
                    wasmtime::Error::msg("doc not found")
                } else {
                    wasmtime::Error::msg(format!("error getting doc: {e}"))
                }
            })?;

        let Some(_doc) = doc else {
            return Ok(Err(drawer::UpdateDocError::DocNotFound));
        };

        // Apply the patch using update_batch
        // Note: update_batch uses the latest version, but we've verified the doc exists at the specified heads
        match plugin.drawer_repo.update_batch(vec![doc_patch]).await {
            Ok(()) => Ok(Ok(())),
            Err(e) => {
                if e.to_string().contains("doc not found") {
                    Ok(Err(drawer::UpdateDocError::DocNotFound))
                } else if e.to_string().contains("invalid heads") || e.to_string().contains("hash")
                {
                    Ok(Err(drawer::UpdateDocError::InvalidHeads(e.to_string())))
                } else if e.to_string().contains("invalid patch")
                    || e.to_string().contains("deserializ")
                {
                    Ok(Err(drawer::UpdateDocError::InvalidPatch(e.to_string())))
                } else {
                    Ok(Err(drawer::UpdateDocError::Other(e.to_string())))
                }
            }
        }
    }
}
