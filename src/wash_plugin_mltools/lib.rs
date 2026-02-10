mod interlude {
    pub use std::sync::Arc;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use wash_runtime::engine::ctx::{Ctx as WashCtx, SharedCtx as SharedWashCtx};
use wash_runtime::wit::{WitInterface, WitWorld};

mod binds_guest {
    wash_runtime::wasmtime::component::bindgen!({
        world: "guest",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
    });
}

use binds_guest::townframe::mltools::{llm_chat, types};

pub struct MltoolsPlugin {
    ollama: ollama_rs::Ollama,
    config: Config,
}

pub struct Config {
    pub ollama_url: String,
    pub ollama_model: String,
}

impl MltoolsPlugin {
    pub fn new(config: Config) -> Res<Arc<Self>> {
        // Parse URL to extract host and port
        let url = url::Url::parse(&config.ollama_url).wrap_err_with(|| {
            format!(
                "invalid OLLAMA_URL: {ollama_url}",
                ollama_url = config.ollama_url
            )
        })?;
        let host = url
            .host_str()
            .ok_or_else(|| ferr!("OLLAMA_URL missing host"))?;
        let scheme = url.scheme();
        let port = url.port().unwrap_or(11434);

        let ollama = ollama_rs::Ollama::new(format!("{scheme}://{host}"), port);

        Ok(Arc::new(Self { ollama, config }))
    }

    const ID: &str = "townframe:mltools";

    fn from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
            panic!("plugin not on ctx");
        };
        this
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for MltoolsPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:mltools/types,llm-chat",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        interface_configs: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Validate that we can handle the requested interfaces
        for iface in &interface_configs {
            if iface.namespace == "townframe"
                && iface.package == "mltools"
                && !iface.interfaces.contains("types")
                && !iface.interfaces.contains("llm-chat")
            {
                anyhow::bail!("unsupported mltools interface: {iface:?}");
            }
        }
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe" && iface.package == "mltools" {
                if iface.interfaces.contains("types") {
                    types::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("llm-chat") {
                    llm_chat::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
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

impl types::Host for SharedWashCtx {
    async fn noop(
        &mut self,
        _inc: (
            types::ErrorsValidation,
            types::ErrorInternal,
            types::Datetime,
            types::Uuid,
        ),
    ) -> wasmtime::Result<Result<(), ()>> {
        // Implementation for the noop function from the types interface
        // This is a copy of the utils interface, so we can just return Ok
        Ok(Ok(()))
    }
}

impl llm_chat::Host for SharedWashCtx {
    async fn respond(
        &mut self,
        request: llm_chat::Request,
    ) -> wasmtime::Result<Result<llm_chat::Response, wasmtime::component::__internal::String>> {
        let plugin = MltoolsPlugin::from_ctx(&self.active_ctx);

        // Extract message text from request.input
        let llm_chat::RequestInput::Text(message_text) = request.input;

        // Call Ollama
        use ollama_rs::generation::completion::request::GenerationRequest;
        let generation_request =
            GenerationRequest::new(plugin.config.ollama_model.clone(), message_text);

        let ollama_response = plugin
            .ollama
            .generate(generation_request)
            .await
            .map_err(|err| wasmtime::Error::msg(format!("ollama error: {err:?}")))?;

        let response_text = ollama_response.response;

        // Build the response
        let response = llm_chat::Response {
            items: vec![llm_chat::ResponseItem::Message(llm_chat::ResponseMessage {
                role: llm_chat::Role::Assitant,
                text: response_text.clone(),
            })],
            text: response_text,
        };

        Ok(Ok(response))
    }
}
