use super::*;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;
use clay_host::LocalFuture;
use std::rc::Rc;

/// The prepared lifecycle fixture, incarnated by this engine.
struct Wasmtime(WasmPackage);

impl Wasmtime {
    async fn load() -> Self {
        Self(
            WasmPackage::from_env("lifecycle")
                .await
                .expect("prepared lifecycle component is valid"),
        )
    }
}

impl Incarnations for Wasmtime {
    type Runtime = WasmRuntime;

    fn initialize(
        self: Rc<Self>,
        host: Host,
        instance: Instance,
        context: InitialContext,
    ) -> LocalFuture<'static, Result<(WasmRuntime, LocalFuture<'static, ()>), String>> {
        Box::pin(async move {
            let (runtime, driver) = WasmRuntime::initialize(&host, instance, &self.0, context)
                .await
                .map_err(|error| error.to_string())?;
            let driver: LocalFuture<'static, ()> = Box::pin(driver);
            Ok((runtime, driver))
        })
    }

    fn provision(runtime: &WasmRuntime, contract: &str) -> Option<CapabilityPublication> {
        runtime.provision(contract)
    }

    fn inspect(runtime: &WasmRuntime) -> GuestResidue {
        let active = runtime.incarnation.lock().unwrap();
        let state = active.as_ref().unwrap().store.data();
        GuestResidue::of(
            state.table.is_empty(),
            &state.capabilities,
            &state.collections,
        )
    }

    fn close(runtime: WasmRuntime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)> {
        Box::pin(async move {
            let taken = runtime.incarnation.lock().unwrap().take();
            let mut active = taken.expect("only a live incarnation is closed");
            let closed = active.close().await;
            let state = active.store.data();
            (
                closed,
                GuestResidue::of(
                    state.table.is_empty(),
                    &state.capabilities,
                    &state.collections,
                ),
            )
        })
    }
}

#[tokio::test]
async fn lifecycle_conformance_registration_scope_ownership() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::registration_scope_ownership(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_failed_registration_initialization() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(
            lifecycle_conformance::failed_registration_initialization_publishes_nothing(engine),
        )
        .await;
}

#[tokio::test]
async fn provider_consumer_graph_activates_and_closes_in_order() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::provider_consumer_graph(engine))
        .await;
}

#[tokio::test]
async fn failing_provider_publishes_nothing() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::failing_provider_publishes_nothing(
            engine,
        ))
        .await;
}
