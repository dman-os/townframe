use super::tests::RecordingSink;
use super::*;
use clay_guest_session::GuestClose;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;
use clay_host::LocalFuture;
use clay_surface::SurfaceTree;
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

    fn publications(runtime: &WasmRuntime) -> BTreeMap<String, CapabilityPublication> {
        runtime.publications()
    }

    fn mount(runtime: &WasmRuntime, publication: &str) -> Result<Box<dyn std::any::Any>, String> {
        let sink = Arc::new(RecordingSink::new());
        let session = runtime
            .mount(publication, Arc::clone(&sink) as _, &[])
            .map_err(|error| format!("{error:?}"))?;
        let rendered = sink.tree(SurfaceTree::text_content);
        if rendered != "lifecycle provider" {
            return Err(format!("the guest rendered {rendered:?}"));
        }
        Ok(Box::new(session))
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

    fn close(
        runtime: WasmRuntime,
    ) -> LocalFuture<'static, (Result<GuestClose, String>, GuestResidue)> {
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

#[tokio::test]
async fn leased_provider_closes_when_idle() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::leased_provider_closes_when_idle(
            engine,
        ))
        .await;
}

#[tokio::test]
async fn lazy_references_activate_retain_and_release() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::lazy_references_activate_retain_and_release(engine))
        .await;
}

#[tokio::test]
async fn undeclared_publications_fail_initialization() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::undeclared_publications_fail_initialization(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_suspended_call_does_not_block_sync_entry() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::suspended_call_does_not_block_sync_entry(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_calls_into_one_micro_are_serialized() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::calls_into_one_micro_are_serialized(
            engine,
        ))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_reentrant_call_chain_never_hangs() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::reentrant_call_chain_never_hangs(
            engine,
        ))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_close_revokes_calls_in_flight() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::close_revokes_calls_in_flight(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_cycle_through_queued_call_is_refused() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::cycle_through_queued_call_is_refused(
            engine,
        ))
        .await;
}
