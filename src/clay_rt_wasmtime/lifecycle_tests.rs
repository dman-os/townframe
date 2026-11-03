use super::tests::RecordingSink;
use super::*;
use clay_guest_session::GuestClose;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;
use clay_guest_session::lifecycle_conformance::Mounted;
use clay_host::CloseMode;
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

    fn mount(runtime: &WasmRuntime, publication: &str) -> Result<Mounted, String> {
        let sink = Arc::new(RecordingSink::new());
        let session = runtime
            .mount(publication, Arc::clone(&sink) as _, &[])
            .map_err(|error| format!("{error:?}"))?;
        Ok(Mounted {
            session: Box::new(session),
            rendered: sink.tree(SurfaceTree::text_content),
        })
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
        mode: CloseMode,
    ) -> LocalFuture<'static, (Result<GuestClose, String>, GuestResidue)> {
        Box::pin(async move {
            let taken = runtime.incarnation.lock().unwrap().take();
            let mut active = taken.expect("only a live incarnation is closed");
            let closed = active.close(mode).await;
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
async fn lifecycle_conformance_two_calls_into_one_micro_are_in_flight_together() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::two_calls_into_one_micro_are_in_flight_together(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_reentrant_call_chain_completes() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::reentrant_call_chain_completes(
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
async fn lifecycle_conformance_orderly_close_drains() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::orderly_close_drains(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_cancellation_reaches_the_guest() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::cancellation_reaches_the_guest(
            engine,
        ))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_requests_over_the_limit_are_refused() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::requests_over_the_limit_are_refused(
            engine,
        ))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_mdk_guest_shares_thread_locals_between_serve_and_sync_exports() {
    let engine = Wasmtime::load().await;
    tokio::task::LocalSet::new()
        .run_until(
            lifecycle_conformance::mdk_guest_shares_thread_locals_between_serve_and_sync_exports(
                engine,
            ),
        )
        .await;
}
