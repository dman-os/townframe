use super::tests::RecordingSink;
use super::*;
use clay_guest_session::capabilities::InFlightCalls;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestIncarnations;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;
use clay_host::LocalFuture;
use clay_surface::SurfaceTree;
use std::rc::Rc;

/// The prepared lifecycle fixture, incarnated by this engine.
struct Wasmtime(WasmPackage);

impl Wasmtime {
    async fn load() -> Self {
        Self::load_fixture("lifecycle").await
    }

    /// A prepared fixture component by name (`lifecycle`, or `context` for the authoring
    /// surface's scenario).
    async fn load_fixture(name: &str) -> Self {
        Self(
            WasmPackage::from_env(name)
                .await
                .unwrap_or_else(|error| panic!("prepared {name} component is valid: {error}")),
        )
    }
}

/// What the incarnation's store holds, with the call tasks the host still has in it.
fn residue_of(active: &WasmIncarnation) -> GuestResidue {
    let state = active.store.data();
    GuestResidue {
        calls: active.calls_in_flight.count(),
        ..GuestResidue::of(
            state.table.is_empty(),
            &state.capabilities,
            &state.collections,
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

    fn inspect(runtime: &WasmRuntime) -> GuestResidue {
        let active = runtime.incarnation.lock().unwrap();
        residue_of(active.as_ref().unwrap())
    }

    fn close(runtime: WasmRuntime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)> {
        Box::pin(async move {
            let taken = runtime.incarnation.lock().unwrap().take();
            let mut active = taken.expect("only a live incarnation is closed");
            let closed = active.close().await;
            (closed, residue_of(&active))
        })
    }
}

impl GuestIncarnations for Wasmtime {
    fn mount(runtime: &WasmRuntime, publication: &str) -> Result<Box<dyn std::any::Any>, String> {
        let sink = Arc::new(RecordingSink::new());
        let session = runtime
            .mount(publication, Arc::clone(&sink) as _)
            .map_err(|error| format!("{error:?}"))?;
        let rendered = sink.tree(SurfaceTree::text_content);
        if rendered != "lifecycle provider" {
            return Err(format!("the guest rendered {rendered:?}"));
        }
        Ok(Box::new(session))
    }

    fn calls_in_flight(runtime: &WasmRuntime) -> InFlightCalls {
        let active = runtime.incarnation.lock().unwrap();
        active.as_ref().unwrap().calls_in_flight.clone()
    }
}

/// One test per conformance scenario, each in a fresh engine and `LocalSet`.
macro_rules! conformance {
    ($($name:ident => $scenario:ident),+ $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let engine = Wasmtime::load().await;
            tokio::task::LocalSet::new()
                .run_until(lifecycle_conformance::$scenario(engine))
                .await;
        }
    )+};
}

conformance! {
    lifecycle_conformance_registration_scope_ownership => registration_scope_ownership,
    lifecycle_conformance_failed_registration_initialization => failed_registration_initialization_publishes_nothing,
    provider_consumer_graph_activates_and_closes_in_order => provider_consumer_graph,
    failing_provider_publishes_nothing => failing_provider_publishes_nothing,
    leased_provider_closes_when_idle => leased_provider_closes_when_idle,
    lazy_references_activate_retain_and_release => lazy_references_activate_retain_and_release,
    undeclared_publications_fail_initialization => undeclared_publications_fail_initialization,
    overlapping_calls_complete_out_of_order => overlapping_calls_complete_out_of_order,
    many_overlapping_calls_complete => many_overlapping_calls_complete,
    sync_exports_interleave_with_suspended_calls => sync_exports_interleave_with_suspended_calls,
    calls_reenter_their_provider => calls_reenter_their_provider,
    guest_trap_fails_outstanding_calls => guest_trap_fails_outstanding_calls,
    disable_with_calls_in_flight_revokes_callers => disable_with_calls_in_flight_revokes_callers,
    shutdown_with_calls_in_flight_revokes_callers => shutdown_with_calls_in_flight_revokes_callers,
    dropped_call_finishes_in_the_guest_without_leaks => dropped_call_finishes_in_the_guest_without_leaks,
}

/// The shared authoring `Context` on this engine: a guest collection read and a guest
/// registration write, through the `context` fixture built with `export_micro!`.
#[tokio::test]
async fn context_collections_and_calls() {
    let engine = Wasmtime::load_fixture("context").await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::context_collections_and_calls(engine))
        .await;
}

/// A guest slot's two waits on this engine: `status-changed` wakes a redemption that failed and
/// `revoked` ends a mounted one, both driven by the supervisor's status ticks.
#[tokio::test]
async fn context_follows_a_provider() {
    let engine = Wasmtime::load_fixture("context").await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::context_follows_a_provider(engine))
        .await;
}

/// `Capability::retain_call` in a guest on this engine: one incarnation across several calls,
/// released when the binding drops.
#[tokio::test]
async fn context_retains_a_call() {
    let engine = Wasmtime::load_fixture("context").await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::context_retains_a_call(engine))
        .await;
}

/// A guest's bound reference to a provider that idle-closed reads `revoked` through the WIT on
/// this engine, and a call or retain through it fails as revoked without activating anything.
#[tokio::test]
async fn context_sees_a_closed_provider_revoked() {
    let engine = Wasmtime::load_fixture("context").await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::context_sees_a_closed_provider_revoked(engine))
        .await;
}

/// Two guests retaining each other's calls close together once no outside lease reaches them,
/// on this engine.
#[tokio::test]
async fn context_peers_retaining_each_other_close_together() {
    let engine = Wasmtime::load_fixture("context").await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::context_peers_retaining_each_other_close_together(engine))
        .await;
}

/// Two guests whose activations demand each other fail as unavailable instead of deadlocking,
/// on this engine.
#[tokio::test]
async fn context_peers_demanding_each_other_fail_as_unavailable() {
    let engine = Wasmtime::load_fixture("context").await;
    tokio::task::LocalSet::new()
        .run_until(
            lifecycle_conformance::context_peers_demanding_each_other_fail_as_unavailable(engine),
        )
        .await;
}
