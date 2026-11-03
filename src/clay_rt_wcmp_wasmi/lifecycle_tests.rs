use super::WcmpPackage;
use super::WcmpRuntime;
use super::tests::RecordingSink;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;
use clay_host::Host;
use clay_host::Instance;
use clay_host::LocalFuture;
use clay_rt_wcmp::InitialContext;
use clay_rt_wcmp::test_utils::close_inspecting;
use clay_surface::SurfaceTree;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

/// The prepared lifecycle fixture, incarnated by the `wcmp` engine over Wasmi.
struct Wasmi(WcmpPackage);

impl Wasmi {
    async fn load() -> Self {
        Self(
            WcmpPackage::from_env("lifecycle")
                .await
                .expect("prepared lifecycle component is valid"),
        )
    }
}

/// What the incarnation's store holds, with the call tasks the host still has in it.
fn residue_of(state: &clay_rt_wcmp::test_utils::WcmpStoreState) -> GuestResidue {
    GuestResidue {
        calls: state.calls_in_flight(),
        ..GuestResidue::of(
            state.table.is_empty(),
            &state.capabilities,
            &state.collections,
        )
    }
}

impl Incarnations for Wasmi {
    type Runtime = WcmpRuntime;

    fn initialize(
        self: Rc<Self>,
        host: Host,
        instance: Instance,
        context: InitialContext,
    ) -> LocalFuture<'static, Result<(WcmpRuntime, LocalFuture<'static, ()>), String>> {
        Box::pin(async move {
            let (runtime, driver) =
                WcmpRuntime::initialize(&host, instance, self.0.as_shared(), context)
                    .await
                    .map_err(|error| error.to_string())?;
            let driver: LocalFuture<'static, ()> = Box::pin(driver);
            Ok((runtime, driver))
        })
    }

    fn publications(runtime: &WcmpRuntime) -> BTreeMap<String, CapabilityPublication> {
        runtime.publications()
    }

    fn mount(runtime: &WcmpRuntime, publication: &str) -> Result<Box<dyn std::any::Any>, String> {
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

    fn inspect(runtime: &WcmpRuntime) -> GuestResidue {
        clay_rt_wcmp::test_utils::inspect(runtime, residue_of)
    }

    fn close(runtime: WcmpRuntime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)> {
        Box::pin(async move { close_inspecting(&runtime, residue_of).await })
    }
}

/// One test per conformance scenario, each in a fresh engine and `LocalSet`.
macro_rules! conformance {
    ($($name:ident => $scenario:ident),+ $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let engine = Wasmi::load().await;
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
