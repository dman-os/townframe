use super::WcmpPackage;
use super::WcmpRuntime;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;
use clay_host::Host;
use clay_host::Instance;
use clay_host::LocalFuture;
use clay_rt_wcmp::InitialContext;
use clay_rt_wcmp::test_utils::close_inspecting;
use std::rc::Rc;

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

    fn provision(runtime: &WcmpRuntime, contract: &str) -> Option<CapabilityPublication> {
        runtime.provision(contract)
    }

    fn close(runtime: WcmpRuntime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)> {
        Box::pin(async move {
            close_inspecting(&runtime, |state| {
                GuestResidue::of(state.table.is_empty(), &state.capabilities)
            })
            .await
        })
    }
}

#[tokio::test]
async fn provider_consumer_graph_activates_and_closes_in_order() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::provider_consumer_graph(engine))
        .await;
}

#[tokio::test]
async fn failing_provider_publishes_nothing() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::failing_provider_publishes_nothing(
            engine,
        ))
        .await;
}
