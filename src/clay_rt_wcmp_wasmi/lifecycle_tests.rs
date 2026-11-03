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
        clay_rt_wcmp::test_utils::inspect(runtime, |state| {
            GuestResidue::of(
                state.table.is_empty(),
                &state.capabilities,
                &state.collections,
            )
        })
    }

    fn close(runtime: WcmpRuntime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)> {
        Box::pin(async move {
            close_inspecting(&runtime, |state| {
                GuestResidue::of(
                    state.table.is_empty(),
                    &state.capabilities,
                    &state.collections,
                )
            })
            .await
        })
    }
}

#[tokio::test]
async fn lifecycle_conformance_registration_scope_ownership() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::registration_scope_ownership(engine))
        .await;
}

#[tokio::test]
async fn lifecycle_conformance_failed_registration_initialization() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(
            lifecycle_conformance::failed_registration_initialization_publishes_nothing(engine),
        )
        .await;
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

#[tokio::test]
async fn leased_provider_closes_when_idle() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::leased_provider_closes_when_idle(
            engine,
        ))
        .await;
}

#[tokio::test]
async fn lazy_references_activate_retain_and_release() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::lazy_references_activate_retain_and_release(engine))
        .await;
}

#[tokio::test]
async fn undeclared_publications_fail_initialization() {
    let engine = Wasmi::load().await;
    tokio::task::LocalSet::new()
        .run_until(lifecycle_conformance::undeclared_publications_fail_initialization(engine))
        .await;
}
