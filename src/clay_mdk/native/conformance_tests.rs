//! The engine-conformance scenarios of the `context` micro, run in-process.
//!
//! The scenarios are the ones the Wasmtime and wcmp/Wasmi engines run
//! (`clay_guest_session::lifecycle_conformance::context_*`), and the micro is the very source
//! file the guest component is built from (`context_micro.rs`, written against
//! [`crate::Context`]), so the native, Wasmtime and Wasmi runs execute one micro against one
//! scenario list. Only the engine differs: [`InProcess`] assembles the micro's [`Granted`]
//! from the scenario's `InitialContext` grants and runs `activate` the way the supervisor's
//! [`factory`] does, then closes the incarnation's root.
//!
//! Every scenario that needs only `Incarnations` runs here. The three over the raw-WIT
//! `lifecycle` fixture (`guest_trap_*`, `sync_exports_*`, `dropped_call_*`) need
//! `GuestIncarnations` (see its docs), which an in-process micro cannot offer.
use super::*;
use clay_guest_session::InitialContext;
use clay_guest_session::capabilities::InFlightCalls;
use clay_guest_session::lifecycle_conformance;
use clay_guest_session::lifecycle_conformance::GuestResidue;
use clay_guest_session::lifecycle_conformance::Incarnations;

use crate::context_micro as micro;

/// The in-process engine: stateless, since each incarnation brings its own grants.
struct InProcess;

/// A running incarnation: what `activate` published and the root that owns its leases and
/// tasks, with the sessions it was granted through so what survives a close can be counted.
struct Running {
    publications: BTreeMap<String, CapabilityPublication>,
    root: Box<dyn Root>,
    capabilities: CapabilitySession,
    collections: CollectionSession,
    calls: InFlightCalls,
}

impl Running {
    /// An in-process micro has no resource table; the sessions and its running handlers are all
    /// it holds.
    fn residue(&self) -> GuestResidue {
        GuestResidue {
            calls: self.calls.count(),
            ..GuestResidue::of(true, &self.capabilities, &self.collections)
        }
    }
}

impl Incarnations for InProcess {
    type Runtime = Running;

    fn initialize(
        self: Rc<Self>,
        host: Host,
        instance: Instance,
        context: InitialContext,
    ) -> LocalFuture<'static, Result<(Running, LocalFuture<'static, ()>), String>> {
        Box::pin(async move {
            let manifest = micro::manifest();
            // The manifest is what the micro publishes against, as in the supervisor's factory.
            // A guest engine is told what its package declares through `context.publications`
            // and admits against that, so this does too: the scenarios that make the two
            // disagree get the refusal a guest's sidecar mismatch gets.
            let declared = declared_publications(&manifest);
            let capabilities =
                CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
            capabilities.attach_activator(
                context
                    .activator
                    .expect("the scenario supplies the supervisor's demand handle"),
            );
            capabilities.attach_own(
                &context
                    .micro
                    .expect("the scenario names the micro its publications belong to"),
                &declared,
            );
            let collections =
                CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
            let mut projections = context.collections;
            let mut registrations = context.registrations;
            let slots = manifest
                .collection_requests
                .iter()
                .map(|request| {
                    let reader = collections
                        .grant(
                            projections
                                .remove(&request.binding)
                                .expect("the scenario grants every collection request"),
                        )
                        .map_err(|error| error.to_string())?;
                    let writer = (request.access == Some(Access::Write))
                        .then(|| {
                            let grant = registrations
                                .remove(&request.binding)
                                .expect("the scenario grants every write request");
                            collections
                                .grant_writer(grant)
                                .map(Rc::new)
                                .map_err(|error| error.to_string())
                        })
                        .transpose()?;
                    Ok((
                        request.binding.clone(),
                        Some(CollectionSlot { reader, writer }),
                    ))
                })
                .collect::<Result<_, String>>()?;
            let requirements =
                resolve_requirements(&capabilities, &manifest, &context.requirements)?;
            let calls = InFlightCalls::default();
            let initialized = Granted {
                instance,
                components: NativeComponents::new(&host),
                capabilities: capabilities.clone(),
                configs: context
                    .configs
                    .into_iter()
                    .map(|(binding, document)| (binding, Config::in_process(document)))
                    .collect(),
                requirements,
                collections: slots,
                requests: manifest.collection_requests.clone(),
                declared,
                calls: calls.clone(),
            }
            .activate(
                &|context| Box::pin(micro::activate(context)),
                &context.publications,
            )
            .await?;
            // Native tasks are the incarnation's own local tasks, so there is no driver to poll.
            let driver: LocalFuture<'static, ()> = Box::pin(std::future::ready(()));
            Ok((
                Running {
                    publications: initialized.publications,
                    root: initialized.root,
                    capabilities,
                    collections,
                    calls,
                },
                driver,
            ))
        })
    }

    fn publications(runtime: &Running) -> BTreeMap<String, CapabilityPublication> {
        runtime.publications.clone()
    }

    fn inspect(runtime: &Running) -> GuestResidue {
        runtime.residue()
    }

    fn calls_in_flight(runtime: &Running) -> InFlightCalls {
        runtime.calls.clone()
    }

    fn close(runtime: Running) -> LocalFuture<'static, (Result<(), String>, GuestResidue)> {
        Box::pin(async move {
            let Running {
                root,
                capabilities,
                collections,
                calls,
                ..
            } = runtime;
            let closed = root.close().await;
            // The root's tasks and leases are gone; what the sessions still count is a leak.
            let residue = GuestResidue {
                calls: calls.count(),
                ..GuestResidue::of(true, &capabilities, &collections)
            };
            (closed, residue)
        })
    }
}

/// One test per `context` scenario, each in a fresh `LocalSet`.
macro_rules! conformance {
    ($($name:ident),+ $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            tokio::task::LocalSet::new()
                .run_until(lifecycle_conformance::$name(InProcess))
                .await;
        }
    )+};
}

conformance! {
    registration_scope_ownership,
    failed_registration_initialization_publishes_nothing,
    provider_consumer_graph,
    failing_provider_publishes_nothing,
    leased_provider_closes_when_idle,
    lazy_references_activate_retain_and_release,
    undeclared_publications_fail_initialization,
    overlapping_calls_complete_out_of_order,
    many_overlapping_calls_complete,
    calls_reenter_their_provider,
    disable_with_calls_in_flight_revokes_callers,
    shutdown_with_calls_in_flight_revokes_callers,
    context_collections_and_calls,
    context_follows_a_provider,
    context_retains_a_call,
    context_sees_a_closed_provider_revoked,
    context_peers_retaining_each_other_close_together,
    context_peers_demanding_each_other_fail_as_unavailable,
}
