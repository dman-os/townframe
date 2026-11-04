//! The call-concurrency scenarios: many calls into one incarnation in flight at once, each a
//! task of its own, a call that traps the guest, and a forced close with calls in flight.
use super::*;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// A host-side call the scenario answers by hand: each call parks until [`Gate::release`] names
/// its request, then answers with that request. A handler that awaits it is a call suspended on
/// something the test controls.
struct Gate {
    held: Mutex<BTreeMap<Vec<u8>, oneshot::Sender<()>>>,
    arrivals: watch::Sender<usize>,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::default(),
            arrivals: watch::channel(0).0,
        })
    }

    /// Resolves once `count` calls have reached the gate in total.
    async fn arrived(&self, count: usize) {
        self.arrivals
            .subscribe()
            .wait_for(|&arrivals| arrivals >= count)
            .await
            .unwrap();
    }

    /// Release the call whose request was the text `request`.
    fn release(&self, request: &str) {
        let parked = self
            .held
            .lock()
            .unwrap()
            .remove(&cbor_text(request))
            .expect("the released call reached the gate");
        parked
            .send(())
            .expect("the released call is still waiting at the gate");
    }
}

impl CallTarget for Gate {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let (release, released) = oneshot::channel();
        let previous = self.held.lock().unwrap().insert(request.clone(), release);
        assert!(previous.is_none(), "scenario requests are unique");
        self.arrivals.send_modify(|arrivals| *arrivals += 1);
        async move {
            released
                .await
                .map_err(|_dropped| CapabilityFailure::Revoked)?;
            Ok(request)
        }
        .boxed()
    }
}

/// A host-side call that forwards to a reference the scenario points it at after the graph
/// exists (a guest's `peer` must be granted before the micros it reaches are started).
#[derive(Default)]
struct Forward(OnceLock<CapabilityRef>);

impl CallTarget for Forward {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let target = self
            .0
            .get()
            .expect("the scenario points the forward before any call")
            .share();
        async move { target.call(request).await }.boxed()
    }
}

/// One lazy relay provider `a`, held live by a lease, whose `peer` is a [`Gate`]: every call of
/// its `relay` parks in the guest until the scenario releases it.
struct Gated<E: Incarnations> {
    harness: Harness<E>,
    supervisor: Supervisor<CapabilityPublication>,
    gate: Arc<Gate>,
    /// The gate as published to the guest; revoked when dropped.
    _peer: PublishedCapability,
    lease: Lease<CapabilityPublication>,
    relay: CapabilityRef,
}

impl<E: Incarnations> Gated<E> {
    async fn start(engine: E) -> Self {
        let harness = Harness::new(engine);
        let gate = Gate::new();
        let peer = harness.publish_peer(&gate);
        let supervisor = harness.relays([("a", peer.publication())]);
        let lease = supervisor
            .acquire(&MicroId::new("a"), RELAY)
            .await
            .expect("the relay provider starts");
        let relay = harness.observe(&lease);
        Self {
            harness,
            supervisor,
            gate,
            _peer: peer,
            lease,
            relay,
        }
    }

    /// Start one `relay` call from outside the incarnation. The returned task is the caller:
    /// aborting it drops the call.
    fn call(&self, request: impl AsRef<str>) -> JoinHandle<Result<Vec<u8>, CallError>> {
        let relay = self.relay.share();
        let request = cbor_text(request.as_ref());
        tokio::task::spawn_local(async move { relay.call(request).await })
    }

    fn residue(&self) -> GuestResidue {
        self.harness.residue("a")
    }

    fn instance(&self) -> Instance {
        self.supervisor.instance(&MicroId::new("a")).unwrap()
    }

    /// Run `scenario` while the supervisor's driver serves idle closes.
    async fn run(&self, scenario: impl Future<Output = ()>) {
        tokio::select! {
            never = self.supervisor.drive() => never,
            () = scenario => {}
        }
    }

    async fn finish(self) {
        let Self {
            harness,
            supervisor,
            lease,
            relay,
            ..
        } = self;
        drop((relay, lease));
        harness.finish(supervisor).await;
    }
}

/// Two calls into one incarnation are both in flight, suspended in the guest, and complete in
/// the opposite order to the one they started in. Each call is a task of its own: the second
/// does not wait for the first, and neither's state is mistaken for the other's.
pub async fn overlapping_calls_complete_out_of_order<E: Incarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let baseline = g.residue();
        let first = g.call("one");
        let second = g.call("two");
        g.gate.arrived(2).await;
        assert_eq!(g.residue().calls, 2, "both handlers are in the guest");
        g.gate.release("two");
        assert_eq!(second.await.unwrap(), Ok(cbor_text("relay:two")));
        assert!(!first.is_finished(), "the first call is still suspended");
        g.gate.release("one");
        assert_eq!(first.await.unwrap(), Ok(cbor_text("relay:one")));
        g.harness.calls_drained("a").await;
        assert_eq!(g.residue(), baseline, "no call left anything behind");
    })
    .await;
    g.finish().await;
}

/// Fifty calls in flight at once, released in an interleaved order, all answered with their own
/// request.
pub async fn many_overlapping_calls_complete<E: Incarnations>(engine: E) {
    const CALLS: usize = 50;
    let g = Gated::start(engine).await;
    g.run(async {
        let baseline = g.residue();
        let calls: Vec<_> = (0..CALLS)
            .map(|index| (index, g.call(format!("call-{index}"))))
            .collect();
        g.gate.arrived(CALLS).await;
        assert_eq!(g.residue().calls, CALLS);
        // Odd calls first, newest first, then the even ones oldest first.
        let order = (0..CALLS)
            .rev()
            .filter(|index| index % 2 == 1)
            .chain((0..CALLS).filter(|index| index % 2 == 0));
        for index in order {
            g.gate.release(&format!("call-{index}"));
        }
        for (index, call) in calls {
            assert_eq!(
                call.await.unwrap(),
                Ok(cbor_text(&format!("relay:call-{index}")))
            );
        }
        g.harness.calls_drained("a").await;
        assert_eq!(g.residue(), baseline);
    })
    .await;
    g.finish().await;
}

/// Synchronous exports (`mount`, unmount) and an unrelated async export (`echo`) run while a
/// `relay` call is suspended in the guest. `mount` reads the guest's `ROLE`, which `initialize`
/// (a different task, long finished) wrote: it only works because all tasks share one TLS.
pub async fn sync_exports_interleave_with_suspended_calls<E: GuestIncarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let parked = g.call("parked");
        g.gate.arrived(1).await;
        let runtime = g
            .harness
            .shared
            .runtime("a")
            .downcast::<E::Runtime>()
            .unwrap_or_else(|_| panic!("a runs this engine"));
        let mounted = E::mount(&runtime, PUBLICATION).expect("a live presentation mounts");
        let echo = g
            .supervisor
            .acquire(&MicroId::new("a"), ECHO)
            .await
            .unwrap();
        assert_eq!(
            g.harness.observe(&echo).call(cbor_text("x")).await,
            Ok(cbor_text("x")),
            "another call completes while the relay is suspended"
        );
        assert!(!parked.is_finished());
        drop((mounted, echo));
        drop(runtime);
        g.gate.release("parked");
        assert_eq!(parked.await.unwrap(), Ok(cbor_text("relay:parked")));
    })
    .await;
    g.finish().await;
}

/// A guest import that reaches back into its own incarnation completes: A calls B which calls
/// A's `echo` while A's `relay` is suspended (A to B to A), and `c` calls its own `echo` (A to
/// A). The host answers an import by queueing into the provider's driver, never by entering the
/// provider's store from the import's first poll. The hops go through lazy references.
pub async fn calls_reenter_their_provider<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let (a_to_b, b_to_a, c_to_c) = (
        Arc::new(Forward::default()),
        Arc::new(Forward::default()),
        Arc::new(Forward::default()),
    );
    let peers = [&a_to_b, &b_to_a, &c_to_c].map(|forward| harness.publish_peer(forward));
    let supervisor = harness.relays([
        ("a", peers[0].publication()),
        ("b", peers[1].publication()),
        ("c", peers[2].publication()),
    ]);
    let lazy = |micro: &str, publication: &str, contract| {
        harness.lazy_ref(&supervisor, micro, publication, contract)
    };
    assert!(a_to_b.0.set(lazy("b", RELAY, relay_contract())).is_ok());
    assert!(b_to_a.0.set(lazy("a", ECHO, echo_contract())).is_ok());
    assert!(c_to_c.0.set(lazy("c", ECHO, echo_contract())).is_ok());
    let relay_a = lazy("a", RELAY, relay_contract());
    let relay_c = lazy("c", RELAY, relay_contract());
    tokio::select! {
        never = supervisor.drive() => never,
        () = async {
            assert_eq!(
                relay_a.call(cbor_text("x")).await,
                Ok(cbor_text("relay:relay:x"))
            );
            assert_eq!(
                relay_c.call(cbor_text("y")).await,
                Ok(cbor_text("relay:y"))
            );
            for micro in ["a", "b", "c"] {
                let micro = MicroId::new(micro);
                settle(&supervisor, || supervisor.state(&micro) == MicroState::Idle).await;
            }
        } => {}
    }
    drop((relay_a, relay_c, a_to_b, b_to_a, c_to_c));
    harness.finish(supervisor).await;
}

/// A guest trap fails every call outstanding on the incarnation instead of leaving them
/// suspended forever: they answer `Revoked`, the incarnation closes, and host teardown runs.
pub async fn guest_trap_fails_outstanding_calls<E: GuestIncarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let instance = g.instance();
        let outstanding: Vec<_> = (0..3)
            .map(|index| g.call(format!("call-{index}")))
            .collect();
        g.gate.arrived(3).await;
        let trapper = g.call("trap").await.unwrap();
        assert!(trapper.is_err(), "the trapping call fails: {trapper:?}");
        for call in outstanding {
            assert_eq!(
                call.await.unwrap(),
                Err(CapabilityFailure::Revoked.into()),
                "a call suspended in a trapped guest is revoked"
            );
        }
        g.harness.host.await_close(&instance).await;
        assert_eq!(g.harness.host.lifecycle(&instance), Lifecycle::Closed);
        assert_eq!(
            g.relay.call(cbor_text("after")).await,
            Err(CapabilityFailure::Revoked.into())
        );
    })
    .await;
    g.finish().await;
}

/// The guest's async `instance-root.close` runs beside calls that are still in flight, and the
/// forced close then drops the store: the callers answer `Revoked` rather than hang. This one
/// is the supervisor disabling the micro.
pub async fn disable_with_calls_in_flight_revokes_callers<E: Incarnations>(engine: E) {
    close_with_calls_in_flight(engine, false).await;
}

/// [`disable_with_calls_in_flight_revokes_callers`], with the supervisor shutting down.
pub async fn shutdown_with_calls_in_flight_revokes_callers<E: Incarnations>(engine: E) {
    close_with_calls_in_flight(engine, true).await;
}

async fn close_with_calls_in_flight<E: Incarnations>(engine: E, shutdown: bool) {
    let g = Gated::start(engine).await;
    g.run(async {
        let instance = g.instance();
        let outstanding: Vec<_> = (0..3)
            .map(|index| g.call(format!("call-{index}")))
            .collect();
        g.gate.arrived(3).await;
        if shutdown {
            g.supervisor.shutdown().await.unwrap();
        } else {
            g.supervisor.disable(&MicroId::new("a")).await.unwrap();
        }
        assert_eq!(g.harness.log(), ["initialize a", "close a"]);
        assert_eq!(g.harness.host.lifecycle(&instance), Lifecycle::Closed);
        for call in outstanding {
            assert_eq!(
                call.await.unwrap(),
                Err(CapabilityFailure::Revoked.into()),
                "a call in flight at a forced close is revoked"
            );
        }
    })
    .await;
    g.finish().await;
}

/// A caller that gives up on a call cannot cancel the guest handler: it runs to completion and
/// its answer is discarded. Nothing leaks when it does: the guest holds what it did before the
/// call, and the lease that kept the provider live is released normally, after which the
/// provider idles closed with nothing left behind (the fixture root asserts that at its close).
pub async fn dropped_call_finishes_in_the_guest_without_leaks<E: GuestIncarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let baseline = g.residue();
        let call = g.call("abandoned");
        g.gate.arrived(1).await;
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert_eq!(
            g.residue().calls,
            1,
            "the guest handler outlives its caller"
        );
        g.gate.release("abandoned");
        g.harness.calls_drained("a").await;
        assert_eq!(g.residue(), baseline);
    })
    .await;
    let Gated {
        harness,
        supervisor,
        relay,
        lease,
        ..
    } = g;
    drop((relay, lease));
    let micro = MicroId::new("a");
    tokio::select! {
        never = supervisor.drive() => never,
        () = settle(&supervisor, || supervisor.state(&micro) == MicroState::Idle) => {}
    }
    assert_eq!(harness.log(), ["initialize a", "close a"]);
    harness.finish(supervisor).await;
}
