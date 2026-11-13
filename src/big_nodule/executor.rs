//! The executor seam: whatever hosts nodule handlers (a Wasm host, or a fake in tests).
//!
//! The reconciler is the only writer of router registrations. For each admitted, enabled nodule
//! that has a handler it asks the configured [`NoduleExecutor`] to [`ensure`](NoduleExecutor::ensure)
//! the revision's workload and registers whatever [`Hosted`] state comes back; when a nodule is
//! disabled, refused, uninstalled or upgraded to another revision it
//! [`retire`](NoduleExecutor::retire)s the old workload first. `ensure` is called on every
//! reconcile, so an executor treats a repeat call for a revision it already hosts as a lookup.
//!
//! Readiness can change after `ensure` returned (the handler bytes arrive later, a host comes
//! back). The executor does not touch the router: it calls [`Reensure::request`] on the handle
//! in the spec and the state actor runs the reconcile that calls `ensure` again.

use async_trait::async_trait;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenedActor;

use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component as ManifestComponent;

use big_cartridges_core::Revision;
use big_dispatch::Provider;
use big_dispatch_core::Readiness;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;

/// A routine's executable export and the component that contains it.
#[derive(Clone)]
pub struct RoutineSpec {
    pub component: String,
    pub export: String,
}

/// Everything an executor needs to host one nodule revision's routine components.
pub struct HandlerSpec {
    pub cartridge_id: CartridgeId,
    pub revision: Revision,
    /// The wasm components declared by the envelope, keyed by component key. Each component is
    /// loaded once per cartridge revision; routines below select which one serves an operation.
    pub components: BTreeMap<String, ManifestComponent>,
    /// The operation-to-export routing established by the admitted nodule routines.
    pub routines: BTreeMap<String, RoutineSpec>,
    /// The cartridge config document and its attenuated settings capability delivered to the guest.
    pub config_doc: big_cabinet_core::dmeta::DocId,
    pub config_capability: Option<CapId>,
    /// The nodule's durable actor, which component outgoing calls are made as: the executor signs
    /// them with `actor.capability`, never with anything the component says about itself.
    pub actor: OpenedActor,
    /// The dispatch capability minted from the nodule's operation grants, which components are told
    /// so they can present it on their own outgoing calls. `None` when nothing is granted. It
    /// changes whenever the grants do, and `ensure` runs on every reconcile, so an executor that
    /// already hosts the revision takes the new value from the spec.
    pub capability: Option<CapId>,
    /// Ask the state actor to call `ensure` again for this cartridge.
    pub reensure: Reensure,
}
/// What an executor reports for a revision it was asked to host.
pub enum Hosted {
    /// The handler can be called now through this provider.
    Ready(Arc<dyn Provider>),
    /// It cannot, for this reason: [`Readiness::ArtifactsUnavailable`] when the handler bytes
    /// are not in the store yet, [`Readiness::ExecutorUnavailable`] when they are but the host
    /// cannot run them. Any other readiness is a programming error in the executor.
    NotReady(Readiness),
}

#[async_trait]
pub trait NoduleExecutor: Send + Sync {
    /// The provider type tag dispatch records on the providers this executor hosts, for
    /// type-dependent grants.
    fn kind(&self) -> &'static str;

    /// Host `spec`'s revision, or report why it cannot be hosted yet. Runs inside the state
    /// actor's reconcile, so it must not block on slow work: a not-yet-ready workload returns
    /// [`Hosted::NotReady`] and calls [`Reensure::request`] when that changes.
    async fn ensure(&self, spec: &HandlerSpec) -> Hosted;

    /// Whether a workload is running for `cartridge_id`: it was ensured and not retired.
    fn hosts(&self, cartridge_id: &CartridgeId) -> bool;

    /// Stop hosting whatever was ensured for `cartridge_id`. Called after dispatch already stopped
    /// routing to it.
    async fn retire(&self, cartridge_id: &CartridgeId);
}

/// The cartridges whose executor asked to be ensured again, and the wakeup for the task that does it.
/// A set plus a [`Notify`] permit rather than a channel: a request is never lost to a wakeup
/// that races it (`notify_one` stores a permit), repeated requests for one cartridge coalesce, and
/// `request` has no failure to hide, because the executor is constructed before the state actor
/// and may outlive it, so a request with nobody left to read it is not an invariant break.
#[derive(Default)]
pub(crate) struct ReensureQueue {
    pending: Mutex<BTreeSet<CartridgeId>>,
    wake: Notify,
}

impl ReensureQueue {
    pub(crate) fn handle(self: &Arc<Self>, cartridge_id: CartridgeId) -> Reensure {
        Reensure {
            cartridge_id,
            queue: Arc::clone(self),
        }
    }

    pub(crate) async fn requested(&self) -> BTreeSet<CartridgeId> {
        loop {
            let taken = std::mem::take(&mut *self.pending.lock().expect("reensure queue lock"));
            if !taken.is_empty() {
                return taken;
            }
            self.wake.notified().await;
        }
    }
}

/// A cartridge-bound request for the state actor to call [`NoduleExecutor::ensure`] again.
#[derive(Clone)]
pub struct Reensure {
    cartridge_id: CartridgeId,
    queue: Arc<ReensureQueue>,
}

impl Reensure {
    pub fn request(&self) {
        self.queue
            .pending
            .lock()
            .expect("reensure queue lock")
            .insert(self.cartridge_id.clone());
        self.queue.wake.notify_one();
    }
}
