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

use crate::dispatch::Provider;
use async_trait::async_trait;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::operation::OperationKind;
use big_nodule_core::inventory::Readiness;
use big_plugs_core::Artifact;
use big_plugs_core::PlugId;
use big_plugs_core::Revision;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;

/// Everything an executor needs to host one nodule revision's handler.
pub struct HandlerSpec {
    pub plug_id: PlugId,
    pub revision: Revision,
    pub nodule_id: String,
    /// The handler artifact as the envelope declares it: key, digest, size and media type. Its
    /// bytes are retained and fetched through the `Blob` datum the config document carries.
    pub handler: Artifact,
    /// The nodule's durable actor, which the handler's outgoing calls are made as: the executor
    /// signs them with `actor.capability`, never with anything the handler says about itself.
    pub actor: Actor,
    /// The dispatch capability minted from the nodule's operation grants, which the handler is
    /// told so it can present it on its own outgoing calls. `None` when nothing is granted. It
    /// changes whenever the grants do, and `ensure` runs on every reconcile, so an executor that
    /// already hosts the revision takes the new value from the spec.
    pub capability: Option<CapId>,
    /// The operations the handler implements, by NSID (`None` kind for a foreign contract).
    pub operations: BTreeMap<String, Option<OperationKind>>,
    /// Ask the state actor to call `ensure` again for this plug.
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

    /// Stop hosting whatever was ensured for `plug_id`. Called after dispatch already stopped
    /// routing to it.
    async fn retire(&self, plug_id: &PlugId);
}

/// The plugs whose executor asked to be ensured again, and the wakeup for the task that does it.
/// A set plus a [`Notify`] permit rather than a channel: a request is never lost to a wakeup
/// that races it (`notify_one` stores a permit), repeated requests for one plug coalesce, and
/// `request` has no failure to hide, because the executor is constructed before the state actor
/// and may outlive it, so a request with nobody left to read it is not an invariant break.
#[derive(Default)]
pub(crate) struct ReensureQueue {
    pending: Mutex<BTreeSet<PlugId>>,
    wake: Notify,
}

impl ReensureQueue {
    pub(crate) fn handle(self: &Arc<Self>, plug_id: PlugId) -> Reensure {
        Reensure {
            plug_id,
            queue: Arc::clone(self),
        }
    }

    pub(crate) async fn requested(&self) -> BTreeSet<PlugId> {
        loop {
            let taken = std::mem::take(&mut *self.pending.lock().expect("reensure queue lock"));
            if !taken.is_empty() {
                return taken;
            }
            self.wake.notified().await;
        }
    }
}

/// A plug-bound request for the state actor to call [`NoduleExecutor::ensure`] again.
#[derive(Clone)]
pub struct Reensure {
    plug_id: PlugId,
    queue: Arc<ReensureQueue>,
}

impl Reensure {
    pub fn request(&self) {
        self.queue
            .pending
            .lock()
            .expect("reensure queue lock")
            .insert(self.plug_id.clone());
        self.queue.wake.notify_one();
    }
}
