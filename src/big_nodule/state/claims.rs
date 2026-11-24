//! Following dispatch's claims: a request resolves against the providers that claim its contract,
//! and some providers register after this service has booted and reconciled (the node's own
//! nodules and info providers, see `BigNode::register_late_built_ins`). The router's claims
//! revision is how this service learns an answer it gave may be stale. The other direction, the
//! nodules' own registrations, is withdrawn here at stop.

use super::*;
use tokio::sync::watch;

/// The state actor's view of the router's claims revision.
///
/// The revision is a watch, so it coalesces: a burst of registrations while a reconcile runs is
/// one pending change, and the actor is its only follower, so there is never more than the
/// running reconcile plus one follow-up. The follow-up compares claims (see
/// [`State::claims_moved`]), so a change that moves nothing a request resolved against (an
/// unrelated provider, or this service's own registration of a nodule) reconciles nothing.
pub(super) struct Claims {
    /// `None` once the router closed the signal.
    changes: Option<watch::Receiver<u64>>,
}

impl Claims {
    /// Follows `router`'s claims from now. Called before the boot reconcile reads any claim, so a
    /// provider registering while it runs is a change [`Self::changed`] reports.
    pub(super) fn follow(router: &Router) -> Self {
        Self {
            changes: Some(router.follow_claims()),
        }
    }

    /// Resolves when the claims moved since the last call. A closed signal is the node telling us
    /// it is coming down (it retires the claims before it unbinds the providers it registered),
    /// so following ends and this never resolves again: reconciling against claims being
    /// withdrawn would race the services stopping behind them.
    pub(super) async fn changed(&mut self) {
        while let Some(changes) = &mut self.changes {
            if changes.changed().await.is_ok() {
                return;
            }
            self.changes = None;
        }
        std::future::pending::<()>().await;
    }
}

/// Drops the nodules from `claimants`: the claims a reconcile's resolution depends on, since the
/// snapshot decides a nodule's own registrations (see [`ProviderIndex::offers`]). It is also what
/// keeps a reconcile's own registrations from being a reason to reconcile again.
pub(super) fn external(mut claimants: Vec<String>) -> Vec<String> {
    claimants.retain(|provider_id| provider_id.parse::<CartridgeId>().is_err());
    claimants
}

impl State {
    /// Whether a claim a request resolved against has moved since the last reconcile read it.
    pub(super) fn claims_moved(&self) -> bool {
        self.observed_claims
            .iter()
            .any(|(contract, seen)| external(self.stores.dispatch.claimants(contract)) != *seen)
    }

    /// Reconciles when a claim a request resolved against has moved. A claim that moved while a
    /// reconcile was running is caught the same way: the reconcile recorded the claims it read,
    /// and this compares them against the router's now.
    pub(super) async fn reconcile_if_claims_moved(&mut self) -> Res<()> {
        if self.claims_moved() {
            let snapshot = self.config().await?;
            drop(
                self.reconcile(&RequestContext::root(), snapshot, Trigger::Change)
                    .await?,
            );
        }
        Ok(())
    }

    /// Withdraws every nodule's registration from dispatch at shutdown.
    ///
    /// Dispatch outlives this service and its router keeps every provider registered, but
    /// an executor's provider reaches back into dispatch (a Wasm guest's outgoing calls
    /// go through dispatch's client), which would make dispatch own itself through the
    /// router. Withdrawing the registrations at shutdown breaks that cycle; a restart
    /// derives them again from the config.
    pub(super) fn unbind_nodules(&self) {
        for cartridge_id in self.records.keys() {
            self.stores.dispatch.unbind(&cartridge_id.to_string());
        }
    }
}
