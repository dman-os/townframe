use crate::interlude::*;

use crate::{
    part_store::PartStoreReadOnly,
    rpc,
    rpc::BigSyncRpcClient,
    tasks::{TaskCtx, TaskResultDeets},
};

/// One round of a logical replay subscription: at most one `Update`, then at most one page.
///
/// The round is discrete. A round that carries a subscription change and a page makes both
/// calls in order; a round that only reconfigures the subscription carries no targets and
/// therefore no page call (ADR 012 decision 9: `Update` and `Next` are each discrete calls).
#[derive(Debug, Clone)]
pub struct ReplaySubscriptionTaskState {
    pub subscription_id: rpc::ReplaySubscriptionId,
    /// The generation this round's `Update` carries.
    pub generation: u64,
    /// The one `Update` this round makes.
    pub request: Option<rpc::ReplaySubscriptionRequest>,
}

/// What the responder answered about this round's `Update`.
#[derive(Debug, Clone)]
pub enum ReplayUpdateOutcome {
    /// The responder applied the update. Every entry it carried that is not named in
    /// `rejected` landed.
    Applied {
        /// The generation the round's update carried.
        generation: u64,
        rejected: Vec<(rpc::ReplayTargetId, rpc::TargetVerdict)>,
    },
    /// A newer update had already reached the responder, so this one did not apply. The
    /// round's own generation is `generation`; `current` is the one the responder holds.
    Superseded { generation: u64, current: u64 },
}

#[derive(Debug, Clone)]
pub struct ReplayPageTask {
    pub peer_id: PeerKey,
    /// Names the client-owned replay session for this round and its request ids.
    pub session_id: rpc::ReplaySessionId,
    /// This request's id, so the round that replaces it can name it.
    pub request_id: rpc::ReplayRequestId,
    /// The targets this round pages: the responder's entry id for each route, with the cursor
    /// this round resumes from. The wire names entries by id, so the round carries the id it
    /// was scheduled with instead of looking it up by route again.
    pub targets: Vec<(rpc::ReplayTargetId, rpc::SubscriptionTarget)>,
    /// The page this round supersedes, if it is replacing one still in flight. The responder
    /// drops that request only if it is still waiting, so a page already holding rows ships
    /// them and superseding never discards delivered work.
    pub supersede: Option<rpc::ReplayRequestId>,
    pub limit: u32,
    /// Long-poll duration for this page. Catch-up requests are drain-only; only an
    /// established live lane waits.
    pub hold_ms: u32,
    /// The stateful target-set operation this round performs. A round without one is a plain
    /// page over the subscription the responder already holds.
    pub subscription: Option<ReplaySubscriptionTaskState>,
}

#[derive(Debug)]
pub struct ReplayPageResult {
    pub peer_id: PeerKey,
    pub page: rpc::ReplayPage,
    /// The outcome of the `Update` this round carried, when it carried one.
    pub update: Option<ReplayUpdateOutcome>,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct ReplayPageTaskError {
        pub peer_id: PeerKey,
        pub targets: Vec<rpc::SubscriptionTarget>,
        /// The subscription this round carried an `Update` for. A round whose update failed
        /// never asked for its page, so the machine retries the update rather than a page.
        pub updated_subscription_id: Option<rpc::ReplaySubscriptionId>,
        pub deets:
            pub enum ReplayPageTaskErrorDeets {
                #![derive(thiserror::Error, displaydoc::Display)]
                /// {0}
                Rpc(#[from] rpc::RpcError),
            }
    }
}

impl ReplayPageTask {
    /// Events per page. A knob rather than a threshold, but it was measured: on
    /// the 100k-object catchup case, 256-event pages took 64s against 31s for
    /// 1024, because every page costs a fresh responder-side read. The number
    /// exists to cap a page, so it should not be small.
    pub const LIMIT: u32 = 1024;

    /// How long the client is willing to have a request held while every target is
    /// caught up. The client's processing rate is the flow control, so this is
    /// the client's own pacing choice; the responder caps it.
    pub const HOLD_MS: u32 = 15_000;

    pub async fn run<K, PStore, Rpc, Rng>(
        self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, ReplayPageTaskError>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let peer_id = self.peer_id.clone();
        let updated_subscription_id = self.subscription.as_ref().and_then(|subscription| {
            subscription
                .request
                .as_ref()
                .map(|_| subscription.subscription_id)
        });
        // The routes are cloned only on the way out of a failed round: a successful round would
        // otherwise copy every target, cursors and object keys included, for nobody.
        self.run_run(cx).await.map_err(|deets| ReplayPageTaskError {
            peer_id,
            targets: self
                .targets
                .iter()
                .map(|(_, target)| target.clone())
                .collect(),
            updated_subscription_id,
            deets,
        })
    }

    async fn run_run<K, PStore, Rpc, Rng>(
        &self,
        cx: &mut TaskCtx<K, PStore, Rpc, Rng>,
    ) -> Result<TaskResultDeets, ReplayPageTaskErrorDeets>
    where
        K: FutureForm,
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let Some(peer_rpc) = cx.rpc_clients.get(&self.peer_id) else {
            // No client for this peer means the route is gone, which is a
            // transport failure rather than an empty page.
            return Err(ReplayPageTaskErrorDeets::Rpc(rpc::RpcError::TransportError));
        };
        let rpc_started = std::time::Instant::now();
        let update = self.update_subscription::<K, Rpc>(peer_rpc).await?;
        let page = if self.targets.is_empty()
            || matches!(update, Some(ReplayUpdateOutcome::Superseded { .. }))
        {
            // Nothing to ask for: an update-only round pages nothing, and a round whose
            // update was overtaken must not ask for ids the responder may not hold — the
            // machine re-sends the update and pages once it lands.
            rpc::ReplayPage {
                events: Vec::new(),
                targets: Vec::new(),
            }
        } else {
            self.fetch_page::<K, Rpc>(peer_rpc, update.as_ref()).await?
        };
        tracing::debug!(
            peer_id = %self.peer_id,
            ?self.request_id,
            elapsed_ms = rpc_started.elapsed().as_millis(),
            event_count = page.events.len(),
            target_count = page.targets.len(),
            "replay page rpc completed",
        );
        Ok(TaskResultDeets::ReplayPage(ReplayPageResult {
            peer_id: self.peer_id.clone(),
            page,
            update,
        }))
    }

    /// Make this round's `Update`, if it carries one.
    async fn update_subscription<K, Rpc>(
        &self,
        peer_rpc: &Rpc,
    ) -> Result<Option<ReplayUpdateOutcome>, ReplayPageTaskErrorDeets>
    where
        K: FutureForm,
        Rpc: BigSyncRpcClient<K>,
    {
        let Some(subscription) = &self.subscription else {
            return Ok(None);
        };
        let Some(request) = subscription.request.clone() else {
            return Ok(None);
        };
        match peer_rpc.replay_subscription(request).await? {
            rpc::ReplaySubscriptionResponse::Updated {
                generation,
                rejected,
            } => Ok(Some(if generation == subscription.generation {
                ReplayUpdateOutcome::Applied {
                    generation,
                    rejected,
                }
            } else {
                // The responder holds a newer generation, so this update did not apply and
                // its entries still owe an answer.
                ReplayUpdateOutcome::Superseded {
                    generation: subscription.generation,
                    current: generation,
                }
            })),
            rpc::ReplaySubscriptionResponse::Closed => Err(rpc::RpcError::InvalidRequest(
                "replay subscription closed while updating".into(),
            )
            .into()),
            response => Err(rpc::RpcError::InvalidRequest(format!(
                "unexpected replay subscription response to an update: {response:?}"
            ))
            .into()),
        }
    }

    /// Ask for the one page this round carries.
    async fn fetch_page<K, Rpc>(
        &self,
        peer_rpc: &Rpc,
        update: Option<&ReplayUpdateOutcome>,
    ) -> Result<rpc::ReplayPage, ReplayPageTaskErrorDeets>
    where
        K: FutureForm,
        Rpc: BigSyncRpcClient<K>,
    {
        let subscription = self
            .subscription
            .as_ref()
            .expect("a page round belongs to a subscription");
        // An entry this round's own update refused is not in the responder's target set, so
        // it must not be named here: the responder has nothing to answer for it, and naming
        // it would fail the whole page instead of reporting it per target.
        let refused: std::collections::HashSet<rpc::ReplayTargetId> = match update {
            Some(ReplayUpdateOutcome::Applied { rejected, .. }) => {
                rejected.iter().map(|(id, _)| *id).collect()
            }
            _ => std::collections::HashSet::new(),
        };
        let targets: Vec<_> = self
            .targets
            .iter()
            .filter(|(id, _)| !refused.contains(id))
            .map(|(id, target)| (*id, target.cursor()))
            .collect();
        if targets.is_empty() {
            // Every target of the round was refused by its own update, so there is nothing
            // the responder can answer for. The round reports the refusals instead.
            return Ok(rpc::ReplayPage {
                events: Vec::new(),
                targets: Vec::new(),
            });
        }
        let next = rpc::ReplaySubscriptionRequest::Next {
            session_id: self.session_id,
            subscription_id: subscription.subscription_id,
            request_id: self.request_id,
            supersede: self.supersede,
            targets,
            limit: self.limit,
            hold_ms: self.hold_ms,
        };
        tracing::debug!(
            peer_id = %self.peer_id,
            ?self.request_id,
            supersede = ?self.supersede,
            target_count = self.targets.len(),
            hold_ms = self.hold_ms,
            "replay subscription next request",
        );
        let response = peer_rpc.replay_subscription(next).await?;
        tracing::debug!(
            peer_id = %self.peer_id,
            ?self.request_id,
            "replay subscription next response received",
        );
        let rpc::ReplaySubscriptionResponse::Page(subscription_page) = response else {
            return Err(rpc::RpcError::InvalidRequest(
                "replay subscription did not return a page".into(),
            )
            .into());
        };
        // A verdict names an entry of this round; an id the round does not carry is a superseded
        // round's answer and is dropped. `Events` moves the route to the position the responder
        // resumed from. A refusal carries no position, so the route keeps the one this round asked
        // from, and the machine re-adds it under a fresh id (ADR 012 decision 9).
        let page_targets = subscription_page
            .targets
            .into_iter()
            .filter_map(|(id, verdict)| {
                let (_, target) = self
                    .targets
                    .iter()
                    .find(|(target_id, _)| *target_id == id)?;
                let target = match verdict {
                    rpc::TargetVerdict::Events { resume, .. } => {
                        rpc::ReplaySubscriptionTarget::from(target).with_cursor(resume)
                    }
                    _ => target.clone(),
                };
                Some((target, verdict))
            })
            .collect();
        Ok(rpc::ReplayPage {
            events: subscription_page.page.events,
            targets: page_targets,
        })
    }
}
