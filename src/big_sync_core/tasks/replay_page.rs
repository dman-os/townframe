use crate::interlude::*;

use crate::{
    part_store::PartStoreReadOnly,
    rpc,
    rpc::BigSyncRpcClient,
    tasks::{TaskCtx, TaskResultDeets},
};

/// One bounded page over a set of targets.
///
/// The task is discrete: it asks for one page and completes. Re-issuing is the machine's job,
/// so a page cannot outlive the connection and a client's processing rate is what decides how
/// fast events arrive (ADR 012 decision 9). One request carries every target of the round:
/// each target's own cursor travels with it, so a page per part would be one request per part
/// over the same connection.
#[derive(Debug, Clone)]
pub struct ReplaySubscriptionTaskState {
    pub subscription_id: rpc::ReplaySubscriptionId,
    pub generation: u64,
    pub targets: Vec<rpc::ReplaySubscriptionTargetEntry>,
    pub request: Option<rpc::ReplaySubscriptionRequest>,
}

#[derive(Debug, Clone)]
pub struct ReplayPageTask {
    pub peer_id: PeerKey,
    /// This request's id, so the round that replaces it can name it.
    pub request_id: rpc::ReplayRequestId,
    pub targets: Vec<rpc::SubscriptionTarget>,
    /// The page this round supersedes, if it is replacing one still in flight. The responder
    /// drops that request only if it is still waiting, so a page already holding rows ships
    /// them and superseding never discards delivered work.
    pub supersede: Option<rpc::ReplayRequestId>,
    pub limit: u32,
    /// Optional stateful target-set operation to perform before fetching this page.
    pub subscription: Option<ReplaySubscriptionTaskState>,
}

#[derive(Debug)]
pub struct ReplayPageResult {
    pub peer_id: PeerKey,
    pub page: rpc::ReplayPage,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct ReplayPageTaskError {
        pub peer_id: PeerKey,
        pub targets: Vec<rpc::SubscriptionTarget>,
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
        let targets = self.targets.clone();
        self.run_run(cx).await.map_err(|deets| ReplayPageTaskError {
            peer_id,
            targets,
            deets,
        })
    }

    async fn run_run<K, PStore, Rpc, Rng>(
        self,
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
        let page = if let Some(subscription) = self.subscription.clone() {
            if let Some(request) = subscription.request.clone() {
                match peer_rpc.replay_subscription(request).await {
                    Ok(rpc::ReplaySubscriptionResponse::Opened { .. })
                    | Ok(rpc::ReplaySubscriptionResponse::Updated { .. }) => {}
                    Err(rpc::RpcError::UnknownSubscription)
                    | Err(rpc::RpcError::StaleSubscriptionGeneration)
                    | Err(rpc::RpcError::InvalidRequest(_)) => {
                        peer_rpc
                            .replay_subscription(rpc::ReplaySubscriptionRequest::Open {
                                subscription_id: subscription.subscription_id,
                                generation: subscription.generation,
                                targets: subscription.targets.clone(),
                            })
                            .await?;
                    }
                    Err(error) => return Err(error.into()),
                    Ok(response) => {
                        return Err(rpc::RpcError::InvalidRequest(format!(
                            "unexpected replay subscription response before page: {response:?}"
                        ))
                        .into());
                    }
                }
            }
            let next = rpc::ReplaySubscriptionRequest::Next {
                subscription_id: subscription.subscription_id,
                request_id: self.request_id,
                supersede: self.supersede,
                targets: self
                    .targets
                    .iter()
                    .map(|target| {
                        let descriptor = rpc::ReplaySubscriptionTarget::from(target);
                        let entry = subscription
                            .targets
                            .iter()
                            .find(|entry| entry.target == descriptor)
                            .expect("every replay page target has a subscription id");
                        (
                            entry.id,
                            match target {
                                rpc::SubscriptionTarget::Part { cursor, .. }
                                | rpc::SubscriptionTarget::Object { cursor, .. } => *cursor,
                            },
                        )
                    })
                    .collect(),
                limit: self.limit,
                hold_ms: Self::HOLD_MS,
            };
            let response = match peer_rpc.replay_subscription(next.clone()).await {
                Ok(response) => response,
                Err(rpc::RpcError::UnknownSubscription) | Err(rpc::RpcError::InvalidRequest(_)) => {
                    peer_rpc
                        .replay_subscription(rpc::ReplaySubscriptionRequest::Open {
                            subscription_id: subscription.subscription_id,
                            generation: subscription.generation,
                            targets: subscription.targets.clone(),
                        })
                        .await?;
                    peer_rpc.replay_subscription(next).await?
                }
                Err(error) => return Err(error.into()),
            };
            let rpc::ReplaySubscriptionResponse::Page(subscription_page) = response else {
                return Err(rpc::RpcError::InvalidRequest(
                    "replay subscription did not return a page".into(),
                )
                .into());
            };
            let page_targets = subscription_page
                .targets
                .into_iter()
                .filter_map(|(id, verdict)| {
                    let entry = subscription.targets.iter().find(|entry| entry.id == id)?;
                    let current = self
                        .targets
                        .iter()
                        .find(|target| rpc::ReplaySubscriptionTarget::from(*target) == entry.target)
                        .cloned()
                        .unwrap_or_else(|| entry.target.with_cursor(0));
                    let target = match verdict {
                        rpc::TargetVerdict::Events { resume, .. } => {
                            entry.target.with_cursor(resume)
                        }
                        _ => current,
                    };
                    Some((target, verdict))
                })
                .collect();
            rpc::ReplayPage {
                events: subscription_page.page.events,
                targets: page_targets,
            }
        } else {
            peer_rpc
                .replay_page(rpc::ReplayPageRequest {
                    request_id: self.request_id,
                    supersede: self.supersede,
                    targets: self.targets.clone(),
                    limit: self.limit,
                    hold_ms: Self::HOLD_MS,
                })
                .await?
        };
        Ok(TaskResultDeets::ReplayPage(ReplayPageResult {
            peer_id: self.peer_id,
            page,
        }))
    }
}
