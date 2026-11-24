//! `docs.datums`: one shared feed per distinct datum query.

use crate::CabinetDocs;
use crate::Spawn;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Projection;
use clay_collection::ProjectionLease;
use clay_collection::RowEvidence;
use clay_collection::Version;
use futures::channel::oneshot;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::watch;

/// What a micro mounts: `pollDatums`' own query, unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatumSelector {
    pub predicate: Predicate,
    pub datums: DatumSelection,
}

pub type RowKey = (DocId, DatumKey);

/// One selected datum of a matching document, as the index knows it: its version, not its
/// value. Reading the value hydrates the document, which a feed never does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatumRow {
    pub doc_heads: Vec<ChangeHash>,
    pub version: DatumVersion,
}

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    /// The node refused the query under the authority's capability.
    #[error("the authority's capability does not reach this query")]
    Denied,
    /// The node failed to answer the feed's poll.
    #[error("the node failed: {0}")]
    Node(String),
    #[error("the feed refused the lease: {0:?}")]
    Acquire(AcquireError),
}

/// Who a feed polls as: one actor's view of the node, and the capability bound to that actor.
///
/// Today that is one micro (ADR 054 §5). `id` names the actor; feeds are shared only between
/// leases presenting the same one, because the node filters each poll by its caller's reach.
#[derive(Clone)]
pub struct Authority {
    pub id: String,
    pub node: Arc<dyn CabinetDocs>,
    pub capability: CapId,
}

/// The feeds of one host: one per `(authority, selector)`, shared by every lease on it.
pub struct DatumFeeds {
    spawn: Spawn,
    page_limit: u32,
    hold_ms: u32,
    feeds: Mutex<Vec<(FeedKey, Weak<Feed>)>>,
}

/// The actor a feed polls as, and its query.
#[derive(Clone, PartialEq, Eq)]
struct FeedKey {
    authority: String,
    selector: DatumSelector,
}

/// A lease on a shared feed. Holding it keeps the feed's upstream poll running.
pub struct DatumLease {
    pub lease: ProjectionLease<RowKey, DatumRow>,
    _feed: Arc<Feed>,
}

struct Feed {
    projection: Projection<RowKey, DatumRow>,
    state: watch::Receiver<FeedState>,
    // Dropping the last lease drops this sender, which ends the feed's loop.
    _stop: oneshot::Sender<()>,
}

#[derive(Clone, Debug)]
enum FeedState {
    Bootstrapping,
    Live,
    Denied,
    Failed(String),
}

impl DatumFeeds {
    /// `hold_ms` is how long a caught-up feed lets the node hold an empty page; the node caps it.
    pub fn new(spawn: Spawn, page_limit: u32, hold_ms: u32) -> Self {
        Self {
            spawn,
            page_limit,
            hold_ms,
            feeds: Mutex::new(Vec::new()),
        }
    }

    /// Leases the feed for `selector` as `authority`, starting it when no lease holds it, and
    /// returns once it has bootstrapped. The node authorizes the feed's first poll, so a query
    /// the authority cannot reach fails here with [`FeedError::Denied`].
    pub async fn acquire(
        &self,
        authority: &Authority,
        selector: DatumSelector,
    ) -> Result<DatumLease, FeedError> {
        let feed = self.feed(authority, selector);
        let mut state = feed.state.clone();
        let state = state
            .wait_for(|state| !matches!(state, FeedState::Bootstrapping))
            .await
            .expect("a feed's loop reports its state before it ends")
            .clone();
        match state {
            FeedState::Bootstrapping => unreachable!(),
            FeedState::Denied => return Err(FeedError::Denied),
            FeedState::Failed(error) => return Err(FeedError::Node(error)),
            FeedState::Live => {}
        }
        let lease = feed
            .projection
            .acquire((), None)
            .map_err(FeedError::Acquire)?;
        Ok(DatumLease { lease, _feed: feed })
    }

    fn feed(&self, authority: &Authority, selector: DatumSelector) -> Arc<Feed> {
        let key = FeedKey {
            authority: authority.id.clone(),
            selector,
        };
        let mut feeds = self.feeds.lock().expect("feed registry poisoned");
        feeds.retain(|(_, feed)| feed.strong_count() > 0);
        if let Some(feed) = feeds
            .iter()
            .find(|(existing, _)| *existing == key)
            .and_then(|(_, feed)| feed.upgrade())
            // An ended feed stays alive while its old leases drain; a new lease starts afresh.
            .filter(|feed| {
                matches!(
                    *feed.state.borrow(),
                    FeedState::Bootstrapping | FeedState::Live
                )
            })
        {
            return feed;
        }
        let projection = Projection::new([0, 0]);
        let (state_tx, state) = watch::channel(FeedState::Bootstrapping);
        let (stop_tx, stop) = oneshot::channel();
        let feed = Arc::new(Feed {
            projection: projection.clone(),
            state,
            _stop: stop_tx,
        });
        feeds.retain(|(existing, _)| *existing != key);
        feeds.push((key.clone(), Arc::downgrade(&feed)));
        drop(feeds);
        let FeedKey { selector, .. } = key;
        (self.spawn)(Box::pin(drive(
            Arc::clone(&authority.node),
            PollDatums {
                capability: authority.capability,
                predicate: selector.predicate,
                datums: selector.datums,
                after: 0,
                limit: self.page_limit,
                hold_ms: 0,
            },
            self.hold_ms,
            projection,
            state_tx,
            stop,
        )));
        feed
    }

    #[cfg(test)]
    pub(crate) fn live_feeds(&self) -> usize {
        let feeds = self.feeds.lock().expect("feed registry poisoned");
        feeds
            .iter()
            .filter(|(_, feed)| feed.strong_count() > 0)
            .count()
    }
}

/// One feed's upstream loop: page from the start until caught up, publish that as one
/// bootstrap transaction, then hold for changes. It ends when the last lease drops the feed,
/// or when the node fails, which marks the projection unavailable so its leases reacquire.
async fn drive(
    node: Arc<dyn CabinetDocs>,
    mut request: PollDatums,
    hold_ms: u32,
    projection: Projection<RowKey, DatumRow>,
    state: watch::Sender<FeedState>,
    mut stop: oneshot::Receiver<()>,
) {
    // The datum keys each held document has, so a move-out can remove every one of its rows.
    let mut held: BTreeMap<DocId, BTreeSet<DatumKey>> = BTreeMap::new();
    let mut bootstrap: BTreeMap<RowKey, Change<RowKey, DatumRow>> = BTreeMap::new();
    let mut live = false;
    loop {
        let poll = node.poll_datums(request.clone());
        let page = match futures::future::select(poll, &mut stop).await {
            futures::future::Either::Left((page, _)) => page,
            futures::future::Either::Right(_) => return,
        };
        let page = match page {
            Ok(page) => page,
            // The authority lost the reach this query needs: its leases stop for good.
            Err(DocsError::Denied) => {
                projection.revoke();
                state.send_replace(FeedState::Denied);
                return;
            }
            Err(error) => {
                projection.mark_source_unavailable();
                state.send_replace(FeedState::Failed(error.to_string()));
                return;
            }
        };
        request.after = page.through;
        let mut changes = BTreeMap::new();
        for event in page.events {
            for change in translate(event, &mut held) {
                changes.insert(change.key().clone(), change);
            }
        }
        if live {
            if !changes.is_empty() {
                projection.publish(changes.into_values().collect());
            }
            continue;
        }
        bootstrap.extend(changes);
        if page.caught_up {
            projection.publish(std::mem::take(&mut bootstrap).into_values().collect());
            live = true;
            request.hold_ms = hold_ms;
            state.send_replace(FeedState::Live);
        }
    }
}

fn translate(
    event: DatumEvent,
    held: &mut BTreeMap<DocId, BTreeSet<DatumKey>>,
) -> Vec<Change<RowKey, DatumRow>> {
    match event {
        DatumEvent::Datum {
            revision,
            doc_id,
            datum_key,
            datum,
        } => {
            let row_version = Version::new(0, revision);
            let keys = held.entry(doc_id.clone()).or_default();
            match datum {
                ObservedDatum::Present { doc_heads, version } => {
                    keys.insert(datum_key.clone());
                    vec![Change::Upsert {
                        key: (doc_id, datum_key),
                        row_version,
                        value: DatumRow { doc_heads, version },
                        evidence: RowEvidence::Authoritative,
                        write_context: None,
                    }]
                }
                ObservedDatum::Removed { .. } => {
                    keys.remove(&datum_key);
                    if keys.is_empty() {
                        held.remove(&doc_id);
                    }
                    vec![Change::Remove {
                        key: (doc_id, datum_key),
                        row_version,
                    }]
                }
            }
        }
        DatumEvent::Leave { revision, doc_id } => held
            .remove(&doc_id)
            .unwrap_or_default()
            .into_iter()
            .map(|datum_key| Change::Remove {
                key: (doc_id.clone(), datum_key),
                row_version: Version::new(0, revision),
            })
            .collect(),
    }
}
