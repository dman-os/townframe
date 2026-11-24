//! The facts hub: where every driver publishes its `Synced`/`Unsynced` edges, and where the
//! layer above reads them. See the parent module's law 8 and its full-sync section.

use crate::interlude::*;

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Mutex;

/// What is known of one (peer, part) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fact {
    /// Wanted from the peer and not known to be in sync: not caught up, not connected, or not
    /// connected yet.
    Unsynced,
    Synced,
    /// No longer wanted from the peer: its route was removed, or the peer's driver ended.
    Removed,
}

/// One edge of a (peer, part) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactEvent {
    pub peer: PeerKey,
    pub part: PartKey,
    pub fact: Fact,
}

/// A full-sync wait ended because this pair is not wanted (any more).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("part {part:?} is not wanted from peer {peer:?}")]
pub struct Removed {
    pub peer: PeerKey,
    pub part: PartKey,
}

#[derive(Default)]
struct Inner {
    /// Every wanted pair and whether it is synced. Absent is [`Fact::Removed`].
    current: BTreeMap<(PeerKey, PartKey), bool>,
    subscribers: Vec<async_channel::Sender<FactEvent>>,
}

/// The facts of every driver that publishes to it, shared by clone. The lock is never held
/// across an await.
#[derive(Clone, Default)]
pub struct Facts {
    inner: Arc<Mutex<Inner>>,
}

impl Facts {
    pub fn new() -> Self {
        default()
    }

    /// Record `fact` for the pair and tell every subscriber, if it is an edge: a fact the pair
    /// already has is not republished.
    pub(crate) fn publish(&self, peer: &PeerKey, part: &PartKey, fact: Fact) {
        let mut inner = self.inner.lock().expect("the facts lock poisoned");
        let key = (peer.clone(), part.clone());
        let changed = match fact {
            Fact::Synced | Fact::Unsynced => {
                let synced = fact == Fact::Synced;
                inner.current.insert(key, synced) != Some(synced)
            }
            Fact::Removed => inner.current.remove(&key).is_some(),
        };
        if !changed {
            return;
        }
        let event = FactEvent {
            peer: peer.clone(),
            part: part.clone(),
            fact,
        };
        // A subscriber that dropped its stream is the one way a send fails: subscribers are
        // 0..N and leaving is not an error.
        inner
            .subscribers
            .retain(|tx| tx.try_send(event.clone()).is_ok());
    }

    /// Read the current facts under the lock and register a subscriber under the same lock, so
    /// no edge falls between the read and the first event of the channel.
    fn open<T>(
        &self,
        read: impl FnOnce(&BTreeMap<(PeerKey, PartKey), bool>) -> T,
    ) -> (T, async_channel::Receiver<FactEvent>) {
        let mut inner = self.inner.lock().expect("the facts lock poisoned");
        let current = read(&inner.current);
        let (tx, rx) = async_channel::unbounded();
        inner.subscribers.push(tx);
        (current, rx)
    }

    /// A stream that first yields the current fact of every wanted pair, then every edge after
    /// it (law 8). It never ends while any clone of the hub lives.
    pub fn subscribe(&self) -> FactStream {
        let (current, events) = self.open(|current| {
            current
                .iter()
                .map(|((peer, part), synced)| FactEvent {
                    peer: peer.clone(),
                    part: part.clone(),
                    fact: if *synced {
                        Fact::Synced
                    } else {
                        Fact::Unsynced
                    },
                })
                .collect::<VecDeque<_>>()
        });
        FactStream {
            _hub: self.clone(),
            current,
            events,
        }
    }

    /// Resolves once every asked pair is `Synced` at the same time. A pair that is not wanted,
    /// at the call or at any time while waiting, resolves the wait with it: a waiter is never
    /// stranded on a pair that cannot become synced. No pairs resolve at once.
    pub async fn wait_for_full_sync(
        &self,
        pairs: impl IntoIterator<Item = (PeerKey, PartKey)>,
    ) -> Result<(), Removed> {
        let pairs: Vec<_> = pairs.into_iter().collect();
        let (asked, events) = self.open(|current| {
            pairs
                .iter()
                .map(|pair| (pair.clone(), current.get(pair).copied()))
                .collect::<BTreeMap<_, _>>()
        });
        let mut state = asked
            .into_iter()
            .map(|((peer, part), synced)| match synced {
                Some(synced) => Ok(((peer, part), synced)),
                None => Err(Removed { peer, part }),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        while !state.values().all(|synced| *synced) {
            // The hub holds this subscriber's sender while the receiver lives.
            let event = events
                .recv()
                .await
                .expect("the facts hub outlives its waiters");
            let key = (event.peer, event.part);
            let Some(slot) = state.get_mut(&key) else {
                continue;
            };
            match event.fact {
                Fact::Synced => *slot = true,
                Fact::Unsynced => *slot = false,
                Fact::Removed => {
                    let (peer, part) = key;
                    return Err(Removed { peer, part });
                }
            }
        }
        Ok(())
    }
}

/// Current facts, then edges: see [`Facts::subscribe`].
pub struct FactStream {
    /// Keeps the hub, and so this stream's sender, alive.
    _hub: Facts,
    current: VecDeque<FactEvent>,
    events: async_channel::Receiver<FactEvent>,
}

impl FactStream {
    /// The next fact or edge, in the order they happened.
    pub async fn next(&mut self) -> FactEvent {
        if let Some(event) = self.current.pop_front() {
            return event;
        }
        // The hub holds this subscriber's sender while the receiver lives.
        self.events
            .recv()
            .await
            .expect("the facts hub outlives its streams")
    }
}
