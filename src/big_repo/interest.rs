use crate::interlude::*;
use crate::{BigDocHandle, BigRepo, DocumentId};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::watch;

const SYNC_INTEREST_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct DocInterest {
    repo: Arc<BigRepo>,
    doc_id: DocumentId,
    _lease: Arc<crate::runtime2::DocLease>,
    _presence: Arc<()>,
    usable: watch::Receiver<bool>,
}

impl DocInterest {
    pub fn document_id(&self) -> DocumentId {
        self.doc_id.clone()
    }

    pub async fn wait_until_ready(&self) -> Res<()> {
        let mut usable = self.usable.clone();
        usable
            .wait_for(|ready| *ready)
            .await
            .map_err(|_| ferr!(ERROR_ACTOR))?;
        Ok(())
    }

    pub async fn ready(&self) -> Res<BigDocHandle> {
        self.wait_until_ready().await?;
        self.repo
            .get_doc(&self.doc_id)
            .await?
            .into_ready(self.doc_id.clone())
            .map_err(Into::into)
    }

    pub(crate) fn new(repo: Arc<BigRepo>, doc_id: DocumentId, interest: PendingDocLease) -> Self {
        Self {
            repo,
            doc_id,
            _lease: interest.lease,
            _presence: interest.presence,
            usable: interest.usable,
        }
    }
}

pub(crate) struct PendingDocLease {
    pub lease: Arc<crate::runtime2::DocLease>,
    pub presence: Arc<()>,
    pub usable: watch::Receiver<bool>,
}

#[derive(Clone)]
pub struct DocSyncDemand {
    docs: watch::Receiver<Arc<HashSet<DocumentId>>>,
}

impl DocSyncDemand {
    pub fn documents(&self) -> Arc<HashSet<DocumentId>> {
        Arc::clone(&self.docs.borrow())
    }

    pub async fn changed(&mut self) -> Res<Arc<HashSet<DocumentId>>> {
        self.docs.changed().await.map_err(|_| ferr!(ERROR_ACTOR))?;
        Ok(Arc::clone(&self.docs.borrow_and_update()))
    }

    pub async fn drive(
        mut self,
        source: big_sync::ObjectInterestSource,
        mut peers: watch::Receiver<HashSet<PeerKey>>,
    ) -> Res<()> {
        loop {
            let objects = self
                .docs
                .borrow_and_update()
                .iter()
                .cloned()
                .map(|doc| (doc, Arc::from(BigRepo::BACKEND_ID)))
                .collect();
            let targets = peers.borrow_and_update().clone();
            source.replace(targets, objects).await?;
            tokio::select! {
                changed = self.docs.changed() => if changed.is_err() { return Ok(()); },
                changed = peers.changed() => if changed.is_err() { return Ok(()); },
            }
        }
    }
}

pub(crate) struct SyncInterests {
    entries: Mutex<HashMap<DocumentId, InterestEntry>>,
    docs: watch::Sender<Arc<HashSet<DocumentId>>>,
    clock: Arc<dyn crate::runtime2::Clock>,
}

struct InterestEntry {
    callers: usize,
    expires: Option<Instant>,
}

pub(crate) struct SyncInterestLease {
    interests: Arc<SyncInterests>,
    doc_id: DocumentId,
}

impl SyncInterests {
    pub(crate) fn new(clock: Arc<dyn crate::runtime2::Clock>) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            docs: watch::channel(Arc::new(HashSet::new())).0,
            clock,
        })
    }

    pub(crate) fn subscribe(&self) -> DocSyncDemand {
        DocSyncDemand {
            docs: self.docs.subscribe(),
        }
    }

    pub(crate) fn acquire(self: &Arc<Self>, doc_id: DocumentId) -> SyncInterestLease {
        let mut entries = self.entries.lock().expect(ERROR_MUTEX);
        let is_new = !entries.contains_key(&doc_id);
        let entry = entries.entry(doc_id.clone()).or_insert(InterestEntry {
            callers: 0,
            expires: None,
        });
        entry.callers += 1;
        entry.expires = None;
        if is_new {
            self.docs.send_modify(|docs| {
                assert!(Arc::make_mut(docs).insert(doc_id.clone()));
            });
        }
        SyncInterestLease {
            interests: Arc::clone(self),
            doc_id,
        }
    }

    pub(crate) fn expire(&self) {
        let now = self.clock.instant();
        let mut entries = self.entries.lock().expect(ERROR_MUTEX);
        let expired: Vec<_> = entries
            .iter()
            .filter(|(_, entry)| entry.expires.is_some_and(|due| due <= now))
            .map(|(doc, _)| doc.clone())
            .collect();
        if !expired.is_empty() {
            for doc in &expired {
                entries.remove(doc);
            }
            self.docs.send_modify(|docs| {
                let docs = Arc::make_mut(docs);
                for doc in expired {
                    assert!(docs.remove(&doc));
                }
            });
        }
    }
}

impl Drop for SyncInterestLease {
    fn drop(&mut self) {
        let mut entries = self.interests.entries.lock().expect(ERROR_MUTEX);
        let entry = entries.get_mut(&self.doc_id).expect(ERROR_IMPOSSIBLE);
        entry.callers = entry.callers.checked_sub(1).expect(ERROR_IMPOSSIBLE);
        if entry.callers == 0 {
            entry.expires = Some(self.interests.clock.instant() + SYNC_INTEREST_GRACE);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PausedClock;

    impl crate::runtime2::Clock for PausedClock {
        fn instant(&self) -> Instant {
            tokio::time::Instant::now().into_std()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sync_interest_shared_grace_and_reacquisition() -> Res<()> {
        let interests = SyncInterests::new(Arc::new(PausedClock));
        let mut demand = interests.subscribe();
        let doc_id = DocumentId::random();
        let handle = interests.acquire(doc_id.clone());
        assert_eq!(*demand.changed().await?, HashSet::from([doc_id.clone()]));
        let pending = interests.acquire(doc_id.clone());
        drop(handle);
        tokio::time::advance(Duration::from_secs(60)).await;
        interests.expire();
        assert_eq!(*demand.documents(), HashSet::from([doc_id.clone()]));
        drop(pending);
        tokio::time::advance(Duration::from_secs(29)).await;
        interests.expire();
        assert!(!demand.docs.has_changed()?);
        let reacquired = interests.acquire(doc_id.clone());
        tokio::time::advance(Duration::from_secs(2)).await;
        interests.expire();
        assert_eq!(*demand.documents(), HashSet::from([doc_id.clone()]));
        drop(reacquired);
        tokio::time::advance(Duration::from_secs(29)).await;
        interests.expire();
        assert!(!demand.docs.has_changed()?);
        tokio::time::advance(Duration::from_secs(1)).await;
        interests.expire();
        assert_eq!(*demand.changed().await?, HashSet::new());
        Ok(())
    }
}
