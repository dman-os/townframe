//! Observational live tail of a revisioned source.
//!
//! The watch samples the source revision before opening its reader. The
//! reader then replays anything committed after that sample, so a commit
//! racing the two calls is observed rather than lost. It has no durable
//! cursor and owns no task or callback.
//!
//! The watch never sees source selection. `open` samples the source's latest
//! revision itself, then hands that revision to the caller-supplied
//! `open_reader` closure, which opens the store's reader with whatever filter
//! it needs (selection is a store-open concern, compiled into the store's
//! query). Keeping the sample-then-open order inside `open` is what makes the
//! race guarantee un-botchable by callers.

use crate::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use std::future::Future;

pub struct LiveRevisionWatch<'a, S>
where
    S: RevisionedStore + 'a,
{
    reader: S::Reader<'a>,
}

impl<'a, S> LiveRevisionWatch<'a, S>
where
    S: RevisionedStore + 'a,
{
    /// Sample `source`'s latest revision, then let `open_reader` open the
    /// reader at that revision. The closure receives the sampled revision and
    /// typically closes over the store and its selection:
    ///
    /// ```ignore
    /// LiveRevisionWatch::open(&store, |after| async move {
    ///     store.open(selector, after).await
    /// })
    /// .await?
    /// ```
    pub async fn open<F, Fut>(source: &'a S, open_reader: F) -> Result<Self, S::Error>
    where
        F: FnOnce(S::Revision) -> Fut,
        Fut: Future<Output = Result<S::Reader<'a>, S::Error>>,
    {
        let latest = source.latest_revision().await?;
        let reader = open_reader(latest).await?;
        Ok(Self { reader })
    }

    /// Yield only revisions committed after the initial observation. The
    /// source's initial replay boundary is an implementation detail here.
    pub async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<S::Revision, S::Entry>, S::Error> {
        loop {
            match self.reader.next(limits).await? {
                RevisionRead::ReplayComplete { .. } => continue,
                entries @ RevisionRead::Entries { .. } => return Ok(entries),
            }
        }
    }
}
