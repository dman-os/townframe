//! Observational live tail of a revisioned source.
//!
//! The watch samples the source revision before opening its reader. The
//! reader then replays anything committed after that sample, so a commit
//! racing the two calls is observed rather than lost. It has no durable
//! cursor and owns no task or callback.

use crate::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};

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
    pub async fn open(source: &'a S, selector: S::Selector) -> Result<Self, S::Error> {
        let latest = source.latest_revision().await?;
        let reader = source.open(selector, latest).await?;
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
