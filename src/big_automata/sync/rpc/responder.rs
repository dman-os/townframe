//! The responder of the summary and bucket calls: sans-transport, answering one principal's
//! request from any [`PartStoreServe`]. The laws are in [the module docs](super).
//!
//! It holds no state across calls: every answer is one store call (one snapshot, law 2) for the
//! principal the embedder authenticated, so there is nothing to forget, sweep or lock.

use crate::interlude::*;

use crate::part_store::LEAF_PAGE_BYTE_BUDGET;
use crate::part_store::LeafEntry;
use crate::part_store::LeafPage;
use crate::part_store::PartStoreServe;
use crate::part_store::bucket_summary::BucketSummary;
use crate::sync::rpc::MAX_LEAF_BUCKETS;
use crate::sync::rpc::MAX_LEAF_PAGE;
use crate::sync::rpc::MAX_LIST_BUCKETS;
use crate::sync::rpc::MAX_SUMMARY_PARTS;
use crate::sync::rpc::bucket::BucketObjPageEntry;
use crate::sync::rpc::bucket::BucketsError;
use crate::sync::rpc::bucket::GetChangedBucketsRequest;
use crate::sync::rpc::bucket::GetChangedBucketsResponse;
use crate::sync::rpc::bucket::LeafBucketPage;
use crate::sync::rpc::bucket::LeafBucketRequest;
use crate::sync::rpc::bucket::LeafBucketsRequest;
use crate::sync::rpc::bucket::LeafBucketsResponse;
use crate::sync::rpc::summary::PartSummariesError;
use crate::sync::rpc::summary::PartSummariesRequest;
use crate::sync::rpc::summary::PartSummariesResponse;

#[cfg(test)]
mod tests;

/// Answers the three calls from a part store. Store failures are the caller's `Err`; the
/// protocol has no variant for them (a durable store's failure is not an outcome the asker can
/// act on).
#[derive(Debug, Clone)]
pub struct RpcResponder<S> {
    store: S,
    /// The most entries one leaf page carries; at most [`MAX_LEAF_PAGE`].
    leaf_cap: u32,
    /// The most encoded bytes one leaf page's entries add up to, past its first entry; at most
    /// [`LEAF_PAGE_BYTE_BUDGET`].
    leaf_byte_budget: usize,
}

impl<S: PartStoreServe> RpcResponder<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            leaf_cap: MAX_LEAF_PAGE,
            leaf_byte_budget: LEAF_PAGE_BYTE_BUDGET,
        }
    }

    /// A responder that pages leaves below the protocol's maximum.
    pub fn with_leaf_cap(mut self, leaf_cap: u32) -> Self {
        assert!(
            (1..=MAX_LEAF_PAGE).contains(&leaf_cap),
            "a leaf cap is within 1..={MAX_LEAF_PAGE}"
        );
        self.leaf_cap = leaf_cap;
        self
    }

    /// A responder that stops a leaf page at fewer encoded bytes than the protocol's maximum.
    pub fn with_leaf_byte_budget(mut self, leaf_byte_budget: usize) -> Self {
        assert!(
            (1..=LEAF_PAGE_BYTE_BUDGET).contains(&leaf_byte_budget),
            "a leaf byte budget is within 1..={LEAF_PAGE_BYTE_BUDGET}"
        );
        self.leaf_byte_budget = leaf_byte_budget;
        self
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// Law 3: one summary per asked part the principal may read, nothing for the rest; a request
    /// over [`MAX_SUMMARY_PARTS`] is refused whole before the store is read.
    pub async fn part_summaries(
        &self,
        principal: &PeerKey,
        req: PartSummariesRequest,
    ) -> Res<Result<PartSummariesResponse, PartSummariesError>> {
        if req.parts.len() > MAX_SUMMARY_PARTS {
            return Ok(Err(PartSummariesError::TooManyParts {
                cap: MAX_SUMMARY_PARTS,
            }));
        }
        let asked = req.parts.into_iter().collect::<Vec<_>>();
        let summaries = self.store.part_summaries(principal, &asked).await?;
        Ok(Ok(asked
            .into_iter()
            .zip(summaries)
            .filter_map(|((part, _), summary)| Some((part, summary?)))
            .collect()))
    }

    /// Law 4: the part's changed buckets from `offset` through `to_level`, in bucket order,
    /// `limit_hint` of them (at most [`MAX_LIST_BUCKETS`]) plus the remaining siblings of the
    /// last one.
    pub async fn changed_buckets(
        &self,
        principal: &PeerKey,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<GetChangedBucketsResponse, BucketsError>> {
        let GetChangedBucketsRequest {
            part_key,
            offset,
            to_level,
            since,
            limit_hint,
        } = req;
        if offset.level() > BuckId::MAX_LEVEL {
            return Ok(Err(BucketsError::Invalid(format!(
                "offset {offset:?} is below the deepest level"
            ))));
        }
        // A zero hint is a zero-bucket page, not an unspecified one, and the part is still
        // checked, so a refusal reads the same at any limit.
        let limit = usize::try_from(limit_hint.min(MAX_LIST_BUCKETS)).expect(ERROR_IMPOSSIBLE);
        let Some(listed) = self
            .store
            .bucket_list(
                principal,
                &part_key,
                offset,
                to_level,
                since,
                limit + usize::from(BuckId::ARITY),
            )
            .await?
        else {
            return Ok(Err(BucketsError::UnknownPart));
        };
        let mut out = Vec::<BucketSummary>::with_capacity(listed.len().min(limit));
        for bucket in listed {
            // Past the limit, only the last bucket's siblings: the same level and parent (the
            // root is its children's parent, not their sibling). The store read is capped at
            // `ARITY` past the limit.
            let sibling = out.last().is_some_and(|last| {
                last.id.level() == bucket.id.level() && last.id.parent() == bucket.id.parent()
            });
            if out.len() >= limit && !sibling {
                break;
            }
            out.push(bucket);
        }
        Ok(Ok(out))
    }

    /// Law 5: per asked bucket, its members after `after`, at most the limit and the byte budget,
    /// each page echoing the `after` it answers.
    pub async fn leaf_buckets(
        &self,
        principal: &PeerKey,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketsResponse, BucketsError>> {
        let LeafBucketsRequest {
            part_key,
            since,
            buckets,
            seed,
            limit_hint,
        } = req;
        if buckets.len() > MAX_LEAF_BUCKETS {
            return Ok(Err(BucketsError::TooManyBuckets {
                cap: MAX_LEAF_BUCKETS,
            }));
        }
        // The store takes an `after` of another bucket as a caller bug and panics, and a
        // non-canonical bucket id reaches arithmetic that assumes canonical ones; both are
        // remote input, refused here before any part is looked at.
        for LeafBucketRequest { buck_id, after } in &buckets {
            if BuckId::new(buck_id.level(), buck_id.index()) != *buck_id {
                return Ok(Err(BucketsError::Invalid(format!(
                    "bucket {buck_id:?} is not a bucket id"
                ))));
            }
            if after
                .as_ref()
                .is_some_and(|after| BuckId::from_obj_key(buck_id.level(), after) != *buck_id)
            {
                return Ok(Err(BucketsError::Invalid(format!(
                    "the cursor is not in bucket {buck_id:?}"
                ))));
            }
        }
        let limit = usize::try_from(limit_hint.map_or(1, |hint| hint.max(1)).min(self.leaf_cap))
            .expect(ERROR_IMPOSSIBLE);
        let bucks = buckets
            .into_iter()
            .map(|LeafBucketRequest { buck_id, after }| (buck_id, after))
            .collect::<Vec<_>>();
        // The store cuts each page at the limit and the byte budget.
        let Some(leaves) = self
            .store
            .bucket_leaves(
                principal,
                &part_key,
                &bucks,
                since,
                limit,
                self.leaf_byte_budget,
            )
            .await?
        else {
            return Ok(Err(BucketsError::UnknownPart));
        };
        Ok(Ok(bucks
            .into_iter()
            .zip(leaves)
            .map(|((buck_id, after), LeafPage { entries, more })| {
                // The next page resumes after the last entry taken.
                let next_after = more.then(|| {
                    entries
                        .last()
                        .expect("a page of a bucket with more is not empty")
                        .obj
                        .clone()
                });
                let entries = entries
                    .into_iter()
                    .map(|LeafEntry { obj, live }| BucketObjPageEntry {
                        fp: live.map(|digest| BucketObjPageEntry::fingerprint(&seed, &obj, digest)),
                        obj_key: obj,
                    })
                    .collect();
                (
                    buck_id,
                    LeafBucketPage {
                        after,
                        entries,
                        next_after,
                    },
                )
            })
            .collect()))
    }
}
