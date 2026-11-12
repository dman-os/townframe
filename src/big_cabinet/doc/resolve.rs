//! ResolveKeyRef: the content-addressed datum a `#keyRef` names (ADR 035 §2.9).
//!
//! A key reference names no document, because a datum whose slot is a hash of its value sits
//! identically in every document that recorded it. Resolution finds the documents holding the key
//! through the datum-set index (the `(datum_key, doc_id)` index `Predicate::HasKey` compiles to),
//! reads each one the caller may read, and answers with the first copy whose value hashes to the
//! key. A copy that does not hash to it is skipped as if absent, never taken as another version of
//! the datum: equivalence between copies is verified rather than assumed.

use super::*;
use crate::index::datum_set::DatumSetQuery;
use big_cabinet_core::doc::ResolvedKeyRef;

impl DocsService {
    async fn resolve_key_ref(
        &self,
        context: &RequestContext,
        params: ResolveKeyRef,
    ) -> Result<ResolvedKeyRef, DocsError> {
        let ResolveKeyRef { capability, key } = params;
        // The capability is resolved before any document is tried, so that a refusal below means
        // "no readable copy" and an invalid capability stays its own error.
        self.invocation(context, capability).await?;
        let hash = self
            .schema_repo
            .resolve_for_read(key.tag().as_str())
            .and_then(|schema| schema.compiled().content_hash().cloned())
            .ok_or_else(|| DocsError::NotContentAddressed {
                datum_key: key.clone(),
            })?;
        let Some(store) = self.datum_set.as_ref() else {
            return Err(DocsError::Repository(
                "datum-set index is not configured on this repository".into(),
            ));
        };
        // The index is fed by the documents this node holds, so every holder it names is local.
        // One whose materialization is still pending is remembered and reported only when no
        // other holder resolved.
        let mut pending = None;
        for holder in store
            .query(DatumSetQuery::Key(key.clone()))
            .await
            .map_err(repository)?
        {
            let read = self
                .read(
                    context,
                    ReadDatums {
                        capability,
                        doc_id: holder.route.doc_id.clone(),
                        at: At::Current,
                        datums: Some(BTreeSet::from([key.clone()])),
                        optimistic: false,
                    },
                )
                .await;
            match read {
                Ok(mut read) => {
                    if let Some(DatumRead::Accepted { value, .. }) = read.datums.remove(&key)
                        && hash.verify(&key, &value).is_ok()
                    {
                        return Ok(ResolvedKeyRef {
                            doc_id: holder.route.doc_id,
                            heads: read.heads,
                            value,
                        });
                    }
                }
                // A holder the caller cannot read is, to the caller, not a holder: answering
                // differently would tell it which documents hold the key.
                Err(DocsError::Denied | DocsError::DocNotFound) => {}
                Err(error @ DocsError::Pending { .. }) => pending = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(pending.unwrap_or(DocsError::KeyRefUnresolved { datum_key: key }))
    }
}

impl Handler<ResolveKeyRef> for DocsService {
    async fn call(
        &self,
        request: Envelope<ResolveKeyRef>,
    ) -> Result<ResponseEnvelope<ResolvedKeyRef>, DocsError> {
        Ok(self
            .resolve_key_ref(&request.context, request.params)
            .await?
            .into())
    }
}
