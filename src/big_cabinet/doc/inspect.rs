//! The unsafe inspection pair: raw claim evidence for diagnostics and tests.

use super::*;

fn history_reason(error: ClaimHistoryError) -> String {
    match error {
        ClaimHistoryError::ResourceRefused(reason) => format!("resource limit: {reason}"),
        ClaimHistoryError::Unsupported(reason) => format!("unsupported: {reason}"),
        ClaimHistoryError::MalformedWitness(reason) => format!("malformed witness: {reason}"),
        ClaimHistoryError::InvalidSignature => "invalid signature".into(),
        ClaimHistoryError::Automerge(reason) => format!("Automerge: {reason}"),
    }
}

impl From<ClaimHistoryError> for DocsError {
    fn from(error: ClaimHistoryError) -> Self {
        match error {
            ClaimHistoryError::ResourceRefused(reason) => Self::ResourceRefused {
                reason: reason.into(),
            },
            ClaimHistoryError::Unsupported(reason) => Self::Unsupported {
                reason: reason.into(),
            },
            ClaimHistoryError::Automerge(reason) => Self::Repository(reason),
            error @ (ClaimHistoryError::MalformedWitness(_)
            | ClaimHistoryError::InvalidSignature) => Self::MalformedEvidence {
                reason: history_reason(error),
            },
        }
    }
}

impl From<ClaimHistoryEntry> for DatumClaimEvidence {
    fn from(entry: ClaimHistoryEntry) -> Self {
        let result = match entry.result {
            ClaimHistoryResult::Accepted(claim) => SignatureEvidence::VerifiedSignature {
                evidence: Box::new(VerifiedSignatureEvidence {
                    signature: claim.signature,
                    actor_public_key: claim.actor_public_key,
                    claim: claim.claim,
                    introduction_marker: claim.introduction.marker.to_string(),
                    introduction_hash: claim.introduction.introduction_hash,
                    parents: claim.introduction.parents,
                    resulting_heads: claim.introduction.resulting_heads,
                    automerge_actor: claim
                        .introduction
                        .automerge_actor
                        .to_bytes()
                        .to_vec()
                        .into(),
                }),
            },
            ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(reason)) => {
                SignatureEvidence::ResourceRefused {
                    reason: reason.into(),
                }
            }
            ClaimHistoryResult::Pending(ClaimHistoryError::Unsupported(reason)) => {
                SignatureEvidence::Unsupported {
                    reason: reason.into(),
                }
            }
            ClaimHistoryResult::Pending(error) => SignatureEvidence::Pending {
                reason: history_reason(error),
            },
            ClaimHistoryResult::ProvenForgery(error) => SignatureEvidence::ProvenForgery {
                reason: history_reason(error),
            },
        };
        Self {
            marker: entry.marker.to_string(),
            result,
        }
    }
}

impl DocsService {
    async fn inspected(
        &self,
        context: &RequestContext,
        capability: CapId,
        doc_id: &DocId,
        heads: &[ChangeHash],
    ) -> Result<BigDocHandle, DocsError> {
        let caller = self
            .inspection_capabilities
            .resolve::<DocId>(context, capability)
            .await
            .map_err(authority_error)?;
        if !matches!(caller.authority, DomainAuthority::Scoped(ref inspected) if inspected == doc_id)
        {
            return Err(DocsError::Denied);
        }
        if heads.len() > HistoryBudget::default().max_entries_per_frontier {
            return Err(DocsError::ResourceRefused {
                reason: "heads frontier".into(),
            });
        }
        context.validate()?;
        self.handle(doc_id).await
    }

    fn claims(
        doc: &Automerge,
        doc_id: DocId,
        datum_key: DatumKey,
        heads: Vec<ChangeHash>,
    ) -> Result<DatumClaimsInspection, DocsError> {
        let evidence =
            reconstruct_current_claims(doc, &datum_key, &heads, HistoryBudget::default())?
                .into_iter()
                .map(DatumClaimEvidence::from)
                .collect();
        Ok(DatumClaimsInspection {
            doc_id,
            datum_key,
            heads,
            scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
            evidence,
        })
    }
}

impl Handler<UnsafeInspectDatum> for DocsService {
    async fn call(
        &self,
        request: Envelope<UnsafeInspectDatum>,
    ) -> Result<ResponseEnvelope<UnsafeDatumInspection>, DocsError> {
        let params = request.params;
        let document = self
            .inspected(
                &request.context,
                params.capability,
                &params.doc_id,
                &params.heads,
            )
            .await?;
        let schema_known = self
            .schema_repo
            .resolve_for_read(params.datum_key.tag().as_str())
            .is_some();
        let output = document
            .with_document_read(|doc| {
                request.context.validate()?;
                check_heads(doc, &params.heads)?;
                let native_value = match doc
                    .get_at(automerge::ROOT, "datums", &params.heads)
                    .map_err(ClaimHistoryError::from)?
                {
                    None => None,
                    Some((Value::Object(ObjType::Map), datums)) => NativeValue::read_at_heads(
                        doc,
                        &datums,
                        params.datum_key.to_string(),
                        &params.heads,
                        &mut NativeReadBudget::default(),
                    )
                    .map_err(ClaimHistoryError::from)?
                    .map(|value| encode_native(&value).into()),
                    Some(_) => {
                        return Err(DocsError::MalformedEvidence {
                            reason: "datums is not a map".into(),
                        });
                    }
                };
                let claims = Self::claims(doc, params.doc_id, params.datum_key, params.heads)?;
                Ok::<_, DocsError>(UnsafeDatumInspection {
                    native_value,
                    schema_known,
                    claims,
                })
            })
            .await?;
        Ok(output.into())
    }
}

impl Handler<InspectDatumClaims> for DocsService {
    async fn call(
        &self,
        request: Envelope<InspectDatumClaims>,
    ) -> Result<ResponseEnvelope<DatumClaimsInspection>, DocsError> {
        let params = request.params;
        let document = self
            .inspected(
                &request.context,
                params.capability,
                &params.doc_id,
                &params.heads,
            )
            .await?;
        let output = document
            .with_document_read(|doc| {
                request.context.validate()?;
                check_heads(doc, &params.heads)?;
                Self::claims(doc, params.doc_id, params.datum_key, params.heads)
            })
            .await?;
        Ok(output.into())
    }
}

#[cfg(test)]
mod tests;
