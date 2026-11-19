//! QueryDatumRefs: authority-filtered point queries over the `datum_ref` projection (ADR 019).
//! Cross-document datum reads are `PollDatums` (ADR 025).
//!
//! When `drawer_id` is given, membership is decided by the drawer policy's verified roster
//! projection (`DocAccess::readable_in_drawer`), never by unverified index presence: one
//! drawer-level walk with weakest-link attenuation, joined against the index. When `drawer_id`
//! is omitted, each candidate document is checked against `AccessRequest::Read`.

use super::*;
use crate::index::datum_ref::DatumRefEdge;
use crate::index::datum_ref::DatumRefQuery;
use crate::index::datum_set::DatumRoute;

trait DatumRefSelectorQuery {
    fn doc_id(&self) -> &DocId;
    fn into_query(self) -> DatumRefQuery;
}

impl DatumRefSelectorQuery for DatumRefSelector {
    fn doc_id(&self) -> &DocId {
        match self {
            Self::Origin { doc_id, .. } | Self::Target { doc_id, .. } => doc_id,
        }
    }

    fn into_query(self) -> DatumRefQuery {
        match self {
            Self::Origin {
                doc_id,
                datum_key: Some(datum_key),
            } => DatumRefQuery::Origin(DatumRoute { doc_id, datum_key }),
            Self::Origin {
                doc_id,
                datum_key: None,
            } => DatumRefQuery::OriginDoc(doc_id),
            Self::Target {
                doc_id,
                datum_key: Some(datum_key),
            } => DatumRefQuery::Target(DatumRoute { doc_id, datum_key }),
            Self::Target {
                doc_id,
                datum_key: None,
            } => DatumRefQuery::TargetDoc(doc_id),
        }
    }
}

impl From<DatumRefEdge> for QueriedDatumRef {
    fn from(edge: DatumRefEdge) -> Self {
        Self {
            origin_doc_id: edge.origin.doc_id,
            origin_datum_key: edge.origin.datum_key,
            path: edge.path,
            target_doc_id: edge.target.doc_id,
            target_datum_key: edge.target.datum_key,
            ref_heads: edge.ref_heads,
        }
    }
}

impl DocsService {
    async fn query_datum_refs(
        &self,
        context: &RequestContext,
        params: QueryDatumRefs,
    ) -> Result<Vec<QueriedDatumRef>, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        use big_cabinet_core::drawer::DrawerAccess;
        if let Some(drawer_id) = &params.drawer_id {
            DocsError::ensure_doc_id(drawer_id)?;
        }
        DocsError::ensure_doc_id(params.selector.doc_id())?;
        let drawer = if let Some(drawer_id) = &params.drawer_id {
            let DomainAuthority::Scoped(scope) = &caller.authority else {
                return Err(DocsError::Denied);
            };
            let reach = scope
                .drawers
                .get(drawer_id)
                .filter(|held| held.access >= DrawerAccess::Read)
                .ok_or(DocsError::Denied)?;
            Some(BTreeMap::from([(drawer_id.clone(), reach.clone())]))
        } else {
            None
        };
        // Owned: `into_query` below consumes the selector.
        let selector_doc = &params.selector.doc_id().clone();
        let selector_kinds = match &drawer {
            Some(drawers) => Self::reach_drawers(
                self.access.as_ref(),
                context,
                drawers,
                &BTreeSet::from([selector_doc.clone()]),
                DrawerAccess::Read,
            )
            .await?
            .remove(selector_doc),
            None => Some(
                self.authorize_read(context, &caller.authority, selector_doc)
                    .await?,
            ),
        };
        let Some(selector_kinds) = selector_kinds else {
            return Ok(Vec::new());
        };
        let Some(store) = &self.datum_ref else {
            return Err(DocsError::Repository(
                "datum-ref index is not configured on this repository".into(),
            ));
        };
        let edges = store
            .query(params.selector.into_query())
            .await
            .map_err(repository)?;
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        // Both ends of an edge are resolved at the kinds the caller reaches them at, so an edge
        // is shown only for datums of those kinds on each end. The selector's own document is
        // already resolved above, so only the other ends are looked up.
        let candidates: BTreeSet<_> = edges
            .iter()
            .flat_map(|edge| [&edge.origin.doc_id, &edge.target.doc_id])
            .filter(|doc| *doc != selector_doc)
            .cloned()
            .collect();
        let mut reached = if candidates.is_empty() {
            BTreeMap::new()
        } else {
            match &drawer {
                Some(drawers) => {
                    Self::reach_drawers(
                        self.access.as_ref(),
                        context,
                        drawers,
                        &candidates,
                        DrawerAccess::Read,
                    )
                    .await?
                }
                None => {
                    Self::reach(
                        self.access.as_ref(),
                        context,
                        &caller.authority,
                        &candidates,
                        DrawerAccess::Read,
                    )
                    .await?
                }
            }
        };
        reached.insert(selector_doc.clone(), selector_kinds);
        Ok(edges
            .into_iter()
            .filter(|edge| {
                // The origin must be readable. A target the caller does not reach is shown as
                // before, unnarrowed: reading the edge discloses its id and datum key only.
                reached
                    .get(&edge.origin.doc_id)
                    .is_some_and(|kinds| kinds.allows(&edge.origin.datum_key))
                    && reached
                        .get(&edge.target.doc_id)
                        .is_none_or(|kinds| kinds.allows(&edge.target.datum_key))
            })
            .map(QueriedDatumRef::from)
            .collect())
    }
}

impl Handler<QueryDatumRefs> for DocsService {
    async fn call(
        &self,
        request: Envelope<QueryDatumRefs>,
    ) -> Result<ResponseEnvelope<QueriedDatumRefs>, DocsError> {
        let refs = self
            .query_datum_refs(&request.context, request.params)
            .await?;
        Ok(QueriedDatumRefs { refs }.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_datum_refs_wire_shape() {
        let refs = QueryDatumRefs {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: None,
            selector: DatumRefSelector::Target {
                doc_id: DocId::new([3; 32]),
                datum_key: Some(DatumKey::from_text("org.example.note", "main").unwrap()),
            },
        };
        let value = serde_json::to_value(&refs).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "selector": {
                    "by": "target",
                    "docId": DocId::new([3; 32]).to_string(),
                    "datumKey": "org.example.note/main",
                },
            })
        );
        assert_eq!(
            serde_json::from_value::<QueryDatumRefs>(value).unwrap(),
            refs
        );
    }
}
