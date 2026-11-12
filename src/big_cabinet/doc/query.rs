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
use crate::index::datum_ref::DatumRefTarget;
use crate::index::datum_set::DatumRoute;

trait DatumRefSelectorQuery {
    /// The document the selector names, which the caller must be able to read. A key selector
    /// names none: its referrers are filtered by readability of the documents holding them.
    fn doc_id(&self) -> Option<&DocId>;
    fn into_query(self) -> DatumRefQuery;
}

impl DatumRefSelectorQuery for DatumRefSelector {
    fn doc_id(&self) -> Option<&DocId> {
        match self {
            Self::Origin { doc_id, .. } | Self::Target { doc_id, .. } => Some(doc_id),
            Self::Key { .. } => None,
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
            Self::Key { key } => DatumRefQuery::Key(key),
        }
    }
}

impl From<DatumRefEdge> for QueriedDatumRef {
    fn from(edge: DatumRefEdge) -> Self {
        let (target_doc_id, target_datum_key, ref_heads) = match edge.target {
            DatumRefTarget::Route { route, heads } => (Some(route.doc_id), route.datum_key, heads),
            DatumRefTarget::Key(key) => (None, key, Vec::new()),
        };
        Self {
            origin_doc_id: edge.origin.doc_id,
            origin_datum_key: edge.origin.datum_key,
            path: edge.path,
            target_doc_id,
            target_datum_key,
            ref_heads,
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
        if let Some(doc_id) = params.selector.doc_id() {
            DocsError::ensure_doc_id(doc_id)?;
        }
        let drawer = if let Some(drawer_id) = &params.drawer_id {
            let DomainAuthority::Scoped(scope) = &caller.authority else {
                return Err(DocsError::Denied);
            };
            let access = scope
                .drawers
                .get(drawer_id)
                .copied()
                .filter(|held| *held >= DrawerAccess::Read)
                .ok_or(DocsError::Denied)?;
            Some(BTreeMap::from([(drawer_id.clone(), access)]))
        } else {
            None
        };
        match (params.selector.doc_id(), &drawer) {
            (Some(selector_doc), Some(drawers)) => {
                let candidates = BTreeSet::from([selector_doc.clone()]);
                let readable = self
                    .access
                    .scoped_docs(context, drawers, Some(&candidates))
                    .await
                    .map_err(access_error)?;
                if !readable
                    .get(selector_doc)
                    .is_some_and(|held| *held >= DrawerAccess::Read)
                {
                    return Ok(Vec::new());
                }
            }
            (Some(selector_doc), None) => {
                self.authorize(
                    context,
                    &caller.authority,
                    AccessRequest::Read(selector_doc),
                )
                .await?;
            }
            (None, _) => {}
        }
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
        let candidates = edges
            .iter()
            .map(|edge| edge.origin.doc_id.clone())
            .collect();
        let readable: BTreeSet<_> = match drawer {
            Some(drawers) => self
                .access
                .scoped_docs(context, &drawers, Some(&candidates))
                .await
                .map_err(access_error)?
                .into_iter()
                .filter_map(|(doc, held)| (held >= DrawerAccess::Read).then_some(doc))
                .collect(),
            None => {
                Self::allowed_docs(
                    self.access.as_ref(),
                    context,
                    &caller.authority,
                    &candidates,
                    DrawerAccess::Read,
                )
                .await?
            }
        };
        Ok(edges
            .into_iter()
            .filter(|edge| readable.contains(&edge.origin.doc_id))
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
