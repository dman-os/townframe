//! QueryDatums and QueryDatumRefs: authority-filtered queries over the `datum_set` and
//! `datum_ref` projections (ADR 009, ADR 019).
//!
//! When `drawer_id` is given, membership is decided by the drawer policy's verified roster
//! projection (`DocAccess::readable_in_drawer`), never by unverified `datum_set` tag presence:
//! one drawer-level walk with weakest-link attenuation, joined against the index. When
//! `drawer_id` is omitted, each candidate document is checked against `AccessRequest::Read`.

use super::*;
use crate::index::datum_ref::DatumRefEdge;
use crate::index::datum_ref::DatumRefQuery;
use crate::index::datum_set::DatumRecord;
use crate::index::datum_set::DatumRoute;
use crate::index::datum_set::DatumSetQuery;

impl From<DatumRecord> for QueriedDatum {
    fn from(record: DatumRecord) -> Self {
        Self {
            doc_id: record.route.doc_id,
            datum_key: record.route.datum_key,
            doc_heads: record.doc_heads,
            version: DatumVersion {
                heads: record.datum_heads,
                clock: record.clock,
            },
        }
    }
}

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
    pub(crate) async fn readable_in_drawer(
        access_policy: &dyn DocAccess,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        drawer_id: &DocId,
    ) -> Result<BTreeSet<DocId>, DocsError> {
        match authority {
            DomainAuthority::Identity(actor) => access_policy
                .readable_in_drawer(context, *actor, drawer_id)
                .await
                .map_err(access_error)?
                .ok_or(DocsError::Denied),
            DomainAuthority::Scoped(scope) => {
                let access = scope
                    .drawers
                    .get(drawer_id)
                    .copied()
                    .filter(|held| *held >= big_cabinet_core::drawer::DrawerAccess::Read)
                    .ok_or(DocsError::Denied)?;
                Ok(access_policy
                    .scoped_docs(
                        context,
                        &BTreeMap::from([(drawer_id.clone(), access)]),
                        None,
                    )
                    .await
                    .map_err(access_error)?
                    .into_iter()
                    .filter_map(|(doc, held)| {
                        (held >= big_cabinet_core::drawer::DrawerAccess::Read).then_some(doc)
                    })
                    .collect())
            }
        }
    }

    async fn query_datums(
        &self,
        context: &RequestContext,
        params: QueryDatums,
    ) -> Result<Vec<QueriedDatum>, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        let Some(store) = &self.datum_set else {
            return Err(DocsError::Repository(
                "datum-set index is not configured on this repository".into(),
            ));
        };
        if let Some(drawer_id) = &params.drawer_id {
            // The policy decides which documents the caller reads through the drawer; the index
            // is only ever asked about that set. Which tables say what a drawer contains is the
            // policy's business, not the index's.
            let mut docs = Self::readable_in_drawer(
                self.access.as_ref(),
                context,
                &caller.authority,
                drawer_id,
            )
            .await?;
            let (tag, key) = match &params.selector {
                DatumSelector::All => (None, None),
                DatumSelector::Tag { tag } => (Some(tag), None),
                DatumSelector::Key { datum_key } => (None, Some(datum_key)),
                DatumSelector::Doc { doc_id } => {
                    docs.retain(|doc| doc == doc_id);
                    (None, None)
                }
                DatumSelector::Route { doc_id, datum_key } => {
                    docs.retain(|doc| doc == doc_id);
                    (None, Some(datum_key))
                }
            };
            let records = store
                .query_docs(&docs, tag, key)
                .await
                .map_err(repository)?;
            return Ok(records.into_iter().map(QueriedDatum::from).collect());
        }

        match params.selector {
            DatumSelector::All => Err(DocsError::Denied),
            DatumSelector::Doc { doc_id } => {
                self.authorize(context, &caller.authority, AccessRequest::Read(&doc_id))
                    .await?;
                Ok(store
                    .query(DatumSetQuery::Doc(doc_id))
                    .await
                    .map_err(repository)?
                    .into_iter()
                    .map(QueriedDatum::from)
                    .collect())
            }
            DatumSelector::Route { doc_id, datum_key } => {
                self.authorize(context, &caller.authority, AccessRequest::Read(&doc_id))
                    .await?;
                Ok(store
                    .query(DatumSetQuery::Route(DatumRoute { doc_id, datum_key }))
                    .await
                    .map_err(repository)?
                    .into_iter()
                    .map(QueriedDatum::from)
                    .collect())
            }
            DatumSelector::Tag { tag } => {
                let records = store
                    .query(DatumSetQuery::Tag(tag))
                    .await
                    .map_err(repository)?;
                self.filter_records(context, &caller.authority, records)
                    .await
            }
            DatumSelector::Key { datum_key } => {
                let records = store
                    .query(DatumSetQuery::Key(datum_key))
                    .await
                    .map_err(repository)?;
                self.filter_records(context, &caller.authority, records)
                    .await
            }
        }
    }

    async fn filter_records(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        records: Vec<DatumRecord>,
    ) -> Result<Vec<QueriedDatum>, DocsError> {
        let candidates = records
            .iter()
            .map(|record| record.route.doc_id.clone())
            .collect();
        let readable = Self::allowed_docs(
            self.access.as_ref(),
            context,
            authority,
            &candidates,
            big_cabinet_core::drawer::DrawerAccess::Read,
        )
        .await?;
        Ok(records
            .into_iter()
            .filter(|record| readable.contains(&record.route.doc_id))
            .map(QueriedDatum::from)
            .collect())
    }

    async fn query_datum_refs(
        &self,
        context: &RequestContext,
        params: QueryDatumRefs,
    ) -> Result<Vec<QueriedDatumRef>, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        use big_cabinet_core::drawer::DrawerAccess;
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
        let selector_doc = params.selector.doc_id();
        match &drawer {
            Some(drawers) => {
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
            None => {
                self.authorize(
                    context,
                    &caller.authority,
                    AccessRequest::Read(selector_doc),
                )
                .await?;
            }
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

impl Handler<QueryDatums> for DocsService {
    async fn call(
        &self,
        request: Envelope<QueryDatums>,
    ) -> Result<ResponseEnvelope<Vec<QueriedDatum>>, DocsError> {
        Ok(self
            .query_datums(&request.context, request.params)
            .await?
            .into())
    }
}

impl Handler<QueryDatumRefs> for DocsService {
    async fn call(
        &self,
        request: Envelope<QueryDatumRefs>,
    ) -> Result<ResponseEnvelope<Vec<QueriedDatumRef>>, DocsError> {
        Ok(self
            .query_datum_refs(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::DatumTag;

    #[test]
    fn query_datums_and_refs_wire_shapes() {
        let query = QueryDatums {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: Some(DocId::new([2; 32])),
            selector: DatumSelector::Tag {
                tag: DatumTag::new("org.example.note").unwrap(),
            },
        };
        let value = serde_json::to_value(&query).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "drawerId": DocId::new([2; 32]).to_string(),
                "selector": {
                    "by": "tag",
                    "tag": "org.example.note",
                },
            })
        );
        assert_eq!(serde_json::from_value::<QueryDatums>(value).unwrap(), query);

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
