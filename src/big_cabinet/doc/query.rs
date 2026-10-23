//! QueryFacets and QueryFacetRefs: authority-filtered queries over the `facet_set` and
//! `facet_ref` projections (ADR 009, ADR 014).
//!
//! When `drawer_id` is given, membership is decided by the drawer policy's verified roster
//! projection (`DocAccess::readable_in_drawer`), never by unverified `facet_set` tag presence:
//! one drawer-level walk with weakest-link attenuation, joined against the index. When
//! `drawer_id` is omitted, each candidate document is checked against `AccessRequest::Read`.

use super::*;
use crate::index::facet_ref::{FacetRefEdge, FacetRefQuery};
use crate::index::facet_set::{FacetRecord, FacetRoute, FacetSetQuery};

impl From<FacetRecord> for QueriedFacet {
    fn from(record: FacetRecord) -> Self {
        Self {
            doc_id: record.route.doc_id,
            facet_key: record.route.facet_key,
            doc_heads: record.doc_heads,
            version: FacetVersion {
                heads: record.facet_heads,
                clock: record.clock,
            },
        }
    }
}

trait FacetRefSelectorQuery {
    fn doc_id(&self) -> &DocId;
    fn into_query(self) -> FacetRefQuery;
}

impl FacetRefSelectorQuery for FacetRefSelector {
    fn doc_id(&self) -> &DocId {
        match self {
            Self::Origin { doc_id, .. } | Self::Target { doc_id, .. } => doc_id,
        }
    }

    fn into_query(self) -> FacetRefQuery {
        match self {
            Self::Origin {
                doc_id,
                facet_key: Some(facet_key),
            } => FacetRefQuery::Origin(FacetRoute { doc_id, facet_key }),
            Self::Origin {
                doc_id,
                facet_key: None,
            } => FacetRefQuery::OriginDoc(doc_id),
            Self::Target {
                doc_id,
                facet_key: Some(facet_key),
            } => FacetRefQuery::Target(FacetRoute { doc_id, facet_key }),
            Self::Target {
                doc_id,
                facet_key: None,
            } => FacetRefQuery::TargetDoc(doc_id),
        }
    }
}

impl From<FacetRefEdge> for QueriedFacetRef {
    fn from(edge: FacetRefEdge) -> Self {
        Self {
            origin_doc_id: edge.origin.doc_id,
            origin_facet_key: edge.origin.facet_key,
            path: edge.path,
            target_doc_id: edge.target.doc_id,
            target_facet_key: edge.target.facet_key,
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
                    .scoped_docs(context, drawer_id, access, None)
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

    async fn query_facets(
        &self,
        context: &RequestContext,
        params: QueryFacets,
    ) -> Result<Vec<QueriedFacet>, DocsError> {
        let caller = self.invocation(context, params.authorization).await?;
        let Some(store) = &self.facet_set else {
            return Err(DocsError::Repository(
                "facet-set index is not configured on this repository".into(),
            ));
        };
        if let Some(drawer_id) = &params.drawer_id {
            let level = match &caller.authority {
                DomainAuthority::Identity(actor) => self
                    .access
                    .drawer_access(context, *actor, drawer_id)
                    .await
                    .map_err(access_error)?,
                DomainAuthority::Scoped(scope) => scope.drawers.get(drawer_id).copied(),
            }
            .filter(|access| *access >= big_cabinet_core::drawer::DrawerAccess::Read)
            .ok_or(DocsError::Denied)?;
            let (tag, key, doc_id) = match &params.selector {
                FacetSelector::All => (None, None, None),
                FacetSelector::Tag { tag } => (Some(tag), None, None),
                FacetSelector::Key { facet_key } => (None, Some(facet_key), None),
                FacetSelector::Doc { doc_id } => (None, None, Some(doc_id)),
                FacetSelector::Route { doc_id, facet_key } => (None, Some(facet_key), Some(doc_id)),
            };
            let records = store
                .query_drawer(drawer_id, level, tag, key, doc_id)
                .await
                .map_err(repository)?;
            return Ok(records.into_iter().map(QueriedFacet::from).collect());
        }

        match params.selector {
            FacetSelector::All => Err(DocsError::Denied),
            FacetSelector::Doc { doc_id } => {
                self.authorize(context, &caller.authority, AccessRequest::Read(&doc_id))
                    .await?;
                Ok(store
                    .query(FacetSetQuery::Doc(doc_id))
                    .await
                    .map_err(repository)?
                    .into_iter()
                    .map(QueriedFacet::from)
                    .collect())
            }
            FacetSelector::Route { doc_id, facet_key } => {
                self.authorize(context, &caller.authority, AccessRequest::Read(&doc_id))
                    .await?;
                Ok(store
                    .query(FacetSetQuery::Route(FacetRoute { doc_id, facet_key }))
                    .await
                    .map_err(repository)?
                    .into_iter()
                    .map(QueriedFacet::from)
                    .collect())
            }
            FacetSelector::Tag { tag } => {
                let records = store
                    .query(FacetSetQuery::Tag(tag))
                    .await
                    .map_err(repository)?;
                self.filter_records(context, &caller.authority, records)
                    .await
            }
            FacetSelector::Key { facet_key } => {
                let records = store
                    .query(FacetSetQuery::Key(facet_key))
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
        records: Vec<FacetRecord>,
    ) -> Result<Vec<QueriedFacet>, DocsError> {
        let mut checks: BTreeMap<DocId, bool> = BTreeMap::new();
        let mut out = Vec::new();
        for record in records {
            let allowed = match checks.get(&record.route.doc_id) {
                Some(allowed) => *allowed,
                None => {
                    let checked = Self::allowed(
                        self.access.as_ref(),
                        context,
                        authority,
                        AccessRequest::Read(&record.route.doc_id),
                    )
                    .await?;
                    checks.insert(record.route.doc_id.clone(), checked);
                    checked
                }
            };
            if allowed {
                out.push(QueriedFacet::from(record));
            }
        }
        Ok(out)
    }

    async fn query_facet_refs(
        &self,
        context: &RequestContext,
        params: QueryFacetRefs,
    ) -> Result<Vec<QueriedFacetRef>, DocsError> {
        let caller = self.invocation(context, params.authorization).await?;
        let Some(store) = &self.facet_ref else {
            return Err(DocsError::Repository(
                "facet-ref index is not configured on this repository".into(),
            ));
        };
        if let Some(drawer_id) = &params.drawer_id {
            let readable = Self::readable_in_drawer(
                self.access.as_ref(),
                context,
                &caller.authority,
                drawer_id,
            )
            .await?;
            if !readable.contains(params.selector.doc_id()) {
                return Ok(Vec::new());
            }
            let edges = store
                .query(params.selector.into_query())
                .await
                .map_err(repository)?;
            return Ok(edges
                .into_iter()
                .filter(|edge| readable.contains(&edge.origin.doc_id))
                .map(QueriedFacetRef::from)
                .collect());
        }

        self.authorize(
            context,
            &caller.authority,
            AccessRequest::Read(params.selector.doc_id()),
        )
        .await?;
        let edges = store
            .query(params.selector.into_query())
            .await
            .map_err(repository)?;
        let mut checks: BTreeMap<DocId, bool> = BTreeMap::new();
        let mut out = Vec::new();
        for edge in edges {
            let allowed = match checks.get(&edge.origin.doc_id) {
                Some(allowed) => *allowed,
                None => {
                    let checked = Self::allowed(
                        self.access.as_ref(),
                        context,
                        &caller.authority,
                        AccessRequest::Read(&edge.origin.doc_id),
                    )
                    .await?;
                    checks.insert(edge.origin.doc_id.clone(), checked);
                    checked
                }
            };
            if allowed {
                out.push(QueriedFacetRef::from(edge));
            }
        }
        Ok(out)
    }
}

impl Handler<QueryFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<QueryFacets>,
    ) -> Result<ResponseEnvelope<Vec<QueriedFacet>>, DocsError> {
        Ok(self
            .query_facets(&request.context, request.params)
            .await?
            .into())
    }
}

impl Handler<QueryFacetRefs> for DocsService {
    async fn call(
        &self,
        request: Envelope<QueryFacetRefs>,
    ) -> Result<ResponseEnvelope<Vec<QueriedFacetRef>>, DocsError> {
        Ok(self
            .query_facet_refs(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::FacetTag;

    #[test]
    fn query_facets_and_refs_wire_shapes() {
        let query = QueryFacets {
            authorization: Authorization::identity(CapId::from_bytes([1; 16])),
            drawer_id: Some(DocId::new([2; 32])),
            selector: FacetSelector::Tag {
                tag: FacetTag::new("org.example.note").unwrap(),
            },
        };
        let value = serde_json::to_value(&query).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "authorization": Authorization::identity(CapId::from_bytes([1; 16])),
                "drawerId": DocId::new([2; 32]).to_string(),
                "selector": {
                    "by": "tag",
                    "tag": "org.example.note",
                },
            })
        );
        assert_eq!(serde_json::from_value::<QueryFacets>(value).unwrap(), query);

        let refs = QueryFacetRefs {
            authorization: Authorization::identity(CapId::from_bytes([1; 16])),
            drawer_id: None,
            selector: FacetRefSelector::Target {
                doc_id: DocId::new([3; 32]),
                facet_key: Some(FacetKey::from_text("org.example.note", "main").unwrap()),
            },
        };
        let value = serde_json::to_value(&refs).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "authorization": Authorization::identity(CapId::from_bytes([1; 16])),
                "selector": {
                    "by": "target",
                    "docId": DocId::new([3; 32]).to_string(),
                    "facetKey": "org.example.note/main",
                },
            })
        );
        assert_eq!(
            serde_json::from_value::<QueryFacetRefs>(value).unwrap(),
            refs
        );
    }
}
