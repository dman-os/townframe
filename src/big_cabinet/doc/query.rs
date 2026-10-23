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

/// Which facet routes a [`QueryFacets`] call selects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "by",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FacetSelector {
    /// Every live facet route in the scope (`drawer_id` is required).
    All,
    /// Every live facet route in `doc_id`.
    Doc { doc_id: DocId },
    /// Every live facet route carrying `tag`.
    Tag { tag: FacetTag },
    /// One exact `(doc_id, facet_key)` route.
    Route { doc_id: DocId, facet_key: FacetKey },
    /// Every live facet route with `facet_key`, across documents.
    Key { facet_key: FacetKey },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFacets {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: FacetSelector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedFacet {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
    pub doc_heads: Vec<ChangeHash>,
    pub version: FacetVersion,
}

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

impl Operation for QueryFacets {
    type Output = Vec<QueriedFacet>;
    type Error = DocsError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.docs.queryFacets")
        .with_schemas("QueriedFacet[]", "DocsError");
}

/// Which reference edges a [`QueryFacetRefs`] call selects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "by",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FacetRefSelector {
    /// Outgoing reference edges from `doc_id` (or from `facet_key` on `doc_id` when given).
    Origin {
        doc_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        facet_key: Option<FacetKey>,
    },
    /// Incoming reference edges pointing at `doc_id` (or at `facet_key` on `doc_id` when given).
    Target {
        doc_id: DocId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        facet_key: Option<FacetKey>,
    },
}

impl FacetRefSelector {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFacetRefs {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DocId>,
    pub selector: FacetRefSelector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueriedFacetRef {
    pub origin_doc_id: DocId,
    pub origin_facet_key: FacetKey,
    pub path: String,
    pub target_doc_id: DocId,
    pub target_facet_key: FacetKey,
    pub ref_heads: Vec<ChangeHash>,
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

impl Operation for QueryFacetRefs {
    type Output = Vec<QueriedFacetRef>;
    type Error = DocsError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.docs.queryFacetRefs")
            .with_schemas("QueriedFacetRef[]", "DocsError");
}

impl DocsService {
    pub(crate) async fn readable_in_drawer(
        &self,
        context: &RequestContext,
        actor: &Actor,
        drawer_id: &DocId,
    ) -> Result<BTreeSet<DocId>, DocsError> {
        self.access
            .readable_in_drawer(context, actor.uuid, drawer_id)
            .await
            .map_err(access_error)?
            .ok_or(DocsError::Denied)
    }

    async fn query_facets(
        &self,
        context: &RequestContext,
        params: QueryFacets,
    ) -> Result<Vec<QueriedFacet>, DocsError> {
        let actor = self.actor(context, params.capability).await?;
        let Some(store) = &self.facet_set else {
            return Err(DocsError::Repository(
                "facet-set index is not configured on this repository".into(),
            ));
        };
        if let Some(drawer_id) = &params.drawer_id {
            let readable = self.readable_in_drawer(context, &actor, drawer_id).await?;
            let records = match &params.selector {
                FacetSelector::All => store.query_docs(&readable, None, None).await,
                FacetSelector::Tag { tag } => store.query_docs(&readable, Some(tag), None).await,
                FacetSelector::Key { facet_key } => {
                    store.query_docs(&readable, None, Some(facet_key)).await
                }
                FacetSelector::Doc { doc_id } => {
                    if !readable.contains(doc_id) {
                        return Ok(Vec::new());
                    }
                    store.query(FacetSetQuery::Doc(doc_id.clone())).await
                }
                FacetSelector::Route { doc_id, facet_key } => {
                    if !readable.contains(doc_id) {
                        return Ok(Vec::new());
                    }
                    store
                        .query(FacetSetQuery::Route(FacetRoute {
                            doc_id: doc_id.clone(),
                            facet_key: facet_key.clone(),
                        }))
                        .await
                }
            }
            .map_err(repository)?;
            return Ok(records.into_iter().map(QueriedFacet::from).collect());
        }

        match params.selector {
            FacetSelector::All => Err(DocsError::Denied),
            FacetSelector::Doc { doc_id } => {
                self.authorize(context, &actor, AccessRequest::Read(&doc_id))
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
                self.authorize(context, &actor, AccessRequest::Read(&doc_id))
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
                self.filter_records(context, &actor, records).await
            }
            FacetSelector::Key { facet_key } => {
                let records = store
                    .query(FacetSetQuery::Key(facet_key))
                    .await
                    .map_err(repository)?;
                self.filter_records(context, &actor, records).await
            }
        }
    }

    async fn filter_records(
        &self,
        context: &RequestContext,
        actor: &Actor,
        records: Vec<FacetRecord>,
    ) -> Result<Vec<QueriedFacet>, DocsError> {
        let mut checks: BTreeMap<DocId, bool> = BTreeMap::new();
        let mut out = Vec::new();
        for record in records {
            let allowed = match checks.get(&record.route.doc_id) {
                Some(allowed) => *allowed,
                None => {
                    let checked = self
                        .access
                        .check(context, actor.uuid, AccessRequest::Read(&record.route.doc_id))
                        .await
                        .map_err(access_error)?;
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
        let actor = self.actor(context, params.capability).await?;
        let Some(store) = &self.facet_ref else {
            return Err(DocsError::Repository(
                "facet-ref index is not configured on this repository".into(),
            ));
        };
        if let Some(drawer_id) = &params.drawer_id {
            let readable = self.readable_in_drawer(context, &actor, drawer_id).await?;
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

        self.authorize(context, &actor, AccessRequest::Read(params.selector.doc_id()))
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
                    let checked = self
                        .access
                        .check(context, actor.uuid, AccessRequest::Read(&edge.origin.doc_id))
                        .await
                        .map_err(access_error)?;
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

    #[test]
    fn query_facets_and_refs_wire_shapes() {
        let query = QueryFacets {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: Some(DocId::new([2; 32])),
            selector: FacetSelector::Tag {
                tag: FacetTag::new("org.example.note").unwrap(),
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
        assert_eq!(serde_json::from_value::<QueryFacets>(value).unwrap(), query);

        let refs = QueryFacetRefs {
            capability: CapId::from_bytes([1; 16]),
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
                "capability": CapId::from_bytes([1; 16]),
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
