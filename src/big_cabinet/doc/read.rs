//! ReadFacets: gated reads at explicit heads.

use super::*;

impl DocsService {
    fn json<D: ReadDoc>(&self, doc: &D, facets: &ObjId, key: &FacetKey) -> Result<Json, String> {
        let handle = self.schema_repo.resolve_for_read(key.tag().as_str());
        let schema = handle.as_ref().map_or(&self.raw, SchemaHandle::compiled);
        hydrate_value(doc, facets, key.to_string().as_str().into(), schema)
            .map_err(|error| error.to_string())
    }

    async fn read(
        &self,
        context: &RequestContext,
        params: ReadFacets,
    ) -> Result<FacetsRead, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        self.authorize(
            context,
            &caller.authority,
            AccessRequest::Read(&params.doc_id),
        )
        .await?;
        if let Some(key) = params
            .facets
            .iter()
            .flatten()
            .find(|key| key.tag().as_str() == DMETA_FACET_TAG)
        {
            return Err(DocsError::SystemFacet {
                facet_key: key.clone(),
            });
        }
        let document = self.handle(&params.doc_id).await?;
        document
            .with_document(|doc| -> Result<FacetsRead, DocsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                let keys = match &params.facets {
                    Some(keys) => keys.clone(),
                    None => present_keys(doc, &heads)?,
                };
                let verdicts: Vec<_> = {
                    let gate = Gate::new(doc, &heads);
                    keys.into_iter()
                        .map(|key| {
                            let verdict = gate.verify(&key);
                            (key, verdict)
                        })
                        .collect()
                };
                // Versions are computed before opening the read transaction: the
                // transaction borrows the document mutably, while `facet_version`
                // reads its changes.
                let verdicts: Vec<_> = verdicts
                    .into_iter()
                    .map(|(key, verdict)| {
                        let version = match &verdict {
                            Verdict::Accepted { facet_heads, .. }
                            | Verdict::Pending { facet_heads, .. } => {
                                facet_version(doc, facet_heads.clone())
                            }
                            Verdict::Rejected { .. } => FacetVersion {
                                heads: Vec::new(),
                                clock: 0,
                            },
                        };
                        (key, verdict, version)
                    })
                    .collect();
                let tx = doc
                    .transaction_at(PatchLog::null(), &heads)
                    .expect("a null patch log matches any document");
                let facets_obj = match tx.get(automerge::ROOT, "facets").map_err(repository)? {
                    Some((Value::Object(ObjType::Map), facets)) => Some(facets),
                    _ => None,
                };
                let present = |key: &FacetKey| {
                    facets_obj.as_ref().filter(|facets| {
                        tx.get(*facets, key.to_string())
                            .is_ok_and(|value| value.is_some())
                    })
                };
                let mut facets = BTreeMap::new();
                for (key, verdict, version) in verdicts {
                    let read = match verdict {
                        Verdict::Accepted { native: None, .. } => FacetRead::Absent { version },
                        Verdict::Accepted { .. } => {
                            let facets = present(&key).expect("an accepted value is present");
                            match self.json(&tx, facets, &key) {
                                Ok(value) => FacetRead::Accepted { value, version },
                                Err(reason) => FacetRead::Undecodable { reason, version },
                            }
                        }
                        Verdict::Pending { reason, .. } => FacetRead::Pending {
                            value: present(&key)
                                .filter(|_| params.optimistic)
                                .and_then(|facets| self.json(&tx, facets, &key).ok()),
                            reason,
                            version,
                        },
                        Verdict::Rejected { reason } => FacetRead::Rejected { reason },
                    };
                    facets.insert(key, read);
                }
                tx.rollback();
                Ok(FacetsRead { heads, facets })
            })
            .await
            .map_err(repository)?
    }
}

impl Handler<ReadFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<ReadFacets>,
    ) -> Result<ResponseEnvelope<FacetsRead>, DocsError> {
        Ok(self.read(&request.context, request.params).await?.into())
    }
}
