//! ReadDatums: gated reads at explicit heads.

use super::*;

impl DocsService {
    fn json<D: ReadDoc>(&self, doc: &D, datums: &ObjId, key: &DatumKey) -> Result<Json, String> {
        let handle = self.schema_repo.resolve_for_read(key.tag().as_str());
        let schema = handle.as_ref().map_or(&self.raw, SchemaHandle::compiled);
        hydrate_value(doc, datums, key.to_string().as_str().into(), schema)
            .map_err(|error| error.to_string())
    }

    async fn read(
        &self,
        context: &RequestContext,
        params: ReadDatums,
    ) -> Result<DatumsRead, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        self.authorize(
            context,
            &caller.authority,
            AccessRequest::Read(&params.doc_id),
        )
        .await?;
        if let Some(key) = params
            .datums
            .iter()
            .flatten()
            .find(|key| key.tag().as_str() == DMETA_DATUM_TAG)
        {
            return Err(DocsError::SystemDatum {
                datum_key: key.clone(),
            });
        }
        let document = self.handle(&params.doc_id).await?;
        document
            .with_document(|doc| -> Result<DatumsRead, DocsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                let keys = match &params.datums {
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
                // transaction borrows the document mutably, while `datum_version`
                // reads its changes.
                let verdicts: Vec<_> = verdicts
                    .into_iter()
                    .map(|(key, verdict)| {
                        let version = match &verdict {
                            Verdict::Accepted { datum_heads, .. }
                            | Verdict::Pending { datum_heads, .. } => {
                                datum_version(doc, datum_heads.clone())
                            }
                            Verdict::Rejected { .. } => DatumVersion {
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
                let datums_obj = match tx.get(automerge::ROOT, "datums").map_err(repository)? {
                    Some((Value::Object(ObjType::Map), datums)) => Some(datums),
                    _ => None,
                };
                let present = |key: &DatumKey| {
                    datums_obj.as_ref().filter(|datums| {
                        tx.get(*datums, key.to_string())
                            .is_ok_and(|value| value.is_some())
                    })
                };
                let mut datums = BTreeMap::new();
                for (key, verdict, version) in verdicts {
                    let read = match verdict {
                        Verdict::Accepted { native: None, .. } => DatumRead::Absent { version },
                        Verdict::Accepted { .. } => {
                            let datums = present(&key).expect("an accepted value is present");
                            match self.json(&tx, datums, &key) {
                                Ok(value) => DatumRead::Accepted { value, version },
                                Err(reason) => DatumRead::Undecodable { reason, version },
                            }
                        }
                        Verdict::Pending { reason, .. } => DatumRead::Pending {
                            value: present(&key)
                                .filter(|_| params.optimistic)
                                .and_then(|datums| self.json(&tx, datums, &key).ok()),
                            reason,
                            version,
                        },
                        Verdict::Rejected { reason } => DatumRead::Rejected { reason },
                    };
                    datums.insert(key, read);
                }
                tx.rollback();
                Ok(DatumsRead { heads, datums })
            })
            .await
            .map_err(repository)?
    }
}

impl Handler<ReadDatums> for DocsService {
    async fn call(
        &self,
        request: Envelope<ReadDatums>,
    ) -> Result<ResponseEnvelope<DatumsRead>, DocsError> {
        Ok(self.read(&request.context, request.params).await?.into())
    }
}
