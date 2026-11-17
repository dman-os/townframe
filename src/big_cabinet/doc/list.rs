//! ListDatums: the slots present at some heads and their versions, without reading values.

use super::*;

impl DocsService {
    async fn list(
        &self,
        context: &RequestContext,
        params: ListDatums,
    ) -> Result<DatumListing, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        self.authorize(
            context,
            &caller.authority,
            AccessRequest::Read(&params.doc_id),
        )
        .await?;
        let document = self.handle(&params.doc_id).await?;
        document
            .with_document_read(|doc| {
                context.validate()?;
                list_datums(doc, &params.at, params.with_authors)
            })
            .await
    }
}

/// The slots present at `at` and their versions.
///
/// Explicit-heads reads cost time proportional to the document each, so a historical listing
/// materialises the document at those heads once and reads that plainly; current heads read the
/// live document directly.
///
/// `with_authors` additionally runs the gate on every slot to report who wrote its accepted
/// version. That reconstructs claims per slot, which a plain listing never does, so the default
/// path is unchanged; slots the gate does not accept report no authors.
pub(super) fn list_datums(
    doc: &Automerge,
    at: &At,
    with_authors: bool,
) -> Result<DatumListing, DocsError> {
    let heads = resolve(doc, at)?;
    let historical = match at {
        At::Current => None,
        At::Heads(_) => Some(doc.fork_at(&heads).map_err(repository)?),
    };
    let view = historical.as_ref().unwrap_or(doc);
    let keys = present_keys(doc, &heads)?;
    let mut authors = BTreeMap::new();
    if with_authors {
        let gate = Gate::new(doc, &heads);
        for key in &keys {
            if let Verdict::Accepted { authors: found, .. } = gate.verify(key)
                && !found.is_empty()
            {
                authors.insert(key.clone(), found);
            }
        }
    }
    let datums = keys
        .into_iter()
        .map(|key| {
            let version = datum_version(view, signature_heads(view, &key));
            (key, version)
        })
        .collect();
    Ok(DatumListing {
        heads,
        datums,
        authors,
    })
}

impl Handler<ListDatums> for DocsService {
    async fn call(
        &self,
        request: Envelope<ListDatums>,
    ) -> Result<ResponseEnvelope<DatumListing>, DocsError> {
        Ok(self.list(&request.context, request.params).await?.into())
    }
}
