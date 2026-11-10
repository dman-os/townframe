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
                list_datums(doc, &params.at)
            })
            .await
    }
}

/// The slots present at `at` and their versions.
///
/// Explicit-heads reads cost time proportional to the document each, so a historical listing
/// materialises the document at those heads once and reads that plainly; current heads read the
/// live document directly.
pub(super) fn list_datums(doc: &Automerge, at: &At) -> Result<DatumListing, DocsError> {
    let heads = resolve(doc, at)?;
    let historical = match at {
        At::Current => None,
        At::Heads(_) => Some(doc.fork_at(&heads).map_err(repository)?),
    };
    let view = historical.as_ref().unwrap_or(doc);
    let datums = present_keys(doc, &heads)?
        .into_iter()
        .map(|key| {
            let version = datum_version(view, signature_heads(view, &key));
            (key, version)
        })
        .collect();
    Ok(DatumListing { heads, datums })
}

impl Handler<ListDatums> for DocsService {
    async fn call(
        &self,
        request: Envelope<ListDatums>,
    ) -> Result<ResponseEnvelope<DatumListing>, DocsError> {
        Ok(self.list(&request.context, request.params).await?.into())
    }
}
