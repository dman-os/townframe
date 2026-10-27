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
                let heads = resolve(doc, &params.at)?;
                let datums = present_keys(doc, &heads)?
                    .into_iter()
                    .map(|key| {
                        let version = datum_version(doc, signature_heads(doc, &key, &heads));
                        (key, version)
                    })
                    .collect();
                Ok(DatumListing { heads, datums })
            })
            .await
    }
}

impl Handler<ListDatums> for DocsService {
    async fn call(
        &self,
        request: Envelope<ListDatums>,
    ) -> Result<ResponseEnvelope<DatumListing>, DocsError> {
        Ok(self.list(&request.context, request.params).await?.into())
    }
}
