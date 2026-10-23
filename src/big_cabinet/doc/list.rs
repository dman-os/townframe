//! ListFacets: the slots present at some heads and their versions, without reading values.

use super::*;

impl DocsService {
    async fn list(
        &self,
        context: &RequestContext,
        params: ListFacets,
    ) -> Result<FacetListing, DocsError> {
        let caller = self.invocation(context, params.authorization).await?;
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
                let facets = present_keys(doc, &heads)?
                    .into_iter()
                    .map(|key| {
                        let version = facet_version(doc, signature_heads(doc, &key, &heads));
                        (key, version)
                    })
                    .collect();
                Ok(FacetListing { heads, facets })
            })
            .await
    }
}

impl Handler<ListFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<ListFacets>,
    ) -> Result<ResponseEnvelope<FacetListing>, DocsError> {
        Ok(self.list(&request.context, request.params).await?.into())
    }
}
