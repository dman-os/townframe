//! ListDocDrawers: the drawers through which the presented authority reaches a document.
//!
//! The caller presents an identity or a scoped drawers handle in the request's `Authorization`
//! metadata (it has no handle on the document yet, which is why it asks). The candidates are the
//! drawers `ancestors` maps the document to, each with the bound that drawer confers; the answer
//! keeps those the authority reaches and the access it gets through each, the same per-drawer
//! computation `doc_level` folds into one maximum.

use super::*;

impl DrawerService {
    async fn list_doc_drawers(
        &self,
        request: &Envelope<ListDocDrawers>,
    ) -> Result<Vec<DocDrawer>, DrawersError> {
        let context = &request.context;
        let core = &self.core;
        let authorization = Authorization::from_metadata(&request.metadata)?;
        let caller = core
            .capabilities
            .source::<DrawerScope>(context, authorization)
            .await
            .map_err(authority_error)?;
        let doc = &request.params.doc_id;
        let mut bounds = core.state.ancestors(state::ScopeTarget::Doc(doc)).await?;
        if bounds.is_empty() {
            // The doc's drawers may have arrived via sync without a local projection row.
            core.sync_discovered(context).await?;
            bounds = core.state.ancestors(state::ScopeTarget::Doc(doc)).await?;
        }
        let mut drawers = Vec::new();
        // `ancestors` is keyed by drawer id, so iterating it yields the deterministic order.
        for (drawer_id, bound) in bounds {
            let access = match &caller.authority {
                DomainAuthority::Scoped(scope) => scope
                    .drawers
                    .get(&drawer_id)
                    .and_then(|held| bound.apply(*held)),
                // Identity access on a drawer is what `Projection::granted` computes over the
                // bounds the doc's drawers confer; offering it this drawer's bound alone yields
                // what the actor reaches through this drawer and no other.
                DomainAuthority::Identity(actor) => core
                    .granted(
                        context,
                        *actor,
                        BTreeMap::from([(drawer_id.clone(), bound)]),
                    )
                    .await?
                    .map(|(access, _)| access),
            };
            if let Some(access) = access {
                drawers.push(DocDrawer { drawer_id, access });
            }
        }
        Ok(drawers)
    }
}

impl Handler<ListDocDrawers> for DrawerService {
    async fn call(
        &self,
        request: Envelope<ListDocDrawers>,
    ) -> Result<ResponseEnvelope<DocDrawers>, DrawersError> {
        let drawers = self.list_doc_drawers(&request).await?;
        Ok(DocDrawers { drawers }.into())
    }
}
