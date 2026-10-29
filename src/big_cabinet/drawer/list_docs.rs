//! ListDocs: the docs a drawer holds, as the caller may see them.
//!
//! One authorization, then a scoped query: the caller's effective level on the drawer is
//! checked once — Relay suffices, as for `GetDrawer`, because Relay exists to enumerate —
//! and the answer is the roster as the local projection carries it: the drawer's own
//! entries first (a child drawer's entry caps what it confers), then every child reachable
//! by links the level reaches, child access attenuated by the same weakest-link rule the
//! parent traversal applies in reverse. N roster rows, not N actor checks: no entry costs
//! another ledger lookup, because an entry's cap is the drawer grant it stands for.
//!
//! What is deliberately not here: a doc the caller could reach through *another* drawer or
//! through a Keyhive grant that outlives its entry. This is the drawer's own listing — the
//! read-through of a shared roster, filtered by what the drawer grants the caller — and
//! Reach-based doc access remains the policy DocsRepo checks a doc operation against.

use super::*;

impl DrawerService {
    async fn list_docs(
        &self,
        context: &RequestContext,
        params: ListDocs,
    ) -> Result<Vec<ListedDoc>, DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        let level = core
            .level(context, &caller.authority, &params.drawer_id)
            .await?
            .ok_or(DrawersError::Denied)?;
        let best = core.state.scoped_docs(&params.drawer_id, level).await?;
        Ok(best
            .into_iter()
            .skip(params.offset as usize)
            .take(
                params
                    .limit
                    .map(|limit| limit as usize)
                    .unwrap_or(usize::MAX),
            )
            .map(|(doc_id, (access, via))| ListedDoc {
                doc_id,
                access,
                via,
            })
            .collect())
    }
}

impl Handler<ListDocs> for DrawerService {
    async fn call(
        &self,
        request: Envelope<ListDocs>,
    ) -> Result<ResponseEnvelope<Vec<ListedDoc>>, DrawersError> {
        Ok(self
            .list_docs(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_pages_and_a_listing_spells_what_the_drawer_confers() {
        let request = ListDocs {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: DocId::new([2; 32]),
            offset: 2,
            limit: Some(10),
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "drawerId": DocId::new([2; 32]).to_string(),
                "offset": 2,
                "limit": 10,
            })
        );
        assert_eq!(
            serde_json::from_value::<ListDocs>(value).expect("deserialize"),
            request
        );

        let listed = ListedDoc {
            doc_id: DocId::new([3; 32]),
            access: Access::Read,
            via: Some(DocId::new([4; 32])),
        };
        assert_eq!(
            serde_json::to_value(&listed).expect("serialize"),
            serde_json::json!({
                "docId": DocId::new([3; 32]).to_string(),
                "access": "Read",
                "via": DocId::new([4; 32]).to_string(),
            })
        );
    }
}
