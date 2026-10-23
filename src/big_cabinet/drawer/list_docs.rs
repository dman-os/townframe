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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDocs {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    /// Skip this many entries of the ordered result, for paging.
    #[serde(default)]
    pub offset: u64,
    /// At most this many entries back; unbounded when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDoc {
    pub doc_id: DocId,
    /// The strongest access the drawer confers on the doc to the caller: the entry cap
    /// (and, through children, the child-entry cap and the child's own entry).
    pub access: Access,
    /// The drawer whose roster lists the doc, when it is not the one asked about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<DrawerId>,
}

impl Operation for ListDocs {
    type Output = Vec<ListedDoc>;
    type Error = DrawersError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.drawers.listDocs")
        .with_schemas("ListedDoc[]", "DrawersError");
}

impl Core {
    /// Every doc `drawer_id` confers on `actor` at `Access::Relay` or stronger, paired with the
    /// conferred level and the child drawer whose roster lists it (`None` when listed directly on
    /// `drawer_id`). Checks `effective` once on `drawer_id`, rescans `drawer_id` (and any known
    /// child drawer visited), and walks the projection downward with weakest-link attenuation.
    pub(crate) async fn scoped_docs(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        drawer_id: &DrawerId,
    ) -> Result<BTreeMap<DocId, (Access, Option<DrawerId>)>, DrawersError> {
        let level = if actor == self.actor().uuid {
            Access::Admin
        } else {
            self.effective(context, actor, drawer_id)
                .await?
                .ok_or(DrawersError::Denied)?
        };
        let record = self
            .ensure_drawer(context, drawer_id)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(drawer_id.clone()))?;
        let record = self.rescan(context, &record).await?.0;
        if record.layout_kind() == Some(LayoutKind::SelfDoc) {
            return Ok(if level >= Access::Read {
                BTreeMap::from([(drawer_id.clone(), (level, None))])
            } else {
                BTreeMap::new()
            });
        }
        let mut best: BTreeMap<DocId, (Access, Option<DrawerId>)> = BTreeMap::new();
        // `bound` is the strongest level the caller's grant on the root can confer through
        // the child links walked so far, weakest link along the path, strongest path winning
        // — `effective` walked parents upward; this is the same algebra walked downward.
        let mut frontier = vec![(record.drawer_id.clone(), level, None::<DrawerId>)];
        let mut visited = BTreeMap::from([(drawer_id.clone(), level)]);
        let mut rescanned = std::collections::BTreeSet::from([drawer_id.clone()]);
        while let Some((drawer, bound, via)) = frontier.pop() {
            if rescanned.insert(drawer.clone())
                && let Some(child_record) = self.ensure_drawer(context, &drawer).await?
            {
                self.rescan(context, &child_record).await?;
            }
            for (doc_id, entry) in self.state.docs_of(&drawer).await? {
                let conferred = entry.min(bound);
                if conferred < Access::Relay {
                    continue;
                }
                let better = match best.get(&doc_id) {
                    Some((known, _)) => conferred > *known,
                    None => true,
                };
                if better {
                    best.insert(doc_id, (conferred, via.clone()));
                }
            }
            for (child, link) in self.state.children_of(&drawer).await? {
                let through = link.min(bound);
                if visited.get(&child).is_some_and(|known| *known >= through) {
                    continue;
                }
                visited.insert(child.clone(), through);
                frontier.push((child.clone(), through, Some(child)));
            }
        }
        Ok(best)
    }
}

impl DrawerService {
    async fn list_docs(
        &self,
        context: &RequestContext,
        params: ListDocs,
    ) -> Result<Vec<ListedDoc>, DrawersError> {
        let core = &self.core;
        let actor = core.resolve(context, params.capability).await?;
        let best = core
            .scoped_docs(context, actor.uuid, &params.drawer_id)
            .await?;
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
