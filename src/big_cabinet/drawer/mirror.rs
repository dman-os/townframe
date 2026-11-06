//! The per-peer partition query of ADR 019 (Boundaries): which drawer parts are worth
//! considering given this node's own mirroring policy.
//!
//! The answer is the node's policy only. It is not an access answer: what a peer may serve is
//! BigRepo's projection of Keyhive access into big_sync parts, and the embedder intersects the two
//! (`big_node`'s default peer policy does). It also certifies no bytes: the same drawer is
//! "worth considering" before, during and after its content arrives.

use super::*;
use big_cabinet_core::drawer::GroupKey;
use std::collections::BTreeSet;

/// What this node's mirroring policy wants fetched from peers. A group names the big_sync part
/// BigRepo projects for it (`big_repo::group_part_id`); a doc is pulled as an object.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Mirrored {
    /// The meta, docs and sensitive groups of every mirrored drawer whose descriptor this node
    /// carries.
    pub groups: BTreeSet<GroupKey>,
    /// The descriptor-bearing doc of every mirrored drawer. It is how a descriptor reaches a node
    /// that only knows the drawer's id: until it arrives the groups above are not known, and for a
    /// self-doc drawer it is the whole content.
    pub docs: BTreeSet<DocId>,
}

impl Drawers {
    /// The parts and docs this node's mirror intent (the known-drawers list's `mirror`
    /// entries) asks peers for. Re-ask when [`Drawers::subscribe_changes`] fires and when a doc in
    /// the answer's `docs` arrives, since the descriptor then names more groups.
    pub async fn mirrored(&self, context: &RequestContext) -> Result<Mirrored, DrawersError> {
        let core = &self.core;
        let mut mirrored = Mirrored::default();
        for (drawer_id, entry) in core.known_entries(context).await? {
            if entry.mirror != Some(true) {
                continue;
            }
            if let Some(record) = core.ensure_drawer(context, &drawer_id).await?
                && let Some(groups) = record.descriptor.groups()
            {
                mirrored.groups.extend([
                    groups.meta_group,
                    groups.docs_group,
                    groups.sensitive_group,
                ]);
            }
            mirrored.docs.insert(drawer_id);
        }
        Ok(mirrored)
    }

    /// Fires when the drawer projection changes (a descriptor is projected, a roster edit lands).
    /// Registry edits go through roster writes and so fire it too.
    pub fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.core.state.subscribe_changes()
    }
}
