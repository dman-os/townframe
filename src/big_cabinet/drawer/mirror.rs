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
///
/// Two tiers (ADR 019, ADR 054 section 5). Every *known* drawer contributes its meta group,
/// which carries the descriptor and the roster and never a member's content. Only a *mirrored*
/// drawer also contributes its docs and sensitive groups, and with them member content.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Mirrored {
    /// The meta group of every known drawer whose descriptor this node carries, plus the docs and
    /// sensitive groups of the mirrored ones.
    pub groups: BTreeSet<GroupKey>,
    /// The descriptor-bearing doc of every known drawer whose groups this node does not yet
    /// name: a drawer known only by id, and a self-doc drawer, whose doc is its whole content.
    /// It is how a descriptor reaches a node that only knows the drawer's id. Once the
    /// descriptor names the groups the doc is left out, because the meta group's part streams it.
    pub docs: BTreeSet<DocId>,
}

impl Drawers {
    /// The parts and docs this node's known-drawers list asks peers for: the meta group of every
    /// known drawer, the docs and sensitive groups of the `mirror` entries, and the descriptor
    /// doc of every known drawer whose groups are not yet known. Re-ask when
    /// [`Drawers::subscribe_changes`] fires and when a doc in the answer's `docs` arrives, since
    /// the descriptor then names the groups.
    ///
    /// The descriptor doc is pulled for known drawers that are not mirrored too. Remembering a
    /// drawer is an explicit act (`rememberDrawer`), not a query-driven fetch, so it does not
    /// widen what a read can cause; and the doc is one this node was already granted to learn the
    /// drawer by. Without it a drawer known only by id never learns its meta group, and while the
    /// roster is inline on the descriptor doc (layout `inline`) the roster is that same doc, so a
    /// roster read of an unmirrored drawer would list nothing. Keyhive access still bounds what a
    /// peer serves of either.
    ///
    /// Once the descriptor is known the doc is dropped from the answer: creation grants the
    /// descriptor doc to the drawer's meta group (`create.rs`, `grant_doc_access(.., meta, ..)`),
    /// and BigRepo's group-part worker puts every doc a group contains into that group's part
    /// (`group_ids_containing_document`), so the meta part already streams it. Keeping the doc as
    /// well would be one object subscription per known drawer per peer for bytes the part
    /// carries.
    pub async fn mirrored(&self, context: &RequestContext) -> Result<Mirrored, DrawersError> {
        let core = &self.core;
        let known = core.known_entries(context).await?;
        // One query for every projected record rather than one per known entry: this is re-asked
        // on every projection change for every connected peer, and per-entry reads made it cost
        // a SQLite round trip per known drawer. Only entries the projection does not have yet
        // (known by id, descriptor not arrived) fall through to discovery.
        let mut projected: std::collections::HashMap<DrawerId, DrawerRecord> = core
            .state
            .drawers()
            .await?
            .into_iter()
            .map(|record| (record.drawer_id.clone(), record))
            .collect();
        let mut mirrored = Mirrored::default();
        for (drawer_id, entry) in known {
            let record = match projected.remove(&drawer_id) {
                Some(record) => Some(record),
                None => core.ensure_drawer(context, &drawer_id).await?,
            };
            let groups = record.as_ref().and_then(|record| record.descriptor.groups());
            let Some(groups) = groups else {
                mirrored.docs.insert(drawer_id);
                continue;
            };
            mirrored.groups.insert(groups.meta_group);
            if entry.mirror == Some(true) {
                mirrored
                    .groups
                    .extend([groups.docs_group, groups.sensitive_group]);
            }
        }
        Ok(mirrored)
    }

    /// Fires when the drawer projection changes (a descriptor is projected, a roster edit lands).
    /// Registry edits go through roster writes and so fire it too.
    pub fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.core.state.subscribe_changes()
    }
}
