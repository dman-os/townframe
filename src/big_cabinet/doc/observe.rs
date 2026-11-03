//! `PollDatums`: one long poll over the datum-set frontier that selects documents with a
//! predicate and delivers the selected datums of the ones the caller may read (ADR 025).
//!
//! The frontier is the cursor, so replay from `after = 0` yields the current state and a
//! later poll yields only what changed after `through`. The predicate is re-evaluated for the
//! documents a page names, in SQL, so nothing a clause can select needs a document hydrated.
//! Every event is stamped with the revision of the page it came from; the server keeps no
//! state between polls.

use super::*;
use crate::drawer::entry_target;
use crate::index::datum_set::DatumMembership;
use crate::index::datum_set::DatumRoute;
use crate::index::datum_set::DatumSetStore;
use big_cabinet_core::DatumTag;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::DrawerChildEntry;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::drawer::DrawerMetaDoc;
use big_cabinet_core::drawer::DrawerRoster;
use big_cabinet_core::query::Anchor;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use std::num::NonZeroUsize;
use std::time::Duration;

/// The page size a request that names none asks for.
const DEFAULT_PAGE: u32 = 256;

/// The longest a poll holds an otherwise empty page open. ADR 023: the hold is pacing, so a
/// caller that is revoked does not wait out a hold longer than this before re-authorizing.
const MAX_HOLD: Duration = Duration::from_secs(15);

/// The tags the drawer projection is fed by (ADR 024). A page carries them on the drawer doc, so
/// a roster edit names the drawer, not the documents whose membership it changed.
const ROSTER_TAGS: [&str; 5] = [
    Drawer::ID,
    DrawerRoster::ID,
    DrawerDocEntry::ID,
    DrawerChildEntry::ID,
    DrawerMetaDoc::ID,
];

impl Handler<PollDatums> for DocsService {
    async fn call(
        &self,
        request: Envelope<PollDatums>,
    ) -> Result<ResponseEnvelope<DatumPage>, DocsError> {
        let params = request.params;
        let caller = self.invocation(&request.context, params.capability).await?;
        let anchors = params.predicate.anchors().map_err(|_| DocsError::Denied)?;
        // An unanchored predicate is a node-wide request; a scoped caller may not make one.
        if anchors.is_empty() && matches!(caller.authority, DomainAuthority::Scoped(_)) {
            return Err(DocsError::Denied);
        }
        for anchor in &anchors {
            match anchor {
                Anchor::Doc { doc_id } => {
                    self.authorize(
                        &request.context,
                        &caller.authority,
                        AccessRequest::Read(doc_id),
                    )
                    .await?;
                }
                Anchor::Drawer {
                    drawer_id,
                    at_least,
                } => {
                    self.authorize_drawer(
                        &request.context,
                        &caller.authority,
                        drawer_id,
                        *at_least,
                    )
                    .await?;
                }
            }
        }
        let Some(store) = self.datum_set.as_ref() else {
            return Err(DocsError::Repository(
                "datum polling is not configured on this repository".into(),
            ));
        };
        // A held poll keeps a named document in sync for its duration; the grace on a dropped
        // interest is longer than `MAX_HOLD`, so a caller that keeps polling stays synced.
        let mut interests = Vec::new();
        for anchor in &anchors {
            if let Anchor::Doc { doc_id } = anchor {
                interests.push(
                    self.big_repo
                        .interest(doc_id.clone())
                        .await
                        .map_err(repository)?,
                );
            }
        }
        let page = self.poll_datums(params, store).await?;
        drop(interests);
        Ok(page.into())
    }
}

impl DocsService {
    /// Requires the caller to hold at least `at_least` on the anchor drawer. The reach SQL in
    /// the predicate attenuates documents by this same level, so the check is the authorization
    /// and not a post-filter.
    async fn authorize_drawer(
        &self,
        context: &RequestContext,
        authority: &DomainAuthority<DocScope>,
        drawer_id: &DocId,
        at_least: big_cabinet_core::drawer::DrawerAccess,
    ) -> Result<(), DocsError> {
        match authority {
            DomainAuthority::Scoped(scope) => scope
                .drawers
                .get(drawer_id)
                .copied()
                .filter(|held| *held >= at_least)
                .map(|_| ())
                .ok_or(DocsError::Denied),
            DomainAuthority::Identity(actor) => {
                let readable = self
                    .access
                    .readable_in_drawer(context, *actor, drawer_id)
                    .await
                    .map_err(access_error)?;
                if readable.is_none() {
                    return Err(DocsError::Denied);
                }
                // The predicate's reach CTE is rooted at `at_least`, which a caller can only
                // ask for if it holds that level. `readable_in_drawer` proves `Read`; anything
                // stronger is proved by the drawer policy itself.
                if at_least > big_cabinet_core::drawer::DrawerAccess::Read {
                    return Err(DocsError::Denied);
                }
                Ok(())
            }
        }
    }

    async fn poll_datums(
        &self,
        params: PollDatums,
        store: &DatumSetStore,
    ) -> Result<DatumPage, DocsError> {
        let limit = NonZeroUsize::new(
            usize::try_from(if params.limit == 0 {
                DEFAULT_PAGE
            } else {
                params.limit
            })
            .unwrap_or(usize::MAX),
        )
        .unwrap_or(NonZeroUsize::MIN);
        let selection = &params.datums;
        let all = selection.tags().is_none();
        // The reader opens on the selection's tags, the tags the predicate names, and the roster
        // tags: a roster edit is the only frontier fact that can change membership, so a poll
        // that left those namespaces out would never see a document join or leave.
        let mut namespaces: BTreeSet<DatumTag> = params.predicate.tags();
        if let Some(tags) = selection.tags() {
            namespaces.extend(tags.iter().cloned());
        }
        if let DatumSelection::Of { keys, .. } = selection {
            namespaces.extend(keys.iter().map(|key| key.tag().clone()));
        }
        if !predicate_drawers(&params.predicate).is_empty() {
            namespaces.extend(
                ROSTER_TAGS
                    .iter()
                    .map(|tag| DatumTag::new(*tag).expect("a roster tag is well formed")),
            );
        }
        let mut reader = if all {
            store.open_all(params.after).await
        } else {
            store
                .open_tags(namespaces.into_iter().map(|tag| (tag, params.after)))
                .await
        }
        .map_err(repository)?;

        let limit_reads = RevisionReadLimits { max_entries: limit };
        let mut events = Vec::new();
        let mut through = params.after;
        let mut caught_up = false;
        let mut depth: BTreeMap<DocId, bool> = BTreeMap::new();
        loop {
            match reader.next(limit_reads).await.map_err(repository)? {
                RevisionRead::ReplayComplete { through: boundary } => {
                    through = through.max(boundary);
                    caught_up = true;
                    break;
                }
                RevisionRead::Entries { revision, entries } => {
                    through = through.max(revision);
                    self.page_events(
                        store,
                        &params.predicate,
                        selection,
                        revision,
                        entries,
                        &mut depth,
                        &mut events,
                    )
                    .await?;
                    if events.len() >= limit.get() {
                        break;
                    }
                }
            }
        }
        let hold = Duration::from_millis(u64::from(params.hold_ms)).min(MAX_HOLD);
        if caught_up && events.is_empty() && !hold.is_zero() {
            // A live revision may only touch documents the predicate does not select, so
            // keep reading against the same deadline until there is something to answer.
            let deadline = tokio::time::Instant::now() + hold;
            // The hold expiring with nothing to answer ends the loop as a normal empty page,
            // not an error.
            while let Ok(read) = tokio::time::timeout_at(deadline, reader.next(limit_reads)).await {
                let read = read.map_err(repository)?;
                match read {
                    RevisionRead::Entries { revision, entries } => {
                        through = through.max(revision);
                        self.page_events(
                            store,
                            &params.predicate,
                            selection,
                            revision,
                            entries,
                            &mut depth,
                            &mut events,
                        )
                        .await?;
                        if !events.is_empty() {
                            break;
                        }
                    }
                    RevisionRead::ReplayComplete { .. } => {
                        unreachable!("a frontier reader emits its replay boundary once")
                    }
                }
            }
        }
        Ok(DatumPage {
            events,
            through,
            caught_up,
        })
    }

    /// Turns one revision's frontier entries into `enter`, `datum` and `leave` events.
    ///
    /// A page carries two kinds of fact. A document's own datum entry says that document
    /// changed. A roster entry says a drawer projection changed: the document whose membership
    /// it moved then joins or leaves the predicate without any entry of its own, so the poll
    /// re-evaluates the documents the entry affects. Without that, a caller watching a drawer
    /// would never see an already-settled document added to it, nor see it leave when the
    /// roster withdrew its access.
    ///
    /// Documents are deduplicated: the first event for a document that matches the predicate
    /// delivers its whole selected state as `enter`, and later entries for that document deliver
    /// only the changed datum. A document that no longer matches leaves.
    #[expect(clippy::too_many_arguments, reason = "the page context is one unit")]
    async fn page_events(
        &self,
        store: &DatumSetStore,
        predicate: &Predicate,
        selection: &DatumSelection,
        revision: u64,
        entries: Vec<FrontierEntry<DatumRoute, DatumMembership>>,
        depth: &mut BTreeMap<DocId, bool>,
        events: &mut Vec<DatumEvent>,
    ) -> Result<(), DocsError> {
        if entries.is_empty() {
            return Ok(());
        }
        let drawers = predicate_drawers(predicate);
        let predicate_tags = predicate.tags();
        let mut changed: BTreeMap<DocId, Vec<FrontierEntry<DatumRoute, DatumMembership>>> =
            BTreeMap::new();
        let mut affected: BTreeSet<DocId> = BTreeSet::new();
        for entry in entries {
            if ROSTER_TAGS.contains(&entry.key.datum_key.tag().as_str()) {
                if !drawers.is_empty() {
                    affected.extend(
                        Self::roster_affected(
                            store,
                            &entry.key.doc_id,
                            &entry.key.datum_key,
                            &drawers,
                        )
                        .await?,
                    );
                }
                if !Self::selects(selection, &entry.key.datum_key)
                    && !predicate_tags.contains(entry.key.datum_key.tag())
                {
                    continue;
                }
            }
            changed
                .entry(entry.key.doc_id.clone())
                .or_default()
                .push(entry);
        }
        let own: BTreeSet<DocId> = changed.keys().cloned().collect();
        let candidates: BTreeSet<DocId> = own.union(&affected).cloned().collect();
        let matching = store
            .matching_docs(predicate, &candidates)
            .await
            .map_err(repository)?;
        for (doc_id, entries) in changed {
            if !matching.contains(&doc_id) {
                let known = depth.insert(doc_id.clone(), false);
                if known != Some(false) {
                    events.push(DatumEvent::Leave { revision, doc_id });
                }
                continue;
            }
            let entered = depth.insert(doc_id.clone(), true) == Some(true);
            if !entered {
                // A doc first named by this page delivers its current selected state as
                // `enter`. Removals are absent from that state, so a resumed caller that
                // already knew the doc would otherwise never see them; deliver those from
                // the page's own entries.
                let datums = Self::selected_datums(store, &doc_id, selection).await?;
                events.push(DatumEvent::Enter {
                    revision,
                    doc_id: doc_id.clone(),
                    datums,
                });
            }
            for entry in entries {
                if !Self::selects(selection, &entry.key.datum_key) {
                    continue;
                }
                let Some(membership) = entry.value else {
                    return Err(repository(
                        "datum-set frontier published an entry without membership",
                    ));
                };
                // A present entry already reached the caller inside `enter`; only a removal
                // is new information on a doc whose current state was just delivered.
                if !matches!(membership, DatumMembership::Removed { .. }) {
                    continue;
                }
                let datum = observed(membership);
                events.push(DatumEvent::Datum {
                    revision,
                    doc_id: doc_id.clone(),
                    datum_key: entry.key.datum_key,
                    datum,
                });
            }
        }
        // A membership move names only the drawer, so these documents have no entry in this
        // page to carry the event. The predicate decides both directions: a document that now
        // matches enters with its current selected state, one that stopped matching leaves.
        for doc_id in affected {
            if own.contains(&doc_id) {
                continue;
            }
            if matching.contains(&doc_id) {
                let entered = depth.insert(doc_id.clone(), true) == Some(true);
                if !entered {
                    let datums = Self::selected_datums(store, &doc_id, selection).await?;
                    events.push(DatumEvent::Enter {
                        revision,
                        doc_id,
                        datums,
                    });
                }
            } else {
                let known = depth.insert(doc_id.clone(), false);
                if known != Some(false) {
                    events.push(DatumEvent::Leave { revision, doc_id });
                }
            }
        }
        Ok(())
    }

    /// The documents a roster entry can change membership for.
    ///
    /// A `docEntry` names its document in the key, so adding, narrowing or removing it
    /// re-evaluates exactly that document. A `childEntry` names a drawer whose link moved:
    /// transitively the child's whole reachable set joins or leaves with it, so that set is
    /// re-evaluated even after the link is gone (the child still reaches its own documents);
    /// non-transitively only a self-doc child moves, as one of the drawer's own docs. The
    /// drawer's descriptor, roster and meta datums can change what its layout reaches, so
    /// those re-evaluate the drawer's own reach at the clause's transitivity.
    ///
    /// A roster edit is only re-evaluated when its drawer and the predicate's drawer can see
    /// each other in the containment tree, so an unrelated drawer's edit costs nothing.
    async fn roster_affected(
        store: &DatumSetStore,
        owner: &DocId,
        key: &DatumKey,
        drawers: &[(DocId, bool, DrawerAccess)],
    ) -> Result<BTreeSet<DocId>, DocsError> {
        let target = entry_target(key);
        let owner_ancestors = store.drawer_ancestors(owner).await.map_err(repository)?;
        let mut affected = BTreeSet::new();
        for (drawer_id, transitive, at_least) in drawers {
            let related = owner == drawer_id
                || owner_ancestors.contains(drawer_id)
                || store
                    .drawer_ancestors(drawer_id)
                    .await
                    .map_err(repository)?
                    .contains(owner);
            if !related {
                continue;
            }
            // Non-transitively a drawer reaches only its own entries and its own self-doc
            // children (ADR 026 §6), so a roster move in a descendant drawer cannot move
            // membership here at all. Pulling the descendant's reachable set in anyway
            // would make `page_events` emit `leave` for documents that never entered.
            if !transitive && owner != drawer_id {
                continue;
            }
            match key.tag().as_str() {
                DrawerDocEntry::ID => {
                    if let Some(doc_id) = &target {
                        affected.insert(doc_id.clone());
                    }
                }
                DrawerChildEntry::ID => {
                    if let Some(child) = &target {
                        if *transitive {
                            affected.extend(
                                store
                                    .reachable_docs(child, true, *at_least)
                                    .await
                                    .map_err(repository)?,
                            );
                        } else if store.is_self_doc(child).await.map_err(repository)? {
                            affected.insert(child.clone());
                        }
                    }
                }
                _ => {
                    affected.extend(
                        store
                            .reachable_docs(owner, *transitive, *at_least)
                            .await
                            .map_err(repository)?,
                    );
                }
            }
        }
        Ok(affected)
    }

    /// The caller's current selected datums of one document, keyed by datum key.
    async fn selected_datums(
        store: &DatumSetStore,
        doc_id: &DocId,
        selection: &DatumSelection,
    ) -> Result<BTreeMap<DatumKey, ObservedDatum>, DocsError> {
        let docs = BTreeSet::from([doc_id.clone()]);
        let mut records = Vec::new();
        match selection {
            DatumSelection::All => {
                records.extend(
                    store
                        .query_docs(&docs, None, None)
                        .await
                        .map_err(repository)?,
                );
            }
            DatumSelection::Of { tags, keys } => {
                for tag in tags {
                    records.extend(
                        store
                            .query_docs(&docs, Some(tag), None)
                            .await
                            .map_err(repository)?,
                    );
                }
                for key in keys {
                    records.extend(
                        store
                            .query_docs(&docs, None, Some(key))
                            .await
                            .map_err(repository)?,
                    );
                }
            }
        }
        let mut datums = BTreeMap::new();
        for record in records {
            datums.insert(
                record.route.datum_key,
                ObservedDatum::Present {
                    doc_heads: record.doc_heads,
                    version: DatumVersion {
                        heads: record.datum_heads,
                        clock: record.clock,
                    },
                },
            );
        }
        Ok(datums)
    }

    fn selects(selection: &DatumSelection, key: &DatumKey) -> bool {
        selection.selects(key)
    }
}

fn observed(membership: DatumMembership) -> ObservedDatum {
    match membership {
        DatumMembership::Present {
            doc_heads,
            datum_heads,
            clock,
        } => ObservedDatum::Present {
            doc_heads,
            version: DatumVersion {
                heads: datum_heads,
                clock,
            },
        },
        DatumMembership::Removed {
            doc_heads,
            datum_heads,
            clock,
        } => ObservedDatum::Removed {
            doc_heads,
            version: DatumVersion {
                heads: datum_heads,
                clock,
            },
        },
    }
}

/// Every `InDrawer` clause the predicate names, nested ones included, with the transitivity
/// and level it asks for. A roster change is re-evaluated against this set: a drawer under a `Not` or an `Or`
/// still decides membership, so collecting only the anchors would miss it.
fn predicate_drawers(predicate: &Predicate) -> Vec<(DocId, bool, DrawerAccess)> {
    fn collect(predicate: &Predicate, drawers: &mut Vec<(DocId, bool, DrawerAccess)>) {
        match predicate {
            Predicate::InDrawer {
                drawer_id,
                transitive,
                at_least,
            } => drawers.push((drawer_id.clone(), *transitive, *at_least)),
            Predicate::And { clauses } | Predicate::Or { clauses } => {
                for clause in clauses {
                    collect(clause, drawers);
                }
            }
            Predicate::Not { inner } => collect(inner, drawers),
            Predicate::HasTag { .. }
            | Predicate::HasKey { .. }
            | Predicate::IsDoc { .. }
            | Predicate::HasReferenceTo { .. } => {}
        }
    }
    let mut drawers = Vec::new();
    collect(predicate, &mut drawers);
    drawers
}
