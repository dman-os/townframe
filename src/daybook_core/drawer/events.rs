use crate::interlude::*;

use super::DrawerRepo;

use crate::drawer::types::{DocEntry, DrawerEvent};

use daybook_types::doc::{ChangeHashSet, DocId, FacetKey};
use tokio_util::sync::CancellationToken;

// observability support
impl DrawerRepo {
    #[tracing::instrument(skip(self, notif_rx, cancel_token))]
    pub(super) async fn notifs_loop(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<am_utils_rs::repo::BigRepoChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    debug!("cancel token lit");
                    break
                },
                msg = notif_rx.recv() => {
                    match msg {
                        Some(notifs) => notifs,
                        None => break,
                    }
                }
            };

            events.clear();

            for notif in notifs {
                let am_utils_rs::repo::BigRepoChangeNotification::DocChanged {
                    doc_id,
                    patch,
                    heads,
                    origin,
                    ..
                } = notif
                else {
                    continue;
                };
                if doc_id != self.drawer_doc_id {
                    eyre::bail!(
                        "invariant break: drawer listener received change for wrong doc id: expected={} got={}",
                        self.drawer_doc_id,
                        doc_id
                    );
                }
                *self.current_heads.lock().expect(ERROR_MUTEX) = ChangeHashSet(Arc::clone(&heads));
                if let Err(err) = self
                    .events_for_patch(
                        &patch,
                        &heads,
                        &mut events,
                        Some(&origin),
                        Some(self.local_peer_id.as_str()),
                    )
                    .await
                {
                    if cancel_token.is_cancelled() || self.cancel_token.is_cancelled() {
                        return Ok(());
                    }
                    return Err(err);
                }
            }

            if !events.is_empty() {
                // Invalidate caches for updated docs
                for event in &events {
                    match event {
                        DrawerEvent::DocUpdated { id, .. } | DrawerEvent::DocAdded { id, .. } => {
                            self.invalidate_entry_cache(id);
                            self.invalidate_facet_cache_doc(id);
                        }
                        DrawerEvent::DocDeleted { id, .. } => {
                            self.invalidate_entry_cache(id);
                            self.invalidate_facet_cache_doc(id);
                        }
                        _ => {}
                    }
                }

                self.registry.notify(events.drain(..));
            }
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<DrawerEvent>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let (patches, heads) = self.drawer_am_handle.with_document(|am_doc| {
            let heads = to.unwrap_or_else(|| ChangeHashSet(am_doc.get_heads().into()));
            let patches = am_doc.diff_obj(&automerge::ROOT, &from, &heads, true)?;
            eyre::Ok((patches, heads))
        })?;

        let mut events = vec![];
        for patch in patches {
            // Replay path: do not apply live-origin filtering.
            self.events_for_patch(&patch, &heads.0, &mut events, None, None)
                .await?;
        }
        Ok(events)
    }

    pub async fn events_for_init(&self) -> Res<Vec<DrawerEvent>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let (drawer_heads, entries) = self.current_drawer_entries()?;

        {
            let mut pool = self.entry_pool.lock().unwrap();
            for (doc_id, entry) in &entries {
                let pruned = pool.insert_key(doc_id, 1);
                for pkey in pruned {
                    self.entry_cache.remove(&pkey);
                }
                self.entry_cache.insert(doc_id.clone(), entry.clone());
            }
        }

        // Init snapshot is synthesized from current drawer + branch heads.
        let mut events = Vec::with_capacity(entries.len() + 1);
        for (id, _entry) in entries {
            events.push(DrawerEvent::DocAdded {
                id: id.clone(),
                entry: self
                    .current_doc_branches(&id)
                    .await?
                    .ok_or_eyre("current branch state missing during drawer init")?,
                drawer_heads: drawer_heads.clone(),
                origin: self.local_origin(),
            });
        }
        events.push(DrawerEvent::ListChanged {
            drawer_heads,
            origin: self.local_origin(),
        });
        Ok(events)
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<DrawerEvent>,
        live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
        exclude_peer_id: Option<&str>,
    ) -> Res<()> {
        // Live notification path: local writes are emitted by mutators.
        // Replay/diff paths pass `live_origin = None`.
        if crate::repos::should_skip_live_patch(live_origin, exclude_peer_id) {
            return Ok(());
        }
        // Prefix: docs.map
        if !am_utils_rs::repo::big_repo_path_prefix_matches(
            &["docs".into(), "map".into()],
            &patch.path,
        ) {
            return Ok(());
        }

        match &patch.action {
            automerge::PatchAction::PutMap {
                key,
                value: (val, _),
                ..
            } if patch.path.len() == 3 && key == "vtag" => {
                let vtag = match val {
                    automerge::Value::Scalar(scalar) => match &**scalar {
                        automerge::ScalarValue::Bytes(bytes) => bytes,
                        _ => return Ok(()),
                    },
                    _ => return Ok(()),
                };
                let vtag = VersionTag::hydrate_bytes(vtag)?;
                let event_origin = crate::repos::resolve_origin_from_vtag_actor(
                    &self.local_actor_id,
                    &vtag.actor_id,
                    live_origin,
                );
                // docs.map.<doc_id>.version changed
                let Some((_obj, automerge::Prop::Map(doc_id_str))) = patch.path.get(2) else {
                    return Ok(());
                };
                let doc_id = DocId::from(doc_id_str.clone());

                // Hydrate the entry at patch heads.
                let path = vec![
                    "docs".into(),
                    "map".into(),
                    autosurgeon::Prop::Key(doc_id.to_string().into()),
                ];
                let (new_entry, drawer_heads) = self
                    .big_repo
                    .hydrate_path_at_heads::<DocEntry>(
                        &self.drawer_doc_id,
                        patch_heads,
                        automerge::ROOT,
                        path,
                    )
                    .await?
                    .ok_or_else(|| {
                        ferr!(
                            "drawer entry missing while handling vtag patch: doc_id={} patch_path={:?} patch_heads_len={}",
                            doc_id,
                            patch.path,
                            patch_heads.len()
                        )
                    })?;
                let drawer_heads = ChangeHashSet(drawer_heads);

                if new_entry.previous_version_heads.is_none() {
                    let entry = self
                        .current_doc_branches(&doc_id)
                        .await?
                        .ok_or_eyre("drawer doc added but branch state missing")?;
                    out.push(DrawerEvent::DocAdded {
                        id: doc_id,
                        entry,
                        drawer_heads: drawer_heads.clone(),
                        origin: event_origin.clone(),
                    });
                    out.push(DrawerEvent::ListChanged {
                        drawer_heads,
                        origin: event_origin,
                    });
                } else {
                    let previous_heads = new_entry
                        .previous_version_heads
                        .as_ref()
                        .ok_or_eyre("doc update missing previous_version_heads")?;
                    let old_entry = self
                        .get_entry_at_heads(&doc_id, previous_heads)
                        .await?
                        .ok_or_eyre(
                            "doc update previous entry not found at previous_version_heads",
                        )?;
                    if old_entry.branches != new_entry.branches {
                        out.push(DrawerEvent::ListChanged {
                            drawer_heads,
                            origin: event_origin,
                        });
                    }
                }
            }
            automerge::PatchAction::DeleteMap { key, .. } if patch.path.len() == 2 => {
                // docs.map.<doc_id> deleted
                let doc_id = DocId::from(key.clone());
                let drawer_heads = ChangeHashSet(Arc::clone(patch_heads));
                // Delete patches have no vtag; use docs.map_deleted actor evidence when replaying.
                let tombstone = self
                    .latest_doc_delete_tombstone(&doc_id, patch_heads)
                    .await?;
                let event_origin = crate::repos::resolve_origin_for_delete(
                    &self.local_actor_id,
                    live_origin,
                    tombstone.as_ref().map(|record| &record.vtag.actor_id),
                );
                let mut deleted_facet_keys_set = HashSet::new();
                if let Some(tombstone) = &tombstone {
                    for snapshot in tombstone.branches.values() {
                        deleted_facet_keys_set.extend(
                            self.facet_keys_at_branch_snapshot(&doc_id, snapshot)
                                .await?,
                        );
                    }
                }
                let mut deleted_facet_keys: Vec<FacetKey> =
                    deleted_facet_keys_set.into_iter().collect();
                deleted_facet_keys.sort();

                // We don't have the entry anymore in the current heads,
                // but V1 includes a placeholder entry.
                out.push(DrawerEvent::DocDeleted {
                    id: doc_id,
                    drawer_heads: drawer_heads.clone(),
                    deleted_facet_keys,
                    entry: None,
                    origin: event_origin.clone(),
                });
                out.push(DrawerEvent::ListChanged {
                    drawer_heads,
                    origin: event_origin,
                });
            }
            _ => {}
        }
        Ok(())
    }
}
