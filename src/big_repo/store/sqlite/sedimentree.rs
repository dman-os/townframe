use super::ids::IdCodec;
use super::*;

pub(crate) struct SignedBlobRow {
    pub(crate) signed: Vec<u8>,
    pub(crate) blob: Vec<u8>,
}

/// A replay page covers at most this many rows per part: the backlog a page must be
/// able to describe before it can claim the part is caught up.
#[cfg(test)]
const REPLAY_RAW_BATCH_SIZE: u32 = 256;

impl SqliteBigRepoStore {
    /// Ensure the part row exists (idempotent). Inherent mirror of the
    /// `HostPartStore` method so runtime workers can call it without the
    /// crate-private trait in scope — a part is advertiseable once its row
    /// exists.
    pub(crate) async fn ensure_part(&self, part_id: PartKey) -> Res<()> {
        queries::ensure_part(&self.sql, self.scope().id(), Self::part_blob(part_id)).await?;
        Ok(())
    }

    pub(crate) async fn set_obj_payload_in_tx(
        &self,
        tx: &mut Tx,
        obj_id: ObjKey,
        payload: ObjPayload,
    ) -> Res<Vec<PartEvent>> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id.clone()).await?;
        let old_payload_json = queries::obj_payload_by_ref(tx, obj_ref).await?.flatten();
        queries::set_obj_payload(tx, payload_json, obj_ref).await?;
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|str| !str.is_empty())
            .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let parts =
            queries::live_member_parts(tx, self.scope().id(), obj_ref, EVENT_REMOVED).await?;
        let pending_parts = queries::pending_member_parts(tx, self.scope().id(), obj_ref).await?;
        // Re-writing the payload an object already holds, with its membership already live and
        // no membership transition riding along, is not a change. `next_cursor` reserves the
        // next cursor, so a repeat that got past this point would restamp every live part's
        // `txid`/`latest_cursor` and publish a `Changed` per part. The presence plane's boot
        // announce replays every blob this node holds, so a repeat here turns each restart into
        // a revision of the whole blob plane for bytes that never moved. `add_obj_to_parts`
        // already refuses a repeat for a present membership; the payload write makes the same
        // refusal. The parts-less case keeps its emit: an object with no live membership may
        // still be one the object route has yet to hear about.
        if old_payload_json
            .as_deref()
            .is_some_and(|stored| !stored.is_empty())
            && old_payload == payload
            && pending_parts.is_empty()
            && !parts.is_empty()
        {
            return Ok(Vec::new());
        }
        let cursor = Self::next_cursor(tx).await?;
        // The object's own row carries a change only when no part row carries it. A part is
        // the finer key, and it is the object route's rows that a client pages through, so
        // stamping the object row as well would overwrite the position of the change made
        // while the object had no parts — the change the object route has yet to deliver.
        if parts.is_empty() {
            // Stamp `added_at` on a row that becomes present; preserve it when the row was
            // already present, so a touch is not a new add.
            queries::insert_live_member(
                tx,
                self.scope().id(),
                obj_ref,
                0,
                EVENT_CHANGED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                EVENT_REMOVED,
            )
            .await?;
        }
        let mut changed_part_ids = Vec::new();
        let mut added_events = Vec::new();
        for row in &parts {
            let part_id = Self::part_from_blob(row.part_id.clone());
            changed_part_ids.push(part_id.clone());
            queries::touch_member(
                tx,
                EVENT_CHANGED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                obj_ref,
                row.maybe_part_ref,
            )
            .await?;
            self.core
                .apply_bucket_transition(
                    tx,
                    part_id,
                    obj_id.clone(),
                    cursor,
                    &MemberState::Live(old_payload.clone()),
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            queries::advance_part_cursor(
                tx,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                row.maybe_part_ref,
            )
            .await?;
        }
        for row in pending_parts {
            let part_id = Self::part_from_blob(row.part_id);
            let part_ref = row.part_ref;
            queries::insert_live_member(
                tx,
                self.scope().id(),
                obj_ref,
                part_ref,
                EVENT_CHANGED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                EVENT_REMOVED,
            )
            .await?;
            queries::delete_pending_member(tx, self.scope().id(), obj_ref, part_ref).await?;
            self.core
                .apply_bucket_transition(
                    tx,
                    part_id.clone(),
                    obj_id.clone(),
                    cursor,
                    &MemberState::Absent,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            queries::advance_part_cursor(
                tx,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                part_ref,
            )
            .await?;
            added_events.push(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor,
                part_ids: vec![part_id],
                obj_id: obj_id.clone(),
                payload: payload.clone(),
            }));
        }
        let mut events = added_events;
        events.push(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: changed_part_ids,
            obj_id,
            payload,
        }));
        Ok(events)
    }

    pub(crate) async fn init_subduction_schema(&self) -> Result<(), SqliteBigRepoStoreError> {
        self.backfill_causal_ciphertext_index().await?;
        Ok(())
    }
    /// Reconcile one bounded document batch transactionally.
    ///
    /// The batch writes part-domain state only: each part's `latest_cursor` and the
    /// membership rows those transitions produce. It writes no consumer watermark —
    /// the caller's delta walker commits the "reconciled through" revision itself
    /// when the work this batch produced is acknowledged, so there is one writer of
    /// that fact and it cannot disagree with the effects it names.
    pub(crate) async fn reconcile_group_part_batch(
        &self,
        mutations: &[GroupPartReconciliation],
    ) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let mut transitions = Vec::new();
        let mut transition_event_payloads = HashMap::new();

        for mutation in mutations {
            let obj_ref = self
                .core
                .ensure_obj_ref(&mut tx, mutation.doc.clone())
                .await?;
            let payload_json = queries::obj_payload_by_ref(&tx, obj_ref)
                .await?
                .expect(ERROR_IMPOSSIBLE);
            let payload_json = payload_json.filter(|value| !value.is_empty());
            let event_payload: Option<ObjPayload> = payload_json
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| serde_json::from_str(value).wrap_err(ERROR_JSON))
                .transpose()?;
            queries::coalesce_obj_payload(&tx, payload_json, obj_ref).await?;

            // Rows are per part, so "prior" is the union over the parts this doc
            // currently resides in — the same shape the grant re-emit below compares.
            let prior_agent_ids: HashSet<Vec<u8>> =
                queries::prior_agent_ids(&tx, self.scope().id(), obj_ref)
                    .await?
                    .into_iter()
                    .collect();

            let current_rows =
                queries::current_member_parts(&tx, self.scope().id(), obj_ref).await?;
            let current_parts: HashSet<PartKey> =
                current_rows.into_iter().map(Self::part_from_blob).collect();
            let desired_parts = mutation.desired_group_parts.clone();
            let stale = current_parts
                .intersection(&mutation.managed_group_parts)
                .filter(|part| !desired_parts.contains(part))
                .cloned()
                .collect::<HashSet<_>>();
            let additions = desired_parts.difference(&current_parts).cloned();

            for part_id in stale.into_iter().chain(additions) {
                queries::ensure_part(&tx, self.scope().id(), Self::part_blob(part_id.clone()))
                    .await?;
                let old = self
                    .load_member_state(&mut tx, part_id.clone(), mutation.doc.clone())
                    .await?;
                if desired_parts.contains(&part_id) {
                    let Some(payload) = event_payload.clone() else {
                        // Pending member: the local principal wants the doc in
                        // this part but has no payload yet (fetcher/relay). The
                        // part row must exist anyway — a pending want is pull
                        // access, and the part must be advertiseable
                        // (`summarize_parts` succeeds) so a sync route can be
                        // established and the first pull promotes the member.
                        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
                        queries::insert_pending_member(&tx, self.scope().id(), obj_ref, part_ref)
                            .await?;
                        continue;
                    };
                    transition_event_payloads
                        .insert((part_id.clone(), mutation.doc.clone()), payload.clone());
                    transitions.push((
                        part_id,
                        mutation.doc.clone(),
                        old,
                        MemberState::Live(payload),
                    ));
                } else {
                    let part_ref = self.core.ensure_part_ref(&mut tx, part_id.clone()).await?;
                    queries::delete_pending_member(&tx, self.scope().id(), obj_ref, part_ref)
                        .await?;
                    if !matches!(old, MemberState::Absent) {
                        transitions.push((part_id, mutation.doc.clone(), old, MemberState::Dead));
                    }
                }
            }

            // Replace this doc's access rows — one set per part. Rows come from
            // `part_agents` (per group), never from the doc-level union, so a principal of
            // one group never gains another group's part. Only a part with a derived
            // agent set carries rows this reconciliation owns: the delete below is
            // part-wide, so for any other part it would revoke rows belonging to that
            // part's own access owner — an explicit mirror grant on `/seds`, or a
            // part-level reconciler mirroring a derived part's closure. A part the doc
            // merely happens to occupy (materialized by `add_obj_to_parts`) is exactly
            // such a part, which is why occupancy cannot decide the delete.
            let changed_at =
                i64::try_from(Self::next_cursor(&mut tx).await?).expect(ERROR_IMPOSSIBLE);
            let mut access_parts = current_parts.clone();
            access_parts.extend(desired_parts.iter().cloned());
            for part_id in access_parts {
                let Some(agents) = mutation.part_agents.get(&part_id) else {
                    continue;
                };
                let part_ref = self.core.ensure_part_ref(&mut tx, part_id.clone()).await?;
                queries::delete_part_members(&tx, self.scope().id(), part_ref).await?;
                for (principal, access) in agents.iter() {
                    queries::insert_part_member(
                        &tx,
                        self.scope().id(),
                        part_ref,
                        Self::peer_blob(principal.clone()),
                        encode_access(access),
                        changed_at,
                    )
                    .await?;
                }
            }

            // A principal granted here (absent before, present now) may have
            // missed earlier membership touches that delivery-time policy filtering
            // denied while subscription cursors advanced past them. Re-emit a
            // Live→Live transition for the parts the doc is already live in so
            // the subscriber's existing subscription delivers a fresh event it
            // is now permitted to receive. Every persisted access level grants
            // fetch, so only absence→presence changes deliverability.
            if mutation
                .agents
                .keys()
                .any(|principal| !prior_agent_ids.contains(&Self::peer_blob(principal.clone())))
                && let Some(payload) = event_payload.clone()
            {
                let live_part_rows =
                    queries::live_member_parts(&tx, self.scope().id(), obj_ref, EVENT_REMOVED)
                        .await?;
                for row in live_part_rows {
                    let part_id = Self::part_from_blob(row.part_id);
                    if !desired_parts.contains(&part_id)
                        || transitions
                            .iter()
                            .any(|(part, doc, _, _)| *part == part_id && *doc == mutation.doc)
                    {
                        continue;
                    }
                    let old = self
                        .load_member_state(&mut tx, part_id.clone(), mutation.doc.clone())
                        .await?;
                    transition_event_payloads
                        .insert((part_id.clone(), mutation.doc.clone()), payload.clone());
                    transitions.push((
                        part_id,
                        mutation.doc.clone(),
                        old,
                        MemberState::Live(payload.clone()),
                    ));
                }
            }
        }

        let mut events = Vec::with_capacity(transitions.len());
        for (part_id, doc, old, new) in transitions {
            // Part cursors are scalar high-water marks and pagination
            // resumes with `latest_cursor > cursor`. Sharing one cursor
            // between sibling transitions would let acknowledging either
            // sibling permanently skip the others.
            let cursor = Self::next_cursor(&mut tx).await?;
            let part_ref = self.core.ensure_part_ref(&mut tx, part_id.clone()).await?;
            let obj_ref =
                queries::find_obj_ref(&tx, self.scope().id(), Self::obj_blob(doc.clone()))
                    .await?
                    .expect(ERROR_IMPOSSIBLE);
            match &new {
                MemberState::Live(_) => {
                    // The transition becomes present now (or was already present and is being
                    // re-emitted): only a row that was absent takes the new add cursor.
                    queries::insert_live_member(
                        &tx,
                        self.scope().id(),
                        obj_ref,
                        part_ref,
                        EVENT_CHANGED,
                        i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                        EVENT_REMOVED,
                    )
                    .await?;
                    self.apply_bucket_transition(
                        &mut tx,
                        part_id.clone(),
                        doc.clone(),
                        cursor,
                        &old,
                        &new,
                    )
                    .await?;
                    queries::delete_pending_member(&tx, self.scope().id(), obj_ref, part_ref)
                        .await?;
                    events.push(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor,
                        part_ids: vec![part_id.clone()],
                        obj_id: doc.clone(),
                        payload: transition_event_payloads
                            .get(&(part_id.clone(), doc))
                            .expect("live transition requires payload")
                            .clone(),
                    }));
                }
                MemberState::Dead => {
                    queries::insert_dead_member(
                        &tx,
                        self.scope().id(),
                        obj_ref,
                        part_ref,
                        EVENT_REMOVED,
                        i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                    )
                    .await?;
                    self.apply_bucket_transition(
                        &mut tx,
                        part_id.clone(),
                        doc.clone(),
                        cursor,
                        &old,
                        &new,
                    )
                    .await?;
                    events.push(PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                        cursor,
                        part_id: part_id.clone(),
                        obj_id: doc,
                    }));
                }
                MemberState::Absent => {
                    unreachable!("reconciliation cannot target absent state")
                }
            }
            queries::set_part_cursor(
                &tx,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                part_ref,
            )
            .await?;
        }
        tx.commit().await?;

        if !events.is_empty() {
            self.publish(events).await?;
        }
        Ok(())
    }

    /// Append fully incorporated hashes to the durable incorporation log
    /// inside one transaction.
    ///
    /// Every hash must exist in the arrival log — the reporter only fires
    /// after those rows are committed, so a miss is an invariant break. A
    /// hash admits at most once (`ON CONFLICT DO NOTHING`); re-reports are
    /// no-ops. Admission seqs are monotonic but not gap-free by construction:
    /// gaps are impossible within one scope since each insert allocates
    /// MAX(seq)+1 under the write transaction.
    #[cfg(test)]
    pub(crate) fn fail_next_admission_for_test() {
        FAIL_NEXT_ADMISSION.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    // Current admission-log head (0 when empty).
    // Current event archive watermark (0 when no archive has completed).

    // Advance the archive watermark monotonically.

    // Delete admitted event payloads covered by the archive checkpoint.
    // Run checkpointed WAL maintenance after an archive has advanced.

    // Replay admitted events past `cursor`, oldest first. No error
    // swallowing: consumers must never silently skip incorporations.

    // Return retained event-log rows that lack a durable admission marker.

    pub(crate) fn tree_blob(id: SedimentreeId) -> Vec<u8> {
        IdCodec::tree_blob(id)
    }

    pub(crate) fn obj_id(id: SedimentreeId) -> ObjKey {
        IdCodec::obj_id(id)
    }

    pub(crate) fn commit_blob(id: CommitId) -> Vec<u8> {
        IdCodec::commit_blob(id)
    }

    fn digest_blob<T>(payload: &T) -> Vec<u8>
    where
        T: sedimentree_core::codec::schema::Schema + sedimentree_core::codec::encode::EncodeFields,
    {
        Digest::hash(payload).as_bytes().to_vec()
    }

    pub(crate) fn decode_id(bytes: Vec<u8>) -> Result<[u8; 32], SqliteBigRepoStoreError> {
        IdCodec::decode_id(bytes)
    }

    pub(crate) async fn save_tree(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        queries::save_tree(tx, self.scope().id(), Self::tree_blob(id)).await?;
        Ok(())
    }

    pub(crate) async fn backfill_causal_ciphertext_index(
        &self,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let rows = queries::ciphertext_backfill_rows(&self.sql, self.scope().id()).await?;
        let tx = self.sql.begin_write().await?;
        for row in rows {
            let Ok(encrypted) = crate::encrypted_blob::decode_encrypted_blob(&row.blob) else {
                continue;
            };
            queries::index_causal_ciphertext(
                &tx,
                self.scope().id(),
                row.sedimentree_id,
                row.content_ref,
                row.digest,
                row.kind,
                encrypted.pcs_update_op_hash.raw.as_bytes().to_vec(),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn causal_ciphertexts_by_pcs_update(
        &self,
        sedimentree_id: SedimentreeId,
        pcs_update_hash: &[u8; 32],
    ) -> Result<Vec<Vec<u8>>, SqliteBigRepoStoreError> {
        Ok(queries::causal_ciphertexts_by_pcs_update(
            &self.sql,
            self.scope().id(),
            Self::tree_blob(sedimentree_id),
            pcs_update_hash.to_vec(),
        )
        .await?)
    }

    pub(crate) async fn index_causal_ciphertext(
        &self,
        tx: &mut Tx,
        sedimentree_id: SedimentreeId,
        content_ref: CommitId,
        digest: Vec<u8>,
        kind: i64,
        blob: &[u8],
    ) -> Result<(), SqliteBigRepoStoreError> {
        let Ok(encrypted) = crate::encrypted_blob::decode_encrypted_blob(blob) else {
            return Ok(());
        };
        queries::index_causal_ciphertext(
            tx,
            self.scope().id(),
            Self::tree_blob(sedimentree_id),
            Self::commit_blob(content_ref),
            digest,
            kind,
            encrypted.pcs_update_op_hash.raw.as_bytes().to_vec(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn commit_rows(
        &self,
        id: SedimentreeId,
        commit_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<LooseCommit>, Blob)>, SqliteBigRepoStoreError> {
        let rows = if let Some(commit_id) = commit_id {
            queries::commit_rows_by_id(
                &self.sql,
                self.scope().id(),
                Self::tree_blob(id),
                Self::commit_blob(commit_id),
            )
            .await?
        } else {
            queries::commit_rows(&self.sql, self.scope().id(), Self::tree_blob(id)).await?
        };
        rows.into_iter()
            .map(|row| Ok((Signed::try_decode(&row.signed)?, Blob::new(row.blob))))
            .collect()
    }

    pub(crate) async fn fragment_rows(
        &self,
        id: SedimentreeId,
        head_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<Fragment>, Blob)>, SqliteBigRepoStoreError> {
        let rows = if let Some(head_id) = head_id {
            queries::fragment_rows_by_id(
                &self.sql,
                self.scope().id(),
                Self::tree_blob(id),
                Self::commit_blob(head_id),
            )
            .await?
        } else {
            queries::fragment_rows(&self.sql, self.scope().id(), Self::tree_blob(id)).await?
        };
        rows.into_iter()
            .map(|row| Ok((Signed::try_decode(&row.signed)?, Blob::new(row.blob))))
            .collect()
    }

    /// Raw SQL row insert for a loose commit (plus its causal ciphertext
    /// index). Graph semantics live in the cached projection, not here.
    pub(crate) async fn insert_commit_rows(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let (signed, payload, blob) = verified.into_full_parts();
        let digest = Self::digest_blob(&payload);
        let blob = blob.into_contents();
        self.index_causal_ciphertext(tx, id, payload.head(), digest.clone(), 0, &blob)
            .await?;
        queries::insert_commit(
            tx,
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(payload.head()),
            digest,
            signed.as_bytes().to_vec(),
            blob,
        )
        .await?;
        Ok(())
    }

    /// Raw SQL row insert for a fragment (plus its causal ciphertext index).
    pub(crate) async fn insert_fragment_rows(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let (signed, payload, blob) = verified.into_full_parts();
        let digest = Self::digest_blob(&payload);
        let blob = blob.into_contents();
        self.index_causal_ciphertext(tx, id, payload.head(), digest.clone(), 1, &blob)
            .await?;
        queries::insert_fragment(
            tx,
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(payload.head()),
            digest,
            signed.as_bytes().to_vec(),
            blob,
        )
        .await?;
        Ok(())
    }
}

impl SqliteBigRepoStore {
    /// Apply a tree mutation inside an open `BEGIN IMMEDIATE` write
    /// transaction.
    ///
    /// The cached tree is hydrated/adopted, mutated, durably minimized, and
    /// used to derive the canonical heads via `sedimentree_core`. Raw SQL row
    /// mutations, deletion of items covered by minimization, and the BigSync
    /// payload write happen in the same transaction. Returns the events to
    /// publish and a guard that evicts the speculative cache entry unless the
    /// caller commits and disarms it.
    pub(crate) async fn mutate_tree_in_tx(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
        mutation: TreeStorageMutation,
    ) -> Result<(Vec<PartEvent>, TreeCacheGuard<'_>), SqliteBigRepoStoreError> {
        let mut guard = match &mutation {
            TreeStorageMutation::DeleteCommit(_)
            | TreeStorageMutation::DeleteFragment(_)
            | TreeStorageMutation::DeleteAllCommits
            | TreeStorageMutation::DeleteAllFragments => {
                // Deletes: evict the existing entry — a minimized tree may
                // have discarded metadata that becomes relevant after
                // removal — and rebuild from the remaining durable rows.
                self.tree_cache.lock().expect(ERROR_MUTEX).remove(&id);
                TreeCacheGuard::arm(&self.tree_cache, id, 0)
            }
            TreeStorageMutation::InsertCommit(_)
            | TreeStorageMutation::InsertFragment(_)
            | TreeStorageMutation::InsertBatch { .. } => {
                // Inserts: adopt only a committed cached entry or hydrate
                // from the transaction's view of durable storage.
                let maybe_epoch = self
                    .tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .adopt_committed(&id);
                let epoch = match maybe_epoch {
                    Some(epoch) => epoch,
                    None => {
                        let tree = self.hydrate_tree_in_tx(tx, id).await?;
                        self.tree_cache
                            .lock()
                            .expect(ERROR_MUTEX)
                            .insert_no_evict(id, tree)
                    }
                };
                TreeCacheGuard::arm(&self.tree_cache, id, epoch)
            }
        };

        guard.epoch = match mutation {
            TreeStorageMutation::InsertCommit(verified) => {
                let payload = verified.payload().clone();
                self.insert_commit_rows(tx, id, verified).await?;
                self.tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .apply_commit(&id, payload)
            }
            TreeStorageMutation::InsertFragment(verified) => {
                let payload = verified.payload().clone();
                self.insert_fragment_rows(tx, id, verified).await?;
                self.tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .apply_fragment(&id, payload)
            }
            TreeStorageMutation::InsertBatch { commits, fragments } => {
                let commit_payloads: Vec<LooseCommit> = commits
                    .iter()
                    .map(|commit| commit.payload().clone())
                    .collect();
                let fragment_payloads: Vec<Fragment> = fragments
                    .iter()
                    .map(|fragment| fragment.payload().clone())
                    .collect();
                for commit in commits {
                    self.insert_commit_rows(tx, id, commit).await?;
                }
                for fragment in fragments {
                    self.insert_fragment_rows(tx, id, fragment).await?;
                }
                self.tree_cache.lock().expect(ERROR_MUTEX).apply_batch(
                    &id,
                    commit_payloads,
                    fragment_payloads,
                )
            }
            TreeStorageMutation::DeleteCommit(commit_id) => {
                self.delete_commit_rows(tx, id, commit_id).await?;
                let tree = self.hydrate_tree_in_tx(tx, id).await?;
                self.tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .insert_no_evict(id, tree)
            }
            TreeStorageMutation::DeleteFragment(head_id) => {
                self.delete_fragment_rows(tx, id, head_id).await?;
                let tree = self.hydrate_tree_in_tx(tx, id).await?;
                self.tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .insert_no_evict(id, tree)
            }
            TreeStorageMutation::DeleteAllCommits => {
                self.delete_all_commit_rows(tx, id).await?;
                let tree = self.hydrate_tree_in_tx(tx, id).await?;
                self.tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .insert_no_evict(id, tree)
            }
            TreeStorageMutation::DeleteAllFragments => {
                self.delete_all_fragment_rows(tx, id).await?;
                let tree = self.hydrate_tree_in_tx(tx, id).await?;
                self.tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .insert_no_evict(id, tree)
            }
        };

        // Durable state and the resident projection must be the same minimal
        // tree. Keeping covered rows in SQLite while hiding them in the cache
        // makes the sync representation depend on cache residency: hydration
        // can rediscover a proof that a received loose commit is covered,
        // while an already-minimal cache cannot. Compute the pruning plan via
        // sedimentree_core (the sole owner of graph semantics), install its
        // canonical tree speculatively, and remove every discarded row in
        // this same transaction.
        let (removed_commits, removed_fragments) = {
            let mut cache = self.tree_cache.lock().expect(ERROR_MUTEX);
            let tree = cache
                .entries
                .get_mut(&id)
                .expect("cached entry present after mutation");
            tree.ensure_minimized_with_delta(&CountLeadingZeroBytes)
        };
        for commit_id in removed_commits {
            self.delete_commit_rows(tx, id, commit_id).await?;
        }
        for head_id in removed_fragments {
            self.delete_fragment_rows(tx, id, head_id).await?;
        }

        // Derive canonical heads from the cached projection — sedimentree_core
        // is the sole implementation of graph semantics.
        let heads = {
            let mut cache = self.tree_cache.lock().expect(ERROR_MUTEX);
            let heads = {
                let tree = cache
                    .entries
                    .get_mut(&id)
                    .expect("cached entry present after mutation");
                tree.heads(&CountLeadingZeroBytes)
            };
            // Update the LRU cost from the post-minimization metadata weight.
            // This may evict the entry itself (an oversized tree is used
            // transiently for this transaction and then left uncached) —
            // heads are already captured, so that is safe.
            cache.update_cost(&id);
            heads
        };
        let payload = serde_json::json!({
            "heads": am_utils_rs::serialize_commit_heads(
                &heads
                    .iter()
                    .map(|head| automerge::ChangeHash(*head.as_bytes()))
                    .collect::<Vec<_>>(),
            ),
        });
        let mut events = self
            .set_obj_payload_in_tx(tx, Self::obj_id(id), payload)
            .await?;
        // The payload of a tree-derived object has just landed, and every content mutation comes
        // through here, so this is where the store learns the sedimentree exists locally. `/seds`
        // is the local index of exactly those, which is why the membership is written here and not
        // by the group-part worker, whose subject is keyhive-derived group membership.
        events.extend(
            self.add_obj_to_parts_in_tx(tx, Self::obj_id(id), vec![crate::seds_part_id()])
                .await?,
        );

        Ok((events, guard))
    }

    /// Hydrate a metadata-only tree from the transaction's view of durable
    /// storage (no blobs). Reads through `tx` so it sees the exact state being
    /// mutated.
    pub(crate) async fn hydrate_tree_in_tx(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
    ) -> Result<MinimizedSedimentree, SqliteBigRepoStoreError> {
        let commits = self.commit_meta_rows_in_tx(tx, id).await?;
        let fragments = self.fragment_meta_rows_in_tx(tx, id).await?;
        Ok(MinimizedSedimentree::new(Sedimentree::new(
            fragments, commits,
        )))
    }

    pub(crate) async fn commit_meta_rows_in_tx(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
    ) -> Result<Vec<LooseCommit>, SqliteBigRepoStoreError> {
        let rows = queries::commit_meta_rows(tx, self.scope().id(), Self::tree_blob(id)).await?;
        rows.into_iter()
            .map(|row| Ok(Signed::<LooseCommit>::try_decode(&row)?.try_decode_trusted_payload()?))
            .collect()
    }

    pub(crate) async fn fragment_meta_rows_in_tx(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
    ) -> Result<Vec<Fragment>, SqliteBigRepoStoreError> {
        let rows = queries::fragment_meta_rows(tx, self.scope().id(), Self::tree_blob(id)).await?;
        rows.into_iter()
            .map(|row| Ok(Signed::<Fragment>::try_decode(&row)?.try_decode_trusted_payload()?))
            .collect()
    }

    /// Raw SQL row delete for a loose commit (plus its causal ciphertext
    /// index). The cached projection is rebuilt by the caller.
    pub(crate) async fn delete_commit_rows(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        queries::delete_causal_ciphertext(
            tx,
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(commit_id),
            0,
        )
        .await?;
        queries::delete_commit(
            tx,
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(commit_id),
        )
        .await?;
        Ok(())
    }

    /// Raw SQL row delete for a fragment (plus its causal ciphertext index).
    pub(crate) async fn delete_fragment_rows(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
        head_id: CommitId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        queries::delete_causal_ciphertext(
            tx,
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(head_id),
            1,
        )
        .await?;
        queries::delete_fragment(
            tx,
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(head_id),
        )
        .await?;
        Ok(())
    }

    /// Raw SQL row delete for all loose commits of a tree.
    pub(crate) async fn delete_all_commit_rows(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        queries::delete_causal_ciphertexts(tx, self.scope().id(), Self::tree_blob(id), 0).await?;
        queries::delete_all_commits(tx, self.scope().id(), Self::tree_blob(id)).await?;
        Ok(())
    }

    /// Raw SQL row delete for all fragments of a tree.
    pub(crate) async fn delete_all_fragment_rows(
        &self,
        tx: &mut Tx,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        queries::delete_causal_ciphertexts(tx, self.scope().id(), Self::tree_blob(id), 1).await?;
        queries::delete_all_fragments(tx, self.scope().id(), Self::tree_blob(id)).await?;
        Ok(())
    }

    pub(crate) async fn commit_meta_rows(
        &self,
        id: SedimentreeId,
    ) -> Result<Vec<LooseCommit>, SqliteBigRepoStoreError> {
        let rows =
            queries::commit_meta_rows(&self.sql, self.scope().id(), Self::tree_blob(id)).await?;
        rows.into_iter()
            .map(|row| Ok(Signed::<LooseCommit>::try_decode(&row)?.try_decode_trusted_payload()?))
            .collect()
    }

    pub(crate) async fn fragment_meta_rows(
        &self,
        id: SedimentreeId,
    ) -> Result<Vec<Fragment>, SqliteBigRepoStoreError> {
        let rows =
            queries::fragment_meta_rows(&self.sql, self.scope().id(), Self::tree_blob(id)).await?;
        rows.into_iter()
            .map(|row| Ok(Signed::<Fragment>::try_decode(&row)?.try_decode_trusted_payload()?))
            .collect()
    }

    /// Durable sedimentree frontier, recomputed from the authoritative SQLite
    /// rows (metadata only, no blobs). The projection cache is write-only and
    /// never consulted on read paths.
    pub(crate) async fn durable_sedimentree_heads(
        &self,
        id: SedimentreeId,
    ) -> Result<Vec<CommitId>, SqliteBigRepoStoreError> {
        let commits = self.commit_meta_rows(id).await?;
        let fragments = self.fragment_meta_rows(id).await?;
        if commits.is_empty() && fragments.is_empty() {
            return Ok(Vec::new());
        }
        let mut tree = MinimizedSedimentree::new(Sedimentree::new(fragments, commits));
        let mut heads = tree.heads(&CountLeadingZeroBytes);
        heads.sort_unstable();
        Ok(heads)
    }
}

impl Storage<Sendable> for SqliteBigRepoStore {
    type Error = SqliteBigRepoStoreError;

    fn save_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            self.save_tree(&mut tx, id).await?;
            // The tree exists in this store now, which is exactly what `/seds` lists: this
            // node's local index of the sedimentrees it has saved. This is the only place that
            // learns a tree appeared, so the membership is written here, in the same
            // transaction. It carries no access rows: what a peer may see through the partition
            // is decided per event by the readability filter.
            let events = self
                .add_obj_to_parts_in_tx(&mut tx, Self::obj_id(id), vec![crate::seds_part_id()])
                .await?;
            tx.commit().await?;
            self.publish(events).await?;
            Ok(())
        })
    }

    fn delete_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            // Whole-tree removal: acquire the SQLite writer slot FIRST, then
            // evict the projection cache entry. Evicting before BEGIN
            // IMMEDIATE would race a concurrent writer that installs its
            // entry after our eviction but before we delete the durable tree,
            // leaving a stale entry for a deleted tree. Once we own the
            // writer slot no other writer can be mid-transaction, so the
            // eviction is safe; leaving the entry evicted on rollback is also
            // safe (the next write rehydrates).
            let mut tx = self.sql.begin_write().await?;
            self.tree_cache.lock().expect(ERROR_MUTEX).remove(&id);
            let tree = Self::tree_blob(id);
            queries::delete_all_commits(&tx, self.scope().id(), tree.clone()).await?;
            queries::delete_all_fragments(&tx, self.scope().id(), tree.clone()).await?;
            queries::delete_tree(&tx, self.scope().id(), tree).await?;
            // The tree is gone, so the sedimentree is no longer one this node holds, and its
            // membership of `/seds` goes with it. Publish after committing, like any other
            // membership change.
            let events = self
                .remove_seds_membership_in_tx(&mut tx, id)
                .await?
                .into_iter()
                .collect();
            tx.commit().await?;
            self.publish(events).await?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(&self) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows = queries::load_all_sedimentree_ids(&self.sql, self.scope().id()).await?;
            rows.into_iter()
                .map(|row| Ok(SedimentreeId::new(Self::decode_id(row)?)))
                .collect()
        })
    }

    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        Sendable::from_future(async move {
            let found =
                queries::contains_sedimentree_id(&self.sql, self.scope().id(), Self::tree_blob(id))
                    .await?;
            Ok(found.is_some())
        })
    }

    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            self.save_tree(&mut tx, id).await?;
            let (events, guard) = self
                .mutate_tree_in_tx(&mut tx, id, TreeStorageMutation::InsertCommit(verified))
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(())
        })
    }

    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows =
                queries::list_commit_ids(&self.sql, self.scope().id(), Self::tree_blob(id)).await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row)?)))
                .collect()
        })
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Sendable::from_future(async move {
            self.commit_rows(id, None)
                .await?
                .into_iter()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .collect()
        })
    }

    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Sendable::from_future(async move { self.commit_meta_rows(id).await })
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Sendable::from_future(async move {
            self.commit_rows(id, Some(commit_id))
                .await?
                .into_iter()
                .next()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .transpose()
        })
    }

    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            let (events, guard) = self
                .mutate_tree_in_tx(&mut tx, id, TreeStorageMutation::DeleteCommit(commit_id))
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(())
        })
    }

    fn delete_loose_commits(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            let (events, guard) = self
                .mutate_tree_in_tx(&mut tx, id, TreeStorageMutation::DeleteAllCommits)
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(())
        })
    }

    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            self.save_tree(&mut tx, id).await?;
            let (events, guard) = self
                .mutate_tree_in_tx(&mut tx, id, TreeStorageMutation::InsertFragment(verified))
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(())
        })
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        head_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Sendable::from_future(async move {
            self.fragment_rows(id, Some(head_id))
                .await?
                .into_iter()
                .next()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .transpose()
        })
    }

    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows =
                queries::list_fragment_ids(&self.sql, self.scope().id(), Self::tree_blob(id))
                    .await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row)?)))
                .collect()
        })
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Sendable::from_future(async move {
            self.fragment_rows(id, None)
                .await?
                .into_iter()
                .map(|(signed, blob)| Ok(VerifiedMeta::try_from_trusted(signed, blob)?))
                .collect()
        })
    }

    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        Sendable::from_future(async move { self.fragment_meta_rows(id).await })
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        head_id: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            let (events, guard) = self
                .mutate_tree_in_tx(&mut tx, id, TreeStorageMutation::DeleteFragment(head_id))
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(())
        })
    }

    fn delete_fragments(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let mut tx = self.sql.begin_write().await?;
            let (events, guard) = self
                .mutate_tree_in_tx(&mut tx, id, TreeStorageMutation::DeleteAllFragments)
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(())
        })
    }

    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> BoxFuture<'_, Result<usize, Self::Error>> {
        Sendable::from_future(async move {
            let count = commits.len() + fragments.len();
            let mut tx = self.sql.begin_write().await?;
            self.save_tree(&mut tx, id).await?;
            if count == 0 {
                tx.commit().await?;
                return Ok(count);
            }
            let (events, guard) = self
                .mutate_tree_in_tx(
                    &mut tx,
                    id,
                    TreeStorageMutation::InsertBatch { commits, fragments },
                )
                .await?;
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(count)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use big_sync_core::rpc::SubscriptionTarget;

    /// A replay page covers at most [`REPLAY_RAW_BATCH_SIZE`] rows *per part*, and each part's
    /// position advances from its own page. A replay that carried one high-water mark across
    /// parts let a part whose page stopped at the highest cursor (a later member) move every
    /// sibling's bound past rows those siblings had not delivered: a catch-up replay silently
    /// lost events. 300 objects in one part (just over one page) plus one later member in a
    /// second part is exactly that shape.
    #[tokio::test]
    async fn replay_advances_each_part_from_its_own_page() -> Res<()> {
        let store = SqliteBigRepoStore::new(
            sqlite_rs::SqliteRuntime::new(Default::default()).ephemeral(&[])?,
            "big-repo-sqlite-replay-per-part-page",
            BuckId::MAX_LEVEL,
        )
        .await?;
        let part = PartKey(ByteKey::new([0xe1; 32]));
        let sibling = PartKey(ByteKey::new([0xe2; 32]));
        let peer = PeerKey(ByteKey::new([0xe3; 32]));
        store.ensure_part(part.clone()).await?;
        store.ensure_part(sibling.clone()).await?;
        for part_id in [part.clone(), sibling.clone()] {
            store
                .set_part_members(
                    part_id,
                    HashMap::from([(peer.clone(), keyhive_core::access::Access::Read)]),
                )
                .await?;
        }

        // One page plus a remainder, so the part's replay needs a second page.
        let backlog: usize = usize::try_from(REPLAY_RAW_BATCH_SIZE).expect(ERROR_IMPOSSIBLE) + 44;
        let backlogged: Vec<ObjKey> = (1u32..=u32::try_from(backlog).expect(ERROR_IMPOSSIBLE))
            .map(|index| {
                let mut bytes = [0u8; 32];
                bytes[..4].copy_from_slice(&index.to_be_bytes());
                ObjKey(ByteKey::new(bytes))
            })
            .collect();
        for obj_id in &backlogged {
            store
                .set_obj_payload(obj_id.clone(), serde_json::json!({ "idx": "backlog" }))
                .await?;
            store
                .add_obj_to_parts(obj_id.clone(), vec![part.clone()])
                .await?;
        }

        // Added after every backlogged object, so this member's cursor is the scope's highest:
        // it is the cursor the shared high-water mark used to carry into the other part.
        let late = ObjKey(ByteKey::new([0x7f; 32]));
        store
            .set_obj_payload(late.clone(), serde_json::json!({ "idx": "late" }))
            .await?;
        store
            .add_obj_to_parts(late.clone(), vec![sibling.clone()])
            .await?;

        let mut reader = store
            .open_revision_reader(SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([
                    SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: 0,
                    },
                    SubscriptionTarget::Part {
                        part_id: sibling.clone(),
                        cursor: 0,
                    },
                ]),
            })
            .await??;

        let mut replayed: HashSet<ObjKey> = HashSet::new();
        let mut saw_late = false;
        while let big_sync_core::revisioned_store::RevisionRead::Entries { entries, .. } = reader
            .next(big_sync_core::revisioned_store::RevisionReadLimits::default())
            .await?
        {
            for event in entries {
                match event {
                    PartEvent::Changed(changed) if changed.part_ids.contains(&part) => {
                        replayed.insert(changed.obj_id);
                    }
                    PartEvent::Changed(changed) if changed.part_ids.contains(&sibling) => {
                        saw_late = true;
                    }
                    _ => {}
                }
            }
        }

        let missing: Vec<&ObjKey> = backlogged
            .iter()
            .filter(|obj_id| !replayed.contains(*obj_id))
            .collect();
        assert!(
            missing.is_empty(),
            "{} of {} objects in the page-truncated part were never replayed (first missing: {:?})",
            missing.len(),
            backlogged.len(),
            missing.first()
        );
        assert!(
            saw_late,
            "the sibling part's member must be replayed as well"
        );
        Ok(())
    }
}
