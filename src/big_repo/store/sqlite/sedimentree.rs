use super::ids::IdCodec;
use super::*;

impl SqliteBigRepoStore {
    /// Ensure the part row exists (idempotent). Inherent mirror of the
    /// `HostPartStore` method so runtime workers can call it without the
    /// crate-private trait in scope — a part is advertiseable once its row
    /// exists.
    pub(crate) async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        sqlx::query!(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
            self.scope().id(),
            Self::part_blob(part_id)
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn subscribe_with_policy(
        &self,
        reqs: SubPartsRequest,
        subscriber: Option<PeerId>,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        let part_cursors: HashMap<PartId, CursorIndex> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, cursor } => Some((*part_id, *cursor)),
                SubscriptionTarget::Object { .. } => None,
            })
            .collect();
        let objects: HashSet<ObjId> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id } => Some(*obj_id),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect();
        let parts: HashSet<_> = part_cursors.keys().copied().collect();
        if subscriber.is_some()
            && let Err(err) = self.summarize_parts(parts.clone()).await?
        {
            return Ok(Err(err));
        }

        let (tx, rx) = mpsc::unbounded("SqliteBigRepoStore".into(), "caller".into());
        let sub_id = uuid::Uuid::new_v4();
        if std::env::var("SUBSCRIBE_TRACE").is_ok() {
            eprintln!(
                "SUBSCRIBE parts={:?} cursors={:?} objects={}",
                parts,
                part_cursors,
                objects.len()
            );
        }
        let sub = Arc::new(BigRepoSubscription {
            sender: tx.clone(),
            principal: subscriber,
            pending: PendingSubscription::new(),
        });
        {
            let mut bus = self.bus.write().expect(ERROR_IMPOSSIBLE);
            bus.pending.insert(sub_id);
            bus.subs.insert(sub_id, Arc::clone(&sub));
            bus.parts_by_sub.insert(sub_id, parts.clone());
            for part_id in &parts {
                bus.by_part.entry(*part_id).or_default().insert(sub_id);
            }
            bus.objs_by_sub.insert(sub_id, objects.clone());
            for obj_id in &objects {
                bus.by_obj.entry(*obj_id).or_default().insert(sub_id);
            }
        }

        let store = self.clone();
        tokio::spawn(async move {
            let mut cursor = part_cursors.values().copied().min().unwrap_or_default();
            let mut marker_sent = false;
            let mut object_replay_pending = true;
            loop {
                sub.pending
                    .state
                    .store(SUB_REPLAYING_CLEAN, std::sync::atomic::Ordering::Release);
                let page = store
                    .list_events_with_policy(parts.clone(), cursor, u32::MAX, subscriber.is_some())
                    .await
                    .expect(ERROR_IMPOSSIBLE)
                    .expect(ERROR_IMPOSSIBLE);
                let mut output: Vec<SubEvent> = Vec::new();
                let mut raw_event_count = 0;
                let mut max_cursor = cursor;
                for (part_id, part_page) in page {
                    for event in part_page.events {
                        raw_event_count += 1;
                        let event_cursor = match &event {
                            PartEvent::Changed(inner) => inner.cursor,
                            PartEvent::Added(inner) => inner.cursor,
                            PartEvent::Removed(inner) => inner.cursor,
                        };
                        max_cursor = max_cursor.max(event_cursor);
                        if event_cursor <= part_cursors.get(&part_id).copied().unwrap_or_default() {
                            continue;
                        }
                        let obj_id = match &event {
                            PartEvent::Changed(inner) => inner.obj_id,
                            PartEvent::Added(inner) => inner.obj_id,
                            PartEvent::Removed(inner) => inner.obj_id,
                        };
                        // A policy-check failure must not masquerade as a
                        // denial: this spawned task cannot propagate errors,
                        // so fail loudly instead of silently skipping a
                        // deliverable event.
                        let permitted = matches!(event, PartEvent::Removed(_))
                            || store
                                .is_event_permitted(Some(part_id), obj_id, subscriber)
                                .await
                                .expect(ERROR_IMPOSSIBLE);
                        if !permitted {
                            continue;
                        }
                        match event {
                            PartEvent::Changed(inner) => {
                                if let Some(SubEvent::Changed(existing)) =
                                    output.iter_mut().find(|candidate| {
                                        matches!(
                                            candidate,
                                            SubEvent::Changed(candidate)
                                                if candidate.cursor == inner.cursor
                                                    && candidate.obj_id == inner.obj_id
                                        )
                                    })
                                {
                                    if !existing.part_ids.contains(&part_id) {
                                        existing.part_ids.push(part_id);
                                    }
                                } else {
                                    let mut inner = inner;
                                    inner.part_ids = vec![part_id];
                                    output.push(SubEvent::Changed(inner));
                                }
                            }
                            PartEvent::Added(inner) => output.push(SubEvent::Added(inner)),
                            PartEvent::Removed(inner) => output.push(SubEvent::Removed(inner)),
                        }
                    }
                }
                if object_replay_pending {
                    for obj_id in &objects {
                        let permitted = store
                            .is_event_permitted(None, *obj_id, subscriber)
                            .await
                            .expect(ERROR_IMPOSSIBLE);
                        if permitted
                            && let Some(payload) =
                                store.obj_payload(*obj_id).await.expect(ERROR_IMPOSSIBLE)
                        {
                            output.push(SubEvent::ObjectChanged(
                                big_sync_core::rpc::ObjChangedWithoutPart {
                                    obj_id: *obj_id,
                                    payload,
                                },
                            ));
                        }
                    }
                    object_replay_pending = false;
                }
                if std::env::var("SUBSCRIBE_TRACE").is_ok() {
                    eprintln!(
                        "REPLAY delivered={} cursor={}->{} raw={}",
                        output.len(),
                        cursor,
                        max_cursor,
                        raw_event_count
                    );
                }
                for event in output {
                    if tx.send(event).await.is_err() {
                        store.bus.write().expect(ERROR_IMPOSSIBLE).remove(sub_id);
                        return;
                    }
                }
                cursor = max_cursor;
                if raw_event_count != 0 {
                    continue;
                }
                if !marker_sent {
                    if !sub.pending.begin_finalization() {
                        object_replay_pending = true;
                        continue;
                    }
                    if tx.send(SubEvent::ReplayComplete).await.is_err() {
                        store.bus.write().expect(ERROR_IMPOSSIBLE).remove(sub_id);
                        return;
                    }
                    marker_sent = true;
                    if sub.pending.become_ready() {
                        return;
                    }
                    object_replay_pending = true;
                } else if sub.pending.become_ready() {
                    return;
                } else {
                    object_replay_pending = true;
                }
            }
        });
        Ok(Ok(rx))
    }

    pub(crate) async fn set_obj_payload_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjId,
        payload: ObjPayload,
    ) -> Res<Vec<SubEvent>> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let old_payload_json: Option<String> = sqlx::query_scalar(
            "SELECT payload_json
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope().id())
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut **tx)
        .await?;
        let live_part_ids: Vec<PartId> = sqlx::query_scalar(
            "SELECT part_id FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
        )
        .bind(self.scope().id())
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(Self::part_from_blob)
        .collect();
        let pending_part_ids: Vec<PartId> = sqlx::query_scalar(
            "SELECT part_id FROM big_sync_pending_members
             WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope().id())
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(Self::part_from_blob)
        .collect();
        sqlx::query!(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO UPDATE SET payload_json = excluded.payload_json",
            self.scope().id(),
            Self::obj_blob(obj_id),
            &payload_json
        )
        .execute(&mut **tx)
        .await?;

        if live_part_ids.is_empty() && pending_part_ids.is_empty() {
            return Ok(vec![SubEvent::ObjectChanged(
                big_sync_core::rpc::ObjChangedWithoutPart { obj_id, payload },
            )]);
        }
        assert!(
            live_part_ids.is_empty() || pending_part_ids.is_empty(),
            "readable object cannot retain latent part memberships"
        );
        let cursor = Self::next_cursor(tx).await?;
        if !pending_part_ids.is_empty() {
            let mut events = Vec::with_capacity(pending_part_ids.len());
            for part_id in pending_part_ids {
                let old_state = self.load_member_state(tx, part_id, obj_id).await?;
                assert!(
                    !matches!(old_state, MemberState::Live(_)),
                    "latent membership cannot already be live"
                );
                sqlx::query!(
                    "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                     VALUES (?1, ?2, 0)
                     ON CONFLICT(scope_id, part_id) DO NOTHING",
                    self.scope().id(),
                    Self::part_blob(part_id)
                )
                .execute(&mut **tx)
                .await?;
                sqlx::query!(
                    "INSERT INTO big_sync_members(scope_id, part_id, obj_id, added_at, added_payload_json, changed_at, removed_at, latest_cursor)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?4, NULL, ?4)
                     ON CONFLICT(scope_id, part_id, obj_id) DO UPDATE SET
                        added_at = excluded.added_at,
                        added_payload_json = excluded.added_payload_json,
                        changed_at = excluded.changed_at,
                        removed_at = NULL,
                        latest_cursor = excluded.latest_cursor",
            self.scope().id(),
            Self::part_blob(part_id),
            Self::obj_blob(obj_id),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            &payload_json
        )
                .execute(&mut **tx)
                .await?;
                self.apply_bucket_transition(
                    tx,
                    part_id,
                    obj_id,
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
                sqlx::query!(
                    "UPDATE big_sync_parts SET latest_cursor = ?1
                             WHERE scope_id = ?2 AND part_id = ?3",
                    i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                    self.scope().id(),
                    Self::part_blob(part_id)
                )
                .execute(&mut **tx)
                .await?;
                sqlx::query!(
                    "DELETE FROM big_sync_pending_members
                             WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
                    self.scope().id(),
                    Self::part_blob(part_id),
                    Self::obj_blob(obj_id)
                )
                .execute(&mut **tx)
                .await?;
                events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                    cursor,
                    part_id,
                    obj_id,
                    payload: payload.clone(),
                }));
            }
            return Ok(events);
        }

        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?
            .expect("visible object membership requires an existing payload");
        for part_id in &live_part_ids {
            sqlx::query!(
                "UPDATE big_sync_members
                 SET changed_at = ?1, latest_cursor = ?1
                 WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                Self::part_blob(*part_id),
                Self::obj_blob(obj_id)
            )
            .execute(&mut **tx)
            .await?;
            self.apply_bucket_transition(
                tx,
                *part_id,
                obj_id,
                cursor,
                &MemberState::Live(old_payload.clone()),
                &MemberState::Live(payload.clone()),
            )
            .await?;
            sqlx::query!(
                "UPDATE big_sync_parts SET latest_cursor = ?1
                     WHERE scope_id = ?2 AND part_id = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                Self::part_blob(*part_id)
            )
            .execute(&mut **tx)
            .await?;
        }
        Ok(vec![SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: live_part_ids,
            obj_id,
            payload,
        })])
    }

    pub(crate) fn cursor_reader(&self, name: &str) -> String {
        format!("{name}:{}", self.scope_id)
    }

    fn part_cursor_reader(&self, part_id: PartId) -> String {
        let encoded: String = Self::part_blob(part_id)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("automerge_part:{}:{encoded}", self.scope_id)
    }

    pub(crate) async fn init_subduction_schema(&self) -> Result<(), SqliteBigRepoStoreError> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!(
            "INSERT OR IGNORE INTO cursors(reader, seq)
           VALUES (?1, 0)",
            self.cursor_reader("group_part")
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "INSERT OR IGNORE INTO cursors(reader, seq)
           VALUES (?1, 0)",
            self.cursor_reader("causal_checkpoint")
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.backfill_causal_ciphertext_index().await?;
        Ok(())
    }
    /// Reconcile one bounded document batch transactionally.
    ///
    /// Non-final worker batches leave the durable event cursor untouched so a
    /// crash replays all derived updates safely before the cursor advances.
    pub(crate) async fn reconcile_group_part_batch(
        &self,
        mutations: &[GroupPartReconciliation],
        event_cursor: u64,
        advance_cursor: bool,
    ) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut transitions = Vec::new();
        let mut transition_event_payloads = HashMap::new();
        let mut reconciled_docs = HashMap::new();

        for mutation in mutations {
            let doc_blob = Self::obj_blob(mutation.doc);
            let payload_json: Option<String> = sqlx::query_scalar(
                "SELECT payload_json FROM big_sync_objs
                     WHERE scope_id = ?1 AND obj_id = ?2",
            )
            .bind(self.scope().id())
            .bind(&doc_blob)
            .fetch_optional(&mut *tx)
            .await?;
            let payload_json = payload_json.filter(|value| !value.is_empty());
            let event_payload: Option<ObjPayload> = payload_json
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| serde_json::from_str(value).wrap_err(ERROR_JSON))
                .transpose()?;
            sqlx::query!(
                "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(scope_id, obj_id) DO NOTHING",
                self.scope().id(),
                &doc_blob,
                payload_json.as_deref()
            )
            .execute(&mut *tx)
            .await?;

            let prior_agent_ids: HashSet<Vec<u8>> = sqlx::query_scalar(
                "SELECT principal_id FROM big_sync_syncable
                     WHERE scope_id = ?1 AND obj_id = ?2",
            )
            .bind(self.scope().id())
            .bind(&doc_blob)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
            sqlx::query!(
                "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2",
                self.scope().id(),
                &doc_blob
            )
            .execute(&mut *tx)
            .await?;
            for (principal, access) in &mutation.agents {
                sqlx::query!(
                    "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level)
                     VALUES (?1, ?2, ?3, ?4)",
                    self.scope().id(),
                    &doc_blob,
                    Self::peer_blob(*principal),
                    encode_access(access)
                )
                .execute(&mut *tx)
                .await?;
            }
            reconciled_docs.insert(mutation.doc, mutation.agents.clone());

            let current_rows = sqlx::query(
                "SELECT part_id FROM big_sync_members
                 WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL
                 UNION
                 SELECT part_id FROM big_sync_pending_members
                 WHERE scope_id = ?1 AND obj_id = ?2",
            )
            .bind(self.scope().id())
            .bind(&doc_blob)
            .fetch_all(&mut *tx)
            .await?;
            let current_parts: HashSet<PartId> = current_rows
                .into_iter()
                .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut desired_parts = mutation.desired_group_parts.clone();
            if mutation.desired_global {
                desired_parts.insert(crate::GLOBAL_PART_ID);
            }
            let stale = current_parts
                .intersection(&mutation.managed_group_parts)
                .filter(|part| !desired_parts.contains(part))
                .copied()
                .chain(
                    (!mutation.desired_global && current_parts.contains(&crate::GLOBAL_PART_ID))
                        .then_some(crate::GLOBAL_PART_ID),
                )
                .collect::<HashSet<_>>();
            let additions = desired_parts.difference(&current_parts).copied();

            for part_id in stale.into_iter().chain(additions) {
                sqlx::query!(
                    "INSERT OR IGNORE INTO big_sync_parts(scope_id, part_id, latest_cursor)
                             VALUES (?1, ?2, 0)",
                    self.scope().id(),
                    Self::part_blob(part_id)
                )
                .execute(&mut *tx)
                .await?;
                let old = self
                    .load_member_state(&mut tx, part_id, mutation.doc)
                    .await?;
                if desired_parts.contains(&part_id) {
                    let Some(payload) = event_payload.clone() else {
                        // Pending member: the local principal wants the doc in
                        // this part but has no payload yet (fetcher/relay). The
                        // part row must exist anyway — a pending want is pull
                        // access, and the part must be advertiseable
                        // (`summarize_parts` succeeds) so a sync route can be
                        // established and the first pull promotes the member.
                        sqlx::query!(
                            "INSERT OR IGNORE INTO big_sync_parts(scope_id, part_id, latest_cursor)
                             VALUES (?1, ?2, 0)",
                            self.scope().id(),
                            Self::part_blob(part_id)
                        )
                        .execute(&mut *tx)
                        .await?;
                        sqlx::query!(
                            "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, part_id, obj_id)
                             VALUES (?1, ?2, ?3)",
            self.scope().id(),
            Self::part_blob(part_id),
            &doc_blob
        )
                        .execute(&mut *tx)
                        .await?;
                        continue;
                    };
                    transition_event_payloads.insert((part_id, mutation.doc), payload.clone());
                    transitions.push((part_id, mutation.doc, old, MemberState::Live(payload)));
                } else {
                    sqlx::query!(
                        "DELETE FROM big_sync_pending_members
                                     WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
                        self.scope().id(),
                        Self::part_blob(part_id),
                        &doc_blob
                    )
                    .execute(&mut *tx)
                    .await?;
                    if !matches!(old, MemberState::Absent) {
                        transitions.push((part_id, mutation.doc, old, MemberState::Dead));
                    }
                }
            }

            // A principal granted here (absent before, present now) may have
            // missed earlier Added events that delivery-time policy filtering
            // denied while subscription cursors advanced past them. Re-emit a
            // Live→Live transition for the parts the doc is already live in so
            // the subscriber's existing subscription delivers a fresh event it
            // is now permitted to receive. Every persisted access level grants
            // fetch, so only absence→presence changes deliverability.
            if mutation
                .agents
                .keys()
                .any(|principal| !prior_agent_ids.contains(&Self::peer_blob(*principal)))
                && let Some(payload) = event_payload.clone()
            {
                let live_part_rows = sqlx::query_scalar(
                    "SELECT part_id FROM big_sync_members
                             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
                )
                .bind(self.scope().id())
                .bind(&doc_blob)
                .fetch_all(&mut *tx)
                .await?;
                for part_blob in live_part_rows {
                    let part_id = Self::part_from_blob(part_blob);
                    if !desired_parts.contains(&part_id)
                        || transitions
                            .iter()
                            .any(|(p, d, _, _)| *p == part_id && *d == mutation.doc)
                    {
                        continue;
                    }
                    let old = self
                        .load_member_state(&mut tx, part_id, mutation.doc)
                        .await?;
                    transition_event_payloads.insert((part_id, mutation.doc), payload.clone());
                    transitions.push((
                        part_id,
                        mutation.doc,
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
            sqlx::query!(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                     VALUES (?1, ?2, 0)
                     ON CONFLICT(scope_id, part_id) DO NOTHING",
                self.scope().id(),
                Self::part_blob(part_id)
            )
            .execute(&mut *tx)
            .await?;
            match &new {
                MemberState::Live(_) => {
                    sqlx::query!(
                        "INSERT INTO big_sync_members(
                             scope_id, part_id, obj_id, added_at, added_payload_json,
                             changed_at, removed_at, latest_cursor
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?4, NULL, ?4)
                             ON CONFLICT(scope_id, part_id, obj_id) DO UPDATE SET
                             added_at = excluded.added_at,
                             added_payload_json = excluded.added_payload_json,
                             changed_at = excluded.changed_at, removed_at = NULL,
                             latest_cursor = excluded.latest_cursor",
                        self.scope().id(),
                        Self::part_blob(part_id),
                        Self::obj_blob(doc),
                        i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                        serde_json::to_string(
                            transition_event_payloads
                                .get(&(part_id, doc))
                                .expect("live transition requires payload"),
                        )
                        .expect(ERROR_JSON),
                    )
                    .execute(&mut *tx)
                    .await?;
                    self.apply_bucket_transition(&mut tx, part_id, doc, cursor, &old, &new)
                        .await?;
                    sqlx::query!(
                        "DELETE FROM big_sync_pending_members
                                         WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
                        self.scope().id(),
                        Self::part_blob(part_id),
                        Self::obj_blob(doc)
                    )
                    .execute(&mut *tx)
                    .await?;
                    events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                        cursor,
                        part_id,
                        obj_id: doc,
                        payload: transition_event_payloads
                            .get(&(part_id, doc))
                            .expect("live transition requires payload")
                            .clone(),
                    }));
                }
                MemberState::Dead => {
                    sqlx::query!(
                        "UPDATE big_sync_members
                             SET removed_at = ?1, changed_at = ?1, latest_cursor = ?1
                             WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
                        i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                        self.scope().id(),
                        Self::part_blob(part_id),
                        Self::obj_blob(doc)
                    )
                    .execute(&mut *tx)
                    .await?;
                    self.apply_bucket_transition(&mut tx, part_id, doc, cursor, &old, &new)
                        .await?;
                    events.push(SubEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                        cursor,
                        part_id,
                        obj_id: doc,
                    }));
                }
                MemberState::Absent => {
                    unreachable!("reconciliation cannot target absent state")
                }
            }
            sqlx::query!(
                "UPDATE big_sync_parts SET latest_cursor = ?1
                         WHERE scope_id = ?2 AND part_id = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                Self::part_blob(part_id)
            )
            .execute(&mut *tx)
            .await?;
        }
        if advance_cursor {
            sqlx::query!(
                "UPDATE cursors SET seq = ?1 WHERE reader = ?2",
                i64::try_from(event_cursor).expect(ERROR_IMPOSSIBLE),
                self.cursor_reader("group_part")
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        if !events.is_empty() {
            self.publish(events).await?;
        }
        Ok(())
    }

    pub(crate) async fn keyhive_group_part_cursor(&self) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT seq FROM cursors WHERE reader = ?1",
            self.cursor_reader("group_part")
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    #[allow(dead_code)]

    pub(crate) async fn automerge_part_cursor(&self, part_id: PartId) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT seq FROM cursors WHERE reader = ?1",
            self.part_cursor_reader(part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn commit_automerge_part_cursor(
        &self,
        part_id: PartId,
        cursor: u64,
    ) -> Res<()> {
        sqlx::query!(
            "INSERT INTO cursors(reader, seq) VALUES (?1, ?2)
             ON CONFLICT(reader)
             DO UPDATE SET seq = MAX(seq, excluded.seq)",
            self.part_cursor_reader(part_id),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn automerge_keyhive_cursor(&self) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT seq FROM cursors WHERE reader = ?1",
            self.cursor_reader("automerge_keyhive")
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn commit_automerge_keyhive_cursor(&self, cursor: u64) -> Res<()> {
        sqlx::query!(
            "INSERT INTO cursors(reader, seq) VALUES (?1, ?2)
             ON CONFLICT(reader)
             DO UPDATE SET seq = MAX(seq, excluded.seq)",
            self.cursor_reader("automerge_keyhive"),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
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

    /// Current admission-log head (0 when empty).
    /// Current event archive watermark (0 when no archive has completed).

    /// Advance the archive watermark monotonically.

    /// Delete admitted event payloads covered by the archive checkpoint.
    /// Run checkpointed WAL maintenance after an archive has advanced.

    /// Replay admitted events past `cursor`, oldest first. No error
    /// swallowing: consumers must never silently skip incorporations.

    /// Return retained event-log rows that lack a durable admission marker.

    pub(crate) fn tree_blob(id: SedimentreeId) -> Vec<u8> {
        IdCodec::tree_blob(id)
    }

    pub(crate) fn obj_id(id: SedimentreeId) -> ObjId {
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
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query!(
            "INSERT OR IGNORE INTO big_repo_subduction_trees(scope_id, sedimentree_id)
             VALUES (?1, ?2)",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn backfill_causal_ciphertext_index(
        &self,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let rows = sqlx::query(
            "SELECT sedimentree_id, commit_id AS content_ref, digest, 0 AS kind, blob
             FROM big_repo_subduction_commits WHERE scope_id = ?1
             UNION ALL
             SELECT sedimentree_id, head_id AS content_ref, digest, 1 AS kind, blob
             FROM big_repo_subduction_fragments WHERE scope_id = ?1",
        )
        .bind(self.scope().id())
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        for row in rows {
            let blob: Vec<u8> = row.try_get("blob")?;
            let Ok(encrypted) = crate::encrypted_blob::decode_encrypted_blob(&blob) else {
                continue;
            };
            sqlx::query!(
                "INSERT OR IGNORE INTO big_repo_causal_ciphertext_index(
                    scope_id, sedimentree_id, content_ref, digest, kind, pcs_update_hash
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                self.scope().id(),
                row.try_get::<Vec<u8>, _>("sedimentree_id")?,
                row.try_get::<Vec<u8>, _>("content_ref")?,
                row.try_get::<Vec<u8>, _>("digest")?,
                row.try_get::<i64, _>("kind")?,
                encrypted.pcs_update_op_hash.raw.as_bytes().as_slice()
            )
            .execute(&mut *tx)
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
        let rows = sqlx::query(
            "SELECT commits.blob
             FROM big_repo_causal_ciphertext_index AS causal
             JOIN big_repo_subduction_commits AS commits
               ON commits.scope_id = causal.scope_id
              AND commits.sedimentree_id = causal.sedimentree_id
              AND commits.commit_id = causal.content_ref
              AND commits.digest = causal.digest
             WHERE causal.scope_id = ?1 AND causal.sedimentree_id = ?2
               AND causal.pcs_update_hash = ?3 AND causal.kind = 0
             UNION ALL
             SELECT fragments.blob
             FROM big_repo_causal_ciphertext_index AS causal
             JOIN big_repo_subduction_fragments AS fragments
               ON fragments.scope_id = causal.scope_id
              AND fragments.sedimentree_id = causal.sedimentree_id
              AND fragments.head_id = causal.content_ref
              AND fragments.digest = causal.digest
             WHERE causal.scope_id = ?1 AND causal.sedimentree_id = ?2
               AND causal.pcs_update_hash = ?3 AND causal.kind = 1",
        )
        .bind(self.scope().id())
        .bind(Self::tree_blob(sedimentree_id))
        .bind(pcs_update_hash.as_slice())
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| row.try_get("blob").map_err(Into::into))
            .collect()
    }

    pub(crate) async fn index_causal_ciphertext(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        sedimentree_id: SedimentreeId,
        content_ref: CommitId,
        digest: Vec<u8>,
        kind: i64,
        blob: &[u8],
    ) -> Result<(), SqliteBigRepoStoreError> {
        let Ok(encrypted) = crate::encrypted_blob::decode_encrypted_blob(blob) else {
            return Ok(());
        };
        sqlx::query!(
            "INSERT OR IGNORE INTO big_repo_causal_ciphertext_index(
                scope_id, sedimentree_id, content_ref, digest, kind, pcs_update_hash
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            self.scope().id(),
            Self::tree_blob(sedimentree_id),
            Self::commit_blob(content_ref),
            digest,
            kind,
            encrypted.pcs_update_op_hash.raw.as_bytes().as_slice()
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn commit_rows(
        &self,
        id: SedimentreeId,
        commit_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<LooseCommit>, Blob)>, SqliteBigRepoStoreError> {
        let query = if commit_id.is_some() {
            "SELECT signed, blob FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 AND commit_id = ?3
             ORDER BY digest"
        } else {
            "SELECT signed, blob FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest"
        };
        // Optional filter clauses make this SQL shape dynamic; retain runtime checking.
        let mut request = sqlx::query(query)
            .bind(self.scope().id())
            .bind(Self::tree_blob(id));
        if let Some(commit_id) = commit_id {
            request = request.bind(Self::commit_blob(commit_id));
        }
        let rows = request.fetch_all(&self.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                let blob: Vec<u8> = row.try_get("blob")?;
                Ok((Signed::try_decode(&signed)?, Blob::new(blob)))
            })
            .collect()
    }

    pub(crate) async fn fragment_rows(
        &self,
        id: SedimentreeId,
        head_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<Fragment>, Blob)>, SqliteBigRepoStoreError> {
        let query = if head_id.is_some() {
            "SELECT signed, blob FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 AND head_id = ?3
             ORDER BY digest"
        } else {
            "SELECT signed, blob FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest"
        };
        // Optional filter clauses make this SQL shape dynamic; retain runtime checking.
        let mut request = sqlx::query(query)
            .bind(self.scope().id())
            .bind(Self::tree_blob(id));
        if let Some(head_id) = head_id {
            request = request.bind(Self::commit_blob(head_id));
        }
        let rows = request.fetch_all(&self.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                let blob: Vec<u8> = row.try_get("blob")?;
                Ok((Signed::try_decode(&signed)?, Blob::new(blob)))
            })
            .collect()
    }

    /// Raw SQL row insert for a loose commit (plus its causal ciphertext
    /// index). Graph semantics live in the cached projection, not here.
    pub(crate) async fn insert_commit_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let (signed, payload, blob) = verified.into_full_parts();
        let digest = Self::digest_blob(&payload);
        let blob = blob.into_contents();
        sqlx::query!(
            "INSERT OR IGNORE INTO big_repo_subduction_commits
             (scope_id, sedimentree_id, commit_id, digest, signed, blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(payload.head()),
            &digest,
            signed.as_bytes(),
            &blob
        )
        .execute(&mut **tx)
        .await?;
        self.index_causal_ciphertext(tx, id, payload.head(), digest, 0, &blob)
            .await?;
        Ok(())
    }

    /// Raw SQL row insert for a fragment (plus its causal ciphertext index).
    pub(crate) async fn insert_fragment_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Result<(), SqliteBigRepoStoreError> {
        let (signed, payload, blob) = verified.into_full_parts();
        let digest = Self::digest_blob(&payload);
        let blob = blob.into_contents();
        sqlx::query!(
            "INSERT OR IGNORE INTO big_repo_subduction_fragments
             (scope_id, sedimentree_id, head_id, digest, signed, blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(payload.head()),
            &digest,
            signed.as_bytes(),
            &blob
        )
        .execute(&mut **tx)
        .await?;
        self.index_causal_ciphertext(tx, id, payload.head(), digest, 1, &blob)
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
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        mutation: TreeStorageMutation,
    ) -> Result<(Vec<SubEvent>, TreeCacheGuard<'_>), SqliteBigRepoStoreError> {
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
                // Inserts: adopt the cached entry or hydrate it from the
                // transaction's view of durable storage.
                let maybe_epoch = {
                    let mut cache = self.tree_cache.lock().expect(ERROR_MUTEX);
                    if cache.get(&id).is_some() {
                        Some(cache.current_epoch(&id).unwrap_or(0))
                    } else {
                        None
                    }
                };
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
        let events = self
            .set_obj_payload_in_tx(tx, Self::obj_id(id), payload)
            .await?;

        Ok((events, guard))
    }

    /// Hydrate a metadata-only tree from the transaction's view of durable
    /// storage (no blobs). Reads through `tx` so it sees the exact state being
    /// mutated.
    pub(crate) async fn hydrate_tree_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<Vec<LooseCommit>, SqliteBigRepoStoreError> {
        let rows = sqlx::query(
            "SELECT signed FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest",
        )
        .bind(self.scope().id())
        .bind(Self::tree_blob(id))
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                Ok(Signed::<LooseCommit>::try_decode(&signed)?.try_decode_trusted_payload()?)
            })
            .collect()
    }

    pub(crate) async fn fragment_meta_rows_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<Vec<Fragment>, SqliteBigRepoStoreError> {
        let rows = sqlx::query(
            "SELECT signed FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest",
        )
        .bind(self.scope().id())
        .bind(Self::tree_blob(id))
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                Ok(Signed::<Fragment>::try_decode(&signed)?.try_decode_trusted_payload()?)
            })
            .collect()
    }

    /// Raw SQL row delete for a loose commit (plus its causal ciphertext
    /// index). The cached projection is rebuilt by the caller.
    pub(crate) async fn delete_commit_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query!("DELETE FROM big_repo_causal_ciphertext_index WHERE scope_id = ?1 AND sedimentree_id = ?2 AND content_ref = ?3 AND kind = 0",
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(commit_id)
        )
            .execute(&mut **tx).await?;
        sqlx::query!("DELETE FROM big_repo_subduction_commits WHERE scope_id = ?1 AND sedimentree_id = ?2 AND commit_id = ?3",
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(commit_id)
        )
            .execute(&mut **tx).await?;
        Ok(())
    }

    /// Raw SQL row delete for a fragment (plus its causal ciphertext index).
    pub(crate) async fn delete_fragment_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
        head_id: CommitId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query!("DELETE FROM big_repo_causal_ciphertext_index WHERE scope_id = ?1 AND sedimentree_id = ?2 AND content_ref = ?3 AND kind = 1",
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(head_id)
        )
            .execute(&mut **tx).await?;
        sqlx::query!("DELETE FROM big_repo_subduction_fragments WHERE scope_id = ?1 AND sedimentree_id = ?2 AND head_id = ?3",
            self.scope().id(),
            Self::tree_blob(id),
            Self::commit_blob(head_id)
        )
            .execute(&mut **tx).await?;
        Ok(())
    }

    /// Raw SQL row delete for all loose commits of a tree.
    pub(crate) async fn delete_all_commit_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query!("DELETE FROM big_repo_causal_ciphertext_index WHERE scope_id = ?1 AND sedimentree_id = ?2 AND kind = 0",
            self.scope().id(),
            Self::tree_blob(id)
        ).execute(&mut **tx).await?;
        sqlx::query!(
            "DELETE FROM big_repo_subduction_commits WHERE scope_id = ?1 AND sedimentree_id = ?2",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Raw SQL row delete for all fragments of a tree.
    pub(crate) async fn delete_all_fragment_rows(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<(), SqliteBigRepoStoreError> {
        sqlx::query!("DELETE FROM big_repo_causal_ciphertext_index WHERE scope_id = ?1 AND sedimentree_id = ?2 AND kind = 1",
            self.scope().id(),
            Self::tree_blob(id)
        ).execute(&mut **tx).await?;
        sqlx::query!(
            "DELETE FROM big_repo_subduction_fragments WHERE scope_id = ?1 AND sedimentree_id = ?2",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn commit_meta_rows(
        &self,
        id: SedimentreeId,
    ) -> Result<Vec<LooseCommit>, SqliteBigRepoStoreError> {
        let rows = sqlx::query(
            "SELECT signed FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest",
        )
        .bind(self.scope().id())
        .bind(Self::tree_blob(id))
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                Ok(Signed::<LooseCommit>::try_decode(&signed)?.try_decode_trusted_payload()?)
            })
            .collect()
    }

    pub(crate) async fn fragment_meta_rows(
        &self,
        id: SedimentreeId,
    ) -> Result<Vec<Fragment>, SqliteBigRepoStoreError> {
        let rows = sqlx::query(
            "SELECT signed FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest",
        )
        .bind(self.scope().id())
        .bind(Self::tree_blob(id))
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let signed: Vec<u8> = row.try_get("signed")?;
                Ok(Signed::<Fragment>::try_decode(&signed)?.try_decode_trusted_payload()?)
            })
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.save_tree(&mut tx, id).await?;
            tx.commit().await?;
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.tree_cache.lock().expect(ERROR_MUTEX).remove(&id);
            let tree = Self::tree_blob(id);
            sqlx::query!(
                "DELETE FROM big_repo_subduction_commits
                     WHERE scope_id = ?1 AND sedimentree_id = ?2",
                self.scope().id(),
                &tree
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!(
                "DELETE FROM big_repo_subduction_fragments
                     WHERE scope_id = ?1 AND sedimentree_id = ?2",
                self.scope().id(),
                &tree
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!(
                "DELETE FROM big_repo_subduction_trees
                     WHERE scope_id = ?1 AND sedimentree_id = ?2",
                self.scope().id(),
                tree
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(&self) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Sendable::from_future(async move {
            let rows = sqlx::query(
                "SELECT sedimentree_id FROM big_repo_subduction_trees
                     WHERE scope_id = ?1 ORDER BY sedimentree_id",
            )
            .bind(self.scope().id())
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(SedimentreeId::new(Self::decode_id(
                        row.try_get("sedimentree_id")?,
                    )?))
                })
                .collect()
        })
    }

    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        Sendable::from_future(async move {
            let found: Option<i64> = sqlx::query_scalar!(
                "SELECT 1 AS \"found!: i64\" FROM big_repo_subduction_trees
                     WHERE scope_id = ?1 AND sedimentree_id = ?2",
                self.scope().id(),
                Self::tree_blob(id),
            )
            .fetch_optional(&self.sql.read_pool)
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            let rows = sqlx::query(
                "SELECT DISTINCT commit_id FROM big_repo_subduction_commits
                     WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id",
            )
            .bind(self.scope().id())
            .bind(Self::tree_blob(id))
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row.try_get("commit_id")?)?)))
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            let rows = sqlx::query("SELECT DISTINCT head_id FROM big_repo_subduction_fragments WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id").bind(self.scope().id()).bind(Self::tree_blob(id)).fetch_all(&self.sql.read_pool).await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row.try_get("head_id")?)?)))
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
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
            let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
            self.save_tree(&mut tx, id).await?;
            let (events, guard) = if count == 0 {
                let epoch = self
                    .tree_cache
                    .lock()
                    .expect(ERROR_MUTEX)
                    .current_epoch(&id)
                    .unwrap_or(0);
                (Vec::new(), TreeCacheGuard::arm(&self.tree_cache, id, epoch))
            } else {
                self.mutate_tree_in_tx(
                    &mut tx,
                    id,
                    TreeStorageMutation::InsertBatch { commits, fragments },
                )
                .await?
            };
            tx.commit().await?;
            guard.disarm();
            self.publish(events).await?;
            Ok(count)
        })
    }
}
