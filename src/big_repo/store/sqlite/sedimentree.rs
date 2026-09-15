use super::ids::IdCodec;
use super::*;
use sqlx::{QueryBuilder, Row};

const REPLAY_RAW_BATCH_SIZE: u32 = 256;

struct ReplayCandidate {
    txid: CursorIndex,
    obj_id: ObjId,
    _maybe_part_id: Option<PartId>,
    event_type: i64,
    payload: ObjPayload,
}

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

    async fn replay_candidates(
        &self,
        parts: &HashSet<PartId>,
        objects: &HashSet<ObjId>,
        lower_bound: CursorIndex,
        exact_txid: Option<CursorIndex>,
        limit: Option<u32>,
    ) -> Res<Vec<ReplayCandidate>> {
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT m.txid, o.obj_id, p.part_id, m.event_type, o.payload_json
             FROM big_sync_members m
             JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
             LEFT JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ",
        );
        query.push_bind(self.scope().id());
        if let Some(txid) = exact_txid {
            query.push(" AND m.txid = ");
            query.push_bind(i64::try_from(txid).expect(ERROR_IMPOSSIBLE));
        } else {
            query.push(" AND m.txid > ");
            query.push_bind(i64::try_from(lower_bound).expect(ERROR_IMPOSSIBLE));
        }
        query.push(" AND (");
        if parts.is_empty() {
            query.push("0");
        } else {
            query.push(
                "m.maybe_part_ref IN (
                    SELECT part_ref FROM big_sync_parts
                    WHERE scope_id = ",
            );
            query.push_bind(self.scope().id());
            query.push(" AND part_id IN (");
            let mut separated = query.separated(", ");
            for part_id in parts {
                separated.push_bind(Self::part_blob(*part_id));
            }
            separated.push_unseparated("))");
        }
        query.push(" OR ");
        if objects.is_empty() {
            query.push("0");
        } else {
            query.push(
                "m.maybe_part_ref = 0 AND m.obj_ref IN (
                    SELECT obj_ref FROM big_sync_objs
                    WHERE scope_id = ",
            );
            query.push_bind(self.scope().id());
            query.push(" AND obj_id IN (");
            let mut separated = query.separated(", ");
            for obj_id in objects {
                separated.push_bind(Self::obj_blob(*obj_id));
            }
            separated.push_unseparated("))");
        }
        query.push(") ORDER BY m.txid, m.obj_ref, m.maybe_part_ref");
        if let Some(limit) = limit {
            query.push(" LIMIT ");
            query.push_bind(i64::from(limit));
        }

        let rows = query.build().fetch_all(&self.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let payload = row
                    .try_get::<Option<String>, _>("payload_json")?
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(|value| serde_json::from_str(value).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                Ok(ReplayCandidate {
                    txid: u64::try_from(row.try_get::<i64, _>("txid")?).expect(ERROR_IMPOSSIBLE),
                    obj_id: Self::obj_from_blob(row.try_get("obj_id")?),
                    _maybe_part_id: row
                        .try_get::<Option<Vec<u8>>, _>("part_id")?
                        .map(Self::part_from_blob),
                    event_type: row.try_get("event_type")?,
                    payload,
                })
            })
            .collect()
    }

    pub(crate) async fn subscribe_with_policy(
        &self,
        reqs: SubPartsRequest,
        subscriber: Option<PeerId>,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        let parts: HashSet<PartId> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, .. } => Some(*part_id),
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
        if subscriber.is_some()
            && let Err(err) = self.summarize_parts(parts.clone()).await?
        {
            return Ok(Err(err));
        }

        let (tx, rx) = mpsc::unbounded("SqliteBigRepoStore".into(), "caller".into());
        let sub_id = uuid::Uuid::new_v4();
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
            let mut cursor = reqs.lower_bound;
            let mut marker_sent = false;
            let mut object_replay_pending = true;
            loop {
                sub.pending
                    .state
                    .store(SUB_REPLAYING_CLEAN, std::sync::atomic::Ordering::Release);
                let page = store
                    .list_events_with_policy(
                        parts.clone(),
                        cursor,
                        REPLAY_RAW_BATCH_SIZE,
                        subscriber.is_some(),
                    )
                    .await
                    .expect(ERROR_IMPOSSIBLE)
                    .expect(ERROR_IMPOSSIBLE);
                let mut output: Vec<SubEvent> = Vec::new();
                let mut raw_event_count = 0;
                let mut max_cursor = cursor;
                let mut part_events = page
                    .into_iter()
                    .flat_map(|(part_id, page)| {
                        page.events.into_iter().map(move |event| (part_id, event))
                    })
                    .collect::<Vec<_>>();
                part_events.sort_by_key(|(part_id, event)| {
                    let event_cursor = match event {
                        PartEvent::Changed(inner) => inner.cursor,
                        PartEvent::Added(inner) => inner.cursor,
                        PartEvent::Removed(inner) => inner.cursor,
                    };
                    let obj_id = match event {
                        PartEvent::Changed(inner) => inner.obj_id,
                        PartEvent::Added(inner) => inner.obj_id,
                        PartEvent::Removed(inner) => inner.obj_id,
                    };
                    (event_cursor, obj_id, *part_id)
                });
                for (part_id, event) in part_events {
                    raw_event_count += 1;
                    let event_cursor = match &event {
                        PartEvent::Changed(inner) => inner.cursor,
                        PartEvent::Added(inner) => inner.cursor,
                        PartEvent::Removed(inner) => inner.cursor,
                    };
                    max_cursor = max_cursor.max(event_cursor);
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
                if object_replay_pending {
                    let no_parts: HashSet<PartId> = HashSet::new();
                    let object_candidates = store
                        .replay_candidates(&no_parts, &objects, cursor, None, None)
                        .await
                        .expect(ERROR_IMPOSSIBLE);
                    for candidate in object_candidates {
                        max_cursor = max_cursor.max(candidate.txid);
                        raw_event_count += 1;
                        let permitted = store
                            .is_event_permitted(None, candidate.obj_id, subscriber)
                            .await
                            .expect(ERROR_IMPOSSIBLE);
                        if !permitted || candidate.event_type != EVENT_CHANGED {
                            continue;
                        }
                        // FIXME: use binary search? i imagine the items are in order?
                        if let Some(SubEvent::Changed(existing)) = output.iter_mut().find(|event| {
                            matches!(event, SubEvent::Changed(current)
                                if current.cursor == candidate.txid
                                    && current.obj_id == candidate.obj_id)
                        }) {
                            existing.payload = candidate.payload;
                        } else {
                            output.push(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                                cursor: candidate.txid,
                                part_ids: Vec::new(),
                                obj_id: candidate.obj_id,
                                payload: candidate.payload,
                            }));
                        }
                    }
                    object_replay_pending = false;
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
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id).await?;
        let old_payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        sqlx::query!(
            "UPDATE big_sync_objs SET payload_json = ?1 WHERE obj_ref = ?2",
            &payload_json,
            obj_ref
        )
        .execute(&mut **tx)
        .await?;
        let parts = sqlx::query!(
            "SELECT m.maybe_part_ref, p.part_id
             FROM big_sync_members m JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2 AND m.maybe_part_ref > 0 AND m.event_type != ?3",
            self.scope().id(), obj_ref, EVENT_REMOVED
        ).fetch_all(&mut **tx).await?;
        let pending_parts = sqlx::query!(
            "SELECT m.part_ref, p.part_id
             FROM big_sync_pending_members m JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2",
            self.scope().id(),
            obj_ref
        )
        .fetch_all(&mut **tx)
        .await?;
        let cursor = Self::next_cursor(tx).await?;
        sqlx::query!("INSERT INTO big_sync_members(scope_id,obj_ref,maybe_part_ref,event_type,txid) VALUES (?1,?2,0,?3,?4) ON CONFLICT(obj_ref,maybe_part_ref) DO UPDATE SET event_type=excluded.event_type,txid=excluded.txid", self.scope().id(), obj_ref, EVENT_CHANGED, i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)).execute(&mut **tx).await?;
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|str| !str.is_empty())
            .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let mut changed_part_ids = Vec::new();
        let mut added_events = Vec::new();
        for row in &parts {
            let part_id = Self::part_from_blob(row.part_id.clone());
            changed_part_ids.push(part_id);
            sqlx::query!("UPDATE big_sync_members SET event_type = ?1, txid = ?2 WHERE scope_id = ?3 AND obj_ref = ?4 AND maybe_part_ref = ?5", EVENT_CHANGED, i64::try_from(cursor).expect(ERROR_IMPOSSIBLE), self.scope().id(), obj_ref, row.maybe_part_ref).execute(&mut **tx).await?;
            self.core
                .apply_bucket_transition(
                    tx,
                    part_id,
                    obj_id,
                    cursor,
                    &MemberState::Live(old_payload.clone()),
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!("UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?1) WHERE scope_id = ?2 AND part_ref = ?3", i64::try_from(cursor).expect(ERROR_IMPOSSIBLE), self.scope().id(), row.maybe_part_ref).execute(&mut **tx).await?;
        }
        for row in pending_parts {
            let part_id = Self::part_from_blob(row.part_id);
            let part_ref = row.part_ref;
            sqlx::query!(
                "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET event_type = excluded.event_type, txid = excluded.txid",
                self.scope().id(), obj_ref, part_ref, EVENT_ADDED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
            )
            .execute(&mut **tx)
            .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.scope().id(), obj_ref, part_ref
            )
            .execute(&mut **tx)
            .await?;
            self.core
                .apply_bucket_transition(
                    tx,
                    part_id,
                    obj_id,
                    cursor,
                    &MemberState::Absent,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?1) WHERE scope_id = ?2 AND part_ref = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE), self.scope().id(), part_ref
            ).execute(&mut **tx).await?;
            added_events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        let mut events = added_events;
        events.push(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: changed_part_ids,
            obj_id,
            payload,
        }));
        Ok(events)
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
            let obj_ref = self.core.ensure_obj_ref(&mut tx, mutation.doc).await?;
            let payload_json = sqlx::query_scalar!(
                "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
                obj_ref
            )
            .fetch_one(&mut *tx)
            .await?;
            let payload_json = payload_json.filter(|value| !value.is_empty());
            let event_payload: Option<ObjPayload> = payload_json
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| serde_json::from_str(value).wrap_err(ERROR_JSON))
                .transpose()?;
            sqlx::query!(
                "UPDATE big_sync_objs SET payload_json = COALESCE(payload_json, ?1) WHERE obj_ref = ?2",
                payload_json.as_deref(),
                obj_ref
            )
            .execute(&mut *tx)
            .await?;

            let prior_agent_ids: HashSet<Vec<u8>> = sqlx::query_scalar!(
                "SELECT principal_id FROM big_sync_syncable
                     WHERE scope_id = ?1 AND obj_ref = ?2",
                self.scope().id(),
                obj_ref
            )
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
            sqlx::query!(
                "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2",
                self.scope().id(),
                obj_ref
            )
            .execute(&mut *tx)
            .await?;
            for (principal, access) in &mutation.agents {
                sqlx::query!(
                    "INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level)
                     VALUES (?1, ?2, ?3, ?4)",
                    self.scope().id(),
                    obj_ref,
                    Self::peer_blob(*principal),
                    encode_access(access)
                )
                .execute(&mut *tx)
                .await?;
            }
            reconciled_docs.insert(mutation.doc, mutation.agents.clone());

            let current_rows: Vec<Vec<u8>> = sqlx::query_scalar!(
                "SELECT p.part_id AS 'part_id: Vec<u8>' FROM big_sync_members m
                 JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
                 WHERE m.scope_id = ?1 AND m.obj_ref = ?2
                   AND m.maybe_part_ref > 0 AND m.event_type != 2
                 UNION
                 SELECT p.part_id FROM big_sync_pending_members m
                 JOIN big_sync_parts p ON p.part_ref = m.part_ref
                 WHERE m.scope_id = ?1 AND m.obj_ref = ?2",
                self.scope().id(),
                obj_ref
            )
            .fetch_all(&mut *tx)
            .await?;
            let current_parts: HashSet<PartId> =
                current_rows.into_iter().map(Self::part_from_blob).collect();
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
                            "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, obj_ref, part_ref)
                             VALUES (?1, ?2, ?3)",
            self.scope().id(),
            obj_ref,
            self.core.ensure_part_ref(&mut tx, part_id).await?
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
                                     WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                        self.scope().id(),
                        obj_ref,
                        self.core.ensure_part_ref(&mut tx, part_id).await?
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
                let live_part_rows = sqlx::query_scalar!(
                    "SELECT p.part_id FROM big_sync_members m
                     JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
                     WHERE m.scope_id = ?1 AND m.obj_ref = ?2
                       AND m.maybe_part_ref > 0 AND m.event_type != 2",
                    self.scope().id(),
                    obj_ref
                )
                .fetch_all(&mut *tx)
                .await?;
                for part_blob in live_part_rows {
                    let part_id = Self::part_from_blob(part_blob);
                    if !desired_parts.contains(&part_id)
                        || transitions
                            .iter()
                            .any(|(part, doc, _, _)| *part == part_id && *doc == mutation.doc)
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
                        "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
                             VALUES (?1, ?2, ?3, ?4, ?5)
                             ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
                               event_type = excluded.event_type, txid = excluded.txid",
                        self.scope().id(),
                        self.core.find_obj_ref(doc).await?.expect(ERROR_IMPOSSIBLE),
                        self.core.ensure_part_ref(&mut tx, part_id).await?,
                        EVENT_ADDED,
                        i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                    )
                    .execute(&mut *tx)
                    .await?;
                    self.apply_bucket_transition(&mut tx, part_id, doc, cursor, &old, &new)
                        .await?;
                    sqlx::query!(
                        "DELETE FROM big_sync_pending_members
                                         WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                        self.scope().id(),
                        self.core.find_obj_ref(doc).await?.expect(ERROR_IMPOSSIBLE),
                        self.core.ensure_part_ref(&mut tx, part_id).await?
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
                        "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
                             VALUES (?1, ?2, ?3, ?4, ?5)
                             ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
                               event_type = excluded.event_type, txid = excluded.txid",
                        self.scope().id(),
                        self.core.find_obj_ref(doc).await?.expect(ERROR_IMPOSSIBLE),
                        self.core.ensure_part_ref(&mut tx, part_id).await?,
                        EVENT_REMOVED,
                        i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
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
                         WHERE scope_id = ?2 AND part_ref = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                self.core.ensure_part_ref(&mut tx, part_id).await?
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
            self.advance_keyhive_admission_reader_in_tx(
                &mut tx,
                crate::store::sqlite::KEYHIVE_ADMISSION_READER_GROUP_PART,
                event_cursor,
            )
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
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!(
            "INSERT INTO cursors(reader, seq) VALUES (?1, ?2)
             ON CONFLICT(reader)
             DO UPDATE SET seq = MAX(seq, excluded.seq)",
            self.cursor_reader("automerge_keyhive"),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&mut *tx)
        .await?;
        self.advance_keyhive_admission_reader_in_tx(
            &mut tx,
            crate::store::sqlite::KEYHIVE_ADMISSION_READER_AUTOMERGE_FRONTIER,
            cursor,
        )
        .await?;
        tx.commit().await?;
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
        let rows = sqlx::query!(
            "SELECT sedimentree_id, commit_id AS content_ref, digest, 0 AS kind, blob
             FROM big_repo_subduction_commits WHERE scope_id = ?1
             UNION ALL
             SELECT sedimentree_id, head_id AS content_ref, digest, 1 AS kind, blob
             FROM big_repo_subduction_fragments WHERE scope_id = ?1",
            self.scope().id()
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        for row in rows {
            let Ok(encrypted) = crate::encrypted_blob::decode_encrypted_blob(&row.blob) else {
                continue;
            };
            sqlx::query!(
                "INSERT OR IGNORE INTO big_repo_causal_ciphertext_index(
                    scope_id, sedimentree_id, content_ref, digest, kind, pcs_update_hash
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                self.scope().id(),
                row.sedimentree_id,
                row.content_ref,
                row.digest,
                row.kind,
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
        let rows = sqlx::query!(
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
            self.scope().id(),
            Self::tree_blob(sedimentree_id),
            pcs_update_hash.as_slice()
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter().map(|row| Ok(row.blob)).collect()
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
        let rows: Vec<(Vec<u8>, Vec<u8>)> = if let Some(commit_id) = commit_id {
            sqlx::query!(
                "SELECT signed, blob FROM big_repo_subduction_commits
                 WHERE scope_id = ?1 AND sedimentree_id = ?2 AND commit_id = ?3
                 ORDER BY digest",
                self.scope().id(),
                Self::tree_blob(id),
                Self::commit_blob(commit_id)
            )
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(|row| (row.signed, row.blob))
            .collect()
        } else {
            sqlx::query!(
                "SELECT signed, blob FROM big_repo_subduction_commits
                 WHERE scope_id = ?1 AND sedimentree_id = ?2
                 ORDER BY commit_id, digest",
                self.scope().id(),
                Self::tree_blob(id)
            )
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(|row| (row.signed, row.blob))
            .collect()
        };
        rows.into_iter()
            .map(|(signed, blob)| Ok((Signed::try_decode(&signed)?, Blob::new(blob))))
            .collect()
    }

    pub(crate) async fn fragment_rows(
        &self,
        id: SedimentreeId,
        head_id: Option<CommitId>,
    ) -> Result<Vec<(Signed<Fragment>, Blob)>, SqliteBigRepoStoreError> {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = if let Some(head_id) = head_id {
            sqlx::query!(
                "SELECT signed, blob FROM big_repo_subduction_fragments
                 WHERE scope_id = ?1 AND sedimentree_id = ?2 AND head_id = ?3
                 ORDER BY digest",
                self.scope().id(),
                Self::tree_blob(id),
                Self::commit_blob(head_id)
            )
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(|row| (row.signed, row.blob))
            .collect()
        } else {
            sqlx::query!(
                "SELECT signed, blob FROM big_repo_subduction_fragments
                 WHERE scope_id = ?1 AND sedimentree_id = ?2
                 ORDER BY head_id, digest",
                self.scope().id(),
                Self::tree_blob(id)
            )
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(|row| (row.signed, row.blob))
            .collect()
        };
        rows.into_iter()
            .map(|(signed, blob)| Ok((Signed::try_decode(&signed)?, Blob::new(blob))))
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
        let rows = sqlx::query!(
            "SELECT signed FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Signed::<LooseCommit>::try_decode(&row.signed)?.try_decode_trusted_payload()?)
            })
            .collect()
    }

    pub(crate) async fn fragment_meta_rows_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: SedimentreeId,
    ) -> Result<Vec<Fragment>, SqliteBigRepoStoreError> {
        let rows = sqlx::query!(
            "SELECT signed FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Signed::<Fragment>::try_decode(&row.signed)?.try_decode_trusted_payload()?)
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
        let rows = sqlx::query!(
            "SELECT signed FROM big_repo_subduction_commits
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id, digest",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Signed::<LooseCommit>::try_decode(&row.signed)?.try_decode_trusted_payload()?)
            })
            .collect()
    }

    pub(crate) async fn fragment_meta_rows(
        &self,
        id: SedimentreeId,
    ) -> Result<Vec<Fragment>, SqliteBigRepoStoreError> {
        let rows = sqlx::query!(
            "SELECT signed FROM big_repo_subduction_fragments
             WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id, digest",
            self.scope().id(),
            Self::tree_blob(id)
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Signed::<Fragment>::try_decode(&row.signed)?.try_decode_trusted_payload()?)
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
            let rows = sqlx::query!(
                "SELECT sedimentree_id FROM big_repo_subduction_trees
                 WHERE scope_id = ?1 ORDER BY sedimentree_id",
                self.scope().id()
            )
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| Ok(SedimentreeId::new(Self::decode_id(row.sedimentree_id)?)))
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
            let rows = sqlx::query!(
                "SELECT DISTINCT commit_id FROM big_repo_subduction_commits
                 WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY commit_id",
                self.scope().id(),
                Self::tree_blob(id)
            )
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row.commit_id)?)))
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
            let rows = sqlx::query!(
                "SELECT DISTINCT head_id FROM big_repo_subduction_fragments
                 WHERE scope_id = ?1 AND sedimentree_id = ?2 ORDER BY head_id",
                self.scope().id(),
                Self::tree_blob(id)
            )
            .fetch_all(&self.sql.read_pool)
            .await?;
            rows.into_iter()
                .map(|row| Ok(CommitId::new(Self::decode_id(row.head_id)?)))
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
