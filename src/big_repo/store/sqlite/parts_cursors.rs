use super::*;
use sqlx::{QueryBuilder, Row};

#[async_trait]
impl HostPartStore for SqliteBigRepoStore {
    async fn latest_revision(&self) -> Res<CursorIndex> {
        let revision: i64 =
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&self.sql.read_pool)
                .await?;
        Ok(u64::try_from(revision)?)
    }

    async fn is_event_permitted(
        &self,
        part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> Res<bool> {
        Self::is_event_permitted(self, part_id, obj_id, principal).await
    }
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>> {
        if parts.is_empty() {
            return Ok(Ok(HashMap::new()));
        }
        let mut hidden: Vec<_> = parts.intersection(&self.hidden_parts).copied().collect();
        if !hidden.is_empty() {
            hidden.sort_unstable();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: hidden,
            }));
        }

        // Dynamic IN-list cardinality requires runtime SQL checking here.
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT p.part_id, p.latest_cursor, COALESCE(b.live_count, 0) AS member_count
             FROM big_sync_parts p
             LEFT JOIN big_sync_buckets b
               ON b.scope_id = p.scope_id
              AND b.part_ref = p.part_ref
              AND b.level = 0
              AND b.buck_id = 0
             WHERE p.scope_id = ",
        );
        query.push_bind(self.scope().id());
        query.push(" AND p.part_id IN (");
        let mut separated = query.separated(", ");
        for part_id in &parts {
            separated.push_bind(Self::part_blob(*part_id));
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.sql.read_pool).await?;

        if rows.len() != parts.len() {
            let found: HashSet<PartId> = rows
                .iter()
                .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut missing: Vec<_> = parts.difference(&found).copied().collect();
            missing.sort();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: missing,
            }));
        }

        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let part_id = Self::part_from_blob(row.try_get("part_id")?);
            let latest_cursor: i64 = row.try_get("latest_cursor")?;
            let member_count: i64 = row.try_get("member_count")?;
            out.insert(
                part_id,
                PartSummary {
                    latest_cursor: u64::try_from(latest_cursor).expect(ERROR_IMPOSSIBLE),
                    member_count: u64::try_from(member_count).expect(ERROR_IMPOSSIBLE),
                    deepest_bucket_level: self.core.bucket_depth,
                },
            );
        }
        Ok(Ok(out))
    }

    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        let member_count: Option<i64> = sqlx::query_scalar!(
            "SELECT live_count
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?2
             ) AND level = 0 AND buck_id = 0",
            self.scope().id(),
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(member_count
            .map(|member_count| u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let row = sqlx::query!(
            "SELECT payload_json
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope().id(),
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Option<String> = row.payload_json;
        payload
            .as_deref()
            .filter(|payload| !payload.is_empty())
            .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn set_obj_payload(&self, obj_id: ObjId, payload: ObjPayload) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let events = self.set_obj_payload_in_tx(&mut tx, obj_id, payload).await?;
        tx.commit().await?;
        self.publish(events).await?;
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query!(
            "SELECT p.part_id
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             ) AND m.maybe_part_ref > 0 AND m.event_type != ?3
             UNION
             SELECT p.part_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             )
             ORDER BY part_id ASC",
            self.scope().id(),
            Self::obj_blob(obj_id),
            EVENT_REMOVED
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.part_id))
            .collect())
    }

    async fn obj_exists(&self, obj_id: ObjId) -> Res<bool> {
        let exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope().id(),
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(exists.is_some())
    }

    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        self.core.bucket_summary_for_path(part_id, id).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.scope().id(),
            Self::part_blob(req.part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        let Some(_) = part_exists else {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        };

        // Dynamic IN-list cardinality requires runtime SQL checking here.
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT buck_id, level, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.push_bind(self.scope().id());
        query.push(" AND part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(self.scope().id());
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(")");
        query.push(" AND level = ");
        query.push_bind(i64::from(req.offset.level()));
        query.push(" AND buck_id >= ");
        query.push_bind(Self::buck_i64(req.offset));
        query.push(" AND changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" ORDER BY buck_id ASC LIMIT ");
        query.push_bind(i64::from(req.limit_hint) + i64::from(BuckId::ARITY));
        let rows = query.build().fetch_all(&self.sql.read_pool).await?;

        if rows.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        let mut out = Vec::new();
        let mut last_parent = None;
        for row in rows {
            let bucket = BucketSummary {
                id: Self::buck_id(row.try_get::<i64, _>("buck_id")?),
                len: u32::try_from(
                    u64::try_from(row.try_get::<i64, _>("live_count")?).expect(ERROR_IMPOSSIBLE)
                        + u64::try_from(row.try_get::<i64, _>("dead_count")?)
                            .expect(ERROR_IMPOSSIBLE),
                )
                .expect(ERROR_IMPOSSIBLE),
                live_count: u32::try_from(row.try_get::<i64, _>("live_count")?)
                    .expect(ERROR_IMPOSSIBLE),
                fp: (
                    Self::u64_from_db(row.try_get::<i64, _>("live_fp")?),
                    Self::u64_from_db(row.try_get::<i64, _>("dead_fp")?),
                ),
                changed_at: u64::try_from(row.try_get::<i64, _>("changed_at")?)
                    .expect(ERROR_IMPOSSIBLE),
            };
            if out.len() < usize::try_from(req.limit_hint).expect(ERROR_IMPOSSIBLE) {
                out.push(bucket);
                continue;
            }
            let parent = bucket.id.parent();
            if last_parent.is_none() {
                last_parent = Some(out.last().expect(ERROR_IMPOSSIBLE).id.parent());
            }
            if Some(parent) != last_parent {
                break;
            }
            out.push(bucket);
        }
        Ok(Ok(out))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.scope().id(),
            Self::part_blob(req.part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        if part_exists.is_none() {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }

        if req.buckets.is_empty() {
            return Ok(Ok(LeafBucketResult {
                seed: req.seed,
                bucks: HashMap::new(),
            }));
        }

        struct LeafBucketPageBuilder {
            buck_id: BuckId,
            entries: Vec<BucketObjPageEntry>,
            total_count: u32,
        }

        // Dynamic IN-list cardinality requires runtime SQL checking here.
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "WITH requested(req_ord, buck_id, lower_id, upper_id, after_id) AS (",
        );
        for (req_ord, buck_req) in req.buckets.iter().enumerate() {
            let (lower_id, upper_id) = obj_id_bounds_for_bucket(buck_req.buck_id);
            if req_ord > 0 {
                query.push(" UNION ALL ");
            }
            query.push("SELECT ");
            query.push_bind(i64::try_from(req_ord).expect(ERROR_IMPOSSIBLE));
            query.push(" AS req_ord, ");
            query.push_bind(Self::buck_i64(buck_req.buck_id));
            query.push(" AS buck_id, ");
            query.push_bind(Self::obj_blob(lower_id));
            query.push(" AS lower_id, ");
            if let Some(upper_id) = upper_id {
                query.push_bind(Self::obj_blob(upper_id));
            } else {
                query.push("NULL");
            }
            query.push(" AS upper_id, ");
            if let Some(after) = buck_req.after {
                query.push_bind(Self::obj_blob(after));
            } else {
                query.push("NULL");
            }
            query.push(" AS after_id");
        }
        query.push(
            "), ranked AS (
                SELECT
                    r.req_ord,
                    r.buck_id,
                    o.obj_id,
                    m.event_type,
                    o.payload_json,
                    COUNT(*) OVER (PARTITION BY r.req_ord) AS total_count,
                    ROW_NUMBER() OVER (PARTITION BY r.req_ord ORDER BY o.obj_id ASC) AS row_num
                FROM requested r
                JOIN big_sync_members m
                  ON m.scope_id = ",
        );
        query.push_bind(self.scope().id());
        query.push(" AND m.maybe_part_ref = (");
        query.push("SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(self.scope().id());
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(")");
        query.push(
            "
                JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
                JOIN big_sync_buckets s
                  ON s.scope_id = m.scope_id
                 AND s.part_ref = m.maybe_part_ref
                 AND s.buck_id = r.buck_id
                 AND s.changed_at > ",
        );
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" WHERE o.obj_id >= r.lower_id");
        query.push(" AND (r.upper_id IS NULL OR o.obj_id < r.upper_id)");
        query.push(" AND (r.after_id IS NULL OR o.obj_id > r.after_id)");
        query.push(
            "
            )
            SELECT req_ord, buck_id, obj_id, event_type, payload_json, total_count
            FROM ranked
            WHERE row_num <= ",
        );
        query.push_bind(i64::from(req.limit_hint.max(1)));
        query.push(" ORDER BY req_ord, obj_id ASC");

        let rows = query.build().fetch_all(&self.sql.read_pool).await?;
        let mut pages: Vec<_> = req
            .buckets
            .iter()
            .map(|buck_req| LeafBucketPageBuilder {
                buck_id: buck_req.buck_id,
                entries: Vec::new(),
                total_count: 0,
            })
            .collect();
        for row in rows {
            let req_ord =
                usize::try_from(row.try_get::<i64, _>("req_ord")?).expect(ERROR_IMPOSSIBLE);
            let page = pages.get_mut(req_ord).expect(ERROR_IMPOSSIBLE);
            page.total_count =
                u32::try_from(row.try_get::<i64, _>("total_count")?).expect(ERROR_IMPOSSIBLE);
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let dead = row.try_get::<i64, _>("event_type")? == EVENT_REMOVED;
            let fp = if dead {
                Fingerprint::new(
                    &req.seed,
                    &("big-sync-obj-fp-v1", obj_id, serde_json::Value::Null),
                )
            } else {
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = payload_json
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(&payload_json).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
            };
            page.entries.push(BucketObjPageEntry { obj_id, dead, fp });
        }
        let mut bucks = HashMap::with_capacity(pages.len());
        for page in pages {
            let done =
                u32::try_from(page.entries.len()).expect(ERROR_IMPOSSIBLE) == page.total_count;
            let next_after = if done || page.entries.is_empty() {
                None
            } else {
                Some(page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id)
            };
            bucks.insert(
                page.buck_id,
                LeafBucketPage {
                    entries: page.entries,
                    next_after,
                    done,
                },
            );
        }
        Ok(Ok(LeafBucketResult {
            seed: req.seed,
            bucks,
        }))
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        let parts = parts
            .into_iter()
            .filter(|part_id| *part_id != crate::GLOBAL_PART_ID)
            .collect::<Vec<_>>();
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj_id).await?;
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let Some(payload_json) = payload_json.filter(|str| !str.is_empty()) else {
            for part_id in parts {
                let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
                sqlx::query!("INSERT OR IGNORE INTO big_sync_pending_members(scope_id,obj_ref,part_ref) VALUES (?1,?2,?3)", self.scope().id(), obj_ref, part_ref).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            return Ok(());
        };
        let payload: ObjPayload = serde_json::from_str(&payload_json).wrap_err(ERROR_JSON)?;
        let mut events = Vec::new();
        for part_id in parts {
            let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
            let old = self.load_member_state(&mut tx, part_id, obj_id).await?;
            if matches!(old, MemberState::Live(_)) {
                continue;
            }
            let cursor = Self::next_cursor(&mut tx).await?;
            sqlx::query!("INSERT INTO big_sync_members(scope_id,obj_ref,maybe_part_ref,event_type,txid) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(obj_ref,maybe_part_ref) DO UPDATE SET event_type=excluded.event_type,txid=excluded.txid", self.scope().id(), obj_ref, part_ref, EVENT_ADDED, i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)).execute(&mut *tx).await?;
            self.apply_bucket_transition(
                &mut tx,
                part_id,
                obj_id,
                cursor,
                &old,
                &MemberState::Live(payload.clone()),
            )
            .await?;
            sqlx::query!(
                "UPDATE big_sync_parts
                 SET latest_cursor = MAX(latest_cursor, ?1)
                 WHERE scope_id = ?2 AND part_ref = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                part_ref
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!("DELETE FROM big_sync_pending_members WHERE scope_id=?1 AND obj_ref=?2 AND part_ref=?3", self.scope().id(), obj_ref, part_ref).execute(&mut *tx).await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        tx.commit().await?;
        self.publish(events).await?;
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(obj_ref) = self.core.find_obj_ref(obj_id).await? else {
            tx.commit().await?;
            return Ok(());
        };
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
        sqlx::query!("DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3", self.scope().id(), obj_ref, part_ref).execute(&mut *tx).await?;
        let old_state = self.load_member_state(&mut tx, part_id, obj_id).await?;
        let MemberState::Live(old_payload) = old_state else {
            tx.commit().await?;
            return Ok(());
        };
        let cursor = Self::next_cursor(&mut tx).await?;
        sqlx::query!(
            "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET event_type = excluded.event_type, txid = excluded.txid",
            self.scope().id(), obj_ref, part_ref, EVENT_REMOVED, i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        ).execute(&mut *tx).await?;
        self.core
            .apply_bucket_transition(
                &mut tx,
                part_id,
                obj_id,
                cursor,
                &MemberState::Live(old_payload),
                &MemberState::Dead,
            )
            .await?;
        sqlx::query!("UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?1) WHERE scope_id = ?2 AND part_ref = ?3", i64::try_from(cursor).expect(ERROR_IMPOSSIBLE), self.scope().id(), part_ref).execute(&mut *tx).await?;
        let live_count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM big_sync_members WHERE scope_id = ?1 AND obj_ref = ?2 AND maybe_part_ref > 0 AND event_type != ?3", self.scope().id(), obj_ref, EVENT_REMOVED).fetch_one(&mut *tx).await?;
        if live_count == 0 {
            sqlx::query!(
                "UPDATE big_sync_objs SET payload_json = NULL WHERE obj_ref = ?1",
                obj_ref
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.publish(vec![SubEvent::Removed(
            big_sync_core::rpc::ObjRemovedFromPart {
                cursor,
                part_id,
                obj_id,
            },
        )])
        .await?;
        Ok(())
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT cursor FROM big_sync_peer_cursors
             WHERE scope_id = ?1 AND peer_id = ?2 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?3
             )",
            self.scope().id(),
            Self::peer_blob(peer_id),
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor
            .map(|val| u64::try_from(val).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()> {
        let mut tx = self.sql.write_pool.begin().await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
        sqlx::query!(
            "INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_ref, cursor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, peer_id, part_ref) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)",
            self.scope().id(), Self::peer_blob(peer_id), part_ref,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        ).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        self.list_events_with_policy(parts, cursor, limit, true)
            .await
    }

    async fn list_events_with_policy(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
        enforce_policy: bool,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        if enforce_policy && let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let mut out = HashMap::new();
        for part_id in parts {
            // Select the txid at the requested row limit first, then return the
            // complete txid boundary so events sharing a transaction are not split.
            let rows = sqlx::query(
                "WITH cutoff AS (
                     SELECT MAX(txid) AS txid
                       FROM (
                           SELECT m.txid
                             FROM big_sync_members m
                            WHERE m.scope_id = ?1
                              AND m.maybe_part_ref = (
                                  SELECT part_ref FROM big_sync_parts
                                   WHERE scope_id = ?1 AND part_id = ?2
                              )
                              AND m.txid > ?3
                            ORDER BY m.txid, m.obj_ref
                            LIMIT ?4
                       )
                 )
                 SELECT o.obj_id, o.payload_json, m.event_type, m.txid
                   FROM big_sync_members m
                   JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
                  WHERE m.scope_id = ?1
                    AND m.maybe_part_ref = (
                        SELECT part_ref FROM big_sync_parts
                         WHERE scope_id = ?1 AND part_id = ?2
                    )
                    AND m.txid > ?3
                    AND m.txid <= (SELECT txid FROM cutoff)
                  ORDER BY m.txid, m.obj_ref",
            )
            .bind(self.scope().id())
            .bind(Self::part_blob(part_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(i64::from(limit))
            .fetch_all(&self.sql.read_pool)
            .await?;
            let cutoff_txid: Option<i64> =
                rows.last().map(|row| row.try_get("txid")).transpose()?;
            let has_more = if let Some(cutoff) = cutoff_txid {
                let has_more: i64 = sqlx::query_scalar(
                    "SELECT CASE WHEN EXISTS (
                         SELECT 1
                           FROM big_sync_members m
                          WHERE m.scope_id = ?1
                            AND m.maybe_part_ref = (
                                SELECT part_ref FROM big_sync_parts
                                 WHERE scope_id = ?1 AND part_id = ?2
                            )
                            AND m.txid > ?3
                            AND m.txid > ?4
                     ) THEN 1 ELSE 0 END",
                )
                .bind(self.scope().id())
                .bind(Self::part_blob(part_id))
                .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
                .bind(cutoff)
                .fetch_one(&self.sql.read_pool)
                .await?;
                has_more != 0
            } else {
                false
            };
            let mut events = Vec::new();
            for row in rows {
                let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
                let txid = u64::try_from(row.try_get::<i64, _>("txid")?).expect(ERROR_IMPOSSIBLE);
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = payload_json
                    .filter(|str| !str.is_empty())
                    .map(|str| serde_json::from_str(&str).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                if enforce_policy
                    && !Self::is_event_permitted(self, Some(part_id), obj_id, None).await?
                {
                    continue;
                }
                events.push(match row.try_get::<i64, _>("event_type")? {
                    EVENT_REMOVED => PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                        cursor: txid,
                        part_id,
                        obj_id,
                    }),
                    EVENT_ADDED => PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                        cursor: txid,
                        part_id,
                        obj_id,
                        payload,
                    }),
                    _ => PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor: txid,
                        part_ids: vec![part_id],
                        obj_id,
                        payload,
                    }),
                });
            }
            out.insert(
                part_id,
                PartPage {
                    events,
                    next_cursor: has_more.then(|| {
                        u64::try_from(cutoff_txid.expect(ERROR_IMPOSSIBLE)).expect(ERROR_IMPOSSIBLE)
                    }),
                },
            );
        }
        Ok(Ok(out))
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
        subscriber: PeerId,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        self.subscribe_with_policy(reqs, Some(subscriber)).await
    }

    async fn open_local_revision_reader(
        &self,
        reqs: SubPartsRequest,
        limits: big_sync_core::revisioned_store::RevisionReadLimits,
    ) -> Res<Result<Box<dyn big_sync::LocalPartRevisionReader>, ListPartsError>> {
        open_sqlite_local_revision_reader(
            self.sql.read_pool.clone(),
            self.scope().id(),
            Arc::clone(&self.local_revision_wakeups),
            reqs,
            limits,
        )
        .await
    }

    async fn ensure_part(&self, part_id: PartId) -> Res<()> {
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

    async fn set_obj_members(
        &self,
        doc: ObjId,
        agents: HashMap<PeerId, keyhive_core::access::Access>,
    ) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, doc).await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2",
            self.scope().id(),
            obj_ref
        )
        .execute(&mut *tx)
        .await?;
        for (principal, access) in &agents {
            sqlx::query!("INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)", self.scope().id(), obj_ref, Self::peer_blob(*principal), encode_access(access)).execute(&mut *tx).await?;
        }
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        tx.commit().await?;
        if let Some(payload_json) = payload_json.filter(|value| !value.is_empty()) {
            self.set_obj_payload(doc, serde_json::from_str(&payload_json).expect(ERROR_JSON))
                .await?;
        }
        Ok(())
    }

    async fn add_obj_member(
        &self,
        doc: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, doc).await?;
        sqlx::query!("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2 AND principal_id = ?3", self.scope().id(), obj_ref, Self::peer_blob(member)).execute(&mut *tx).await?;
        sqlx::query!("INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)", self.scope().id(), obj_ref, Self::peer_blob(member), encode_access(&access)).execute(&mut *tx).await?;
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        tx.commit().await?;
        if let Some(payload_json) = payload_json.filter(|value| !value.is_empty()) {
            self.set_obj_payload(doc, serde_json::from_str(&payload_json).expect(ERROR_JSON))
                .await?;
        }
        Ok(())
    }

    async fn remove_obj_member(&self, doc: ObjId, member: PeerId) -> Res<()> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(obj_ref) = self.core.find_obj_ref(doc).await? else {
            tx.commit().await?;
            return Ok(());
        };
        sqlx::query!("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2 AND principal_id = ?3", self.scope().id(), obj_ref, Self::peer_blob(member)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
