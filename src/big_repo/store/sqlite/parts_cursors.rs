use super::*;

#[async_trait]
impl HostPartStore for SqliteBigRepoStore {
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
              AND b.part_id = p.part_id
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
             WHERE scope_id = ?1 AND part_id = ?2 AND level = 0 AND buck_id = 0",
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
        let row = sqlx::query(
            "SELECT payload_json
                 FROM big_sync_objs
                 WHERE scope_id = ?1 AND obj_id = ?2",
        )
        .bind(self.scope().id())
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&self.sql.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Option<String> = row.try_get("payload_json")?;
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
        let rows = sqlx::query(
            "SELECT part_id FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL
             UNION
             SELECT part_id FROM big_sync_pending_members
             WHERE scope_id = ?1 AND obj_id = ?2
             ORDER BY part_id ASC",
        )
        .bind(self.scope().id())
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
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
        self.bucket_summary_for_path(part_id, id).await
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
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
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
                    m.obj_id,
                    m.removed_at,
                    o.payload_json,
                    COUNT(*) OVER (PARTITION BY r.req_ord) AS total_count,
                    ROW_NUMBER() OVER (PARTITION BY r.req_ord ORDER BY m.obj_id ASC) AS row_num
                FROM requested r
                JOIN big_sync_members m
                  ON m.scope_id = ",
        );
        query.push_bind(self.scope().id());
        query.push(" AND m.part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(" JOIN big_sync_buckets s ON s.scope_id = m.scope_id AND s.part_id = m.part_id AND s.buck_id = r.buck_id AND s.changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" AND m.obj_id >= r.lower_id");
        query.push(" AND (r.upper_id IS NULL OR m.obj_id < r.upper_id)");
        query.push(" AND (r.after_id IS NULL OR m.obj_id > r.after_id)");
        query.push(
            "
                LEFT JOIN big_sync_objs o
                  ON o.scope_id = m.scope_id AND o.obj_id = m.obj_id
            )
            SELECT req_ord, buck_id, obj_id, removed_at, payload_json, total_count
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
            let dead = row.try_get::<Option<i64>, _>("removed_at")?.is_some();
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
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut parts = parts;
        // The global partition records local readability decisions made by
        // group-part reconciliation. Remote membership gossip must never
        // populate it: big-sync exists here for live updates, not discovery.
        parts.retain(|part_id| *part_id != crate::GLOBAL_PART_ID);
        parts.sort();
        parts.dedup();
        let mut part_states = Vec::with_capacity(parts.len());
        for part_id in &parts {
            part_states.push((
                *part_id,
                self.load_member_state(&mut tx, *part_id, obj_id).await?,
            ));
        }
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json AS \"payload_json!: String\"
               FROM big_sync_objs
              WHERE scope_id = ?1
                AND obj_id = ?2",
            self.scope().id(),
            Self::obj_blob(obj_id),
        )
        .fetch_optional(&mut *tx)
        .await?;
        let event_payload: Option<ObjPayload> = payload_json
            .as_deref()
            .filter(|payload_json| !payload_json.is_empty())
            .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
            .transpose()?;
        let payload_json = payload_json.filter(|payload_json| !payload_json.is_empty());
        sqlx::query!(
            "INSERT INTO big_sync_objs(scope_id, obj_id, payload_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, obj_id) DO NOTHING",
            self.scope().id(),
            Self::obj_blob(obj_id),
            payload_json.as_deref()
        )
        .execute(&mut *tx)
        .await?;
        let Some(payload) = event_payload else {
            for part_id in parts {
                sqlx::query!(
                    "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, part_id, obj_id)
                     VALUES (?1, ?2, ?3)",
                    self.scope().id(),
                    Self::part_blob(part_id),
                    Self::obj_blob(obj_id)
                )
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            return Ok(());
        };
        let added_payload_json = Some(serde_json::to_string(&payload).wrap_err(ERROR_JSON)?);
        let changed_parts: Vec<_> = part_states
            .into_iter()
            .filter(|(_, old_state)| !matches!(old_state, MemberState::Live(_)))
            .collect();
        if changed_parts.is_empty() {
            tx.commit().await?;
            return Ok(());
        }
        let cursor = Self::next_cursor(&mut tx).await?;
        let mut events = Vec::with_capacity(changed_parts.len());
        for (part_id, old_state) in changed_parts {
            sqlx::query!(
                "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
                 VALUES (?1, ?2, 0)
                 ON CONFLICT(scope_id, part_id) DO NOTHING",
                self.scope().id(),
                Self::part_blob(part_id)
            )
            .execute(&mut *tx)
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
            added_payload_json.as_deref()
        )
            .execute(&mut *tx)
            .await?;
            self.apply_bucket_transition(
                &mut tx,
                part_id,
                obj_id,
                cursor,
                &old_state,
                &MemberState::Live(payload.clone()),
            )
            .await?;
            sqlx::query!(
                "UPDATE big_sync_parts
                 SET latest_cursor = ?1
                 WHERE scope_id = ?2 AND part_id = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.scope().id(),
                Self::part_blob(part_id)
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members
                     WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
                self.scope().id(),
                Self::part_blob(part_id),
                Self::obj_blob(obj_id)
            )
            .execute(&mut *tx)
            .await?;
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
        let obj_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope().id(),
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(_) = obj_exists else {
            tx.commit().await?;
            return Ok(());
        };
        sqlx::query!(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
            self.scope().id(),
            Self::part_blob(part_id)
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "DELETE FROM big_sync_pending_members
             WHERE scope_id = ?1 AND part_id = ?2 AND obj_id = ?3",
            self.scope().id(),
            Self::part_blob(part_id),
            Self::obj_blob(obj_id)
        )
        .execute(&mut *tx)
        .await?;
        let current_state = self.load_member_state(&mut tx, part_id, obj_id).await?;
        let MemberState::Live(old_payload) = current_state else {
            tx.commit().await?;
            return Ok(());
        };

        let cursor = Self::next_cursor(&mut tx).await?;
        sqlx::query!(
            "UPDATE big_sync_members
             SET removed_at = ?1, changed_at = ?1, latest_cursor = ?1
             WHERE scope_id = ?2 AND part_id = ?3 AND obj_id = ?4",
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            self.scope().id(),
            Self::part_blob(part_id),
            Self::obj_blob(obj_id)
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "UPDATE big_sync_parts
             SET latest_cursor = MAX(latest_cursor, ?1)
             WHERE scope_id = ?2 AND part_id = ?3",
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            self.scope().id(),
            Self::part_blob(part_id)
        )
        .execute(&mut *tx)
        .await?;
        self.apply_bucket_transition(
            &mut tx,
            part_id,
            obj_id,
            cursor,
            &MemberState::Live(old_payload),
            &MemberState::Dead,
        )
        .await?;

        let live_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE scope_id = ?1 AND obj_id = ?2 AND removed_at IS NULL",
            self.scope().id(),
            Self::obj_blob(obj_id)
        )
        .fetch_one(&mut *tx)
        .await?;
        if live_count == 0 {
            sqlx::query!(
                "UPDATE big_sync_objs
                 SET payload_json = NULL
                 WHERE scope_id = ?1 AND obj_id = ?2",
                self.scope().id(),
                Self::obj_blob(obj_id)
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
            "SELECT cursor
             FROM big_sync_peer_cursors
             WHERE scope_id = ?1 AND peer_id = ?2 AND part_id = ?3",
            self.scope().id(),
            Self::peer_blob(peer_id),
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor
            .map(|cursor| u64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()> {
        sqlx::query!(
            "INSERT INTO big_sync_parts(scope_id, part_id, latest_cursor)
             VALUES (?1, ?2, 0)
             ON CONFLICT(scope_id, part_id) DO NOTHING",
            self.scope().id(),
            Self::part_blob(part_id)
        )
        .execute(&self.sql.write_pool)
        .await?;
        sqlx::query!(
            "INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_id, cursor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, peer_id, part_id) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)",
            self.scope().id(),
            Self::peer_blob(peer_id),
            Self::part_blob(part_id),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
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
        if enforce_policy {
            let summaries = self.summarize_parts(parts.clone()).await?;
            if let Err(err) = summaries {
                return Ok(Err(err));
            }
        }
        let mut out = HashMap::new();
        for part_id in parts {
            let rows = sqlx::query(
                "SELECT members.obj_id, members.added_at, members.added_payload_json, members.changed_at, members.removed_at, members.latest_cursor, objs.payload_json
                 FROM big_sync_members members
                 LEFT JOIN big_sync_objs objs
                   ON objs.scope_id = members.scope_id AND objs.obj_id = members.obj_id
                 WHERE members.scope_id = ?1 AND members.part_id = ?2 AND members.latest_cursor > ?3
                 ORDER BY latest_cursor ASC
                 LIMIT ?4").bind(self.scope().id()).bind(Self::part_blob(part_id)).bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)).bind(i64::from(limit) + 1)
            .fetch_all(&self.sql.read_pool)
            .await?;
            let mut events = Vec::new();
            for row in rows {
                let row_cursor: i64 = row.try_get("latest_cursor")?;
                let added_at: i64 = row.try_get("added_at")?;
                let removed_at: Option<i64> = row.try_get("removed_at")?;
                let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
                let added_payload_json: Option<String> = row.try_get("added_payload_json")?;
                let added_payload = added_payload_json
                    .as_deref()
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
                    .transpose()?;
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = payload_json
                    .as_deref()
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(payload_json).wrap_err(ERROR_JSON))
                    .transpose()?;
                if let Some(removed_at) = removed_at {
                    if removed_at > i64::try_from(cursor).expect(ERROR_IMPOSSIBLE) {
                        events.push((
                            removed_at,
                            PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                                cursor: u64::try_from(removed_at).expect(ERROR_IMPOSSIBLE),
                                part_id,
                                obj_id,
                            }),
                        ));
                    }
                    continue;
                }
                if added_at > i64::try_from(cursor).expect(ERROR_IMPOSSIBLE) {
                    events.push((
                        added_at,
                        PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                            cursor: u64::try_from(added_at).expect(ERROR_IMPOSSIBLE),
                            part_id,
                            obj_id,
                            payload: added_payload
                                .clone()
                                .expect("visible membership requires added payload"),
                        }),
                    ));
                }
                if row_cursor > added_at
                    && row_cursor > i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
                {
                    events.push((
                        row_cursor,
                        PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                            cursor: u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE),
                            part_ids: vec![part_id],
                            obj_id,
                            payload: payload.expect(ERROR_IMPOSSIBLE),
                        }),
                    ));
                }
            }
            events.sort_by_key(|(cursor, _)| *cursor);
            let mut next_cursor = None;
            let limit_usize = usize::try_from(limit).expect(ERROR_IMPOSSIBLE);
            if limit_usize != 0 && events.len() > limit_usize {
                let next = events[limit_usize - 1].0;
                next_cursor = Some(u64::try_from(next).expect(ERROR_IMPOSSIBLE));
            }
            let events = events
                .into_iter()
                .take(usize::try_from(limit).expect(ERROR_IMPOSSIBLE))
                .map(|(_, event)| event)
                .collect();
            out.insert(
                part_id,
                PartPage {
                    events,
                    next_cursor,
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

    async fn subscribe_local(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        self.subscribe_with_policy(reqs, None).await
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
        let doc_blob = Self::obj_blob(doc);
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope().id(),
            &doc_blob
        )
        .execute(&mut *tx)
        .await?;
        for (principal, access) in &agents {
            sqlx::query!(
                "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)",
            self.scope().id(),
            &doc_blob,
            Self::peer_blob(*principal),
            encode_access(access)
        )
            .execute(&mut *tx)
            .await?;
        }
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope().id(),
            &doc_blob
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        tx.commit().await?;
        if let Some(payload_json) = payload_json.filter(|value| !value.is_empty()) {
            let payload = serde_json::from_str(&payload_json).expect(ERROR_JSON);
            // Re-emit the current object payload after a membership change.
            // A peer may have previously received the membership event while
            // unauthorized, leaving a pending object with no payload. The
            // normal part event promotes that pending object and wakes sync.
            self.set_obj_payload(doc, payload).await?;
        }
        Ok(())
    }
    async fn add_obj_member(
        &self,
        doc: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let doc_blob = Self::obj_blob(doc);
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2 AND principal_id = ?3",
            self.scope().id(),
            &doc_blob,
            Self::peer_blob(member)
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "INSERT INTO big_sync_syncable(scope_id, obj_id, principal_id, access_level) VALUES (?1, ?2, ?3, ?4)",
            self.scope().id(),
            &doc_blob,
            Self::peer_blob(member),
            encode_access(&access)
        )
        .execute(&mut *tx)
        .await?;
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.scope().id(),
            &doc_blob
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        // Re-emit the current object payload as a Changed event in the same
        // transaction as the grant. Delivery-time policy filtering denies
        // events for not-yet-authorized subscribers while cursors keep
        // advancing, so without this resurrection event a peer granted later
        // could never learn an already-advertised object exists.
        let events = match payload_json
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| serde_json::from_str::<ObjPayload>(value).wrap_err(ERROR_JSON))
            .transpose()?
        {
            Some(payload) => self.set_obj_payload_in_tx(&mut tx, doc, payload).await?,
            None => Vec::new(),
        };
        tx.commit().await?;
        self.publish(events).await?;
        Ok(())
    }

    async fn remove_obj_member(&self, doc: ObjId, member: PeerId) -> Res<()> {
        let doc_blob = Self::obj_blob(doc);
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query!("DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_id = ?2 AND principal_id = ?3",
            self.scope().id(),
            &doc_blob,
            Self::peer_blob(member)
        )
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
