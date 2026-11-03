use super::*;

#[async_trait]
impl HostPartStore for SqliteBigRepoStore {
    async fn latest_revision(&self) -> Res<CursorIndex> {
        let revision = queries::latest_revision(&self.sql).await?;
        Ok(u64::try_from(revision)?)
    }

    async fn permitted_parts(
        &self,
        scope: PartScope,
        obj_id: ObjKey,
        principal: Option<PeerKey>,
    ) -> Res<Option<Vec<PartKey>>> {
        Self::permitted_parts(self, scope, obj_id, principal).await
    }
    async fn summarize_parts(
        &self,
        parts: HashSet<PartKey>,
    ) -> Res<Result<HashMap<PartKey, PartSummary>, ListPartsError>> {
        if parts.is_empty() {
            return Ok(Ok(HashMap::new()));
        }
        let mut hidden: Vec<_> = parts.intersection(&self.hidden_parts).cloned().collect();
        if !hidden.is_empty() {
            hidden.sort_unstable();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: hidden,
            }));
        }

        // Dynamic IN-list cardinality requires runtime SQL checking here.
        let mut query = Sql::new(
            "SELECT p.part_id, p.latest_cursor, COALESCE(b.live_count, 0) AS member_count
             FROM big_sync_parts p
             LEFT JOIN big_sync_buckets b
               ON b.scope_id = p.scope_id
              AND b.part_ref = p.part_ref
              AND b.level = 0
              AND b.buck_id = 0
             WHERE p.scope_id = ",
        );
        query.bind(self.scope().id());
        query.push(" AND p.part_id IN ");
        query.bind_list(parts.iter().cloned().map(Self::part_blob));
        let rows = query.query(&self.sql).await?.rows;

        if rows.len() != parts.len() {
            let found: HashSet<PartKey> = rows
                .iter()
                .map(|row| Self::part_from_blob(row.get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut missing: Vec<_> = parts.difference(&found).cloned().collect();
            missing.sort();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: missing,
            }));
        }

        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let part_id = Self::part_from_blob(row.get("part_id")?);
            let latest_cursor: i64 = row.get("latest_cursor")?;
            let member_count: i64 = row.get("member_count")?;
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

    async fn member_count(&self, part_id: PartKey) -> Res<u64> {
        let member_count =
            queries::member_count(&self.sql, self.scope().id(), Self::part_blob(part_id)).await?;
        Ok(member_count
            .map(|member_count| u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn part_dirty_count(
        &self,
        part_id: PartKey,
        principal: Option<PeerKey>,
        since: CursorIndex,
    ) -> Res<PartDirtyCount> {
        let since = i64::try_from(since).expect(ERROR_IMPOSSIBLE);
        // A member row carries the cursor of its last transition, removal included,
        // so a removal counts as a relevant change.
        let member_changes = queries::member_changes(
            &self.sql,
            self.scope().id(),
            Self::part_blob(part_id.clone()),
            since,
        )
        .await?;
        // One row per (part, principal). A revocation deletes that row, so a
        // revocation does not count here. `None` is the local principal, which
        // access rows do not gate, so it has no access half.
        let access_changes: i64 = match principal {
            Some(principal) => {
                queries::access_changes(
                    &self.sql,
                    self.scope().id(),
                    Self::part_blob(part_id),
                    Self::peer_blob(principal),
                    since,
                )
                .await?
            }
            None => 0,
        };
        Ok(PartDirtyCount {
            member_changes: u64::try_from(member_changes).expect(ERROR_IMPOSSIBLE),
            access_changes: u64::try_from(access_changes).expect(ERROR_IMPOSSIBLE),
        })
    }

    async fn obj_payload(&self, obj_id: ObjKey) -> Res<Option<ObjPayload>> {
        let payload = queries::obj_payload(&self.sql, self.scope().id(), Self::obj_blob(obj_id))
            .await?
            .flatten();
        payload
            .as_deref()
            .filter(|payload| !payload.is_empty())
            .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn set_obj_payload(&self, obj_id: ObjKey, payload: ObjPayload) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let events = self.set_obj_payload_in_tx(&mut tx, obj_id, payload).await?;
        tx.commit().await?;
        self.publish(events).await?;
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjKey) -> Res<Vec<PartKey>> {
        let rows = queries::obj_parts(
            &self.sql,
            self.scope().id(),
            Self::obj_blob(obj_id),
            EVENT_REMOVED,
        )
        .await?;
        Ok(rows.into_iter().map(Self::part_from_blob).collect())
    }

    async fn obj_exists(&self, obj_id: ObjKey) -> Res<bool> {
        let exists =
            queries::obj_exists(&self.sql, self.scope().id(), Self::obj_blob(obj_id)).await?;
        Ok(exists.is_some())
    }

    async fn partless_objects(&self, limit: u32, after: Option<ObjKey>) -> Res<Vec<ObjKey>> {
        // A pending want is not a live membership row, so an object that is only wanted
        // somewhere still reads as partless — the payload is what a want waits for.
        let rows = queries::partless_objects(
            &self.sql,
            self.scope().id(),
            after.map(Self::obj_blob),
            EVENT_REMOVED,
            i64::from(limit),
        )
        .await?;
        Ok(rows.into_iter().map(Self::obj_from_blob).collect())
    }

    async fn part_store_stats(&self) -> Res<PartStoreStats> {
        let row = queries::part_store_stats(&self.sql, self.scope().id(), EVENT_REMOVED).await?;
        Ok(PartStoreStats {
            payload_objects: u64::try_from(row.payload_objects).expect(ERROR_IMPOSSIBLE),
            payload_bytes: u64::try_from(row.payload_bytes).expect(ERROR_IMPOSSIBLE),
            partless_objects: u64::try_from(row.partless_objects).expect(ERROR_IMPOSSIBLE),
            live_rows: u64::try_from(row.live_rows).expect(ERROR_IMPOSSIBLE),
            dead_rows: u64::try_from(row.dead_rows).expect(ERROR_IMPOSSIBLE),
        })
    }

    async fn get_bucket_summary(&self, part_id: PartKey, id: BuckId) -> Res<BucketSummary> {
        self.core.bucket_summary_for_path(part_id, id).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        // A part the subscriber may not read has to read as unknown, exactly as a
        // part this scope does not have does, so the refusal cannot be told apart
        // from one. Answered before any other work on the request.
        if self
            .read_denied(ReadTarget::Part(req.part_id.clone()), subscriber)
            .await?
        {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let part_exists = queries::part_exists(
            &self.sql,
            self.scope().id(),
            Self::part_blob(req.part_id.clone()),
        )
        .await?;
        let Some(_) = part_exists else {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        };

        // Dynamic IN-list cardinality requires runtime SQL checking here.
        let mut query = Sql::new(
            "SELECT buck_id, level, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.bind(self.scope().id());
        query.push(" AND part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.bind(self.scope().id());
        query.push(" AND part_id = ");
        query.bind(Self::part_blob(req.part_id));
        query.push(")");
        query.push(" AND level <= ");
        query.bind(i64::from(req.to_level));
        query.push(" AND buck_id >= ");
        query.bind(Self::buck_i64(req.offset));
        query.push(" AND changed_at > ");
        query.bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" ORDER BY buck_id ASC LIMIT ");
        query.bind(i64::from(req.limit_hint) + i64::from(BuckId::ARITY));
        let rows = query.query(&self.sql).await?.rows;

        if rows.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        let mut out = Vec::new();
        let mut last_parent = None;
        for row in rows {
            let bucket = BucketSummary {
                id: Self::buck_id(row.get::<i64>("buck_id")?),
                len: u32::try_from(
                    u64::try_from(row.get::<i64>("live_count")?).expect(ERROR_IMPOSSIBLE)
                        + u64::try_from(row.get::<i64>("dead_count")?).expect(ERROR_IMPOSSIBLE),
                )
                .expect(ERROR_IMPOSSIBLE),
                live_count: u32::try_from(row.get::<i64>("live_count")?).expect(ERROR_IMPOSSIBLE),
                fp: (
                    Self::u64_from_db(row.get::<i64>("live_fp")?),
                    Self::u64_from_db(row.get::<i64>("dead_fp")?),
                ),
                changed_at: u64::try_from(row.get::<i64>("changed_at")?).expect(ERROR_IMPOSSIBLE),
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
        subscriber: PeerKey,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        // As above: an unreadable part reads as unknown, never as an empty page.
        if self
            .read_denied(ReadTarget::Part(req.part_id.clone()), subscriber)
            .await?
        {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        let part_exists = queries::part_exists(
            &self.sql,
            self.scope().id(),
            Self::part_blob(req.part_id.clone()),
        )
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
        let mut query =
            Sql::new("WITH requested(req_ord, buck_id, lower_index, upper_index, after_id) AS (");
        for (req_ord, buck_req) in req.buckets.iter().enumerate() {
            let (lower_index, upper_index) = big_sync::bucket_index_bounds(buck_req.buck_id);
            if req_ord > 0 {
                query.push(" UNION ALL ");
            }
            query.push("SELECT ");
            query.bind(i64::try_from(req_ord).expect(ERROR_IMPOSSIBLE));
            query.push(" AS req_ord, ");
            query.bind(Self::buck_i64(buck_req.buck_id));
            query.push(" AS buck_id, ");
            query.bind(i64::from(lower_index));
            query.push(" AS lower_index, ");
            if let Some(upper_index) = upper_index {
                query.bind(i64::from(upper_index));
            } else {
                query.push("NULL");
            }
            query.push(" AS upper_index, ");
            if let Some(after) = buck_req.after.clone() {
                query.bind(Self::obj_blob(after));
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
        query.bind(self.scope().id());
        query.push(" AND m.maybe_part_ref = (");
        query.push("SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.bind(self.scope().id());
        query.push(" AND part_id = ");
        query.bind(Self::part_blob(req.part_id));
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
        query.bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" WHERE o.buck_index >= r.lower_index");
        query.push(" AND (r.upper_index IS NULL OR o.buck_index < r.upper_index)");
        query.push(" AND (r.after_id IS NULL OR o.obj_id > r.after_id)");
        query.push(
            "
            )
            SELECT req_ord, buck_id, obj_id, event_type, payload_json, total_count
            FROM ranked
            WHERE row_num <= ",
        );
        query.bind(i64::from(req.limit_hint.max(1)));
        query.push(" ORDER BY req_ord, obj_id ASC");

        let rows = query.query(&self.sql).await?.rows;
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
            let req_ord = usize::try_from(row.get::<i64>("req_ord")?).expect(ERROR_IMPOSSIBLE);
            let page = pages.get_mut(req_ord).expect(ERROR_IMPOSSIBLE);
            page.total_count =
                u32::try_from(row.get::<i64>("total_count")?).expect(ERROR_IMPOSSIBLE);
            let obj_id = Self::obj_from_blob(row.get("obj_id")?);
            let dead = row.get::<i64>("event_type")? == EVENT_REMOVED;
            let fp = if dead {
                Fingerprint::new(
                    &req.seed,
                    &(
                        "big-sync-obj-fp-v1",
                        obj_id.clone(),
                        serde_json::Value::Null,
                    ),
                )
            } else {
                let payload_json: Option<String> = row.get("payload_json")?;
                let payload = payload_json
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(&payload_json).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id.clone(), payload))
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
                Some(page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id.clone())
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

    async fn add_obj_to_parts(&self, obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let events = self.add_obj_to_parts_in_tx(&mut tx, obj_id, parts).await?;
        tx.commit().await?;
        self.publish(events).await?;
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjKey, part_id: PartKey) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let Some(obj_ref) = self.core.find_obj_ref(obj_id.clone()).await? else {
            tx.commit().await?;
            return Ok(());
        };
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id.clone()).await?;
        let event = self
            .remove_obj_from_part_in_tx(&mut tx, &obj_id, obj_ref, &part_id, part_ref)
            .await?;
        tx.commit().await?;
        if let Some(event) = event {
            self.publish(vec![event]).await?;
        }
        Ok(())
    }

    /// Drop an object's payload: the object leaves every part it is in, in one transaction,
    /// and only then is the content cleared. The per-part transition is
    /// [`Self::remove_obj_from_part_in_tx`], so an object in several parts costs one commit
    /// rather than one per part. An unknown object and an object with no payload are no-ops.
    async fn remove_obj_payload(&self, obj_id: ObjKey) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let Some(obj_ref) = self.core.find_obj_ref(obj_id.clone()).await? else {
            tx.commit().await?;
            return Ok(());
        };
        let payload_json = queries::obj_payload_by_ref(&tx, obj_ref).await?.flatten();
        if payload_json.filter(|json| !json.is_empty()).is_none() {
            tx.commit().await?;
            return Ok(());
        }
        // The part-less row (`maybe_part_ref = 0`) is a content delivery like any other,
        // and there is no content behind it any more: it goes, so the object route cannot
        // book a payload-less change as a delivery.
        queries::delete_partless_member(&tx, self.scope().id(), obj_ref).await?;
        // Every part the object is in: live membership, and the pending wants that have no
        // live row yet. A want is membership too, and the payload is what it was waiting for.
        let part_rows = queries::member_parts(&tx, self.scope().id(), obj_ref).await?;
        let mut events = Vec::new();
        for row in part_rows {
            let part_id = Self::part_from_blob(row.part_id);
            if let Some(event) = self
                .remove_obj_from_part_in_tx(&mut tx, &obj_id, obj_ref, &part_id, row.part_ref)
                .await?
            {
                events.push(event);
            }
        }
        queries::clear_obj_payload(&tx, obj_ref).await?;
        tx.commit().await?;
        self.publish(events).await?;
        Ok(())
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerKey, part_id: PartKey) -> Res<CursorIndex> {
        let cursor = queries::get_peer_part_cursor(
            &self.sql,
            self.scope().id(),
            Self::peer_blob(peer_id),
            Self::part_blob(part_id),
        )
        .await?;
        Ok(cursor
            .map(|val| u64::try_from(val).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerKey,
        part_id: PartKey,
        cursor: CursorIndex,
    ) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
        queries::set_peer_part_cursor(
            &tx,
            self.scope().id(),
            Self::peer_blob(peer_id),
            part_ref,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_events(
        &self,
        parts: HashSet<PartKey>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>> {
        self.list_events_with_policy(parts, cursor, limit, true)
            .await
    }

    async fn list_events_with_policy(
        &self,
        parts: HashSet<PartKey>,
        cursor: CursorIndex,
        limit: u32,
        enforce_policy: bool,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>> {
        if enforce_policy && let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let mut out = HashMap::new();
        for part_id in parts {
            // Select the txid at the requested row limit first, then return the
            // complete txid boundary so events sharing a transaction are not split.
            // `added_at` gates a tombstone: a reader at `cursor` is told about a removal
            // only if it was also told about the add (`added_at <= cursor < txid`). The
            // same predicate holds in all three statements, so an excluded tombstone
            // neither consumes the page's limit nor reports more work waiting.
            let rows = queries::list_part_events(
                &self.sql,
                self.scope().id(),
                Self::part_blob(part_id.clone()),
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                EVENT_REMOVED,
                // At least one row, so a zero-length page still learns whether anything is
                // waiting: the boundary txid it then resumes from is what keeps "nothing
                // further is waiting" (`drained`) from stranding events.
                i64::from(limit.max(1)),
            )
            .await?;
            let cutoff_txid = rows.last().map(|row| row.txid);
            let has_more = if let Some(cutoff) = cutoff_txid {
                queries::has_more_part_events(
                    &self.sql,
                    self.scope().id(),
                    Self::part_blob(part_id.clone()),
                    i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                    cutoff,
                    EVENT_REMOVED,
                )
                .await?
            } else {
                false
            };
            // A read that returned as many rows as it asked for stopped at the limit
            // rather than at the end of the log, so it may not report caught-up even
            // when nothing lies beyond its boundary.
            let truncated = rows.len() >= usize::try_from(limit.max(1)).expect(ERROR_IMPOSSIBLE);
            let mut events = Vec::new();
            for row in rows {
                let obj_id = Self::obj_from_blob(row.obj_id);
                let txid = u64::try_from(row.txid).expect(ERROR_IMPOSSIBLE);
                let payload_json = row.payload_json;
                let payload = payload_json
                    .filter(|str| !str.is_empty())
                    .map(|str| serde_json::from_str(&str).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                if enforce_policy
                    && Self::permitted_parts(
                        self,
                        PartScope::Part(part_id.clone()),
                        obj_id.clone(),
                        None,
                    )
                    .await?
                    .is_some_and(|readable| readable.is_empty())
                {
                    continue;
                }
                events.push(match row.event_type {
                    EVENT_REMOVED => PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                        cursor: txid,
                        part_id: part_id.clone(),
                        obj_id,
                    }),
                    _ => PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor: txid,
                        part_ids: vec![part_id.clone()],
                        obj_id,
                        payload,
                    }),
                });
            }
            out.insert(
                part_id,
                PartPage {
                    events: if limit == 0 { Vec::new() } else { events },
                    // A zero-length page returns nothing, so the boundary row it read
                    // cannot be its resume point — resuming there would skip that event.
                    // It resumes from the caller's own position; otherwise it resumes
                    // past the last row it returned.
                    resume: if limit == 0 {
                        cursor
                    } else {
                        cutoff_txid.map_or(cursor, |cutoff| {
                            u64::try_from(cutoff).expect(ERROR_IMPOSSIBLE)
                        })
                    },
                    // A zero-length page may only claim caught-up when nothing is
                    // waiting; otherwise the page did not fill, so nothing further is.
                    // Only an un-truncated read that reached the end of the log can
                    // claim caught-up: a page that filled its limit stopped before the
                    // end, and a read whose rows were filtered by policy stopped early
                    // too. A zero-length page claims it only when its probe found
                    // nothing waiting.
                    drained: if limit == 0 {
                        cutoff_txid.is_none()
                    } else {
                        !has_more && !truncated
                    },
                },
            );
        }
        Ok(Ok(out))
    }

    async fn open_revision_reader(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn big_sync::LocalPartRevisionReader>, ListPartsError>> {
        open_sqlite_revision_reader(
            self.sql.clone(),
            self.scope().id(),
            Arc::clone(&self.local_revision_wakeups),
            reqs,
        )
        .await
    }

    async fn open_page_reader(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn big_sync::LocalPartRevisionReader>, ListPartsError>> {
        big_sync::open_sqlite_page_reader(
            self.sql.clone(),
            self.scope().id(),
            Arc::clone(&self.local_revision_wakeups),
            reqs,
        )
        .await
    }

    async fn open_revision_reader_all(
        &self,
        after: u64,
    ) -> Res<Result<Box<dyn big_sync::LocalPartRevisionReader>, ListPartsError>> {
        big_sync::open_sqlite_revision_reader_all(
            self.sql.clone(),
            self.scope().id(),
            Arc::clone(&self.local_revision_wakeups),
            after,
        )
        .await
    }

    async fn ensure_part(&self, part_id: PartKey) -> Res<()> {
        queries::ensure_part(&self.sql, self.scope().id(), Self::part_blob(part_id)).await?;
        Ok(())
    }

    async fn set_part_members(
        &self,
        part: PartKey,
        agents: HashMap<PeerKey, keyhive_core::access::Access>,
    ) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part).await?;
        let changed_at = i64::try_from(Self::next_cursor(&mut tx).await?).expect(ERROR_IMPOSSIBLE);
        queries::delete_part_members(&tx, self.scope().id(), part_ref).await?;
        for (principal, access) in &agents {
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
        tx.commit().await?;
        Ok(())
    }

    async fn add_part_member(
        &self,
        part: PartKey,
        member: PeerKey,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let mut tx = self.sql.begin_write().await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part).await?;
        let changed_at = i64::try_from(Self::next_cursor(&mut tx).await?).expect(ERROR_IMPOSSIBLE);
        queries::delete_part_member(
            &tx,
            self.scope().id(),
            part_ref,
            Self::peer_blob(member.clone()),
        )
        .await?;
        queries::insert_part_member(
            &tx,
            self.scope().id(),
            part_ref,
            Self::peer_blob(member),
            encode_access(&access),
            changed_at,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_part_member(&self, part: PartKey, member: PeerKey) -> Res<()> {
        let tx = self.sql.begin_write().await?;
        let Some(part_ref) = self.core.find_part_ref(part).await? else {
            tx.commit().await?;
            return Ok(());
        };
        queries::delete_part_member(&tx, self.scope().id(), part_ref, Self::peer_blob(member))
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl SqliteBigRepoStore {
    /// [`HostPartStore::add_obj_to_parts`] inside a transaction the caller owns, returning the
    /// events for the caller to publish once it commits. A caller that is already recording why
    /// the object exists (the store saving a sedimentree, say) uses this so the membership lands
    /// with that fact rather than after it.
    ///
    /// An object whose payload the store does not hold has no membership to record yet, so it
    /// becomes a pending want.
    pub(crate) async fn add_obj_to_parts_in_tx(
        &self,
        tx: &mut Tx,
        obj_id: ObjKey,
        parts: Vec<PartKey>,
    ) -> Res<Vec<PartEvent>> {
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id.clone()).await?;
        let payload_json = queries::obj_payload_by_ref(tx, obj_ref).await?.flatten();
        let Some(payload_json) = payload_json.filter(|str| !str.is_empty()) else {
            for part_id in parts {
                let part_ref = self.core.ensure_part_ref(tx, part_id).await?;
                queries::insert_pending_member(tx, self.scope().id(), obj_ref, part_ref).await?;
            }
            return Ok(Vec::new());
        };
        let payload: ObjPayload = serde_json::from_str(&payload_json).wrap_err(ERROR_JSON)?;
        let mut events = Vec::new();
        for part_id in parts {
            let part_ref = self.core.ensure_part_ref(tx, part_id.clone()).await?;
            let old = self
                .load_member_state(tx, part_id.clone(), obj_id.clone())
                .await?;
            if matches!(old, MemberState::Live(_)) {
                continue;
            }
            let cursor = Self::next_cursor(tx).await?;
            // `added_at` is stamped by the row that becomes present: the insert arm is a
            // first add, and the conflict arm restamps only a row that was absent
            // (`EVENT_REMOVED`), so a present-to-present touch keeps its add cursor.
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
            self.apply_bucket_transition(
                tx,
                part_id.clone(),
                obj_id.clone(),
                cursor,
                &old,
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
            queries::delete_pending_member(tx, self.scope().id(), obj_ref, part_ref).await?;
            events.push(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor,
                part_ids: vec![part_id],
                obj_id: obj_id.clone(),
                payload: payload.clone(),
            }));
        }
        Ok(events)
    }

    /// Drop an object's membership of `/seds`, in the caller's transaction. The store owns that
    /// partition because it is the side that knows whether a sedimentree exists, so the removal
    /// happens where the tree is deleted. Reports the event to publish after committing, or
    /// `None` when there was no live membership.
    pub(crate) async fn remove_seds_membership_in_tx(
        &self,
        tx: &mut Tx,
        id: sedimentree_core::id::SedimentreeId,
    ) -> Res<Option<PartEvent>> {
        let obj_id = crate::DocumentId::new(*id.as_bytes());
        let part_id = crate::seds_part_id();
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id.clone()).await?;
        let part_ref = self.core.ensure_part_ref(tx, part_id.clone()).await?;
        self.remove_obj_from_part_in_tx(tx, &obj_id, obj_ref, &part_id, part_ref)
            .await
    }
    /// Take the object out of one part, in the caller's transaction: clear the pending want
    /// for that part, mark the member row absent, and advance the part and bucket cursors.
    /// Reports the `Removed` event, or `None` when the part held no live membership to
    /// remove — removal is idempotent per part, which is what makes it reusable for
    /// dropping a payload that spans several parts.
    ///
    /// Both the insert and the update arm of the membership write leave `added_at` alone:
    /// the row being removed is live, so it already carries the cursor it became present at,
    /// and the insert arm of the upsert cannot fire while that holds.
    async fn remove_obj_from_part_in_tx(
        &self,
        tx: &mut Tx,
        obj_id: &ObjKey,
        obj_ref: i64,
        part_id: &PartKey,
        part_ref: i64,
    ) -> Res<Option<PartEvent>> {
        queries::delete_pending_member(tx, self.scope().id(), obj_ref, part_ref).await?;
        let old_state = self
            .load_member_state(tx, part_id.clone(), obj_id.clone())
            .await?;
        let MemberState::Live(old_payload) = old_state else {
            return Ok(None);
        };
        let cursor = Self::next_cursor(tx).await?;
        queries::insert_dead_member(
            tx,
            self.scope().id(),
            obj_ref,
            part_ref,
            EVENT_REMOVED,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
        )
        .await?;
        self.core
            .apply_bucket_transition(
                tx,
                part_id.clone(),
                obj_id.clone(),
                cursor,
                &MemberState::Live(old_payload),
                &MemberState::Dead,
            )
            .await?;
        queries::advance_part_cursor(
            tx,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            self.scope().id(),
            part_ref,
        )
        .await?;
        Ok(Some(PartEvent::Removed(
            big_sync_core::rpc::ObjRemovedFromPart {
                cursor,
                part_id: part_id.clone(),
                obj_id: obj_id.clone(),
            },
        )))
    }
}
