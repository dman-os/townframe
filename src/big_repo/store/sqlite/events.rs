use super::*;

/// The walker identities of the consumers that gate admission-log retention.
///
/// Each pair is the one place its consumer is named: it keys the consumer's
/// `delta_walker_progress` row — which is the authoritative "reconciled through"
/// value — and its `"{namespace}/{consumer_id}"` form (the format
/// `SqliteDeltaWalkerStateRepo::retention_reader_id_of` builds) keys the retention
/// registration that keeps the consumer's unprocessed admission rows from being
/// pruned.
pub(crate) const KEYHIVE_ADMISSION_CONSUMER_GROUP_PART: (&str, &str) =
    ("big_repo.group_part", "admission");
pub(crate) const KEYHIVE_ADMISSION_CONSUMER_CAUSAL_CHECKPOINT: (&str, &str) =
    ("big_repo.causal_checkpoint", "admission");
pub(crate) const KEYHIVE_ADMISSION_CONSUMER_AUTOMERGE_FRONTIER: (&str, &str) =
    ("big_repo.automerge_frontier", "keyhive-admission");
pub(crate) const KEYHIVE_ADMISSION_CONSUMER_PREKEY_JANITOR: (&str, &str) =
    ("big_repo.prekey_janitor", "admission");

impl SqliteBigRepoStore {
    /// All part IDs currently present in this store's scope.
    ///
    /// Used by the `All` worker-scope path to watch every part without
    /// enumerating keyhive groups (a keyhive enumeration would miss parts
    /// for groups not yet in the hive and pays a graph walk).
    pub(crate) async fn list_parts(&self) -> Res<HashSet<PartKey>> {
        let rows = queries::list_parts(&self.sql, self.scope().id()).await?;
        Ok(rows.into_iter().map(Self::part_from_blob).collect())
    }

    #[cfg(test)]
    pub(crate) async fn keyhive_event_log_cursor(&self) -> Res<u64> {
        let cursor = queries::keyhive_event_log_cursor(&self.sql, self.scope().id()).await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn save_keyhive_event(
        &self,
        hash: subduction_keyhive::storage::StorageHash,
        data: Vec<u8>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> Result<bool, SqliteBigRepoStoreError> {
        let tx = self.sql.begin_write().await?;
        let next_seq = queries::next_keyhive_event_seq(&tx, self.scope().id()).await?;
        let inserted = queries::save_keyhive_event(
            &tx,
            self.scope().id(),
            next_seq,
            hash.as_bytes().to_vec(),
            data,
            source.map(|peer| peer.verifying_key().to_vec()),
        )
        .await?
        .rows_affected
            > 0;
        // The event log is the durable WAL. Admission is recorded only after
        // projection effects are applied; archived rows are pruned using the
        // archived_through watermark.
        tx.commit().await?;
        Ok(inserted)
    }

    /// This node's Keyhive ingestion ledger: how many events reached the raw
    /// log, how many Keyhive applied, and which peers the unapplied remainder
    /// came from.
    ///
    /// The unapplied remainder is the question that separates the failure modes
    /// during a divergence: a node with nothing unapplied has applied
    /// everything it received, so an event it is missing never arrived, while a
    /// non-empty remainder points at an apply-side or ordering defect.
    pub(crate) async fn keyhive_event_ledger(&self) -> Res<KeyhiveEventLedger> {
        let scope = self.scope().id();
        let counts = queries::keyhive_event_counts(&self.sql, scope).await?;
        let mut ledger = KeyhiveEventLedger {
            logged: counts.logged.max(0) as u64,
            admitted: counts.admitted.max(0) as u64,
            admission_head: counts.head.max(0) as u64,
            unapplied_by_source: BTreeMap::new(),
        };
        let rows = queries::unapplied_keyhive_event_counts(&self.sql, scope).await?;
        for row in rows {
            let source = row.source_id;
            let count = row.count;
            ledger
                .unapplied_by_source
                .insert(source, count.max(0) as u64);
        }
        Ok(ledger)
    }

    pub(crate) async fn append_admitted_events(
        &self,
        mut hashes: Vec<subduction_keyhive::storage::StorageHash>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> Res<u64> {
        if hashes.is_empty() {
            return self.admission_head().await;
        }
        hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        hashes.dedup_by(|left, right| left.as_bytes() == right.as_bytes());
        let source_id = source.map(|peer| peer.verifying_key().to_vec());
        let tx = self.sql.begin_write().await?;

        let mut known = HashSet::with_capacity(hashes.len());
        for chunk in hashes.chunks(400) {
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            let mut query =
                Sql::new("SELECT event_hash FROM big_repo_keyhive_event_log WHERE scope_id = ");
            query.bind(self.scope().id()).push(" AND event_hash IN ");
            query.bind_list(chunk.iter().map(|hash| hash.as_bytes().as_slice()));
            for row in query.query(&tx).await?.rows {
                known.insert(row.get::<Vec<u8>>("event_hash")?);
            }
        }
        if known.len() != hashes.len() {
            return Err(ferr!(
                "reported incorporated hash missing from raw event log"
            ));
        }
        #[cfg(test)]
        if FAIL_NEXT_ADMISSION.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(ferr!("test fault between event and admission inserts"));
        }

        let mut existing = HashSet::new();
        for chunk in hashes.chunks(400) {
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            let mut query =
                Sql::new("SELECT event_hash FROM big_repo_keyhive_admissions WHERE scope_id = ");
            query.bind(self.scope().id()).push(" AND event_hash IN ");
            query.bind_list(chunk.iter().map(|hash| hash.as_bytes().as_slice()));
            for row in query.query(&tx).await?.rows {
                existing.insert(row.get::<Vec<u8>>("event_hash")?);
            }
        }
        let missing: Vec<_> = hashes
            .into_iter()
            .filter(|hash| !existing.contains(hash.as_bytes().as_slice()))
            .collect();
        let head = queries::admission_head(&tx, self.scope().id()).await?;
        let mut next_seq = head + 1;
        for chunk in missing.chunks(160) {
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            let mut query = Sql::new(
                "INSERT INTO big_repo_keyhive_admissions(scope_id, seq, event_hash, source_id) VALUES ",
            );
            for (offset, hash) in chunk.iter().enumerate() {
                if offset != 0 {
                    query.push(", ");
                }
                query
                    .push("(")
                    .bind(self.scope().id())
                    .push(", ")
                    .bind(next_seq + i64::try_from(offset).expect(ERROR_IMPOSSIBLE))
                    .push(", ")
                    .bind(hash.as_bytes().as_slice())
                    .push(", ")
                    .bind(source_id.as_deref())
                    .push(")");
            }
            query.push(" ON CONFLICT(scope_id, event_hash) DO NOTHING");
            query.execute(&tx).await?;
            next_seq += i64::try_from(chunk.len()).expect(ERROR_IMPOSSIBLE);
        }
        tx.commit().await?;
        let seq = Self::u64_from_db(head + i64::try_from(missing.len()).expect(ERROR_IMPOSSIBLE));
        // Record the committed head at the durable choke point. Read back
        // synchronously by the keyhive sync-done observer to stamp a completion
        // with how far admission has actually progressed. `fetch_max` keeps the
        // watermark monotonic under concurrent appends racing their commits
        // (a lower committed seq never regresses it).
        self.admission_watermark
            .fetch_max(seq, std::sync::atomic::Ordering::SeqCst);
        Ok(seq)
    }

    /// Cheap synchronous read of the last committed admission seq (see the
    /// `admission_watermark` field docs). No DB round-trip.
    pub(crate) fn admission_watermark(&self) -> u64 {
        self.admission_watermark
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) async fn admission_head(&self) -> Res<u64> {
        let head = queries::admission_head(&self.sql, self.scope().id()).await?;
        Ok(Self::u64_from_db(head))
    }

    /// The sequence through which this scope's admission log has actually been
    /// pruned: the bound the DELETE reached, i.e. the highest `seq` whose event
    /// bytes are gone from `big_repo_keyhive_event_log`.
    ///
    /// This is the bound that was deleted, *not* the tombstone prefix, and the
    /// distinction is the contract. A tombstoned admission above this marker
    /// still has its bytes, so a reader below the marker is still served it;
    /// recording the prefix would claim a gap this prune did not create, and a
    /// consumer resuming from that overstated marker would skip wake-ups it
    /// could still have been given.
    ///
    /// Because that bound is `min(tombstone prefix, MIN(progress) over the
    /// registered readers)`, it is at or below every registered consumer's own
    /// durable cursor — a registered consumer cannot open below this floor.
    /// Only a consumer absent from `big_repo_keyhive_admission_readers` when the
    /// prune advanced (one that registers on its first run, or an identity added
    /// to the registry by a later release) can find itself below it; for those,
    /// the floor is the signal that it cannot be woken for the gap (ADR 013 §9)
    /// and must rebuild its sinks from live Keyhive state and resume here rather
    /// than replay a history it cannot be told about. Set by
    /// `prune_admitted_events`, which runs during maintenance.
    pub(crate) async fn archived_through(&self) -> Res<u64> {
        let seq = queries::archived_through(&self.sql, self.scope().id()).await?;
        Ok(Self::u64_from_db(seq))
    }

    pub(crate) async fn set_archived_through(&self, seq: u64) -> Res<()> {
        queries::set_archived_through(
            &self.sql,
            self.scope().id(),
            i64::try_from(seq).expect(ERROR_IMPOSSIBLE),
        )
        .await?;
        Ok(())
    }

    /// Register `reader` as a consumer whose unprocessed admission rows must not
    /// be pruned.
    ///
    /// Registration carries no value: the retention floor is read from the
    /// consumer's `delta_walker_progress` row, so a registered consumer that has
    /// not started (no progress row) pins the floor at 0 until it does.
    /// Idempotent.
    pub(crate) async fn register_keyhive_admission_reader(&self, reader: &str) -> Res<()> {
        queries::register_keyhive_admission_reader(&self.sql, self.scope().id(), reader.to_owned())
            .await?;
        Ok(())
    }

    /// Register every admission consumer this runtime runs, so pruning can never
    /// outrun a consumer that exists but has not started yet.
    ///
    /// The retention floor [`Self::prune_admitted_events`] computes is a `MIN`
    /// over **registered** readers only. The registry therefore has to be
    /// complete before any maintenance run: a consumer that has not registered is
    /// invisible to the floor, and its unprocessed admission events can be pruned
    /// from under it. Registering all four identities here, once, before the
    /// workers and the maintenance loop are spawned, makes the registry complete
    /// by construction instead of depending on each worker's spawn ordering
    /// against the maintenance timer. Idempotent.
    pub(crate) async fn register_admission_consumers(&self) -> Res<()> {
        for (namespace, consumer_id) in [
            KEYHIVE_ADMISSION_CONSUMER_GROUP_PART,
            KEYHIVE_ADMISSION_CONSUMER_CAUSAL_CHECKPOINT,
            KEYHIVE_ADMISSION_CONSUMER_AUTOMERGE_FRONTIER,
            KEYHIVE_ADMISSION_CONSUMER_PREKEY_JANITOR,
        ] {
            self.register_keyhive_admission_reader(
                &big_sync::delta_walker_state::SqliteDeltaWalkerStateRepo::retention_reader_id_of(
                    namespace,
                    consumer_id,
                ),
            )
            .await?;
        }
        Ok(())
    }

    #[cfg(test)]
    /// The consumer's own "reconciled through" revision, read from the delta
    /// walker's `delta_walker_progress` row (0 when the consumer has not started).
    ///
    /// This is the one value fences, observability and the retention floor read:
    /// it is the row the consumer's ack commits inside the same transaction as its
    /// effects, so no second watermark can lag it.
    ///
    /// Test-only: the harness fences (`test2::harness::fixtures`) are the only readers;
    /// the production fences compare the hub's in-memory watermarks instead.
    pub(crate) async fn admission_consumer_progress(
        &self,
        namespace: &str,
        consumer_id: &str,
    ) -> Res<u64> {
        let revision = queries::admission_consumer_progress(
            &self.sql,
            namespace.to_owned(),
            consumer_id.to_owned(),
        )
        .await?;
        Ok(Self::u64_from_db(revision))
    }

    /// The tombstone prefix: the highest `seq` below which every admission has
    /// been tombstoned, i.e. the most this scope may ever delete.
    ///
    /// This is an *upper bound on what may be pruned*, not the prune's result.
    /// The bytes of a tombstoned admission stay readable until a reader's own
    /// progress lets the DELETE reach them, which is why `prune_admitted_events`
    /// intersects this with the reader floor and records that intersection in
    /// `archived_through` rather than this prefix — the prefix can run ahead of
    /// every registered consumer's cursor while their rows are still in the log.
    async fn archived_admission_floor(&self) -> Res<u64> {
        let floor = queries::archived_admission_floor(&self.sql, self.scope().id()).await?;
        Ok(Self::u64_from_db(floor))
    }

    /// Prune the admission log up to the watermark no registered consumer needs.
    ///
    /// The reader floor is the minimum "reconciled through" revision over the
    /// registered consumers, read from each one's `delta_walker_progress` row (the
    /// walker commits it with the effects it covers, so it is the only writer). A
    /// registered consumer whose progress row is absent — it registered but has
    /// not started — reads as 0 and pins the floor there: history a consumer that
    /// has not started cannot be told about must not be pruned from under it.
    ///
    /// `archived_through` advances to the same bound this DELETE applies, never
    /// to the tombstone prefix the reader floor may hold it below.
    pub(crate) async fn prune_admitted_events(&self) -> Res<u64> {
        let archive_floor = self.archived_admission_floor().await?;
        let reader_floor = queries::admission_reader_floor(&self.sql, self.scope().id())
            .await?
            .unwrap_or_default();
        let watermark = archive_floor.min(Self::u64_from_db(reader_floor));
        if watermark == 0 {
            return Ok(0);
        }
        // Record the bound the DELETE below applies rather than `archive_floor`.
        // The tombstone prefix can run ahead of the reader floor, and the bytes
        // in between are still readable: advancing the marker to the prefix would
        // assert a gap this prune does not create, put a registered consumer
        // below the very floor computed to stay under it, and make a consumer
        // clamping to the marker skip wake-ups that are still in the log.
        self.set_archived_through(watermark).await?;
        let result = queries::prune_admitted_events(
            &self.sql,
            self.scope().id(),
            i64::try_from(watermark).expect(ERROR_IMPOSSIBLE),
        )
        .await?;
        Ok(result.rows_affected)
    }

    pub(crate) async fn run_maintenance(&self) -> Res<u64> {
        let pruned = self.prune_admitted_events().await?;
        self.sql.execute("PRAGMA optimize", &[]).await?;
        self.sql
            .execute("PRAGMA wal_checkpoint(TRUNCATE)", &[])
            .await?;
        Ok(pruned)
    }

    pub(crate) async fn admission_events_after(
        &self,
        cursor: u64,
        limit: u32,
    ) -> Res<Vec<AdmissionEventRow>> {
        let rows = queries::admission_events_after(
            &self.sql,
            self.scope().id(),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            i64::from(limit),
        )
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AdmissionEventRow {
                    seq: Self::u64_from_db(row.seq),
                    event_hash: row.event_hash.try_into().expect(ERROR_IMPOSSIBLE),
                    source_id: row.source_id,
                    bytes: row.event_bytes,
                })
            })
            .collect()
    }

    pub(crate) async fn load_keyhive_events(
        &self,
    ) -> Result<Vec<(subduction_keyhive::storage::StorageHash, Vec<u8>)>, SqliteBigRepoStoreError>
    {
        let rows = queries::load_keyhive_events(&self.sql, self.scope().id()).await?;
        rows.into_iter()
            .map(|row| {
                let hash = Self::decode_id(row.event_hash)?;
                Ok((
                    subduction_keyhive::storage::StorageHash::new(hash),
                    row.event_bytes,
                ))
            })
            .collect()
    }

    pub(crate) async fn unadmitted_keyhive_events(
        &self,
    ) -> Result<
        Vec<([u8; 32], Vec<u8>, Option<subduction_keyhive::KeyhivePeerId>)>,
        SqliteBigRepoStoreError,
    > {
        let rows = queries::unadmitted_keyhive_events(&self.sql, self.scope().id()).await?;
        rows.into_iter()
            .map(|row| {
                let hash = Self::decode_id(row.event_hash)?;
                let source = row.source_id.map(|bytes| {
                    subduction_keyhive::KeyhivePeerId::from_bytes(
                        bytes
                            .try_into()
                            .expect("stored Keyhive peer id must be 32 bytes"),
                    )
                });
                Ok((hash, row.event_bytes, source))
            })
            .collect()
    }

    pub(crate) async fn load_keyhive_events_with_source(
        &self,
    ) -> Result<
        Vec<(
            subduction_keyhive::storage::StorageHash,
            Vec<u8>,
            Option<subduction_keyhive::KeyhivePeerId>,
        )>,
        SqliteBigRepoStoreError,
    > {
        let rows = queries::load_keyhive_events(&self.sql, self.scope().id()).await?;
        rows.into_iter()
            .map(|row| {
                let hash = Self::decode_id(row.event_hash)?;
                let source = row.source_id.map(|bytes| {
                    subduction_keyhive::KeyhivePeerId::from_bytes(
                        bytes
                            .try_into()
                            .expect("stored Keyhive peer id must be 32 bytes"),
                    )
                });
                Ok((
                    subduction_keyhive::storage::StorageHash::new(hash),
                    row.event_bytes,
                    source,
                ))
            })
            .collect()
    }

    pub(crate) async fn delete_keyhive_event(
        &self,
        hash: subduction_keyhive::storage::StorageHash,
    ) -> Result<(), SqliteBigRepoStoreError> {
        queries::delete_keyhive_event(&self.sql, self.scope().id(), hash.as_bytes().to_vec())
            .await?;
        Ok(())
    }
}
