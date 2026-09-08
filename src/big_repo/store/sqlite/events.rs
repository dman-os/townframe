use super::*;
use sqlx::{QueryBuilder, Row};

pub(crate) const KEYHIVE_ADMISSION_READER_GROUP_PART: &str = "group_part";
pub(crate) const KEYHIVE_ADMISSION_READER_CAUSAL_CHECKPOINT: &str = "causal_checkpoint";
pub(crate) const KEYHIVE_ADMISSION_READER_AUTOMERGE_FRONTIER: &str = "automerge_frontier";
pub(crate) const KEYHIVE_ADMISSION_READER_PREKEY_JANITOR: &str = "prekey_janitor";

impl SqliteBigRepoStore {
    /// All part IDs currently present in this store's scope.
    ///
    /// Used by the `All` worker-scope path to watch every part without
    /// enumerating keyhive groups (a keyhive enumeration would miss parts
    /// for groups not yet in the hive and pays a graph walk).
    pub(crate) async fn list_parts(&self) -> Res<HashSet<PartId>> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar!(
            "SELECT part_id AS 'part_id: Vec<u8>'
             FROM big_sync_parts WHERE scope_id = ?",
            self.scope().id()
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
        Ok(rows.into_iter().map(Self::part_from_blob).collect())
    }

    #[cfg(test)]
    pub(crate) async fn keyhive_event_log_cursor(&self) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT MAX(seq) AS \"seq: i64\" FROM big_repo_keyhive_event_log WHERE scope_id = ?",
            self.scope().id()
        )
        .fetch_one(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn save_keyhive_event(
        &self,
        hash: subduction_keyhive::storage::StorageHash,
        data: Vec<u8>,
        source: Option<subduction_keyhive::KeyhivePeerId>,
    ) -> Result<bool, SqliteBigRepoStoreError> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let next_seq: i64 = sqlx::query_scalar!(
            "SELECT COALESCE(MAX(seq), 0) + 1
                 FROM big_repo_keyhive_event_log
                WHERE scope_id = ?1",
            self.scope().id()
        )
        .fetch_one(&mut *tx)
        .await?;
        let inserted = sqlx::query!(
            "INSERT INTO big_repo_keyhive_event_log(
                scope_id, seq, event_hash, event_bytes, source_id
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(scope_id, event_hash) DO NOTHING",
            self.scope().id(),
            next_seq,
            hash.as_bytes().as_slice(),
            &data,
            source.map(|peer| peer.verifying_key().to_vec())
        )
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        // The event log is the durable WAL. Admission is recorded only after
        // projection effects are applied; archived rows are pruned using the
        // archived_through watermark.
        tx.commit().await?;
        Ok(inserted)
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
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;

        let mut known = HashSet::with_capacity(hashes.len());
        for chunk in hashes.chunks(400) {
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            let mut query = QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT event_hash FROM big_repo_keyhive_event_log WHERE scope_id = ",
            );
            query
                .push_bind(self.scope().id())
                .push(" AND event_hash IN (");
            for (index, hash) in chunk.iter().enumerate() {
                if index != 0 {
                    query.push(", ");
                }
                query.push_bind(hash.as_bytes().as_slice());
            }
            query.push(")");
            for row in query.build().fetch_all(&mut *tx).await? {
                known.insert(row.try_get::<Vec<u8>, _>("event_hash")?);
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
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            let mut query = QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT event_hash FROM big_repo_keyhive_admissions WHERE scope_id = ",
            );
            query
                .push_bind(self.scope().id())
                .push(" AND event_hash IN (");
            for (index, hash) in chunk.iter().enumerate() {
                if index != 0 {
                    query.push(", ");
                }
                query.push_bind(hash.as_bytes().as_slice());
            }
            query.push(")");
            for row in query.build().fetch_all(&mut *tx).await? {
                existing.insert(row.try_get::<Vec<u8>, _>("event_hash")?);
            }
        }
        let missing: Vec<_> = hashes
            .into_iter()
            .filter(|hash| !existing.contains(hash.as_bytes().as_slice()))
            .collect();
        let head: i64 = sqlx::query_scalar!(
            "SELECT COALESCE(MAX(seq), 0) AS \"head!: i64\"
               FROM big_repo_keyhive_admissions
              WHERE scope_id = ?1",
            self.scope().id()
        )
        .fetch_one(&mut *tx)
        .await?;
        let mut next_seq = head + 1;
        for chunk in missing.chunks(160) {
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            // Dynamic IN-list cardinality requires runtime SQL checking here.
            let mut query = QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO big_repo_keyhive_admissions(scope_id, seq, event_hash, source_id) VALUES ",
            );
            for (offset, hash) in chunk.iter().enumerate() {
                if offset != 0 {
                    query.push(", ");
                }
                query
                    .push("(")
                    .push_bind(self.scope().id())
                    .push(", ")
                    .push_bind(next_seq + i64::try_from(offset).expect(ERROR_IMPOSSIBLE))
                    .push(", ")
                    .push_bind(hash.as_bytes().as_slice())
                    .push(", ")
                    .push_bind(&source_id)
                    .push(")");
            }
            query.push(" ON CONFLICT(scope_id, event_hash) DO NOTHING");
            query.build().execute(&mut *tx).await?;
            next_seq += i64::try_from(chunk.len()).expect(ERROR_IMPOSSIBLE);
        }
        tx.commit().await?;
        Ok(Self::u64_from_db(
            head + i64::try_from(missing.len()).expect(ERROR_IMPOSSIBLE),
        ))
    }

    pub(crate) async fn admission_head(&self) -> Res<u64> {
        let head: i64 = sqlx::query_scalar!("SELECT COALESCE(MAX(seq), 0) AS \"head!: i64\" FROM big_repo_keyhive_admissions WHERE scope_id = ?",
            self.scope().id()
        )
            .fetch_one(&self.sql.read_pool)
            .await?;
        Ok(Self::u64_from_db(head))
    }

    #[cfg(test)]
    pub(crate) async fn archived_through(&self) -> Res<u64> {
        let seq: i64 = sqlx::query_scalar!("SELECT COALESCE(MAX(seq), 0) AS \"seq!: i64\" FROM big_repo_keyhive_archived_through WHERE scope_id = ?",
            self.scope().id()
        )
        .fetch_one(&self.sql.read_pool)
        .await?;
        Ok(Self::u64_from_db(seq))
    }

    pub(crate) async fn set_archived_through(&self, seq: u64) -> Res<()> {
        sqlx::query!(
            "INSERT INTO big_repo_keyhive_archived_through(
                 scope_id, seq
             )
             VALUES (?1, ?2)
             ON CONFLICT(scope_id) DO UPDATE
                 SET seq = MAX(seq, excluded.seq)",
            self.scope().id(),
            i64::try_from(seq).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn register_keyhive_admission_reader(
        &self,
        reader: &str,
        cursor: u64,
    ) -> Res<()> {
        sqlx::query!(
            "INSERT INTO big_repo_keyhive_admission_readers(scope_id, reader, seq)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, reader) DO UPDATE
                 SET seq = MAX(seq, excluded.seq)",
            self.scope().id(),
            reader,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn advance_keyhive_admission_reader_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        reader: &str,
        cursor: u64,
    ) -> Res<()> {
        sqlx::query!(
            "INSERT INTO big_repo_keyhive_admission_readers(scope_id, reader, seq)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope_id, reader) DO UPDATE
                 SET seq = MAX(seq, excluded.seq)",
            self.scope().id(),
            reader,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Durable admission cursor for the prekey janitor worker. Like the
    /// causal-checkpoint cursor, this is at-least-once: the tail may re-read a
    /// row after a crash, and the janitor's published-set precheck makes the
    /// reprocessing of a consumed prekey a no-op.
    ///
    /// A raw (unchecked) query: adding a new `query!` macro would require a
    /// `cargo sqlx prepare` cache regeneration for one small reader lookup.
    pub(crate) async fn prekey_janitor_cursor(&self) -> Res<u64> {
        let cursor: Option<i64> = sqlx::query_scalar(
            "SELECT seq FROM big_repo_keyhive_admission_readers
              WHERE scope_id = ?1 AND reader = ?2",
        )
        .bind(self.scope().id())
        .bind(KEYHIVE_ADMISSION_READER_PREKEY_JANITOR)
        .fetch_optional(&self.sql.read_pool)
        .await?;
        Ok(cursor.map(Self::u64_from_db).unwrap_or(0))
    }

    pub(crate) async fn advance_prekey_janitor_cursor(&self, cursor: u64) -> Res<()> {
        self.register_keyhive_admission_reader(KEYHIVE_ADMISSION_READER_PREKEY_JANITOR, cursor)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn advance_keyhive_admission_reader(
        &self,
        reader: &str,
        cursor: u64,
    ) -> Res<()> {
        sqlx::query!(
            "UPDATE big_repo_keyhive_admission_readers
                SET seq = MAX(seq, ?3)
              WHERE scope_id = ?1 AND reader = ?2",
            self.scope().id(),
            reader,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }

    async fn archived_admission_floor(&self) -> Res<u64> {
        let floor: i64 = sqlx::query_scalar!(
            "SELECT COALESCE(
                       (SELECT MIN(a.seq)
                          FROM big_repo_keyhive_admissions a
                         WHERE a.scope_id = ?1
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM big_repo_keyhive_event_tombstones t
                                WHERE t.scope_id = a.scope_id
                                  AND t.event_hash = a.event_hash
                           )),
                       (SELECT COALESCE(MAX(seq), 0) + 1
                          FROM big_repo_keyhive_admissions
                         WHERE scope_id = ?1)
                   ) - 1 AS \"floor!: i64\"",
            self.scope().id()
        )
        .fetch_one(&self.sql.read_pool)
        .await?;
        Ok(Self::u64_from_db(floor))
    }

    pub(crate) async fn prune_admitted_events(&self) -> Res<u64> {
        let archive_floor = self.archived_admission_floor().await?;
        self.set_archived_through(archive_floor).await?;
        let reader_floor = sqlx::query!(
            "SELECT MIN(seq) AS \"reader_floor: i64\"
               FROM big_repo_keyhive_admission_readers
              WHERE scope_id = ?1",
            self.scope().id()
        )
        .fetch_one(&self.sql.read_pool)
        .await?;
        let Some(reader_floor) = reader_floor.reader_floor else {
            return Ok(0);
        };
        let watermark = archive_floor.min(Self::u64_from_db(reader_floor));
        if watermark == 0 {
            return Ok(0);
        }
        let result = sqlx::query!(
            "DELETE FROM big_repo_keyhive_event_log
             WHERE scope_id = ?1
               AND EXISTS (
                   SELECT 1
                     FROM big_repo_keyhive_admissions a
                     JOIN big_repo_keyhive_event_tombstones t
                       ON t.scope_id = a.scope_id AND t.event_hash = a.event_hash
                    WHERE a.scope_id = big_repo_keyhive_event_log.scope_id
                      AND a.event_hash = big_repo_keyhive_event_log.event_hash
                      AND a.seq <= ?2
               )",
            self.scope().id(),
            i64::try_from(watermark).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub(crate) async fn run_maintenance(&self) -> Res<u64> {
        let pruned = self.prune_admitted_events().await?;
        // SQLite's compile-time SQLx macros cannot execute no-column PRAGMAs.
        sqlx::query("PRAGMA optimize")
            .execute(&self.sql.write_pool)
            .await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.sql.write_pool)
            .await?;
        Ok(pruned)
    }

    pub(crate) async fn admission_events_after(
        &self,
        cursor: u64,
        limit: u32,
    ) -> Res<Vec<AdmissionEventRow>> {
        let rows = sqlx::query!(
            "SELECT a.seq, a.event_hash, a.source_id, e.event_bytes
             FROM big_repo_keyhive_admissions a
             JOIN big_repo_keyhive_event_log e
               ON e.scope_id = a.scope_id AND e.event_hash = a.event_hash
             WHERE a.scope_id = ?1 AND a.seq > ?2
             ORDER BY a.seq
             LIMIT ?3",
            self.scope().id(),
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
            i64::from(limit)
        )
        .fetch_all(&self.sql.read_pool)
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
        let rows = sqlx::query_as!(
            KeyhiveEventQueryRow,
            "SELECT event_hash, event_bytes, source_id
                 FROM big_repo_keyhive_event_log
                WHERE scope_id = ?1
                ORDER BY seq",
            self.scope().id(),
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
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
        let rows = sqlx::query_as!(
            KeyhiveEventQueryRow,
            "SELECT l.event_hash, l.event_bytes, l.source_id
                 FROM big_repo_keyhive_event_log l
                WHERE l.scope_id = ?1
                  AND NOT EXISTS (
                        SELECT 1
                          FROM big_repo_keyhive_admissions a
                         WHERE a.scope_id = l.scope_id
                           AND a.event_hash = l.event_hash
                  )
                ORDER BY l.seq",
            self.scope().id(),
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
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
        let rows = sqlx::query_as!(
            KeyhiveEventQueryRow,
            "SELECT e.event_hash, e.event_bytes, e.source_id
                 FROM big_repo_keyhive_event_log e
                WHERE e.scope_id = ?1
                ORDER BY e.seq",
            self.scope().id(),
        )
        .fetch_all(&self.sql.read_pool)
        .await?;
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
        sqlx::query!(
            "INSERT OR IGNORE INTO big_repo_keyhive_event_tombstones(
                 scope_id, event_hash
             )
             VALUES (?1, ?2)",
            self.scope().id(),
            hash.as_bytes().as_slice()
        )
        .execute(&self.sql.write_pool)
        .await?;
        Ok(())
    }
}
