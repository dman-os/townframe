use super::*;

#[tokio::test]
async fn sqlite_migrations_and_fingerprint_bitcasts_roundtrip() -> Res<()> {
    let sql = test_sql().await?;
    SqliteCore::init_schema(&sql, BuckId::MAX_LEVEL).await?;
    SqliteCore::init_schema(&sql, BuckId::MAX_LEVEL).await?;
    let status = sql
        .migration_status_for(queries::MIGRATION_TABLE, queries::MIGRATIONS)
        .await?;
    assert_eq!(
        status
            .iter()
            .map(|(migration, state)| (migration.version, *state))
            .collect::<Vec<_>>(),
        vec![
            (1, sqlite_rs::MigrationState::Applied),
            (2, sqlite_rs::MigrationState::Applied),
            (3, sqlite_rs::MigrationState::Applied),
        ]
    );
    let core = SqliteCore::new(sql.clone(), "bitcasts", BuckId::MAX_LEVEL).await?;
    let part_id = test_part_id(1);
    for live_fp in [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX] {
        let dead_fp = !live_fp;
        let mut tx = sql.begin_write().await?;
        let part_ref = core.ensure_part_ref(&mut tx, part_id.clone()).await?;
        queries::upsert_bucket(
            &tx,
            core.scope_id,
            part_ref,
            0,
            0,
            7,
            2,
            3,
            SqliteCore::db_from_u64(live_fp),
            SqliteCore::db_from_u64(dead_fp),
        )
        .await?;
        tx.commit().await?;
        assert_eq!(
            core.bucket_summary_for_path(part_id.clone(), BuckId::ROOT)
                .await?,
            BucketSummary {
                id: BuckId::ROOT,
                len: 5,
                live_count: 2,
                fp: (live_fp, dead_fp),
                changed_at: 7
            }
        );
    }
    Ok(())
}
