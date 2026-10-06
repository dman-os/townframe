//! Durable, node-local origins for finite-lived processor task authorizations.
//! Processor slot/capture identity deduplicates the obligation; task identity
//! identifies one finite execution authorization and changes for a successor.

use crate::interlude::*;
use crate::tasks::PoolTaskId;
use sqlx::Row;
use sqlx_utils_rs::SqlCtx;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAuthorization {
    pub task_id: PoolTaskId,
    pub nonce: [u8; 32],
    pub originated_at_secs: i64,
    pub not_after_secs: i64,
}

impl TaskAuthorization {
    /// The signed task input carries this authorization. Executors recompute
    /// its identity from the proven slot/capture; no origin database is needed
    /// remotely, and changing the deadline cannot renew an existing task ID.
    pub fn expected_task_id(&self, slot_id: [u8; 32], capture_generation: [u8; 32]) -> PoolTaskId {
        let mut hash = blake3::Hasher::new();
        hash.update(b"daybook/processor-task-authorization/v1\0");
        hash.update(&slot_id);
        hash.update(&capture_generation);
        hash.update(&self.nonce);
        hash.update(&self.originated_at_secs.to_be_bytes());
        hash.update(&self.not_after_secs.to_be_bytes());
        PoolTaskId::new(*hash.finalize().as_bytes())
    }
}

pub(crate) struct ProcessorTaskOrigins {
    sql: SqlCtx,
}

impl ProcessorTaskOrigins {
    pub async fn open(sql: SqlCtx) -> Res<Self> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS processor_task_origins (
                 slot_id BLOB NOT NULL
               , capture_generation BLOB NOT NULL
               , successor INTEGER NOT NULL
               , task_id BLOB NOT NULL UNIQUE
               , nonce BLOB NOT NULL
               , originated_at_secs INTEGER NOT NULL
               , not_after_secs INTEGER NOT NULL
               , PRIMARY KEY (slot_id, capture_generation, successor)
             )",
        )
        .execute(&sql.write_pool)
        .await?;
        // Prototype rows lack the nonce/origin witness required by the signed
        // authorization identity. They cannot be upgraded by inventing either
        // value: that would renew or replace an existing authorization silently.
        let columns: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM pragma_table_info('processor_task_origins') ORDER BY cid",
        )
        .fetch_all(&sql.read_pool)
        .await?;
        eyre::ensure!(
            columns
                == [
                    "slot_id",
                    "capture_generation",
                    "successor",
                    "task_id",
                    "nonce",
                    "originated_at_secs",
                    "not_after_secs"
                ],
            "processor_task_origins has unsupported schema; explicit authorization migration is required (cannot reconstruct nonce/origin from prototype rows)"
        );
        Ok(Self { sql })
    }

    /// Reuse the original authorization on replay under the same lifetime policy.
    /// Expiry or a changed lifetime creates a successor for unmet work; neither
    /// changes a predecessor's deadline or the stable capture's settlement.
    /// Keep predecessor rows: durable outcome acknowledgement/retention is a
    /// separate lifecycle and must not be inferred from this selector change.
    pub(super) async fn originate(
        &self,
        slot_id: [u8; 32],
        capture_generation: [u8; 32],
        configuration: &super::domain::ProcessorConfiguration,
        now_secs: i64,
    ) -> Res<TaskAuthorization> {
        let mut tx = self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let previous = sqlx::query(
            "SELECT successor
                  , task_id
                  , nonce
                  , originated_at_secs
                  , not_after_secs
               FROM processor_task_origins
              WHERE slot_id = ?
                AND capture_generation = ?
              ORDER BY successor DESC
              LIMIT 1",
        )
        .bind(slot_id.as_slice())
        .bind(capture_generation.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
        let successor = if let Some(previous) = previous {
            let not_after_secs: i64 = previous.try_get("not_after_secs")?;
            let originated_at_secs: i64 = previous.try_get("originated_at_secs")?;
            let same_lifetime = i128::from(not_after_secs) - i128::from(originated_at_secs)
                == i128::from(configuration.task_lifetime_secs.get());
            if now_secs <= not_after_secs && same_lifetime {
                let task_bytes: Vec<u8> = previous.try_get("task_id")?;
                let nonce_bytes: Vec<u8> = previous.try_get("nonce")?;
                let authorization = TaskAuthorization {
                    task_id: PoolTaskId::new(
                        task_bytes.try_into().expect("stored task ID is 32 bytes"),
                    ),
                    nonce: nonce_bytes
                        .try_into()
                        .expect("stored authorization nonce is 32 bytes"),
                    originated_at_secs: previous.try_get("originated_at_secs")?,
                    not_after_secs,
                };
                tx.commit().await?;
                return Ok(authorization);
            }
            previous
                .try_get::<i64, _>("successor")?
                .checked_add(1)
                .expect("processor task successor sequence exhausted")
        } else {
            0
        };
        let nonce = rand::random();
        let mut authorization = TaskAuthorization {
            task_id: PoolTaskId::new(nonce),
            nonce,
            originated_at_secs: now_secs,
            not_after_secs: configuration.task_deadline_secs(now_secs),
        };
        authorization.task_id = authorization.expected_task_id(slot_id, capture_generation);
        sqlx::query(
            "INSERT INTO processor_task_origins (
                 slot_id
               , capture_generation
               , successor
               , task_id
               , nonce
               , originated_at_secs
               , not_after_secs
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(slot_id.as_slice())
        .bind(capture_generation.as_slice())
        .bind(successor)
        .bind(authorization.task_id.to_bytes32().as_slice())
        .bind(authorization.nonce.as_slice())
        .bind(authorization.originated_at_secs)
        .bind(authorization.not_after_secs)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(authorization)
    }
}

#[cfg(test)]
mod tests {
    use super::super::domain::ProcessorConfiguration;
    use super::*;
    use std::num::NonZeroU64;

    #[tokio::test]
    async fn prototype_origins_require_explicit_migration_without_rewriting_authorizations()
    -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        sqlx::query(
            "CREATE TABLE processor_task_origins (
             slot_id BLOB NOT NULL
           , capture_generation BLOB NOT NULL
           , successor INTEGER NOT NULL
           , task_id BLOB NOT NULL UNIQUE
           , not_after_secs INTEGER NOT NULL
           , PRIMARY KEY (slot_id, capture_generation, successor)
         )",
        )
        .execute(&sql.write_pool)
        .await?;
        sqlx::query("INSERT INTO processor_task_origins VALUES (?, ?, 0, ?, 160)")
            .bind([1_u8; 32].as_slice())
            .bind([2_u8; 32].as_slice())
            .bind([3_u8; 32].as_slice())
            .execute(&sql.write_pool)
            .await?;
        let error = match ProcessorTaskOrigins::open(sql.clone()).await {
            Ok(_) => panic!("prototype origin must not silently acquire a new authorization"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("explicit authorization migration")
        );
        let retained: (Vec<u8>, i64) =
            sqlx::query_as("SELECT task_id, not_after_secs FROM processor_task_origins")
                .fetch_one(&sql.read_pool)
                .await?;
        assert_eq!(retained, (vec![3; 32], 160));
        Ok(())
    }

    #[tokio::test]
    async fn replay_reopen_and_concurrent_origins_preserve_deadline_until_successor() -> Res<()> {
        let temp = tempfile::tempdir()?;
        let config = crate::app::SqlConfig::file(temp.path().join("origins.sqlite"));
        let sql = crate::app::open_sql_ctx(config.clone()).await?;
        let origins = ProcessorTaskOrigins::open(sql).await?;
        let mut configuration = ProcessorConfiguration {
            manifest: daybook_types::url::build_facet_ref(
                &big_repo::DocumentId::new([1; 32]).to_string(),
                &daybook_types::doc::FacetKey::from(
                    daybook_types::doc::WellKnownFacetTag::PlugManifest,
                ),
            )?,
            config_documents: Default::default(),
            task_lifetime_secs: NonZeroU64::new(60).unwrap(),
        };
        let first = origins
            .originate([1; 32], [2; 32], &configuration, 100)
            .await?;
        assert_eq!(first.not_after_secs, 160);
        assert_eq!(
            origins
                .originate([1; 32], [2; 32], &configuration, 150)
                .await?,
            first
        );
        drop(origins);

        let sql = crate::app::open_sql_ctx(config).await?;
        let origins = ProcessorTaskOrigins::open(sql.clone()).await?;
        assert_eq!(
            origins
                .originate([1; 32], [2; 32], &configuration, 160)
                .await?,
            first
        );
        let (next, repeated) = tokio::try_join!(
            origins.originate([1; 32], [2; 32], &configuration, 161),
            origins.originate([1; 32], [2; 32], &configuration, 161),
        )?;
        assert_eq!(next, repeated);
        assert_ne!(next.task_id, first.task_id);
        assert_eq!(next.not_after_secs, 221);
        assert_eq!(
            origins
                .originate([1; 32], [2; 32], &configuration, 200)
                .await?,
            next
        );
        let predecessor_deadline: i64 = sqlx::query_scalar(
            "SELECT not_after_secs FROM processor_task_origins WHERE task_id = ?",
        )
        .bind(first.task_id.to_bytes32().as_slice())
        .fetch_one(&sql.read_pool)
        .await?;
        assert_eq!(predecessor_deadline, first.not_after_secs);
        // A lifetime-policy change renews the authorization, not the work
        // generation or the old deadline. Otherwise the new witness rejects it.
        configuration.task_lifetime_secs = NonZeroU64::new(30).unwrap();
        let (changed, replay) = tokio::try_join!(
            origins.originate([1; 32], [2; 32], &configuration, 201),
            origins.originate([1; 32], [2; 32], &configuration, 201),
        )?;
        assert_eq!(changed, replay);
        assert_ne!(changed.task_id, next.task_id);
        assert_eq!(changed.originated_at_secs, 201);
        assert_eq!(changed.not_after_secs, 231);
        let retained_deadline: i64 = sqlx::query_scalar(
            "SELECT not_after_secs FROM processor_task_origins WHERE task_id = ?",
        )
        .bind(next.task_id.to_bytes32().as_slice())
        .fetch_one(&sql.read_pool)
        .await?;
        assert_eq!(retained_deadline, 221);
        Ok(())
    }

    #[test]
    fn successor_identity_binds_capture_nonce_and_absolute_lifetime() {
        let slot = [1; 32];
        let capture = [2; 32];
        let mut original = TaskAuthorization {
            task_id: PoolTaskId::new([3; 32]),
            nonce: [3; 32],
            originated_at_secs: 100,
            not_after_secs: 160,
        };
        original.task_id = original.expected_task_id(slot, capture);
        let remote: TaskAuthorization =
            serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
        assert_eq!(remote, original);
        assert_eq!(remote.expected_task_id(slot, capture), original.task_id);
        assert_ne!(remote.expected_task_id([4; 32], capture), original.task_id);
        assert_ne!(remote.expected_task_id(slot, [4; 32]), original.task_id);
        let mut changed = remote.clone();
        changed.nonce[0] ^= 1;
        assert_ne!(changed.expected_task_id(slot, capture), original.task_id);
        changed = remote.clone();
        changed.originated_at_secs += 1;
        assert_ne!(changed.expected_task_id(slot, capture), original.task_id);
        changed = remote.clone();
        changed.not_after_secs += 1;
        assert_ne!(changed.expected_task_id(slot, capture), original.task_id);
        changed = remote;
        changed.task_id = PoolTaskId::new([9; 32]);
        assert_ne!(changed.expected_task_id(slot, capture), changed.task_id);
    }
}
