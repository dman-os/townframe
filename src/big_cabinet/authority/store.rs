#[cfg(test)]
use super::provisioning::provisioning_test_faults;
use super::{
    ActorProvisionIntent, ActorPublicKey, AuthorityError, CapId, CapRecord, NODE_KEY, NODE_SERVICE,
    OPERATOR_KEY, Rights, fixed_bytes,
};
use rand::RngCore as _;
use secrets_rs::{SecretStore, SecretsError, SigningKeyHandle};
use sqlx::{Row, Sqlite, SqlitePool, Transaction, sqlite::SqlitePoolOptions};
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub(super) struct AuthorityStateStore {
    pub(super) pool: SqlitePool,
}

impl AuthorityStateStore {
    pub(super) async fn boot(path: impl AsRef<Path>) -> Result<Self, AuthorityError> {
        let path = path.as_ref();
        if path != Path::new(":memory:")
            && let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(std::time::Duration::from_secs(10));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        for legacy in [
            "DROP TABLE IF EXISTS authority_tokens",
            "DROP TABLE IF EXISTS authority_revocations",
            "DROP TABLE IF EXISTS authority_root",
        ] {
            sqlx::query(legacy).execute(&pool).await?;
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_lock (
                 lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1)
               , lock_nonce INTEGER NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query("INSERT OR IGNORE INTO authority_lock (lock_id, lock_nonce) VALUES (1, 0)")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_actors (
                 public_key BLOB PRIMARY KEY
               , key_name TEXT NOT NULL UNIQUE
               , uuid TEXT UNIQUE
               , url TEXT UNIQUE
               , parent_uuid TEXT
               , certificate BLOB
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_actor_intents (
                 intent_id BLOB PRIMARY KEY
               , key_name TEXT NOT NULL UNIQUE
               , public_key BLOB
               , preexisting INTEGER NOT NULL CHECK (preexisting IN (0, 1))
               , cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_pending IN (0, 1))
             )",
        )
        .execute(&pool)
        .await?;
        // Schema migration itself participates in the same SQLite write
        // reservation as authority operations. Two old-schema owners can
        // therefore not both observe a missing column and race ALTER TABLE.
        let mut schema_transaction = pool.begin().await?;
        sqlx::query("UPDATE authority_lock SET lock_nonce = lock_nonce + 1 WHERE lock_id = 1")
            .execute(&mut *schema_transaction)
            .await?;
        let actor_intent_columns = sqlx::query("PRAGMA table_info(authority_actor_intents)")
            .fetch_all(&mut *schema_transaction)
            .await?;
        if !actor_intent_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .is_ok_and(|name| name == "cleanup_pending")
        }) {
            sqlx::query(
                "ALTER TABLE authority_actor_intents
                 ADD COLUMN cleanup_pending INTEGER NOT NULL DEFAULT 0
                    CHECK (cleanup_pending IN (0, 1))",
            )
            .execute(&mut *schema_transaction)
            .await?;
        }
        if let Err(error) = schema_transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "authority intent schema migration commit outcome is unknown: {error}"
            )));
        }
        {
            let actor_cols = sqlx::query("PRAGMA table_info(authority_actors)")
                .fetch_all(&pool)
                .await?;
            let has = |name: &str| {
                actor_cols.iter().any(|row| {
                    row.try_get::<String, _>("name")
                        .is_ok_and(|col_name| col_name == name)
                })
            };
            for (col, ddl) in [
                ("uuid", "ALTER TABLE authority_actors ADD COLUMN uuid TEXT"),
                ("url", "ALTER TABLE authority_actors ADD COLUMN url TEXT"),
                (
                    "parent_uuid",
                    "ALTER TABLE authority_actors ADD COLUMN parent_uuid TEXT",
                ),
                (
                    "certificate",
                    "ALTER TABLE authority_actors ADD COLUMN certificate BLOB",
                ),
            ] {
                if !has(col) {
                    sqlx::query(ddl).execute(&pool).await?;
                }
            }
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_caps (
                 cap_id BLOB PRIMARY KEY
               , object TEXT NOT NULL
               , rights INTEGER NOT NULL
               , parent_id BLOB
               , root_actor BLOB
               , retracted INTEGER NOT NULL DEFAULT 0 CHECK (retracted IN (0, 1))
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_grants (
                 subject TEXT NOT NULL
               , object TEXT NOT NULL
               , access TEXT NOT NULL
               , PRIMARY KEY (subject, object, access)
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_root_cap (
                 root_id INTEGER PRIMARY KEY CHECK (root_id = 1)
               , cap_id BLOB NOT NULL
               , operator_uuid TEXT NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_boot_intents (
                 key_name TEXT PRIMARY KEY
               , public_key BLOB
               , preexisting INTEGER NOT NULL CHECK (preexisting IN (0, 1))
             )",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    pub(super) async fn begin_serialized(&self) -> Result<Transaction<'_, Sqlite>, AuthorityError> {
        #[cfg(test)]
        provisioning_test_faults::notify_serialized_begin_probe();
        let mut transaction = self.pool.begin().await?;
        // The first write takes SQLite's database-wide write reservation. Every
        // current issuance/signing/revocation operation holds it until commit,
        // establishing one cross-process linearization point.
        sqlx::query("UPDATE authority_lock SET lock_nonce = lock_nonce + 1 WHERE lock_id = 1")
            .execute(&mut *transaction)
            .await?;
        Ok(transaction)
    }

    pub(super) async fn create_boot_intents(
        &self,
        node_preexisting: bool,
        operator_preexisting: bool,
    ) -> Result<(), AuthorityError> {
        let mut transaction = self.begin_serialized().await?;
        for (key_name, preexisting) in [
            (NODE_KEY, node_preexisting),
            (OPERATOR_KEY, operator_preexisting),
        ] {
            sqlx::query(
                "INSERT OR IGNORE INTO authority_boot_intents
                 (key_name, public_key, preexisting) VALUES (?1, NULL, ?2)",
            )
            .bind(key_name)
            .bind(i64::from(preexisting))
            .execute(&mut *transaction)
            .await?;
        }
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "boot provisioning intent commit outcome is unknown: {error}"
            )));
        }
        Ok(())
    }

    pub(super) async fn insert_boot_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
        preexisting: bool,
    ) -> Result<(), AuthorityError> {
        sqlx::query(
            "INSERT OR IGNORE INTO authority_boot_intents
             (key_name, public_key, preexisting) VALUES (?1, NULL, ?2)",
        )
        .bind(key_name)
        .bind(i64::from(preexisting))
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn boot_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
    ) -> Result<Option<(Option<ActorPublicKey>, bool)>, AuthorityError> {
        let row = sqlx::query(
            "SELECT public_key, preexisting FROM authority_boot_intents
              WHERE key_name = ?1",
        )
        .bind(key_name)
        .fetch_optional(&mut **transaction)
        .await?;
        row.map(|row| {
            let public_key = row
                .try_get::<Option<Vec<u8>>, _>("public_key")?
                .map(fixed_bytes)
                .transpose()?;
            let preexisting: i64 = row.try_get("preexisting")?;
            if preexisting != 0 && preexisting != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "boot provisioning intent has invalid preexisting flag".into(),
                ));
            }
            Ok((public_key, preexisting != 0))
        })
        .transpose()
    }

    pub(super) async fn reconcile_boot_intents(
        &self,
        store: &Arc<SecretStore>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), AuthorityError> {
        for key_name in [NODE_KEY, OPERATOR_KEY] {
            let Some((expected, _preexisting)) =
                Self::boot_intent_tx(transaction, key_name).await?
            else {
                continue;
            };
            let signer =
                match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, key_name).await {
                    Ok(signer) => signer,
                    Err(SecretsError::Missing(_)) => {
                        // Absence is not proof that the creator stopped. Retain
                        // the durable intent for a live or future owner.
                        continue;
                    }
                    Err(error) => {
                        return Err(AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.into(),
                            reason: format!("boot key read outcome is unknown: {error}"),
                        });
                    }
                };
            let public_key = signer.public_key().await.map_err(|error| {
                AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.into(),
                    reason: format!("boot public-key read outcome is unknown: {error}"),
                }
            })?;
            if expected.is_some_and(|expected| expected != public_key) {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.into(),
                    reason: "boot key disagrees with durable intent".into(),
                });
            }
            // A fixed identity is canonical and may be adopted by the next
            // serialized boot after exact public-key reading; no key is deleted.
            sqlx::query("DELETE FROM authority_boot_intents WHERE key_name = ?1")
                .bind(key_name)
                .execute(&mut **transaction)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.into(),
                    reason: format!("boot intent reconciliation is unresolved: {error}"),
                })?;
        }
        Ok(())
    }

    pub(super) async fn bind_boot_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
        public_key: &ActorPublicKey,
    ) -> Result<(), AuthorityError> {
        let affected = sqlx::query(
            "UPDATE authority_boot_intents SET public_key = ?1
              WHERE key_name = ?2 AND public_key IS NULL",
        )
        .bind(public_key.as_slice())
        .bind(key_name)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(AuthorityError::StateCorrupt(
                "boot intent disappeared or was rebound".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn create_actor_intent(
        &self,
        key_name: &str,
        preexisting: bool,
    ) -> Result<[u8; 16], AuthorityError> {
        let mut intent_id = [0_u8; 16];
        rand::rng().fill_bytes(&mut intent_id);
        let mut transaction = self.begin_serialized().await?;
        sqlx::query(
            "INSERT INTO authority_actor_intents
             (intent_id, key_name, public_key, preexisting, cleanup_pending)
             VALUES (?1, ?2, NULL, ?3, 0)",
        )
        .bind(intent_id.as_slice())
        .bind(key_name)
        .bind(i64::from(preexisting))
        .execute(&mut *transaction)
        .await?;
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "actor provisioning intent commit outcome is unknown for {key_name}: {error}"
            )));
        }
        Ok(intent_id)
    }

    pub(super) async fn actor_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent_id: &[u8; 16],
        key_name: &str,
    ) -> Result<Option<ActorProvisionIntent>, AuthorityError> {
        let row = sqlx::query(
            "SELECT intent_id, key_name, public_key, preexisting, cleanup_pending
               FROM authority_actor_intents
              WHERE intent_id = ?1 AND key_name = ?2",
        )
        .bind(intent_id.as_slice())
        .bind(key_name)
        .fetch_optional(&mut **transaction)
        .await?;
        row.map(|row| {
            let public_key = row
                .try_get::<Option<Vec<u8>>, _>("public_key")?
                .map(fixed_bytes)
                .transpose()?;
            let preexisting: i64 = row.try_get("preexisting")?;
            if preexisting != 0 && preexisting != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "actor provisioning intent has invalid preexisting flag".into(),
                ));
            }
            let cleanup_pending: i64 = row.try_get("cleanup_pending")?;
            if cleanup_pending != 0 && cleanup_pending != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "actor provisioning intent has invalid cleanup phase".into(),
                ));
            }
            Ok(ActorProvisionIntent {
                intent_id: fixed_bytes(row.try_get("intent_id")?)?,
                key_name: row.try_get("key_name")?,
                public_key,
                preexisting: preexisting != 0,
                cleanup_pending: cleanup_pending != 0,
            })
        })
        .transpose()
    }

    pub(super) async fn actor_intents_tx(
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<ActorProvisionIntent>, AuthorityError> {
        let rows = sqlx::query(
            "SELECT intent_id, key_name, public_key, preexisting, cleanup_pending
               FROM authority_actor_intents ORDER BY key_name",
        )
        .fetch_all(&mut **transaction)
        .await?;
        rows.into_iter()
            .map(|row| {
                let public_key = row
                    .try_get::<Option<Vec<u8>>, _>("public_key")?
                    .map(fixed_bytes)
                    .transpose()?;
                let preexisting: i64 = row.try_get("preexisting")?;
                if preexisting != 0 && preexisting != 1 {
                    return Err(AuthorityError::StateCorrupt(
                        "actor provisioning intent has invalid preexisting flag".into(),
                    ));
                }
                let cleanup_pending: i64 = row.try_get("cleanup_pending")?;
                if cleanup_pending != 0 && cleanup_pending != 1 {
                    return Err(AuthorityError::StateCorrupt(
                        "actor provisioning intent has invalid cleanup phase".into(),
                    ));
                }
                Ok(ActorProvisionIntent {
                    intent_id: fixed_bytes(row.try_get("intent_id")?)?,
                    key_name: row.try_get("key_name")?,
                    public_key,
                    preexisting: preexisting != 0,
                    cleanup_pending: cleanup_pending != 0,
                })
            })
            .collect()
    }

    pub(super) async fn set_actor_intent_public_key_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
        public_key: &ActorPublicKey,
    ) -> Result<(), AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_actor_intent_binding_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected actor intent binding failure".into(),
            )));
        }
        let affected = sqlx::query(
            "UPDATE authority_actor_intents SET public_key = ?1
              WHERE intent_id = ?2 AND key_name = ?3 AND public_key IS NULL",
        )
        .bind(public_key.as_slice())
        .bind(intent.intent_id.as_slice())
        .bind(&intent.key_name)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(AuthorityError::StateCorrupt(
                "actor provisioning intent changed before public-key binding".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn mark_actor_cleanup_pending_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
        public_key: &ActorPublicKey,
    ) -> Result<(), AuthorityError> {
        let affected = sqlx::query(
            "UPDATE authority_actor_intents
                SET public_key = ?1, cleanup_pending = 1
              WHERE intent_id = ?2 AND key_name = ?3
                AND cleanup_pending = 0
                AND (public_key IS NULL OR public_key = ?1)",
        )
        .bind(public_key.as_slice())
        .bind(intent.intent_id.as_slice())
        .bind(&intent.key_name)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(AuthorityError::StateCorrupt(
                "actor intent could not enter durable cleanup phase".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn insert_actor_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        public_key: &ActorPublicKey,
        key_name: &str,
    ) -> Result<(), AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_actor_publication_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected actor publication failure".into(),
            )));
        }
        sqlx::query(
            "INSERT INTO authority_actors (public_key, key_name)
             VALUES (?1, ?2)",
        )
        .bind(public_key.as_slice())
        .bind(key_name)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn actor_binding_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
    ) -> Result<Option<ActorPublicKey>, AuthorityError> {
        let row = sqlx::query("SELECT public_key FROM authority_actors WHERE key_name = ?1")
            .bind(key_name)
            .fetch_optional(&mut **transaction)
            .await?;
        row.map(|row| fixed_bytes(row.try_get("public_key")?))
            .transpose()
    }

    pub(super) async fn remove_actor_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
    ) -> Result<(), AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_actor_intent_removal_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected actor intent removal failure".into(),
            )));
        }
        sqlx::query(
            "DELETE FROM authority_actor_intents
              WHERE intent_id = ?1 AND key_name = ?2",
        )
        .bind(intent.intent_id.as_slice())
        .bind(&intent.key_name)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn actor_key_name_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        public_key: &ActorPublicKey,
    ) -> Result<Option<String>, AuthorityError> {
        let row = sqlx::query("SELECT key_name FROM authority_actors WHERE public_key = ?1")
            .bind(public_key.as_slice())
            .fetch_optional(&mut **transaction)
            .await?;
        row.map(|row| row.try_get("key_name"))
            .transpose()
            .map_err(Into::into)
    }

    pub(super) async fn insert_cap_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        cap_id: &CapId,
        object: &str,
        rights: Rights,
        parent: Option<&CapId>,
        root_actor: Option<&[u8; 32]>,
    ) -> Result<(), AuthorityError> {
        sqlx::query(
            "INSERT INTO authority_caps (cap_id, object, rights, parent_id, root_actor, retracted)
             VALUES (?1, ?2, ?3, ?4, ?5, 0)",
        )
        .bind(cap_id.as_slice())
        .bind(object)
        .bind(rights.bits_i64())
        .bind(parent.map(|parent_id| parent_id.to_vec()))
        .bind(root_actor.map(|key| key.to_vec()))
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn cap_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        cap_id: &CapId,
    ) -> Result<Option<CapRecord>, AuthorityError> {
        let row = sqlx::query(
            "SELECT object, rights, parent_id, root_actor, retracted FROM authority_caps WHERE cap_id = ?1",
        )
        .bind(cap_id.as_slice())
        .fetch_optional(&mut **transaction)
        .await?;
        row.map(|row| {
            let rights: i64 = row.try_get("rights")?;
            let parent = row
                .try_get::<Option<Vec<u8>>, _>("parent_id")?
                .map(super::fixed_bytes)
                .transpose()?;
            let root_actor = row
                .try_get::<Option<Vec<u8>>, _>("root_actor")?
                .map(super::fixed_bytes)
                .transpose()?;
            let retracted: i64 = row.try_get("retracted")?;
            Ok(CapRecord {
                object: row.try_get("object")?,
                rights: Rights::from_bits(rights as u32),
                parent,
                root_actor,
                retracted: retracted != 0,
            })
        })
        .transpose()
    }

    pub(super) async fn retract_cap_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        cap_id: &CapId,
    ) -> Result<(), AuthorityError> {
        sqlx::query("UPDATE authority_caps SET retracted = 1 WHERE cap_id = ?1")
            .bind(cap_id.as_slice())
            .execute(&mut **transaction)
            .await?;
        Ok(())
    }

    pub(super) async fn root_cap_tx(
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Option<(CapId, String)>, AuthorityError> {
        let row =
            sqlx::query("SELECT cap_id, operator_uuid FROM authority_root_cap WHERE root_id = 1")
                .fetch_optional(&mut **transaction)
                .await?;
        row.map(|row| {
            Ok((
                super::fixed_bytes(row.try_get("cap_id")?)?,
                row.try_get("operator_uuid")?,
            ))
        })
        .transpose()
    }

    pub(super) async fn install_root_cap_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        cap_id: &CapId,
        operator_uuid: &str,
    ) -> Result<(), AuthorityError> {
        sqlx::query(
            "INSERT OR IGNORE INTO authority_root_cap (root_id, cap_id, operator_uuid) VALUES (1, ?1, ?2)",
        )
        .bind(cap_id.as_slice())
        .bind(operator_uuid)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn grant_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        subject: &str,
        object: &str,
        access: &str,
    ) -> Result<(), AuthorityError> {
        sqlx::query(
            "INSERT OR IGNORE INTO authority_grants (subject, object, access) VALUES (?1, ?2, ?3)",
        )
        .bind(subject)
        .bind(object)
        .bind(access)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn revoke_grant_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        subject: &str,
        object: &str,
        access: &str,
    ) -> Result<(), AuthorityError> {
        sqlx::query(
            "DELETE FROM authority_grants WHERE subject = ?1 AND object = ?2 AND access = ?3",
        )
        .bind(subject)
        .bind(object)
        .bind(access)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub(super) async fn has_grant_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        subject: &str,
        object: &str,
        access: &str,
    ) -> Result<bool, AuthorityError> {
        Ok(sqlx::query(
            "SELECT 1 FROM authority_grants WHERE subject = ?1 AND object = ?2 AND access = ?3",
        )
        .bind(subject)
        .bind(object)
        .bind(access)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some())
    }

    pub(super) async fn actor_by_uuid_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        uuid: &str,
    ) -> Result<Option<super::ActorRecord>, AuthorityError> {
        let row = sqlx::query(
            "SELECT public_key, key_name, uuid, url, parent_uuid, certificate FROM authority_actors WHERE uuid = ?1",
        )
        .bind(uuid)
        .fetch_optional(&mut **transaction)
        .await?;
        super::actor::actor_record(row)
    }

    pub(super) async fn actor_by_key_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        public_key: &[u8; 32],
    ) -> Result<Option<super::ActorRecord>, AuthorityError> {
        let row = sqlx::query(
            "SELECT public_key, key_name, uuid, url, parent_uuid, certificate FROM authority_actors WHERE public_key = ?1",
        )
        .bind(public_key.as_slice())
        .fetch_optional(&mut **transaction)
        .await?;
        super::actor::actor_record(row)
    }

    pub(super) async fn set_actor_binding_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        public_key: &[u8; 32],
        uuid: &str,
        url: &str,
        parent_uuid: Option<&str>,
        certificate: &[u8],
    ) -> Result<(), AuthorityError> {
        let _aff = sqlx::query(
            "UPDATE authority_actors SET uuid = ?1, url = ?2, parent_uuid = ?3, certificate = ?4 WHERE public_key = ?5",
        )
        .bind(uuid)
        .bind(url)
        .bind(parent_uuid)
        .bind(certificate)
        .bind(public_key.as_slice())
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }
}
