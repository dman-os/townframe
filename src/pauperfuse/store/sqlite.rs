use crate::interlude::*;
use crate::store::{PersistedObjectState, PersistedState, StateStore};
use sqlx::Row;

pub struct SqliteStateStore {
    pool: sqlx::SqlitePool,
}

impl SqliteStateStore {
    pub async fn open(path: &Path) -> Res<Self> {
        use std::str::FromStr;

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let database_url = format!("sqlite://{}", path.display());
        let options =
            sqlx::sqlite::SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(options).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pauperfuse_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pauperfuse_object_state (\
                doc_id TEXT PRIMARY KEY, \
                relative_path TEXT NOT NULL, \
                provider_hash TEXT, \
                backend_hash TEXT\
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn load_state(&self) -> Res<PersistedState> {
        let provider_state_id = self.load_meta_u64("provider_state_id", 0).await?;
        let backend_state_id = self.load_meta_u64("backend_state_id", 0).await?;

        let mut objects = BTreeMap::new();
        let rows = sqlx::query(
            "SELECT doc_id, relative_path, provider_hash, backend_hash FROM pauperfuse_object_state",
        )
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let doc_id: String = row.try_get("doc_id")?;
            let relative_path: String = row.try_get("relative_path")?;
            let provider_hash: Option<String> = row.try_get("provider_hash")?;
            let backend_hash: Option<String> = row.try_get("backend_hash")?;

            objects.insert(
                doc_id,
                PersistedObjectState {
                    relative_path: PathBuf::from(relative_path),
                    provider_hash,
                    backend_hash,
                },
            );
        }

        Ok(PersistedState {
            provider_state_id,
            backend_state_id,
            objects,
        })
    }

    pub async fn save_state(&self, state: &PersistedState) -> Res<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO pauperfuse_meta(key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind("provider_state_id")
        .bind(state.provider_state_id.to_string())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO pauperfuse_meta(key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind("backend_state_id")
        .bind(state.backend_state_id.to_string())
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM pauperfuse_object_state")
            .execute(&mut *tx)
            .await?;

        for (doc_id, object_state) in &state.objects {
            sqlx::query(
                "INSERT INTO pauperfuse_object_state(doc_id, relative_path, provider_hash, backend_hash) \
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(doc_id)
            .bind(object_state.relative_path.to_string_lossy().to_string())
            .bind(object_state.provider_hash.clone())
            .bind(object_state.backend_hash.clone())
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn load_meta_u64(&self, key: &str, fallback: u64) -> Res<u64> {
        let row = sqlx::query("SELECT value FROM pauperfuse_meta WHERE key = ?1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        let value = match row {
            Some(row) => {
                let text: String = row.try_get("value")?;
                text.parse::<u64>()?
            }
            None => fallback,
        };

        Ok(value)
    }
}

#[async_trait::async_trait]
impl StateStore for SqliteStateStore {
    async fn load_state(&self) -> Res<PersistedState> {
        Self::load_state(self).await
    }

    async fn save_state(&self, state: &PersistedState) -> Res<()> {
        Self::save_state(self, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_roundtrip_persisted_state() -> Res<()> {
        let tmp_dir = tempfile::tempdir()?;
        let db_path = tmp_dir.path().join("pauperfuse.sqlite");

        let store = SqliteStateStore::open(&db_path).await?;
        let loaded_empty = store.load_state().await?;
        assert_eq!(loaded_empty.provider_state_id, 0);
        assert_eq!(loaded_empty.backend_state_id, 0);
        assert!(loaded_empty.objects.is_empty());

        let mut objects = BTreeMap::new();
        objects.insert(
            "alpha".to_string(),
            PersistedObjectState {
                relative_path: PathBuf::from("alpha.json"),
                provider_hash: Some("prov_hash".to_string()),
                backend_hash: Some("back_hash".to_string()),
            },
        );
        let state = PersistedState {
            provider_state_id: 7,
            backend_state_id: 11,
            objects,
        };

        store.save_state(&state).await?;

        let loaded = store.load_state().await?;
        assert_eq!(loaded, state);

        Ok(())
    }
}
