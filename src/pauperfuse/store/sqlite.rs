use crate::interlude::*;
use crate::sqlite::queries as q;
use crate::store::{PersistedObjectState, PersistedState, StateStore};
use sqlite_rs::{Database, SqliteRuntime};

pub struct SqliteStateStore {
    _runtime: Arc<SqliteRuntime>,
    db: Database,
}

impl SqliteStateStore {
    pub async fn open(runtime: Arc<SqliteRuntime>, path: &Path) -> Res<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let db = runtime.database(path.to_path_buf(), q::MIGRATIONS, q::MIGRATION_TABLE);
        Ok(Self {
            _runtime: runtime,
            db,
        })
    }

    pub async fn load_state(&self) -> Res<PersistedState> {
        let provider_state_id = self.load_meta_u64("provider_state_id", 0).await?;
        let backend_state_id = self.load_meta_u64("backend_state_id", 0).await?;
        let rows = q::list_objects(&self.db).await?;
        let mut objects = BTreeMap::new();
        for row in rows {
            objects.insert(
                row.doc_id,
                PersistedObjectState {
                    relative_path: PathBuf::from(row.relative_path),
                    provider_hash: row.provider_hash,
                    backend_hash: row.backend_hash,
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
        let objects: Vec<(String, String, Option<String>, Option<String>)> = state
            .objects
            .iter()
            .map(|(doc_id, object)| {
                (
                    doc_id.clone(),
                    object.relative_path.to_string_lossy().into_owned(),
                    object.provider_hash.clone(),
                    object.backend_hash.clone(),
                )
            })
            .collect();
        let provider = state.provider_state_id.to_string();
        let backend = state.backend_state_id.to_string();
        let tx = self.db.begin_write().await?;
        q::upsert_meta(&tx, "provider_state_id".to_string(), provider).await?;
        q::upsert_meta(&tx, "backend_state_id".to_string(), backend).await?;
        q::clear_objects(&tx).await?;
        for (doc_id, path, provider_hash, backend_hash) in objects {
            q::insert_object(&tx, doc_id, path, provider_hash, backend_hash).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load_meta_u64(&self, key: &str, fallback: u64) -> Res<u64> {
        match q::get_meta(&self.db, key.to_string()).await? {
            Some(text) => Ok(text.parse::<u64>()?),
            None => Ok(fallback),
        }
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

        let runtime = Arc::new(SqliteRuntime::new(default()));
        let store = SqliteStateStore::open(Arc::clone(&runtime), &db_path).await?;
        assert_eq!(runtime.open_connection_count(), 0);
        let loaded_empty = store.load_state().await?;
        assert_eq!(loaded_empty, PersistedState::default());

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
        assert!(runtime.open_connection_count() > 0);
        drop(store);
        let reopened = SqliteStateStore::open(runtime, &db_path).await?;
        assert_eq!(reopened.load_state().await?, state);

        Ok(())
    }
}
