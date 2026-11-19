use crate::interlude::*;
use tokio_util::sync::CancellationToken;

// FIXME: this is never constructed?
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum LocalStateEvent {
    ListChanged,
}

pub struct SqliteLocalStateRepo {
    local_state_root: PathBuf,
    sqlite_runtime: Arc<sqlite_rs::SqliteRuntime>,
    sqlite_dbs: tokio::sync::RwLock<HashMap<String, sqlite_rs::Database>>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
}

impl crate::repos::Repo for SqliteLocalStateRepo {
    type Event = LocalStateEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl SqliteLocalStateRepo {
    pub async fn boot(
        sqlite_runtime: Arc<sqlite_rs::SqliteRuntime>,
        local_state_root: PathBuf,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let main_cancel_token = CancellationToken::new();
        tokio::fs::create_dir_all(&local_state_root)
            .await
            .wrap_err_with(|| {
                format!(
                    "error creating local state root directory: {}",
                    local_state_root.display()
                )
            })?;

        let repo = Arc::new(Self {
            local_state_root,
            sqlite_runtime,
            sqlite_dbs: tokio::sync::RwLock::new(HashMap::new()),
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: main_cancel_token.child_token(),
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: None,
            },
        ))
    }

    pub fn local_state_id(plug_id: &str, local_state_key: &str) -> String {
        format!("{plug_id}/{local_state_key}")
    }

    pub async fn get_sqlite_file_path(&self, local_state_id: &str) -> Res<PathBuf> {
        let mut path_segments = local_state_id.split('/').collect::<Vec<_>>();
        if path_segments.len() != 3 || !path_segments[0].starts_with('@') {
            eyre::bail!(
                "invalid local_state_id '{local_state_id}', expected format '@namespace/name/key'"
            );
        }

        let local_state_key = path_segments
            .pop()
            .ok_or_eyre("invalid local_state_id missing key")?;
        let plug_name = path_segments
            .pop()
            .ok_or_eyre("invalid local_state_id missing plug name")?;
        let plug_namespace = path_segments
            .pop()
            .ok_or_eyre("invalid local_state_id missing namespace")?;

        let namespace_dir = plug_namespace.trim_start_matches('@');
        if namespace_dir.is_empty() || plug_name.is_empty() || local_state_key.is_empty() {
            eyre::bail!("invalid local_state_id '{local_state_id}'");
        }

        let file_dir = self.local_state_root.join(namespace_dir).join(plug_name);
        tokio::fs::create_dir_all(&file_dir)
            .await
            .wrap_err_with(|| {
                format!(
                    "error creating local state directory: {}",
                    file_dir.display()
                )
            })?;

        Ok(file_dir.join(format!("{local_state_key}.sqlite")))
    }

    pub async fn ensure_sqlite_db(&self, local_state_id: &str) -> Res<sqlite_rs::Database> {
        if let Some(db) = self.sqlite_dbs.read().await.get(local_state_id).cloned() {
            return Ok(db);
        }

        let sqlite_file_path = self.get_sqlite_file_path(local_state_id).await?;

        sqlite_rs::init_sqlite_vec();
        let db = self
            .sqlite_runtime
            .database(sqlite_file_path, &[], "_sqlite_rs_migrations");

        let mut sqlite_dbs = self.sqlite_dbs.write().await;
        let pooled = sqlite_dbs
            .entry(local_state_id.to_string())
            .or_insert_with(|| db.clone())
            .clone();
        Ok(pooled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repositories_share_the_injected_runtime() -> Res<()> {
        let temp = tempfile::tempdir()?;
        let runtime = Arc::new(sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig {
            workers: 1,
            max_readers_per_db: 1,
            cache_size_kib: -256,
            ..default()
        }));
        let (first, first_stop) =
            SqliteLocalStateRepo::boot(Arc::clone(&runtime), temp.path().join("first")).await?;
        let (second, second_stop) =
            SqliteLocalStateRepo::boot(Arc::clone(&runtime), temp.path().join("second")).await?;
        assert!(Arc::ptr_eq(&first.sqlite_runtime, &runtime));
        assert!(Arc::ptr_eq(&second.sqlite_runtime, &runtime));
        let first_db = first.ensure_sqlite_db("@test/plug/state").await?;
        let second_db = second.ensure_sqlite_db("@test/plug/state").await?;
        assert_eq!(runtime.open_connection_count(), 0);

        for db in [&first_db, &second_db] {
            assert_eq!(db.query("PRAGMA cache_size", ()).await?.one::<i64>()?, -256);
            assert!(
                !db.query("SELECT vec_version()", ())
                    .await?
                    .one::<String>()?
                    .is_empty()
            );
        }
        assert!(runtime.open_connection_count() >= 2);
        let connections = runtime.open_connection_count();
        let cached_db = first.ensure_sqlite_db("@test/plug/state").await?;
        assert_eq!(
            cached_db
                .query("PRAGMA cache_size", ())
                .await?
                .one::<i64>()?,
            -256
        );
        assert_eq!(runtime.open_connection_count(), connections);
        drop(cached_db);
        drop(first_db);
        drop(second_db);
        second_stop.stop().await?;
        first_stop.stop().await?;
        drop(second);
        drop(first);
        assert_eq!(runtime.open_connection_count(), 0);
        Ok(())
    }
}
