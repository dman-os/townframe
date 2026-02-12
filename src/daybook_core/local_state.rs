use crate::interlude::*;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum LocalStateEvent {
    ListChanged,
}

pub struct SqliteLocalStateRepo {
    local_state_root: PathBuf,
    sqlite_pools: tokio::sync::RwLock<HashMap<String, sqlx::SqlitePool>>,
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
    pub async fn boot(local_state_root: PathBuf) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
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
            sqlite_pools: tokio::sync::RwLock::new(HashMap::new()),
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: main_cancel_token.child_token(),
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: None,
                broker_stop_tokens: vec![],
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

    pub async fn ensure_sqlite_pool(
        &self,
        local_state_id: &str,
    ) -> Res<(String, sqlx::SqlitePool)> {
        if let Some(pool) = self.sqlite_pools.read().await.get(local_state_id).cloned() {
            let path = self
                .get_sqlite_file_path(local_state_id)
                .await?
                .to_string_lossy()
                .to_string();
            return Ok((path, pool));
        }

        let sqlite_file_path = self
            .get_sqlite_file_path(local_state_id)
            .await?
            .to_string_lossy()
            .to_string();

        crate::init_sqlite_vec();
        let connect_options =
            SqliteConnectOptions::from_str(&format!("sqlite://{sqlite_file_path}"))?
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(5))
                .create_if_missing(true);
        let db_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .wrap_err("error initializing sqlite local state connection")?;

        sqlx::query("select vec_version()")
            .execute(&db_pool)
            .await
            .wrap_err("sqlite-vec extension not available")?;

        let mut pools = self.sqlite_pools.write().await;
        let pooled = pools
            .entry(local_state_id.to_string())
            .or_insert_with(|| db_pool.clone())
            .clone();
        Ok((sqlite_file_path, pooled))
    }
}
