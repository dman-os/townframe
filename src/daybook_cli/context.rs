use crate::interlude::*;

/// Configuration for the daybook core storage systems
#[derive(Debug, Clone)]
pub struct Config {
    pub am: utils_rs::am::Config,
    pub sql: SqlConfig,
    pub blobs_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub database_url: String,
}

pub struct Ctx {
    pub acx: utils_rs::am::AmCtx,
    pub sql: SqlCtx,
    pub doc_app: tokio::sync::OnceCell<samod::DocHandle>,
    pub doc_drawer: tokio::sync::OnceCell<samod::DocHandle>,
}

pub type SharedCtx = Arc<Ctx>;

pub struct SqlCtx {
    pub db_pool: sqlx::SqlitePool,
}

impl Config {
    /// Create a new config with platform-specific defaults
    pub fn new(cli_config: crate::config::CliConfig) -> Res<Self> {
        let (am, sql, blobs_root) = {
            (
                utils_rs::am::Config {
                    storage: utils_rs::am::StorageConfig::Disk {
                        path: cli_config.repo_path.join("samod"),
                    },
                    peer_id: "daybook_client".to_string(),
                },
                SqlConfig {
                    database_url: {
                        let db_path = cli_config.repo_path.join("sqlite.db");
                        format!("sqlite://{}", db_path.display())
                    },
                },
                cli_config.repo_path.join("blobs"),
            )
        };
        Ok(Self {
            am,
            sql,
            blobs_root,
        })
    }
}

impl SqlCtx {
    pub async fn new(config: SqlConfig) -> Res<Self> {
        use std::str::FromStr;

        if !config.database_url.starts_with("sqlite::memory:") {
            if let Some(path) = config.database_url.strip_prefix("sqlite://") {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    std::fs::create_dir_all(parent).wrap_err_with(|| {
                        format!("Failed to create database directory: {}", parent.display())
                    })?;
                }
            }
        }

        let db_pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str(&config.database_url)?
                .create_if_missing(true),
        )
        .await
        .wrap_err("error initializing sqlite db")?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS kvstore (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                "#,
        )
        .execute(&db_pool)
        .await?;

        Ok(Self { db_pool })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum InitState {
    None,
    Created {
        doc_id_app: samod::DocumentId,
        doc_id_drawer: samod::DocumentId,
    },
}

const INIT_STATE_KEY: &str = "init_state";

async fn get_init_state(cx: &Ctx) -> Res<InitState> {
    let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
        .bind(INIT_STATE_KEY)
        .fetch_optional(&cx.sql.db_pool)
        .await?;
    let state = match rec {
        Some(json) => serde_json::from_str::<InitState>(&json)?,
        None => InitState::None,
    };
    Ok(state)
}

async fn set_init_state(cx: &Ctx, state: &InitState) -> Res<()> {
    let json = serde_json::to_string(state)?;
    sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(INIT_STATE_KEY)
        .bind(&json)
        .execute(&cx.sql.db_pool)
        .await?;
    Ok(())
}

impl Ctx {
    pub async fn init(config: Config) -> Result<Arc<Self>, eyre::Report> {
        let sql = SqlCtx::new(config.sql.clone()).await?;
        let acx =
            utils_rs::am::AmCtx::boot(config.am.clone(), Option::<samod::AlwaysAnnounce>::None)
                .await?;
        // acx.spawn_ws_connector("ws://0.0.0.0:8090".into());

        let cx = Arc::new(Self {
            acx,
            sql,
            doc_app: default(),
            doc_drawer: default(),
        });
        init_from_globals(&cx).await?;
        Ok(cx)
    }

    pub fn doc_app(&self) -> &samod::DocHandle {
        self.doc_app.get().expect("ctx was not initialized")
    }

    pub fn doc_drawer(&self) -> &samod::DocHandle {
        self.doc_drawer.get().expect("ctx was not initialized")
    }
}

async fn init_from_globals(cx: &Ctx) -> Res<()> {
    let init_state = get_init_state(cx).await?;
    let (handle_app, handle_drawer) = if let InitState::Created {
        doc_id_app,
        doc_id_drawer,
    } = init_state
    {
        let (handle_app, handle_drawer) = tokio::try_join!(
            cx.acx.find_doc(&doc_id_app),
            cx.acx.find_doc(&doc_id_drawer)
        )?;
        if handle_app.is_none() {
            warn!("doc not found locally for stored doc_id_app; creating new local document");
        }
        if handle_drawer.is_none() {
            warn!("doc not found locally for stored doc_id_drawer; creating new local document");
        }
        (handle_app, handle_drawer)
    } else {
        (None, None)
    };

    let mut doc_handles = vec![];
    let mut update_state = false;
    for (handle, latest_fn) in [
        (
            handle_app,
            daybook_core::app::version_updates::version_latest as fn() -> Res<Vec<u8>>,
        ),
        (
            handle_drawer,
            daybook_core::drawer::version_updates::version_latest,
        ),
    ] {
        let handle = match handle {
            Some(handle) => handle,
            None => {
                update_state = true;
                let doc = latest_fn()?;
                let doc =
                    automerge::Automerge::load(&doc).wrap_err("error loading version_latest")?;
                let handle = cx.acx.add_doc(doc).await?;
                handle
            }
        };
        doc_handles.push(handle)
    }
    if doc_handles.len() != 2 {
        unreachable!();
    }
    for handle in &doc_handles {
        cx.acx.change_manager().clone().add_doc(handle.clone());
    }
    if update_state {
        set_init_state(
            cx,
            &InitState::Created {
                doc_id_app: doc_handles[0].document_id().clone(),
                doc_id_drawer: doc_handles[1].document_id().clone(),
            },
        )
        .await?;
    }
    let (Ok(()), Ok(())) = (
        cx.doc_drawer.set(doc_handles.pop().unwrap_or_log()),
        cx.doc_app.set(doc_handles.pop().unwrap_or_log()),
    ) else {
        eyre::bail!("double ctx initialization");
    };
    Ok(())
}
