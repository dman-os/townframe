use crate::interlude::*;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

pub struct SqlCtx {
    pub db_pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub database_url: String,
}

impl SqlCtx {
    pub async fn new(database_url: &str) -> Res<Self> {
        if !database_url.starts_with("sqlite::memory:") {
            if let Some(path) = database_url.strip_prefix("sqlite://") {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    std::fs::create_dir_all(parent).wrap_err_with(|| {
                        format!("Failed to create database directory: {}", parent.display())
                    })?;
                }
            }
        }

        let db_pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::from_str(database_url)?
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_secs(5))
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
pub enum InitState {
    None,
    Created {
        doc_id_app: DocumentId,
        doc_id_drawer: DocumentId,
    },
}

pub const INIT_STATE_KEY: &str = "init_state";
pub const LOCAL_USER_PATH_KEY: &str = "local_user_path";

pub async fn get_init_state(sql: &SqlitePool) -> Res<InitState> {
    let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
        .bind(INIT_STATE_KEY)
        .fetch_optional(sql)
        .await?;
    let state = match rec {
        Some(json) => serde_json::from_str::<InitState>(&json)?,
        None => InitState::None,
    };
    Ok(state)
}

pub async fn set_init_state(sql: &SqlitePool, state: &InitState) -> Res<()> {
    let json = serde_json::to_string(state)?;
    sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(INIT_STATE_KEY)
        .bind(&json)
        .execute(sql)
        .await?;
    Ok(())
}

pub async fn get_local_user_path(sql: &SqlitePool) -> Res<Option<String>> {
    let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
        .bind(LOCAL_USER_PATH_KEY)
        .fetch_optional(sql)
        .await?;
    Ok(rec)
}

pub async fn set_local_user_path(sql: &SqlitePool, path: &str) -> Res<()> {
    sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(LOCAL_USER_PATH_KEY)
        .bind(path)
        .execute(sql)
        .await?;
    Ok(())
}

pub mod version_updates {
    use crate::interlude::*;

    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    use crate::config::ConfigStore;
    use crate::plugs::PlugsStore;
    use crate::rt::dispatch::DispatchStore;
    use crate::rt::triage::DocTriageWorkerStateStore;
    use crate::tables::TablesStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        use crate::stores::Store;
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        // annotate schema for app document
        doc.put(ROOT, "$schema", "daybook.app")?;
        reconcile_prop(&mut doc, ROOT, TablesStore::PROP, TablesStore::default())?;
        reconcile_prop(&mut doc, ROOT, ConfigStore::PROP, ConfigStore::default())?;
        reconcile_prop(&mut doc, ROOT, PlugsStore::PROP, PlugsStore::default())?;
        reconcile_prop(
            &mut doc,
            ROOT,
            DispatchStore::PROP,
            DispatchStore::default(),
        )?;
        reconcile_prop(
            &mut doc,
            ROOT,
            DocTriageWorkerStateStore::PROP,
            DocTriageWorkerStateStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}

pub async fn init_from_globals(
    acx: &AmCtx,
    sql: &SqlitePool,
    doc_app_cell: &tokio::sync::OnceCell<samod::DocHandle>,
    doc_drawer_cell: &tokio::sync::OnceCell<samod::DocHandle>,
) -> Res<()> {
    let init_state = get_init_state(sql).await?;
    let (handle_app, handle_drawer) = if let InitState::Created {
        doc_id_app,
        doc_id_drawer,
    } = init_state
    {
        let (handle_app, handle_drawer) =
            tokio::try_join!(acx.find_doc(&doc_id_app), acx.find_doc(&doc_id_drawer))?;
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
            version_updates::version_latest as fn() -> Res<Vec<u8>>,
        ),
        (
            handle_drawer,
            crate::drawer::version_updates::version_latest,
        ),
    ] {
        let handle = match handle {
            Some(handle) => handle,
            None => {
                update_state = true;
                let doc = latest_fn()?;
                let doc =
                    automerge::Automerge::load(&doc).wrap_err("error loading version_latest")?;
                let handle = acx.add_doc(doc).await?;
                handle
            }
        };
        doc_handles.push(handle)
    }
    if doc_handles.len() != 2 {
        unreachable!();
    }
    for handle in &doc_handles {
        let _ = acx.change_manager().add_doc(handle.clone()).await?;
    }
    if update_state {
        set_init_state(
            sql,
            &InitState::Created {
                doc_id_app: doc_handles[0].document_id().clone(),
                doc_id_drawer: doc_handles[1].document_id().clone(),
            },
        )
        .await?;
    }
    let (Ok(()), Ok(())) = (
        doc_drawer_cell.set(doc_handles.pop().unwrap_or_log()),
        doc_app_cell.set(doc_handles.pop().unwrap_or_log()),
    ) else {
        eyre::bail!("double ctx initialization");
    };
    Ok(())
}

