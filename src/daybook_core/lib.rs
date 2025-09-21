#[allow(unused)]
mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
    pub use std::{
        borrow::Cow,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use utils_rs::prelude::*;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use interlude::*;

uniffi::setup_scaffolding!();

mod am;
mod docs;
mod ffi;
mod samod;

struct Ctx {
    acx: am::AmCtx,
    // rt: tokio::runtime::Handle,
    sql: sql::SqlCtx,
}
type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn new() -> Result<Arc<Self>, eyre::Report> {
        let sql = sql::SqlCtx::new().await?;
        let acx = am::AmCtx::new().await?;
        let cx = Arc::new(Self {
            acx,
            // rt: tokio::runtime::Handle::current(),
            sql,
        });
        // Initialize automerge document from globals/kv and start sync worker lazily.
        cx.acx.init_from_globals(&cx).await?;
        Ok(cx)
    }
}

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}

mod sql {
    use crate::interlude::*;

    pub struct SqlCtx {
        db_pool: sqlx::SqlitePool,
    }

    impl SqlCtx {
        pub async fn new() -> Res<Self> {
            use std::str::FromStr;
            let db_pool = sqlx::SqlitePool::connect_with(
                sqlx::sqlite::SqliteConnectOptions::from_str("sqlite:///tmp/daybook.db")?
                    .create_if_missing(true),
            )
            .await
            .unwrap_or_log();
            // Initialize schema
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

        pub(crate) fn pool(&self) -> &sqlx::SqlitePool {
            &self.db_pool
        }
    }

    pub mod kv {
        use super::*;

        const TABLE: &str = "kvstore";

        pub async fn get(cx: &crate::Ctx, key: &str) -> Res<Option<String>> {
            let rec = sqlx::query_scalar::<_, String>(&format!(
                "SELECT value FROM {TABLE} WHERE key = ?1"
            ))
            .bind(key)
            .fetch_optional(cx.sql.pool())
            .await?;
            Ok(rec)
        }

        pub async fn set(cx: &crate::Ctx, key: &str, value: &str) -> Res<()> {
            sqlx::query(&format!(
                "INSERT INTO {TABLE}(key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            ))
            .bind(key)
            .bind(value)
            .execute(cx.sql.pool())
            .await?;
            Ok(())
        }
    }
}

mod globals {
    use crate::interlude::*;

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
    pub enum InitState {
        None,
        Created { doc_id: samod::DocumentId },
    }

    const INIT_STATE_KEY: &str = "init_state";

    pub async fn get_init_state(cx: &Ctx) -> Res<InitState> {
        let val = super::sql::kv::get(cx, INIT_STATE_KEY).await?;
        let state = match val {
            Some(json) => serde_json::from_str::<InitState>(&json)?,
            None => InitState::None,
        };
        Ok(state)
    }

    pub async fn set_init_state(cx: &Ctx, state: &InitState) -> Res<()> {
        let json = serde_json::to_string(state)?;
        super::sql::kv::set(cx, INIT_STATE_KEY, &json).await
    }
}
