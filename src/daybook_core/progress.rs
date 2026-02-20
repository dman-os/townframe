use crate::interlude::*;
use jiff::Timestamp;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressUnit {
    Bytes,
    Generic { label: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressUpdateDeets {
    Status {
        severity: ProgressSeverity,
        message: String,
    },
    Amount {
        severity: ProgressSeverity,
        done: u64,
        total: Option<u64>,
        unit: ProgressUnit,
        message: Option<String>,
    },
    Completed {
        state: ProgressFinalState,
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct ProgressUpdate {
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub at: Timestamp,
    pub title: Option<String>,
    #[autosurgeon(with = "utils_rs::am::codecs::json")]
    pub deets: ProgressUpdateDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct ProgressUpdateEntry {
    pub sequence: i64,
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub at: Timestamp,
    #[autosurgeon(with = "utils_rs::am::codecs::json")]
    pub update: ProgressUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressTaskState {
    Active,
    Succeeded,
    Failed,
    Cancelled,
    Dismissed,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressFinalState {
    Succeeded,
    Failed,
    Cancelled,
    Dismissed,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressRetentionPolicy {
    UserDismissable,
    AutoDismissAfter { seconds: u64 },
    DismissAfterViewed,
    Ephemeral,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct ProgressTask {
    pub id: String,
    pub title: Option<String>,
    pub tags: Vec<String>,
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub created_at: Timestamp,
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub updated_at: Timestamp,
    #[autosurgeon(with = "progress_optional_date")]
    pub viewed_at: Option<Timestamp>,
    #[autosurgeon(with = "progress_optional_date")]
    pub dismissed_at: Option<Timestamp>,
    pub state: ProgressTaskState,
    pub retention: ProgressRetentionPolicy,
    pub retention_override: Option<ProgressRetentionPolicy>,
    pub latest_update: Option<ProgressUpdateEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct CreateProgressTaskArgs {
    pub id: String,
    pub tags: Vec<String>,
    pub retention: ProgressRetentionPolicy,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressEvent {
    TaskUpserted { id: String },
    TaskRemoved { id: String },
    UpdateAdded { id: String },
    ListChanged,
}

pub struct ProgressRepo {
    db_pool: sqlx::SqlitePool,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
}

type TaskRow = (
    String,
    Option<String>,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    String,
    String,
    Option<String>,
);

impl crate::repos::Repo for ProgressRepo {
    type Event = ProgressEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl ProgressRepo {
    pub async fn boot(db_pool: sqlx::SqlitePool) -> Res<Arc<Self>> {
        let repo = Arc::new(Self {
            db_pool,
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: CancellationToken::new(),
        });
        repo.init_schema().await?;
        Ok(repo)
    }

    async fn init_schema(&self) -> Res<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS progress_tasks (
                id TEXT PRIMARY KEY,
                title TEXT,
                created_at_unix_secs INTEGER NOT NULL,
                updated_at_unix_secs INTEGER NOT NULL,
                viewed_at_unix_secs INTEGER,
                dismissed_at_unix_secs INTEGER,
                state_json TEXT NOT NULL,
                retention_json TEXT NOT NULL,
                retention_override_json TEXT
            )
            "#,
        )
        .execute(&self.db_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS progress_task_updates (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                at_unix_secs INTEGER NOT NULL,
                update_json TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES progress_tasks(id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.db_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS progress_task_tags (
                task_id TEXT NOT NULL,
                tag_path TEXT NOT NULL,
                PRIMARY KEY(task_id, tag_path),
                FOREIGN KEY(task_id) REFERENCES progress_tasks(id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.db_pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_progress_tasks_state ON progress_tasks(state_json)",
        )
        .execute(&self.db_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_progress_tasks_updated ON progress_tasks(updated_at_unix_secs DESC)",
        )
        .execute(&self.db_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_progress_task_tags_path ON progress_task_tags(tag_path)",
        )
        .execute(&self.db_pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_progress_task_updates_task_seq ON progress_task_updates(task_id, sequence DESC)",
        )
        .execute(&self.db_pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_task(&self, args: CreateProgressTaskArgs) -> Res<()> {
        self.ensure_live()?;
        let now = timestamp_now();
        let state_json = serde_json::to_string(&ProgressTaskState::Active)?;
        let retention_json = serde_json::to_string(&args.retention)?;
        let mut tx = self.db_pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO progress_tasks(
                id, title, created_at_unix_secs, updated_at_unix_secs, viewed_at_unix_secs,
                dismissed_at_unix_secs, state_json, retention_json, retention_override_json
            ) VALUES(?1, '', ?2, ?3, NULL, NULL, ?4, ?5, NULL)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                state_json = excluded.state_json,
                dismissed_at_unix_secs = excluded.dismissed_at_unix_secs,
                updated_at_unix_secs = excluded.updated_at_unix_secs,
                retention_json = excluded.retention_json
            "#,
        )
        .bind(&args.id)
        .bind(now.as_second())
        .bind(now.as_second())
        .bind(state_json)
        .bind(retention_json)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM progress_task_tags WHERE task_id = ?1")
            .bind(&args.id)
            .execute(&mut *tx)
            .await?;
        for tag in args.tags {
            sqlx::query(
                "INSERT INTO progress_task_tags(task_id, tag_path) VALUES(?1, ?2) ON CONFLICT(task_id, tag_path) DO NOTHING",
            )
            .bind(&args.id)
            .bind(normalize_tag_path(&tag))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.registry.notify([
            ProgressEvent::TaskUpserted { id: args.id },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn add_update(&self, task_id: &str, update: ProgressUpdate) -> Res<()> {
        self.ensure_live()?;
        let now = timestamp_now();
        let update = ProgressUpdate { at: now, ..update };
        let update_json = serde_json::to_string(&update)?;
        let mut tx = self.db_pool.begin().await?;

        sqlx::query(
            "INSERT INTO progress_task_updates(task_id, at_unix_secs, update_json) VALUES(?1, ?2, ?3)",
        )
        .bind(task_id)
        .bind(update.at.as_second())
        .bind(update_json)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE progress_tasks SET updated_at_unix_secs = ?2 WHERE id = ?1")
            .bind(task_id)
            .bind(now.as_second())
            .execute(&mut *tx)
            .await?;

        if let Some(title) = &update.title {
            sqlx::query("UPDATE progress_tasks SET title = ?2 WHERE id = ?1")
                .bind(task_id)
                .bind(title)
                .execute(&mut *tx)
                .await?;
        }

        if let ProgressUpdateDeets::Completed { state, .. } = &update.deets {
            sqlx::query(
                "UPDATE progress_tasks SET state_json = ?2, dismissed_at_unix_secs = CASE WHEN ?2 = ?3 THEN ?4 ELSE dismissed_at_unix_secs END WHERE id = ?1",
            )
            .bind(task_id)
            .bind(serde_json::to_string(&state.to_task_state())?)
            .bind(serde_json::to_string(&ProgressTaskState::Dismissed)?)
            .bind(now.as_second())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        self.registry.notify([
            ProgressEvent::UpdateAdded {
                id: task_id.to_string(),
            },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn mark_viewed(&self, task_id: &str) -> Res<()> {
        self.ensure_live()?;
        let now = timestamp_now();
        sqlx::query(
            "UPDATE progress_tasks SET viewed_at_unix_secs = ?2, updated_at_unix_secs = ?2 WHERE id = ?1",
        )
        .bind(task_id)
        .bind(now.as_second())
        .execute(&self.db_pool)
        .await?;
        self.registry.notify([
            ProgressEvent::TaskUpserted {
                id: task_id.to_string(),
            },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn dismiss(&self, task_id: &str) -> Res<()> {
        self.add_update(
            task_id,
            ProgressUpdate {
                at: timestamp_epoch(),
                title: None,
                deets: ProgressUpdateDeets::Completed {
                    state: ProgressFinalState::Dismissed,
                    message: None,
                },
            },
        )
        .await
    }

    pub async fn set_retention_override(
        &self,
        task_id: &str,
        retention_override: Option<ProgressRetentionPolicy>,
    ) -> Res<()> {
        self.ensure_live()?;
        let now = timestamp_now();
        let retention_override_json = match retention_override {
            Some(policy) => Some(serde_json::to_string(&policy)?),
            None => None,
        };
        sqlx::query(
            "UPDATE progress_tasks SET retention_override_json = ?2, updated_at_unix_secs = ?3 WHERE id = ?1",
        )
        .bind(task_id)
        .bind(retention_override_json)
        .bind(now.as_second())
        .execute(&self.db_pool)
        .await?;
        self.registry.notify([
            ProgressEvent::TaskUpserted {
                id: task_id.to_string(),
            },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn clear_completed(&self) -> Res<u64> {
        self.ensure_live()?;
        let done = [
            serde_json::to_string(&ProgressTaskState::Succeeded)?,
            serde_json::to_string(&ProgressTaskState::Failed)?,
            serde_json::to_string(&ProgressTaskState::Cancelled)?,
            serde_json::to_string(&ProgressTaskState::Dismissed)?,
        ];
        let result = sqlx::query("DELETE FROM progress_tasks WHERE state_json IN (?1, ?2, ?3, ?4)")
            .bind(&done[0])
            .bind(&done[1])
            .bind(&done[2])
            .bind(&done[3])
            .execute(&self.db_pool)
            .await?;
        self.registry.notify([ProgressEvent::ListChanged]);
        Ok(result.rows_affected())
    }

    pub async fn get(&self, task_id: &str) -> Res<Option<ProgressTask>> {
        let tasks = self.fetch_tasks(Some(task_id), None).await?;
        Ok(tasks.into_iter().next())
    }

    pub async fn list(&self) -> Res<Vec<ProgressTask>> {
        self.fetch_tasks(None, None).await
    }

    pub async fn list_by_tag_prefix(&self, tag_prefix: &str) -> Res<Vec<ProgressTask>> {
        self.fetch_tasks(None, Some(normalize_tag_path(tag_prefix)))
            .await
    }

    pub async fn list_updates(&self, task_id: &str) -> Res<Vec<ProgressUpdateEntry>> {
        let rows = sqlx::query_as::<_, (i64, i64, String)>(
            "SELECT sequence, at_unix_secs, update_json FROM progress_task_updates WHERE task_id = ?1 ORDER BY sequence ASC",
        )
        .bind(task_id)
        .fetch_all(&self.db_pool)
        .await?;

        rows.into_iter()
            .map(|(sequence, at_unix_secs, update_json)| {
                let update: ProgressUpdate = serde_json::from_str(&update_json)?;
                Ok(ProgressUpdateEntry {
                    sequence,
                    at: timestamp_from_second(at_unix_secs)?,
                    update,
                })
            })
            .collect()
    }

    async fn fetch_tasks(
        &self,
        by_id: Option<&str>,
        by_tag_prefix: Option<String>,
    ) -> Res<Vec<ProgressTask>> {
        let base_rows: Vec<TaskRow> = match (by_id, by_tag_prefix) {
            (Some(id), _) => {
                sqlx::query_as(
                    r#"
                    SELECT id, title, created_at_unix_secs, updated_at_unix_secs, viewed_at_unix_secs,
                           dismissed_at_unix_secs, state_json, retention_json, retention_override_json
                    FROM progress_tasks
                    WHERE id = ?1
                    ORDER BY updated_at_unix_secs DESC
                    "#,
                )
                .bind(id)
                .fetch_all(&self.db_pool)
                .await?
            }
            (None, Some(prefix)) => {
                sqlx::query_as(
                    r#"
                    SELECT DISTINCT t.id, t.title, t.created_at_unix_secs, t.updated_at_unix_secs, t.viewed_at_unix_secs,
                                    t.dismissed_at_unix_secs, t.state_json, t.retention_json, t.retention_override_json
                    FROM progress_tasks t
                    INNER JOIN progress_task_tags tg ON t.id = tg.task_id
                    WHERE tg.tag_path = ?1 OR tg.tag_path LIKE ?2
                    ORDER BY t.updated_at_unix_secs DESC
                    "#,
                )
                .bind(&prefix)
                .bind(format!("{prefix}/%"))
                .fetch_all(&self.db_pool)
                .await?
            }
            (None, None) => {
                sqlx::query_as(
                    r#"
                    SELECT id, title, created_at_unix_secs, updated_at_unix_secs, viewed_at_unix_secs,
                           dismissed_at_unix_secs, state_json, retention_json, retention_override_json
                    FROM progress_tasks
                    ORDER BY updated_at_unix_secs DESC
                    "#,
                )
                .fetch_all(&self.db_pool)
                .await?
            }
        };

        let mut out = Vec::with_capacity(base_rows.len());
        for row in base_rows {
            let (
                id,
                title,
                created_at_unix_secs,
                updated_at_unix_secs,
                viewed_at_unix_secs,
                dismissed_at_unix_secs,
                state_json,
                retention_json,
                retention_override_json,
            ) = row;

            let tags: Vec<String> = sqlx::query_scalar(
                "SELECT tag_path FROM progress_task_tags WHERE task_id = ?1 ORDER BY tag_path ASC",
            )
            .bind(&id)
            .fetch_all(&self.db_pool)
            .await?;

            let latest_update = sqlx::query_as::<_, (i64, i64, String)>(
                "SELECT sequence, at_unix_secs, update_json FROM progress_task_updates WHERE task_id = ?1 ORDER BY sequence DESC LIMIT 1",
            )
            .bind(&id)
            .fetch_optional(&self.db_pool)
            .await?
            .map(|(sequence, at_unix_secs, update_json)| -> Res<ProgressUpdateEntry> {
                Ok(ProgressUpdateEntry {
                    sequence,
                    at: timestamp_from_second(at_unix_secs)?,
                    update: serde_json::from_str(&update_json)?,
                })
            })
            .transpose()?;

            out.push(ProgressTask {
                id,
                title: optional_title_from_db(title),
                tags,
                created_at: timestamp_from_second(created_at_unix_secs)?,
                updated_at: timestamp_from_second(updated_at_unix_secs)?,
                viewed_at: viewed_at_unix_secs.map(timestamp_from_second).transpose()?,
                dismissed_at: dismissed_at_unix_secs
                    .map(timestamp_from_second)
                    .transpose()?,
                state: serde_json::from_str(&state_json)?,
                retention: serde_json::from_str(&retention_json)?,
                retention_override: match retention_override_json {
                    Some(json) => Some(serde_json::from_str(&json)?),
                    None => None,
                },
                latest_update,
            });
        }

        Ok(out)
    }

    fn ensure_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("progress repo is stopped");
        }
        Ok(())
    }
}

impl ProgressFinalState {
    fn to_task_state(&self) -> ProgressTaskState {
        match self {
            Self::Succeeded => ProgressTaskState::Succeeded,
            Self::Failed => ProgressTaskState::Failed,
            Self::Cancelled => ProgressTaskState::Cancelled,
            Self::Dismissed => ProgressTaskState::Dismissed,
        }
    }
}

fn optional_title_from_db(value: Option<String>) -> Option<String> {
    let value = value?;
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn timestamp_now() -> Timestamp {
    Timestamp::now()
}

fn timestamp_epoch() -> Timestamp {
    Timestamp::from_second(0).expect("unix epoch second should always be valid")
}

fn timestamp_from_second(seconds: i64) -> Res<Timestamp> {
    Timestamp::from_second(seconds).map_err(Into::into)
}

fn normalize_tag_path(tag: &str) -> String {
    let trimmed = tag.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    let no_trailing = trimmed.trim_end_matches('/');
    if no_trailing.starts_with('/') {
        no_trailing.to_string()
    } else {
        format!("/{no_trailing}")
    }
}

mod progress_optional_date {
    use super::*;
    use automerge::ObjId;
    use autosurgeon::{HydrateError, ReadDoc, Reconciler};

    pub fn reconcile<R: Reconciler>(
        value: &Option<Timestamp>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        match value {
            Some(ts) => reconciler.timestamp(ts.as_second()),
            None => Ok(()),
        }
    }

    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &ObjId,
        prop: autosurgeon::Prop<'a>,
    ) -> Result<Option<Timestamp>, HydrateError> {
        use automerge::{ScalarValue, Value};

        match doc.get(obj, &prop)? {
            Some((Value::Scalar(scalar), _)) => match scalar.as_ref() {
                ScalarValue::Timestamp(ts) => Timestamp::from_second(*ts)
                    .map(Some)
                    .map_err(|err| HydrateError::unexpected("a valid timestamp", err.to_string())),
                ScalarValue::Str(val) => val.parse::<Timestamp>().map(Some).map_err(|err| {
                    HydrateError::unexpected("a valid ISO 8601 timestamp string", err.to_string())
                }),
                _ => Err(HydrateError::unexpected(
                    "a string or timestamp",
                    format!("unexpected scalar type: {:?}", scalar),
                )),
            },
            None => Ok(None),
            _ => Err(HydrateError::unexpected(
                "a scalar value",
                "value is not a scalar".to_string(),
            )),
        }
    }
}
