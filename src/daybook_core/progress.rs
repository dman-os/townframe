use crate::interlude::*;
use jiff::Timestamp;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const MAX_UPDATES_PER_TASK: usize = 256;
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(150);

enum ProgressMsg {
    UpsertTask {
        args: CreateProgressTaskArgs,
        resp: oneshot::Sender<Res<()>>,
    },
    AddUpdate {
        task_id: String,
        update: ProgressUpdate,
        resp: oneshot::Sender<Res<()>>,
    },
    MarkViewed {
        task_id: String,
        resp: oneshot::Sender<Res<()>>,
    },
    Dismiss {
        task_id: String,
        resp: oneshot::Sender<Res<()>>,
    },
    SetRetentionOverride {
        task_id: String,
        retention_override: Option<ProgressRetentionPolicy>,
        resp: oneshot::Sender<Res<()>>,
    },
    ClearCompleted {
        resp: oneshot::Sender<Res<u64>>,
    },
    Get {
        task_id: String,
        resp: oneshot::Sender<Res<Option<ProgressTask>>>,
    },
    List {
        resp: oneshot::Sender<Res<Vec<ProgressTask>>>,
    },
    ListByTagPrefix {
        tag_prefix: String,
        resp: oneshot::Sender<Res<Vec<ProgressTask>>>,
    },
    ListUpdates {
        task_id: String,
        resp: oneshot::Sender<Res<Vec<ProgressUpdateEntry>>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressUnit {
    Bytes,
    Generic { label: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct ProgressUpdate {
    pub at: Timestamp,
    pub title: Option<String>,
    pub deets: ProgressUpdateDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct ProgressUpdateEntry {
    pub sequence: i64,
    pub at: Timestamp,
    pub update: ProgressUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressTaskState {
    Active,
    Succeeded,
    Failed,
    Cancelled,
    Dismissed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressFinalState {
    Succeeded,
    Failed,
    Cancelled,
    Dismissed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ProgressRetentionPolicy {
    UserDismissable,
    AutoDismissAfter { seconds: u64 },
    DismissAfterViewed,
    Ephemeral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct ProgressTask {
    pub id: String,
    pub title: Option<String>,
    pub tags: Vec<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub viewed_at: Option<Timestamp>,
    pub dismissed_at: Option<Timestamp>,
    pub state: ProgressTaskState,
    pub retention: ProgressRetentionPolicy,
    pub retention_override: Option<ProgressRetentionPolicy>,
    pub latest_update: Option<ProgressUpdateEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    sender: mpsc::UnboundedSender<ProgressMsg>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
}

#[derive(Default)]
struct ProgressPersistQueue {
    dirty_tasks: HashSet<String>,
    dirty_tags: HashSet<String>,
    deleted_tasks: HashSet<String>,
    pending_updates: Vec<(String, ProgressUpdateEntry)>,
    touched_update_tasks: HashSet<String>,
}

impl ProgressPersistQueue {
    fn has_pending(&self) -> bool {
        !self.dirty_tasks.is_empty()
            || !self.dirty_tags.is_empty()
            || !self.deleted_tasks.is_empty()
            || !self.pending_updates.is_empty()
    }
}

#[derive(Default)]
struct UpdatesCacheEntry {
    loaded_from_db: bool,
    updates: VecDeque<ProgressUpdateEntry>,
}

struct ProgressCache {
    tasks_by_id: HashMap<String, ProgressTask>,
    updates_by_task: HashMap<String, UpdatesCacheEntry>,
    next_sequence: i64,
}

impl Default for ProgressCache {
    fn default() -> Self {
        Self {
            tasks_by_id: HashMap::new(),
            updates_by_task: HashMap::new(),
            next_sequence: 1,
        }
    }
}

struct ProgressWorker {
    db_pool: sqlx::SqlitePool,
    cache: ProgressCache,
    persist: ProgressPersistQueue,
}

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
        let mut worker = ProgressWorker {
            db_pool,
            cache: ProgressCache::default(),
            persist: ProgressPersistQueue::default(),
        };
        worker.init_schema().await?;
        worker.preload_cache().await?;

        let (sender, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            worker.run(&mut rx).await.unwrap_or_log();
        });

        Ok(Arc::new(Self {
            sender,
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: CancellationToken::new(),
        }))
    }

    pub async fn upsert_task(&self, args: CreateProgressTaskArgs) -> Res<()> {
        self.ensure_live()?;
        let id = args.id.clone();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::UpsertTask { args, resp: tx })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")??;
        self.registry.notify([
            ProgressEvent::TaskUpserted { id },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn add_update(&self, task_id: &str, update: ProgressUpdate) -> Res<()> {
        self.ensure_live()?;
        let task_id = task_id.to_string();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::AddUpdate {
                task_id: task_id.clone(),
                update,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")??;
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
        let task_id = task_id.to_string();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::MarkViewed {
                task_id: task_id.clone(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")??;
        self.registry.notify([
            ProgressEvent::TaskUpserted { id: task_id },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn dismiss(&self, task_id: &str) -> Res<()> {
        self.ensure_live()?;
        let task_id = task_id.to_string();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::Dismiss {
                task_id: task_id.clone(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")??;
        self.registry.notify([
            ProgressEvent::UpdateAdded { id: task_id },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn set_retention_override(
        &self,
        task_id: &str,
        retention_override: Option<ProgressRetentionPolicy>,
    ) -> Res<()> {
        self.ensure_live()?;
        let task_id = task_id.to_string();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::SetRetentionOverride {
                task_id: task_id.clone(),
                retention_override,
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")??;
        self.registry.notify([
            ProgressEvent::TaskUpserted { id: task_id },
            ProgressEvent::ListChanged,
        ]);
        Ok(())
    }

    pub async fn clear_completed(&self) -> Res<u64> {
        self.ensure_live()?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::ClearCompleted { resp: tx })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        let deleted = rx.await.wrap_err("progress worker response channel")??;
        self.registry.notify([ProgressEvent::ListChanged]);
        Ok(deleted)
    }

    pub async fn get(&self, task_id: &str) -> Res<Option<ProgressTask>> {
        self.ensure_live()?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::Get {
                task_id: task_id.to_string(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")?
    }

    pub async fn list(&self) -> Res<Vec<ProgressTask>> {
        self.ensure_live()?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::List { resp: tx })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")?
    }

    pub async fn list_by_tag_prefix(&self, tag_prefix: &str) -> Res<Vec<ProgressTask>> {
        self.ensure_live()?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::ListByTagPrefix {
                tag_prefix: tag_prefix.to_string(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")?
    }

    pub async fn list_updates(&self, task_id: &str) -> Res<Vec<ProgressUpdateEntry>> {
        self.ensure_live()?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProgressMsg::ListUpdates {
                task_id: task_id.to_string(),
                resp: tx,
            })
            .map_err(|_| eyre::eyre!("progress worker gone"))?;
        rx.await.wrap_err("progress worker response channel")?
    }

    fn ensure_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("progress repo is stopped");
        }
        Ok(())
    }
}

impl ProgressWorker {
    async fn run(&mut self, rx: &mut mpsc::UnboundedReceiver<ProgressMsg>) -> Res<()> {
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.flush_persist_queue().await?;
                }
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    self.handle_msg(msg).await;
                }
            }
        }

        self.flush_persist_queue().await?;
        Ok(())
    }

    async fn handle_msg(&mut self, msg: ProgressMsg) {
        match msg {
            ProgressMsg::UpsertTask { args, resp } => {
                let result = self.handle_upsert_task(args).await;
                resp.send(result).ok();
            }
            ProgressMsg::AddUpdate {
                task_id,
                update,
                resp,
            } => {
                let should_flush = matches!(update.deets, ProgressUpdateDeets::Completed { .. });
                let result = self.handle_add_update(task_id, update).await;
                if result.is_ok() && should_flush {
                    self.flush_persist_queue().await.unwrap_or_log();
                }
                resp.send(result).ok();
            }
            ProgressMsg::MarkViewed { task_id, resp } => {
                let result = self.handle_mark_viewed(task_id).await;
                resp.send(result).ok();
            }
            ProgressMsg::Dismiss { task_id, resp } => {
                let result = self.handle_dismiss(task_id).await;
                self.flush_persist_queue().await.unwrap_or_log();
                resp.send(result).ok();
            }
            ProgressMsg::SetRetentionOverride {
                task_id,
                retention_override,
                resp,
            } => {
                let result = self
                    .handle_set_retention_override(task_id, retention_override)
                    .await;
                resp.send(result).ok();
            }
            ProgressMsg::ClearCompleted { resp } => {
                let result = self.handle_clear_completed().await;
                self.flush_persist_queue().await.unwrap_or_log();
                resp.send(result).ok();
            }
            ProgressMsg::Get { task_id, resp } => {
                let result = self.handle_get(&task_id).await;
                resp.send(result).ok();
            }
            ProgressMsg::List { resp } => {
                let result = self.handle_list().await;
                resp.send(result).ok();
            }
            ProgressMsg::ListByTagPrefix { tag_prefix, resp } => {
                let result = self.handle_list_by_tag_prefix(&tag_prefix).await;
                resp.send(result).ok();
            }
            ProgressMsg::ListUpdates { task_id, resp } => {
                let result = self.handle_list_updates(&task_id).await;
                resp.send(result).ok();
            }
        }
    }

    async fn init_schema(&self) -> Res<()> {
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.db_pool)
            .await?;
        sqlx::query("PRAGMA synchronous=NORMAL")
            .execute(&self.db_pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout=5000")
            .execute(&self.db_pool)
            .await?;

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
                retention_override_json TEXT,
                latest_update_sequence INTEGER,
                latest_update_at_unix_secs INTEGER,
                latest_update_json TEXT
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

        self.ensure_progress_tasks_column("latest_update_sequence", "INTEGER")
            .await?;
        self.ensure_progress_tasks_column("latest_update_at_unix_secs", "INTEGER")
            .await?;
        self.ensure_progress_tasks_column("latest_update_json", "TEXT")
            .await?;

        Ok(())
    }

    async fn preload_cache(&mut self) -> Res<()> {
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
            Option<i64>,
            Option<i64>,
            Option<String>,
        );

        let rows: Vec<TaskRow> = sqlx::query_as(
            r#"
            SELECT id, title, created_at_unix_secs, updated_at_unix_secs, viewed_at_unix_secs,
                   dismissed_at_unix_secs, state_json, retention_json, retention_override_json,
                   latest_update_sequence, latest_update_at_unix_secs, latest_update_json
            FROM progress_tasks
            "#,
        )
        .fetch_all(&self.db_pool)
        .await?;

        for row in rows {
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
                latest_update_sequence,
                latest_update_at_unix_secs,
                latest_update_json,
            ) = row;

            let tags: Vec<String> = sqlx::query_scalar(
                "SELECT tag_path FROM progress_task_tags WHERE task_id = ?1 ORDER BY tag_path ASC",
            )
            .bind(&id)
            .fetch_all(&self.db_pool)
            .await?;

            let latest_update = match (
                latest_update_sequence,
                latest_update_at_unix_secs,
                latest_update_json,
            ) {
                (Some(sequence), Some(at_unix_secs), Some(update_json)) => {
                    Some(ProgressUpdateEntry {
                        sequence,
                        at: Timestamp::from_second(at_unix_secs)?,
                        update: serde_json::from_str(&update_json)?,
                    })
                }
                _ => None,
            };

            self.cache.tasks_by_id.insert(
                id.clone(),
                ProgressTask {
                    id,
                    title: optional_title_from_db(title),
                    tags,
                    created_at: Timestamp::from_second(created_at_unix_secs)?,
                    updated_at: Timestamp::from_second(updated_at_unix_secs)?,
                    viewed_at: viewed_at_unix_secs
                        .map(Timestamp::from_second)
                        .transpose()?,
                    dismissed_at: dismissed_at_unix_secs
                        .map(Timestamp::from_second)
                        .transpose()?,
                    state: serde_json::from_str(&state_json)?,
                    retention: serde_json::from_str(&retention_json)?,
                    retention_override: retention_override_json
                        .map(|json| serde_json::from_str(&json))
                        .transpose()?,
                    latest_update,
                },
            );
        }

        let max_sequence: Option<i64> =
            sqlx::query_scalar("SELECT MAX(sequence) FROM progress_task_updates")
                .fetch_one(&self.db_pool)
                .await?;
        self.cache.next_sequence = max_sequence.unwrap_or(0) + 1;

        Ok(())
    }

    async fn handle_upsert_task(&mut self, args: CreateProgressTaskArgs) -> Res<()> {
        let now = Timestamp::now();
        let task = self
            .cache
            .tasks_by_id
            .entry(args.id.clone())
            .or_insert_with(|| ProgressTask {
                id: args.id.clone(),
                title: None,
                tags: vec![],
                created_at: now,
                updated_at: now,
                viewed_at: None,
                dismissed_at: None,
                state: ProgressTaskState::Active,
                retention: args.retention.clone(),
                retention_override: None,
                latest_update: None,
            });

        task.tags = args
            .tags
            .into_iter()
            .map(|tag| normalize_tag_path(&tag))
            .collect();
        task.tags.sort();
        task.tags.dedup();
        task.updated_at = now;
        task.state = ProgressTaskState::Active;
        task.retention = args.retention;
        task.retention_override = None;
        task.dismissed_at = None;
        task.title = None;

        self.persist.dirty_tasks.insert(task.id.clone());
        self.persist.dirty_tags.insert(task.id.clone());
        Ok(())
    }

    async fn handle_add_update(&mut self, task_id: String, mut update: ProgressUpdate) -> Res<()> {
        let now = Timestamp::now();
        update.at = now;
        let sequence = self.cache.next_sequence;
        self.cache.next_sequence += 1;

        let task = self
            .cache
            .tasks_by_id
            .entry(task_id.clone())
            .or_insert_with(|| ProgressTask {
                id: task_id.clone(),
                title: None,
                tags: vec![],
                created_at: now,
                updated_at: now,
                viewed_at: None,
                dismissed_at: None,
                state: ProgressTaskState::Active,
                retention: ProgressRetentionPolicy::UserDismissable,
                retention_override: None,
                latest_update: None,
            });

        if let Some(title) = &update.title {
            task.title = Some(title.clone());
        }
        task.updated_at = now;

        if let ProgressUpdateDeets::Completed { state, .. } = &update.deets {
            task.state = state.to_task_state();
            if task.state == ProgressTaskState::Dismissed {
                task.dismissed_at = Some(now);
            }
        }

        let entry = ProgressUpdateEntry {
            sequence,
            at: update.at,
            update,
        };
        task.latest_update = Some(entry.clone());

        let updates = self
            .cache
            .updates_by_task
            .entry(task_id.clone())
            .or_default();
        updates.updates.push_back(entry.clone());
        trim_updates(&mut updates.updates);

        self.persist.pending_updates.push((task_id.clone(), entry));
        self.persist.touched_update_tasks.insert(task_id.clone());
        self.persist.dirty_tasks.insert(task_id);

        Ok(())
    }

    async fn handle_mark_viewed(&mut self, task_id: String) -> Res<()> {
        let task = self
            .cache
            .tasks_by_id
            .get_mut(&task_id)
            .ok_or_else(|| eyre::eyre!("progress task not found: {task_id}"))?;
        let now = Timestamp::now();
        task.viewed_at = Some(now);
        task.updated_at = now;
        self.persist.dirty_tasks.insert(task_id);
        Ok(())
    }

    async fn handle_dismiss(&mut self, task_id: String) -> Res<()> {
        self.handle_add_update(
            task_id,
            ProgressUpdate {
                at: Timestamp::now(),
                title: None,
                deets: ProgressUpdateDeets::Completed {
                    state: ProgressFinalState::Dismissed,
                    message: None,
                },
            },
        )
        .await
    }

    async fn handle_set_retention_override(
        &mut self,
        task_id: String,
        retention_override: Option<ProgressRetentionPolicy>,
    ) -> Res<()> {
        let task = self
            .cache
            .tasks_by_id
            .get_mut(&task_id)
            .ok_or_else(|| eyre::eyre!("progress task not found: {task_id}"))?;
        task.retention_override = retention_override;
        task.updated_at = Timestamp::now();
        self.persist.dirty_tasks.insert(task_id);
        Ok(())
    }

    async fn handle_clear_completed(&mut self) -> Res<u64> {
        let before_len = self.cache.tasks_by_id.len();
        let remove_ids: Vec<String> = self
            .cache
            .tasks_by_id
            .iter()
            .filter_map(|(id, task)| {
                if matches!(
                    task.state,
                    ProgressTaskState::Succeeded
                        | ProgressTaskState::Failed
                        | ProgressTaskState::Cancelled
                        | ProgressTaskState::Dismissed
                ) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for task_id in &remove_ids {
            self.cache.tasks_by_id.remove(task_id);
            self.cache.updates_by_task.remove(task_id);
            self.persist.deleted_tasks.insert(task_id.clone());
            self.persist.dirty_tasks.remove(task_id);
            self.persist.dirty_tags.remove(task_id);
        }

        self.persist
            .pending_updates
            .retain(|(task_id, _)| !self.persist.deleted_tasks.contains(task_id));

        Ok((before_len.saturating_sub(self.cache.tasks_by_id.len())) as u64)
    }

    async fn handle_get(&mut self, task_id: &str) -> Res<Option<ProgressTask>> {
        Ok(self.cache.tasks_by_id.get(task_id).cloned())
    }

    async fn handle_list(&mut self) -> Res<Vec<ProgressTask>> {
        let mut tasks: Vec<_> = self.cache.tasks_by_id.values().cloned().collect();
        tasks.sort_by_key(|task| std::cmp::Reverse(task.updated_at.as_second()));
        Ok(tasks)
    }

    async fn handle_list_by_tag_prefix(&mut self, tag_prefix: &str) -> Res<Vec<ProgressTask>> {
        let normalized = normalize_tag_path(tag_prefix);
        let starts_with = format!("{normalized}/");
        let mut tasks: Vec<_> = self
            .cache
            .tasks_by_id
            .values()
            .filter(|task| {
                task.tags
                    .iter()
                    .any(|tag| tag == &normalized || tag.starts_with(&starts_with))
            })
            .cloned()
            .collect();
        tasks.sort_by_key(|task| std::cmp::Reverse(task.updated_at.as_second()));
        Ok(tasks)
    }

    async fn handle_list_updates(&mut self, task_id: &str) -> Res<Vec<ProgressUpdateEntry>> {
        self.ensure_updates_loaded(task_id).await?;
        Ok(self
            .cache
            .updates_by_task
            .get(task_id)
            .map(|entry| entry.updates.iter().cloned().collect())
            .unwrap_or_default())
    }

    async fn ensure_updates_loaded(&mut self, task_id: &str) -> Res<()> {
        let needs_load = match self.cache.updates_by_task.get(task_id) {
            Some(entry) => !entry.loaded_from_db,
            None => true,
        };
        if !needs_load {
            return Ok(());
        }

        let rows = sqlx::query_as::<_, (i64, i64, String)>(
            "SELECT sequence, at_unix_secs, update_json FROM progress_task_updates WHERE task_id = ?1 ORDER BY sequence DESC LIMIT ?2",
        )
        .bind(task_id)
        .bind(MAX_UPDATES_PER_TASK as i64)
        .fetch_all(&self.db_pool)
        .await?;

        let mut from_db: Vec<ProgressUpdateEntry> = rows
            .into_iter()
            .map(
                |(sequence, at_unix_secs, update_json)| -> Res<ProgressUpdateEntry> {
                    Ok(ProgressUpdateEntry {
                        sequence,
                        at: Timestamp::from_second(at_unix_secs)?,
                        update: serde_json::from_str(&update_json)?,
                    })
                },
            )
            .collect::<Res<Vec<_>>>()?;
        from_db.reverse();

        let entry = self
            .cache
            .updates_by_task
            .entry(task_id.to_string())
            .or_default();

        let mut by_sequence: HashMap<i64, ProgressUpdateEntry> = HashMap::new();
        for update in from_db {
            by_sequence.insert(update.sequence, update);
        }
        for update in entry.updates.iter().cloned() {
            by_sequence.insert(update.sequence, update);
        }

        let mut merged: Vec<ProgressUpdateEntry> = by_sequence.into_values().collect();
        merged.sort_by_key(|update| update.sequence);
        if merged.len() > MAX_UPDATES_PER_TASK {
            let keep_from = merged.len() - MAX_UPDATES_PER_TASK;
            merged.drain(0..keep_from);
        }

        entry.updates = VecDeque::from(merged);
        entry.loaded_from_db = true;
        Ok(())
    }

    async fn flush_persist_queue(&mut self) -> Res<()> {
        if !self.persist.has_pending() {
            return Ok(());
        }

        let mut tx = self.db_pool.begin().await?;

        let deleted_tasks: Vec<String> = self.persist.deleted_tasks.drain().collect();
        for task_id in deleted_tasks {
            sqlx::query("DELETE FROM progress_tasks WHERE id = ?1")
                .bind(task_id)
                .execute(&mut *tx)
                .await?;
        }

        let dirty_tasks: Vec<String> = self.persist.dirty_tasks.drain().collect();
        for task_id in dirty_tasks {
            let Some(task) = self.cache.tasks_by_id.get(&task_id) else {
                continue;
            };

            sqlx::query(
                r#"
                INSERT INTO progress_tasks(
                    id, title, created_at_unix_secs, updated_at_unix_secs, viewed_at_unix_secs,
                    dismissed_at_unix_secs, state_json, retention_json, retention_override_json,
                    latest_update_sequence, latest_update_at_unix_secs, latest_update_json
                ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    updated_at_unix_secs = excluded.updated_at_unix_secs,
                    viewed_at_unix_secs = excluded.viewed_at_unix_secs,
                    dismissed_at_unix_secs = excluded.dismissed_at_unix_secs,
                    state_json = excluded.state_json,
                    retention_json = excluded.retention_json,
                    retention_override_json = excluded.retention_override_json,
                    latest_update_sequence = excluded.latest_update_sequence,
                    latest_update_at_unix_secs = excluded.latest_update_at_unix_secs,
                    latest_update_json = excluded.latest_update_json
                "#,
            )
            .bind(&task.id)
            .bind(task.title.clone().unwrap_or_default())
            .bind(task.created_at.as_second())
            .bind(task.updated_at.as_second())
            .bind(task.viewed_at.map(|ts| ts.as_second()))
            .bind(task.dismissed_at.map(|ts| ts.as_second()))
            .bind(serde_json::to_string(&task.state)?)
            .bind(serde_json::to_string(&task.retention)?)
            .bind(
                task.retention_override
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            )
            .bind(task.latest_update.as_ref().map(|update| update.sequence))
            .bind(
                task.latest_update
                    .as_ref()
                    .map(|update| update.at.as_second()),
            )
            .bind(
                task.latest_update
                    .as_ref()
                    .map(|update| serde_json::to_string(&update.update))
                    .transpose()?,
            )
            .execute(&mut *tx)
            .await?;
        }

        let dirty_tags: Vec<String> = self.persist.dirty_tags.drain().collect();
        for task_id in dirty_tags {
            let Some(task) = self.cache.tasks_by_id.get(&task_id) else {
                continue;
            };

            sqlx::query("DELETE FROM progress_task_tags WHERE task_id = ?1")
                .bind(&task_id)
                .execute(&mut *tx)
                .await?;

            for tag in &task.tags {
                sqlx::query(
                    "INSERT INTO progress_task_tags(task_id, tag_path) VALUES(?1, ?2) ON CONFLICT(task_id, tag_path) DO NOTHING",
                )
                .bind(&task_id)
                .bind(tag)
                .execute(&mut *tx)
                .await?;
            }
        }

        for (task_id, update) in self.persist.pending_updates.drain(..) {
            sqlx::query(
                "INSERT OR REPLACE INTO progress_task_updates(sequence, task_id, at_unix_secs, update_json) VALUES(?1, ?2, ?3, ?4)",
            )
            .bind(update.sequence)
            .bind(task_id)
            .bind(update.at.as_second())
            .bind(serde_json::to_string(&update.update)?)
            .execute(&mut *tx)
            .await?;
        }

        let touched_update_tasks: Vec<String> = self.persist.touched_update_tasks.drain().collect();
        for task_id in touched_update_tasks {
            sqlx::query(
                r#"
                DELETE FROM progress_task_updates
                WHERE task_id = ?1
                  AND sequence <= COALESCE(
                        (
                            SELECT sequence
                            FROM progress_task_updates
                            WHERE task_id = ?1
                            ORDER BY sequence DESC
                            LIMIT 1 OFFSET ?2
                        ),
                        -1
                    )
                "#,
            )
            .bind(task_id)
            .bind((MAX_UPDATES_PER_TASK - 1) as i64)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn ensure_progress_tasks_column(&self, name: &str, ty: &str) -> Res<()> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('progress_tasks') WHERE name = ?1",
        )
        .bind(name)
        .fetch_one(&self.db_pool)
        .await?;
        if exists == 0 {
            sqlx::query(&format!(
                "ALTER TABLE progress_tasks ADD COLUMN {name} {ty}"
            ))
            .execute(&self.db_pool)
            .await?;
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

fn trim_updates(updates: &mut VecDeque<ProgressUpdateEntry>) {
    while updates.len() > MAX_UPDATES_PER_TASK {
        updates.pop_front();
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
