use automerge::{Automerge, ChangeHash, ObjId, ObjType, Prop, ROOT, ReadDoc, Value};
use big_cabinet_core::{DocId, FacetKey, FacetTag};
use big_repo::{
    AutomergeFrontierEvent, AutomergeFrontierRevisionStore, AutomergeFrontierSelector,
    AutomergeFrontierTarget, BigRepoLocalFilter, BigRepoLocalNotification, DocLookup,
    SharedBigRepo,
};
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync::keyed_frontier::{SqliteFrontierCodec, SqliteFrontierSelector, SqliteKeyedFrontier};
use big_sync_core::concurrent_delta_walker::{ConcurrentDeltaRead, ConcurrentDeltaWalker};
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::keyed_frontier::{FrontierEntry, KeyedFrontier, KeyedFrontierError};
use big_sync_core::revisioned_store::{KeyedFrontierRevisionReader, RevisionedStore};
use futures::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Row, Sqlite};
use sqlx_utils_rs::SqlCtx;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::{Res, async_trait, ferr};

const FACET_SET_ID: &str = "big-cabinet-facet-set-v2";
const DMETA_KEY: &str = "org.example.daybook.dmeta/main";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FacetRoute {
    pub doc_id: DocId,
    pub facet_key: FacetKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FacetMembership {
    Present {
        doc_heads: Vec<ChangeHash>,
        facet_heads: Vec<ChangeHash>,
        /// Lamport clock of `facet_heads`: the greatest `max_op` among the
        /// introducing changes. See `FacetVersion` in the DocsRepo protocol.
        clock: u64,
    },
    Removed {
        doc_heads: Option<Vec<ChangeHash>>,
        facet_heads: Vec<ChangeHash>,
        clock: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRecord {
    pub route: FacetRoute,
    pub doc_heads: Vec<ChangeHash>,
    pub facet_heads: Vec<ChangeHash>,
    pub clock: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FacetSetQuery {
    Doc(DocId),
    Tag(FacetTag),
    Route(FacetRoute),
    /// Every live route with exactly this facet key, across documents.
    Key(FacetKey),
}

fn facet_record(row: sqlx::sqlite::SqliteRow) -> Res<FacetRecord> {
    Ok(FacetRecord {
        route: FacetRoute {
            doc_id: row.get::<String, _>("doc_id").parse()?,
            facet_key: row.get::<String, _>("facet_key").parse()?,
        },
        doc_heads: serde_json::from_slice(row.get("doc_heads"))?,
        facet_heads: serde_json::from_slice(row.get("facet_heads"))?,
        clock: u64::try_from(row.get::<i64, _>("clock"))
            .map_err(|_| utils_rs::prelude::ferr!("facet-set clock is out of range"))?,
    })
}

#[derive(Clone)]
struct FacetSetCodec;

impl SqliteFrontierCodec for FacetSetCodec {
    type Key = FacetRoute;
    type Value = FacetMembership;

    fn encode_key(&self, key: &Self::Key) -> Vec<u8> {
        serde_json::to_vec(key).expect("facet routes serialize")
    }

    fn decode_key(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Key, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn encode_value(&self, value: &Self::Value) -> Vec<u8> {
        serde_json::to_vec(value).expect("facet membership serializes")
    }

    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn namespace(&self, key: &Self::Key) -> Option<Vec<u8>> {
        Some(key.facet_key.tag().as_str().as_bytes().to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DocState {
    heads: Option<Vec<ChangeHash>>,
    epoch: Option<[u8; 32]>,
    cursor: u64,
}

#[derive(Clone)]
struct Work {
    doc_id: DocId,
    state: DocState,
}

struct Prepared {
    work: Work,
    previous: Option<DocState>,
    slots: BTreeMap<FacetKey, FacetMembership>,
    replace_all: bool,
}

#[derive(Clone)]
pub struct FacetSetStore {
    sql: SqlCtx,
    frontier: SqliteKeyedFrontier<FacetSetCodec>,
    input_state: SqliteDeltaWalkerStateRepo,
}

impl FacetSetStore {
    pub async fn new(sql: SqlCtx) -> Res<Self> {
        for statement in [
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_docs (
                doc_id TEXT PRIMARY KEY NOT NULL
                , state BLOB NOT NULL
            ) STRICT"#,
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_entries (
                doc_id TEXT NOT NULL
                , facet_key TEXT NOT NULL
                , facet_tag TEXT NOT NULL
                , doc_heads BLOB NOT NULL
                , facet_heads BLOB NOT NULL
                , clock INTEGER NOT NULL
                , PRIMARY KEY (doc_id, facet_key)
            ) STRICT"#,
            r#"CREATE INDEX IF NOT EXISTS big_cabinet_facet_set_entries_tag_idx
                 ON big_cabinet_facet_set_entries (facet_tag, doc_id)"#,
            r#"CREATE INDEX IF NOT EXISTS big_cabinet_facet_set_entries_key_idx
                 ON big_cabinet_facet_set_entries (facet_key, doc_id)"#,
        ] {
            sqlx::query(statement).execute(&sql.write_pool).await?;
        }
        let frontier = SqliteKeyedFrontier::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            FACET_SET_ID,
            FacetSetCodec,
            Arc::new(Notify::new()),
        )
        .await?;
        let input_state = SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            FACET_SET_ID,
            "automerge-frontier",
        )
        .await?;
        Ok(Self {
            sql,
            frontier,
            input_state,
        })
    }

    /// Open the frontier across every tag namespace, for consumers (such as
    /// doc-scoped observation) whose filter is not a single tag.
    pub async fn open_all(
        &self,
        after: u64,
    ) -> Result<KeyedFrontierRevisionReader<'_, FacetRoute, FacetMembership>, KeyedFrontierError>
    {
        Ok(KeyedFrontierRevisionReader::new(
            self.frontier
                .open(SqliteFrontierSelector::All { after })
                .await?,
        ))
    }

    /// Open the frontier across several tag namespaces at once, each at its own cursor.
    pub async fn open_tags(
        &self,
        tags: impl IntoIterator<Item = (FacetTag, u64)>,
    ) -> Result<KeyedFrontierRevisionReader<'_, FacetRoute, FacetMembership>, KeyedFrontierError>
    {
        Ok(KeyedFrontierRevisionReader::new(
            self.frontier
                .open(SqliteFrontierSelector::Namespaces(
                    tags.into_iter()
                        .map(|(tag, after)| (tag.as_str().as_bytes().to_vec(), after))
                        .collect(),
                ))
                .await?,
        ))
    }

    pub async fn source_revision(&self) -> Res<u64> {
        Ok(self.input_state.progress().await?.upstream_revision)
    }

    async fn doc_state(&self, doc_id: &DocId) -> Res<Option<DocState>> {
        let bytes: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT state FROM big_cabinet_facet_set_docs WHERE doc_id = ?")
                .bind(doc_id.to_string())
                .fetch_optional(&self.sql.read_pool)
                .await?;
        bytes
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    pub async fn query(&self, selector: FacetSetQuery) -> Res<Vec<FacetRecord>> {
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"SELECT doc_id
                 , facet_key
                 , doc_heads
                 , facet_heads
                 , clock
              FROM big_cabinet_facet_set_entries
             WHERE "#,
        );
        match selector {
            FacetSetQuery::Doc(doc) => {
                query.push("doc_id = ").push_bind(doc.to_string());
            }
            FacetSetQuery::Tag(tag) => {
                query.push("facet_tag = ").push_bind(tag.to_string());
            }
            FacetSetQuery::Route(route) => {
                query
                    .push("doc_id = ")
                    .push_bind(route.doc_id.to_string())
                    .push(" AND facet_key = ")
                    .push_bind(route.facet_key.to_string());
            }
            FacetSetQuery::Key(key) => {
                query.push("facet_key = ").push_bind(key.to_string());
            }
        }
        query.push(" ORDER BY doc_id, facet_key");
        query
            .build()
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(facet_record)
            .collect()
    }

    /// Query every live route in `docs`, optionally narrowed to `tag` or `key`.
    pub async fn query_docs(
        &self,
        docs: &BTreeSet<DocId>,
        tag: Option<&FacetTag>,
        key: Option<&FacetKey>,
    ) -> Res<Vec<FacetRecord>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"SELECT doc_id
                 , facet_key
                 , doc_heads
                 , facet_heads
                 , clock
              FROM big_cabinet_facet_set_entries
              WHERE doc_id IN (SELECT value FROM json_each("#,
        );
        query.push_bind(serde_json::to_string(docs)?).push("))");
        if let Some(tag) = tag {
            query
                .push(" AND facet_tag = ")
                .push_bind(tag.as_str().to_owned());
        }
        if let Some(key) = key {
            query.push(" AND facet_key = ").push_bind(key.to_string());
        }
        query.push(" ORDER BY doc_id, facet_key");
        query
            .build()
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(facet_record)
            .collect()
    }

    async fn settle(&self, prepared: Prepared) -> Res<()> {
        let mut transaction = self.sql.write_pool.begin().await?;
        let doc_id = prepared.work.doc_id.to_string();
        let stored: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT state FROM big_cabinet_facet_set_docs WHERE doc_id = ?")
                .bind(&doc_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let stored: Option<DocState> = stored
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()?;
        if stored
            .as_ref()
            .is_some_and(|state| state.cursor >= prepared.work.state.cursor)
        {
            transaction.rollback().await?;
            return Ok(());
        }
        if stored != prepared.previous {
            return Err(ferr!(
                "facet-set document changed during preparation: {doc_id}"
            ));
        }
        let mut slots = prepared.slots;
        if prepared.replace_all {
            let old: Vec<String> = sqlx::query_scalar(
                "SELECT facet_key FROM big_cabinet_facet_set_entries WHERE doc_id = ?",
            )
            .bind(&doc_id)
            .fetch_all(&mut *transaction)
            .await?;
            for key in old {
                slots
                    .entry(key.parse()?)
                    .or_insert_with(|| FacetMembership::Removed {
                        doc_heads: prepared.work.state.heads.clone(),
                        facet_heads: Vec::new(),
                        clock: 0,
                    });
            }
        }
        let mutations = slots.iter().map(|(key, value)| {
            (
                FacetRoute {
                    doc_id: prepared.work.doc_id.clone(),
                    facet_key: key.clone(),
                },
                Some(value.clone()),
            )
        });
        if !slots.is_empty() {
            self.frontier
                .apply_in_context(&mut transaction, mutations)
                .await?;
        }
        for (key, value) in slots {
            match value {
                FacetMembership::Present {
                    doc_heads,
                    facet_heads,
                    clock,
                } => {
                    sqlx::query(
                        r#"INSERT INTO big_cabinet_facet_set_entries (
                        doc_id
                        , facet_key
                        , facet_tag
                        , doc_heads
                        , facet_heads
                        , clock
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (doc_id, facet_key) DO UPDATE SET
                        doc_heads = excluded.doc_heads
                        , facet_heads = excluded.facet_heads
                        , clock = excluded.clock"#,
                    )
                    .bind(&doc_id)
                    .bind(key.to_string())
                    .bind(key.tag().as_str())
                    .bind(serde_json::to_vec(&doc_heads)?)
                    .bind(serde_json::to_vec(&facet_heads)?)
                    .bind(
                        i64::try_from(clock).map_err(|_| {
                            utils_rs::prelude::ferr!("facet-set clock is out of range")
                        })?,
                    )
                    .execute(&mut *transaction)
                    .await?;
                }
                FacetMembership::Removed { .. } => {
                    sqlx::query("DELETE FROM big_cabinet_facet_set_entries WHERE doc_id = ? AND facet_key = ?")
                        .bind(&doc_id).bind(key.to_string()).execute(&mut *transaction).await?;
                }
            }
        }
        sqlx::query(
            r#"INSERT INTO big_cabinet_facet_set_docs (doc_id, state) VALUES (?, ?)
            ON CONFLICT (doc_id) DO UPDATE SET state = excluded.state"#,
        )
        .bind(doc_id)
        .bind(serde_json::to_vec(&prepared.work.state)?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.frontier.notify_changed();
        Ok(())
    }
}

struct DocView<'a> {
    doc: &'a Automerge,
    heads: &'a [ChangeHash],
    current: bool,
    facets: Vec<ObjId>,
    metadata: Vec<ObjId>,
}

impl<'a> DocView<'a> {
    fn new(doc: &'a Automerge, heads: &'a [ChangeHash]) -> Res<Self> {
        let mut current_heads = doc.get_heads();
        current_heads.sort_unstable();
        let mut view = Self {
            doc,
            heads,
            current: current_heads == heads,
            facets: Vec::new(),
            metadata: Vec::new(),
        };
        view.facets = view.maps(&ROOT, "facets")?;
        for facets in &view.facets {
            for dmeta in view.maps(facets, DMETA_KEY)? {
                view.metadata.extend(view.maps(&dmeta, "facets")?);
            }
        }
        Ok(view)
    }

    fn all(&self, object: &ObjId, prop: impl Into<Prop>) -> Res<Vec<(Value<'a>, ObjId)>> {
        Ok(if self.current {
            self.doc.get_all(object, prop)?
        } else {
            self.doc.get_all_at(object, prop, self.heads)?
        })
    }

    fn maps(&self, object: &ObjId, prop: &str) -> Res<Vec<ObjId>> {
        Ok(self
            .all(object, prop)?
            .into_iter()
            .filter_map(|(value, id)| (value == Value::Object(ObjType::Map)).then_some(id))
            .collect())
    }

    fn keys(&self) -> BTreeSet<FacetKey> {
        self.facets
            .iter()
            .chain(&self.metadata)
            .flat_map(|object| {
                let range = if self.current {
                    self.doc.map_range(object, ..)
                } else {
                    self.doc.map_range_at(object, .., self.heads)
                };
                range
                    .filter_map(|item| item.key.parse::<FacetKey>().ok())
                    .filter(|key| key.to_string() != DMETA_KEY)
            })
            .collect()
    }

    fn slot(&self, key: &FacetKey) -> Res<FacetMembership> {
        let text = key.to_string();
        let mut present = false;
        for facets in &self.facets {
            present |= !self.all(facets, text.as_str())?.is_empty();
        }
        let mut heads = BTreeSet::new();
        for metadata in &self.metadata {
            for facet in self.maps(metadata, &text)? {
                for (value, list) in self.all(&facet, "signatures")? {
                    if value != Value::Object(ObjType::List) {
                        continue;
                    }
                    let range = if self.current {
                        self.doc.list_range(&list, ..)
                    } else {
                        self.doc.list_range_at(&list, .., self.heads)
                    };
                    for item in range {
                        if item.conflict {
                            for (_, marker) in self.all(&list, item.index)? {
                                heads.insert(
                                    self.doc
                                        .hash_for_opid(&marker)
                                        .expect("observed operation has an introducing change"),
                                );
                            }
                        } else {
                            heads.insert(
                                self.doc
                                    .hash_for_opid(&item.id())
                                    .expect("observed operation has an introducing change"),
                            );
                        }
                    }
                }
            }
        }
        let clock = heads
            .iter()
            .filter_map(|head| self.doc.get_change_by_hash(head))
            .map(|change| change.max_op())
            .max()
            .unwrap_or(0);
        let facet_heads = heads.into_iter().collect();
        Ok(if present {
            FacetMembership::Present {
                doc_heads: self.heads.to_vec(),
                facet_heads,
                clock,
            }
        } else {
            FacetMembership::Removed {
                doc_heads: Some(self.heads.to_vec()),
                facet_heads,
                clock,
            }
        })
    }

    fn project(
        &self,
        previous: Option<&DocState>,
    ) -> Res<(BTreeMap<FacetKey, FacetMembership>, bool)> {
        let old_heads = previous.and_then(|state| state.heads.as_deref());
        let excluded: BTreeSet<_> = self
            .doc
            .get_changes(self.heads)
            .into_iter()
            .map(|change| change.hash())
            .collect();
        let mut full = old_heads.is_none_or(|heads| {
            heads.is_empty()
                || !self.doc.get_missing_deps(heads).is_empty()
                || heads.iter().any(|head| excluded.contains(head))
        });
        let mut touched = BTreeSet::new();
        if !full {
            let old_heads = old_heads.expect("incremental projection has prior heads");
            let mut paths = HashMap::new();
            for change in self.doc.get_changes(old_heads) {
                if excluded.contains(&change.hash()) {
                    continue;
                }
                for op in change.decode().operations {
                    // Resolve the object's path as of the change containing
                    // the op, not the current heads: a facet write followed
                    // by a removal inside the same projection window leaves
                    // the op's object tombstoned at the current heads, and
                    // automerge's clock-less parent walk cannot seek an
                    // invisible list insert that trails its object's visible
                    // ops (fast/slow `seek_list_opid` divergence trips a
                    // debug_assert in automerge's op_set). Every op target is
                    // visible at its own change's head, and object paths are
                    // immutable, so the historical path classifies the op the
                    // same way. Writers must not create an object and delete
                    // its container within a single change.
                    let path = paths
                        .entry(op.obj.clone())
                        .or_insert_with(|| match &op.obj {
                            automerge::legacy::ObjectId::Root => Vec::new(),
                            automerge::legacy::ObjectId::Id(id) => self
                                .doc
                                .parents_at(ObjId::Id(id.0, id.1.clone(), 0), &[change.hash()])
                                .expect("applied operation targets an existing object")
                                .path()
                                .into_iter()
                                .map(|(_, prop)| prop)
                                .collect(),
                        });
                    let name = |index| match path.get(index) {
                        Some(Prop::Map(key)) => Some(key.as_str()),
                        None if index == path.len() => match &op.key {
                            automerge::legacy::Key::Map(key) => Some(key.as_str()),
                            automerge::legacy::Key::Seq(_) => None,
                        },
                        Some(Prop::Seq(_)) | None => None,
                    };
                    if name(0) != Some("facets") {
                        continue;
                    }
                    let key = match (name(1), name(2), name(3)) {
                        (None, _, _) => {
                            full = true;
                            None
                        }
                        (Some(DMETA_KEY), Some("facets"), Some(key)) => Some(key),
                        // Aliases, actors and timestamps do not change membership
                        // or signature heads; only the `facets` subtree does.
                        (Some(DMETA_KEY), Some(_), _) => None,
                        (Some(DMETA_KEY), _, _) => {
                            full = true;
                            None
                        }
                        (Some(key), _, _) => Some(key),
                    };
                    if let Some(key) = key.and_then(|key| key.parse::<FacetKey>().ok()) {
                        touched.insert(key);
                    }
                }
            }
        }
        if full {
            touched.extend(self.keys());
        }
        let slots = touched
            .into_iter()
            .map(|key| Ok((key.clone(), self.slot(&key)?)))
            .collect::<Res<_>>()?;
        Ok((slots, full))
    }
}

impl FacetSetStore {
    async fn prepare(&self, repo: &SharedBigRepo, work: Work) -> Res<Option<Prepared>> {
        let previous = self.doc_state(&work.doc_id).await?;
        if previous
            .as_ref()
            .is_some_and(|state| state.cursor >= work.state.cursor)
        {
            return Ok(Some(Prepared {
                work,
                previous,
                slots: BTreeMap::new(),
                replace_all: false,
            }));
        }
        let Some(heads) = &work.state.heads else {
            return Ok(Some(Prepared {
                work,
                previous,
                slots: BTreeMap::new(),
                replace_all: true,
            }));
        };
        if previous
            .as_ref()
            .is_some_and(|state| state.heads == work.state.heads && state.epoch == work.state.epoch)
        {
            return Ok(Some(Prepared {
                work,
                previous,
                slots: BTreeMap::new(),
                replace_all: false,
            }));
        }
        let handle = match repo.get_doc_for_projection(&work.doc_id).await? {
            DocLookup::Ready(handle) => handle,
            DocLookup::Missing | DocLookup::PendingMaterialization => return Ok(None),
        };
        let projection = handle
            .with_document_read(|doc| -> Res<_> {
                if !doc.get_missing_deps(heads).is_empty() {
                    return Ok(None);
                }
                let view = DocView::new(doc, heads)?;
                let baseline = previous
                    .as_ref()
                    .filter(|state| state.epoch == work.state.epoch);
                Ok(Some(view.project(baseline)?))
            })
            .await?;
        Ok(projection.map(|(slots, replace_all)| Prepared {
            work,
            previous,
            slots,
            replace_all,
        }))
    }

    pub async fn run(&self, repo: SharedBigRepo, concurrency: NonZeroUsize) -> Res<()> {
        self.run_until(repo, concurrency, std::future::pending(), &mut None)
            .await
    }

    pub(crate) async fn run_until(
        &self,
        repo: SharedBigRepo,
        concurrency: NonZeroUsize,
        shutdown: impl std::future::Future<Output = ()> + Send,
        ready: &mut Option<tokio::sync::oneshot::Sender<Res<()>>>,
    ) -> Res<()> {
        let source = AutomergeFrontierRevisionStore::new(repo.frontier_part_store());
        self.drive(
            &source,
            AutomergeFrontierSelector {
                targets: vec![AutomergeFrontierTarget::All],
            },
            repo,
            concurrency,
            shutdown,
            ready,
        )
        .await
    }

    async fn drive<S>(
        &self,
        source: &S,
        selector: S::Selector,
        repo: SharedBigRepo,
        concurrency: NonZeroUsize,
        shutdown: impl std::future::Future<Output = ()> + Send,
        ready: &mut Option<tokio::sync::oneshot::Sender<Res<()>>>,
    ) -> Res<()>
    where
        S: RevisionedStore<
                Revision = u64,
                Entry = AutomergeFrontierEvent,
                Error = utils_rs::prelude::eyre::Report,
            >,
    {
        let (_registration, mut wakes) = repo
            .subscribe_local_listener(BigRepoLocalFilter { doc_id: None })
            .await?;
        let reader = source.open(selector, self.source_revision().await?).await?;
        let mut walker: ConcurrentDeltaWalker<'_, S, _, DocId> = ConcurrentDeltaWalker::open(
            reader,
            self.input_state.clone(),
            |event: &AutomergeFrontierEvent| match event {
                AutomergeFrontierEvent::Changed { doc_id, .. }
                | AutomergeFrontierEvent::Removed { doc_id, .. } => doc_id.clone(),
            },
        )
        .await?;
        if let Some(ready) = ready.take() {
            ready.send(Ok(())).expect("index boot waits for readiness");
        }
        let tasks = FuturesUnordered::new();
        let mut pending: BTreeMap<DocId, Work> = BTreeMap::new();
        let mut running = BTreeSet::new();
        let mut ready_work = BTreeSet::new();
        let mut parked = BTreeSet::new();
        let mut early_wakes = BTreeSet::new();
        let mut stopping = false;
        tokio::pin!(shutdown, tasks);
        loop {
            if !stopping {
                while tasks.len() < concurrency.get() {
                    let Some(doc_id) = ready_work.pop_first() else {
                        break;
                    };
                    let work = pending
                        .get(&doc_id)
                        .expect("queued work is pending")
                        .clone();
                    assert!(running.insert(work.doc_id.clone()));
                    let store = self.clone();
                    let repo = Arc::clone(&repo);
                    tasks.push(async move {
                        let prepared = store.prepare(&repo, work.clone()).await;
                        (work, prepared)
                    });
                }
            } else if tasks.is_empty() {
                return Ok(());
            }
            let available = concurrency.get().saturating_sub(tasks.len());
            tokio::select! {
                biased;
                () = &mut shutdown, if !stopping => stopping = true,
                completion = tasks.next(), if !tasks.is_empty() => {
                    let (work, prepared) = completion.expect("active preparation completes");
                    assert!(running.remove(&work.doc_id));
                    let prepared = prepared?;
                    if pending.get(&work.doc_id).is_some_and(|latest| latest.state.cursor > work.state.cursor) {
                        ready_work.insert(work.doc_id);
                        continue;
                    }
                    match prepared {
                        Some(prepared) => {
                            self.settle(prepared).await?;
                            walker.ack(work.doc_id.clone(), work.state.cursor).await?;
                            pending.remove(&work.doc_id);
                            early_wakes.remove(&work.doc_id);
                        }
                        None => {
                            if early_wakes.remove(&work.doc_id) {
                                ready_work.insert(work.doc_id);
                            } else {
                                parked.insert(work.doc_id);
                            }
                        }
                    }
                }
                read = walker.next(NonZeroUsize::new(available.max(1)).expect("positive batch")), if !stopping && available > 0 => {
                    match read? {
                        ConcurrentDeltaRead::ReplayComplete { .. } => {},
                        ConcurrentDeltaRead::Entries { entries, .. } => for delta in entries {
                            let (heads, epoch) = match delta.entry {
                                AutomergeFrontierEvent::Changed { heads, causal_epoch, .. } => {
                                    let mut heads = heads.to_vec(); heads.sort_unstable(); heads.dedup(); (Some(heads), causal_epoch)
                                }
                                AutomergeFrontierEvent::Removed { .. } => (None, None),
                            };
                            let work = Work { doc_id: delta.key, state: DocState { heads, epoch, cursor: delta.cursor } };
                            if pending.get(&work.doc_id).is_some_and(|old| old.state.cursor >= work.state.cursor) { continue; }
                            parked.remove(&work.doc_id);
                            if !running.contains(&work.doc_id) {
                                ready_work.insert(work.doc_id.clone());
                            }
                            pending.insert(work.doc_id.clone(), work);
                        },
                    }
                }
                notifications = wakes.recv(), if !stopping => {
                    for notification in notifications.ok_or_else(|| ferr!("facet-set materialization listener closed"))? {
                        let doc_id = match notification {
                            BigRepoLocalNotification::DocCreated { doc_id, .. } | BigRepoLocalNotification::DocImported { doc_id, .. }
                            | BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. } | BigRepoLocalNotification::DocMaterializationReady { doc_id, .. } => doc_id,
                            BigRepoLocalNotification::DocMaterializationPending { .. } => continue,
                        };
                        if parked.remove(&doc_id) {
                            ready_work.insert(doc_id);
                        } else if running.contains(&doc_id) {
                            early_wakes.insert(doc_id);
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl RevisionedStore for FacetSetStore {
    type Revision = u64;
    type Entry = FrontierEntry<FacetRoute, FacetMembership>;
    type Selector = FacetTag;
    type Error = KeyedFrontierError;
    type Reader<'a> = KeyedFrontierRevisionReader<'a, FacetRoute, FacetMembership>;

    async fn latest_revision(&self) -> Result<u64, Self::Error> {
        self.frontier.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        tag: FacetTag,
        after: u64,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        Ok(KeyedFrontierRevisionReader::new(
            self.frontier
                .open(SqliteFrontierSelector::Namespaces(BTreeMap::from([(
                    tag.as_str().as_bytes().to_vec(),
                    after,
                )])))
                .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use big_sync_core::revisioned_store::{
        RevisionRead, RevisionReadLimits, RevisionedStoreReader,
    };

    fn key(name: &str) -> FacetKey {
        format!("org.example.note/{name}").parse().unwrap()
    }

    fn heads_clock(doc: &Automerge, heads: &[automerge::ChangeHash]) -> u64 {
        heads
            .iter()
            .filter_map(|head| doc.get_change_by_hash(head))
            .map(|change| change.max_op())
            .max()
            .unwrap_or(0)
    }

    fn state(doc: &Automerge, cursor: u64) -> DocState {
        let mut heads = doc.get_heads();
        heads.sort_unstable();
        DocState {
            heads: Some(heads),
            epoch: None,
            cursor,
        }
    }

    fn fixture() -> (Automerge, ObjId, ObjId, ObjId) {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let facets = tx.put_object(ROOT, "facets", ObjType::Map).unwrap();
        let first = tx
            .put_object(&facets, key("a").to_string(), ObjType::Map)
            .unwrap();
        tx.put(&first, "value", "a").unwrap();
        tx.put(&facets, key("b").to_string(), "b").unwrap();
        let dmeta = tx.put_object(&facets, DMETA_KEY, ObjType::Map).unwrap();
        let metadata = tx.put_object(&dmeta, "facets", ObjType::Map).unwrap();
        let entry = tx
            .put_object(&metadata, key("a").to_string(), ObjType::Map)
            .unwrap();
        let signatures = tx.put_object(&entry, "signatures", ObjType::List).unwrap();
        tx.insert_object(&signatures, 0, ObjType::Map).unwrap();
        tx.commit();
        (doc, facets, first, metadata)
    }

    fn project(doc: &Automerge, previous: Option<&DocState>, cursor: u64) -> Prepared {
        let next = state(doc, cursor);
        let (slots, replace_all) = DocView::new(doc, next.heads.as_ref().unwrap())
            .unwrap()
            .project(previous)
            .unwrap();
        Prepared {
            work: Work {
                doc_id: DocId::new([1; 32]),
                state: next,
            },
            previous: previous.cloned(),
            slots,
            replace_all,
        }
    }

    #[test]
    fn raw_initial_and_historical_views_do_not_verify_or_hydrate() {
        let (mut doc, facets, _, _) = fixture();
        let initial = state(&doc, 1);
        let first = project(&doc, None, 1);
        assert!(first.replace_all);
        assert_eq!(
            first.slots,
            BTreeMap::from([
                (
                    key("a"),
                    FacetMembership::Present {
                        doc_heads: initial.heads.clone().unwrap(),
                        clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                        facet_heads: initial.heads.clone().unwrap()
                    }
                ),
                (
                    key("b"),
                    FacetMembership::Present {
                        doc_heads: initial.heads.clone().unwrap(),
                        facet_heads: Vec::new(),
                        clock: 0
                    }
                ),
            ])
        );
        let mut tx = doc.transaction();
        tx.delete(&facets, key("a").to_string()).unwrap();
        tx.commit();
        let view = DocView::new(&doc, initial.heads.as_ref().unwrap()).unwrap();
        assert!(!view.current);
        assert_eq!(view.project(None).unwrap().0, first.slots);
        let next = project(&doc, Some(&initial), 2);
        assert_eq!(
            next.slots,
            BTreeMap::from([(
                key("a"),
                FacetMembership::Removed {
                    doc_heads: Some(doc.get_heads()),
                    clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                    facet_heads: initial.heads.unwrap(),
                }
            )])
        );
    }

    #[test]
    fn nested_unsigned_edits_emit_only_the_touched_slot_with_unchanged_signature_heads() {
        let (mut doc, _, first, _) = fixture();
        let initial = state(&doc, 1);
        let mut tx = doc.transaction();
        tx.put(&first, "value", "changed").unwrap();
        tx.commit();
        let next = project(&doc, Some(&initial), 2);
        assert!(!next.replace_all);
        assert_eq!(
            next.slots,
            BTreeMap::from([(
                key("a"),
                FacetMembership::Present {
                    doc_heads: doc.get_heads(),
                    clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                    facet_heads: initial.heads.unwrap(),
                }
            )])
        );
    }

    #[test]
    fn hidden_and_deleted_object_operations_still_touch_their_facet() {
        let (mut doc, facets, first, _) = fixture();
        let base = state(&doc, 1);
        let mut peer = doc.fork();
        let mut tx = doc.transaction();
        tx.put(&first, "value", "same").unwrap();
        tx.commit();
        let before = state(&doc, 2);
        let mut tx = peer.transaction();
        tx.put(&first, "value", "same").unwrap();
        tx.commit();
        doc.merge(&mut peer).unwrap();
        assert_eq!(
            project(&doc, Some(&before), 3)
                .slots
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            [key("a")]
        );
        let mut tx = doc.transaction();
        tx.delete(&facets, key("a").to_string()).unwrap();
        tx.commit();
        let before = state(&doc, 4);
        let mut peer = doc.fork_at(base.heads.as_ref().unwrap()).unwrap();
        let mut tx = peer.transaction();
        tx.put(&first, "hidden", "invisible").unwrap();
        tx.commit();
        doc.merge(&mut peer).unwrap();
        assert_eq!(
            project(&doc, Some(&before), 5)
                .slots
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            [key("a")]
        );
    }

    #[test]
    fn concurrent_metadata_maps_contribute_all_signature_introductions() {
        let (mut doc, _, _, metadata) = fixture();
        let before = state(&doc, 1);
        let mut peer = doc.fork();
        for branch in [&mut doc, &mut peer] {
            let mut tx = branch.transaction();
            let entry = tx
                .put_object(&metadata, key("b").to_string(), ObjType::Map)
                .unwrap();
            let list = tx.put_object(&entry, "signatures", ObjType::List).unwrap();
            tx.insert_object(&list, 0, ObjType::Map).unwrap();
            tx.commit();
        }
        let mut introductions = doc.get_heads();
        introductions.extend(peer.get_heads());
        introductions.sort_unstable();
        doc.merge(&mut peer).unwrap();
        let next = project(&doc, Some(&before), 2);
        assert_eq!(
            next.slots,
            BTreeMap::from([(
                key("b"),
                FacetMembership::Present {
                    doc_heads: introductions.clone(),
                    clock: heads_clock(&doc, &introductions),
                    facet_heads: introductions,
                }
            )])
        );
    }

    #[test]
    fn dmeta_fields_other_than_facets_do_not_touch_slots() {
        let (mut doc, facets, _, _) = fixture();
        let before = state(&doc, 1);
        let (_, dmeta) = doc.get(&facets, DMETA_KEY).unwrap().unwrap();
        let mut tx = doc.transaction();
        tx.put(&dmeta, "aliases", "renamed").unwrap();
        tx.commit();
        let next = project(&doc, Some(&before), 2);
        assert!(!next.replace_all);
        assert!(next.slots.is_empty(), "{:?}", next.slots);
    }

    #[test]
    fn projection_across_a_tombstoned_signature_uses_historical_paths() {
        let (mut doc, _, _, metadata) = fixture();
        let before = state(&doc, 1);
        let (_, entry) = doc.get(&metadata, key("a").to_string()).unwrap().unwrap();
        let (_, signatures) = doc.get(&entry, "signatures").unwrap().unwrap();
        let (_, signature) = doc.get(&signatures, 0).unwrap().unwrap();
        let mut tx = doc.transaction();
        tx.put(&signature, "actor", "z").unwrap();
        tx.commit();
        // Tombstone the signature map in a later change of the same
        // projection window: its creator insert becomes the list's only,
        // invisible op, which automerge's clock-less parent walk cannot
        // seek (fast/slow `seek_list_opid` divergence).
        let mut tx = doc.transaction();
        tx.delete(&signatures, 0).unwrap();
        tx.commit();
        let next = project(&doc, Some(&before), 2);
        assert!(!next.replace_all);
        assert_eq!(
            next.slots,
            BTreeMap::from([(
                key("a"),
                FacetMembership::Present {
                    doc_heads: doc.get_heads(),
                    facet_heads: Vec::new(),
                    clock: 0,
                }
            )])
        );
    }

    #[tokio::test]
    async fn key_query_returns_records_with_signature_clocks() {
        let sql = SqlCtx::memory().await.unwrap();
        let store = FacetSetStore::new(sql).await.unwrap();
        let (mut doc, _, first, _) = fixture();
        let initial = state(&doc, 1);
        store.settle(project(&doc, None, 1)).await.unwrap();
        let records = store.query(FacetSetQuery::Key(key("a"))).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].route.doc_id, DocId::new([1; 32]));
        assert_eq!(
            records[0].clock,
            heads_clock(&doc, initial.heads.as_ref().unwrap())
        );
        assert!(records[0].clock > 0);
        let first_clock = records[0].clock;
        let mut tx = doc.transaction();
        tx.put(&first, "value", "changed").unwrap();
        tx.commit();
        // An unsigned value edit does not move the facet version: its clock is
        // over the signature introductions, which are unchanged.
        store
            .settle(project(&doc, Some(&initial), 2))
            .await
            .unwrap();
        let records = store.query(FacetSetQuery::Key(key("a"))).await.unwrap();
        assert_eq!(records[0].clock, first_clock);
        assert!(
            store
                .query(FacetSetQuery::Key(key("missing")))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn envelope_replacement_rescans_and_ignores_later_materialized_changes() {
        let (mut doc, _, _, _) = fixture();
        let before = state(&doc, 1);
        let mut tx = doc.transaction();
        let facets = tx.put_object(ROOT, "facets", ObjType::Map).unwrap();
        tx.put(&facets, key("c").to_string(), "c").unwrap();
        tx.commit();
        let replacement = state(&doc, 2);
        let next = project(&doc, Some(&before), 2);
        assert!(next.replace_all);
        assert_eq!(next.slots.keys().cloned().collect::<Vec<_>>(), [key("c")]);
        let mut tx = doc.transaction();
        tx.put(&facets, key("later").to_string(), "later").unwrap();
        tx.commit();
        let historic = DocView::new(&doc, replacement.heads.as_ref().unwrap())
            .unwrap()
            .project(Some(&before))
            .unwrap();
        assert_eq!(historic, (next.slots, next.replace_all));
    }

    async fn next_entries(
        store: &FacetSetStore,
        tag: &str,
        after: u64,
    ) -> (u64, Vec<FrontierEntry<FacetRoute, FacetMembership>>) {
        let mut reader = store
            .open(FacetTag::new(tag).unwrap(), after)
            .await
            .unwrap();
        loop {
            match reader.next(RevisionReadLimits::default()).await.unwrap() {
                RevisionRead::Entries { revision, entries } if !entries.is_empty() => {
                    return (revision, entries);
                }
                RevisionRead::Entries { .. } | RevisionRead::ReplayComplete { .. } => {}
            }
        }
    }

    #[tokio::test]
    async fn settlements_are_atomic_idempotent_and_removals_preserve_tombstone_heads() {
        let sql = SqlCtx::memory().await.unwrap();
        let store = FacetSetStore::new(sql.clone()).await.unwrap();
        let (mut doc, facets, _, _) = fixture();
        let initial = state(&doc, 1);
        sqlx::query("CREATE TRIGGER reject_facet_set BEFORE INSERT ON big_cabinet_facet_set_docs BEGIN SELECT RAISE(ABORT, 'injected'); END")
            .execute(&sql.write_pool).await.unwrap();
        assert!(store.settle(project(&doc, None, 1)).await.is_err());
        assert_eq!(store.latest_revision().await.unwrap(), 0);
        assert_eq!(store.doc_state(&DocId::new([1; 32])).await.unwrap(), None);
        assert!(
            store
                .query(FacetSetQuery::Tag(
                    FacetTag::new("org.example.note").unwrap()
                ))
                .await
                .unwrap()
                .is_empty()
        );
        sqlx::query("DROP TRIGGER reject_facet_set")
            .execute(&sql.write_pool)
            .await
            .unwrap();
        store.settle(project(&doc, None, 1)).await.unwrap();
        let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
        assert_eq!((revision, entries.len()), (1, 2));
        store.settle(project(&doc, None, 1)).await.unwrap();
        assert_eq!(store.latest_revision().await.unwrap(), 1);
        let mut tx = doc.transaction();
        tx.delete(&facets, key("a").to_string()).unwrap();
        tx.commit();
        store
            .settle(project(&doc, Some(&initial), 2))
            .await
            .unwrap();
        let (revision, entries) = next_entries(&store, "org.example.note", 1).await;
        assert_eq!(
            entries,
            vec![FrontierEntry {
                revision,
                key: FacetRoute {
                    doc_id: DocId::new([1; 32]),
                    facet_key: key("a")
                },
                value: Some(FacetMembership::Removed {
                    doc_heads: Some(doc.get_heads()),
                    clock: heads_clock(&doc, initial.heads.as_ref().unwrap()),
                    facet_heads: initial.heads.unwrap()
                })
            }]
        );
        let reopened = FacetSetStore::new(sql).await.unwrap();
        assert_eq!(
            reopened
                .query(FacetSetQuery::Doc(DocId::new([1; 32])))
                .await
                .unwrap()
                .into_iter()
                .map(|record| record.route.facet_key)
                .collect::<Vec<_>>(),
            [key("b")]
        );
        assert_eq!(store.source_revision().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn replacement_and_physical_removal_remove_all_old_routes_without_regressing_state() {
        let store = FacetSetStore::new(SqlCtx::memory().await.unwrap())
            .await
            .unwrap();
        let (mut doc, _, _, _) = fixture();
        let initial = state(&doc, 1);
        store.settle(project(&doc, None, 1)).await.unwrap();
        let stale = project(&doc, Some(&initial), 2);
        let mut tx = doc.transaction();
        tx.put_object(ROOT, "facets", ObjType::Map).unwrap();
        tx.commit();
        store
            .settle(project(&doc, Some(&initial), 3))
            .await
            .unwrap();
        store.settle(stale).await.unwrap();
        assert_eq!(
            store.doc_state(&DocId::new([1; 32])).await.unwrap(),
            Some(state(&doc, 3))
        );
        assert!(
            store
                .query(FacetSetQuery::Doc(DocId::new([1; 32])))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn projection_does_not_acquire_sync_demand() {
        let (document, _, _, _) = fixture();
        let next = state(&document, 1);
        let heads = next.heads.clone().unwrap();
        let (_directory, repo, stop, doc_id) =
            crate::index::test_support::reopen_document(document).await;
        let mut demand = repo.sync_demand();
        let sql = SqlCtx::memory().await.unwrap();
        let store = FacetSetStore::new(sql.clone()).await.unwrap();
        let prepared = store
            .prepare(
                &repo,
                Work {
                    doc_id: doc_id.clone(),
                    state: next,
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            prepared.slots.keys().cloned().collect::<Vec<_>>(),
            [key("a"), key("b")]
        );
        assert!(demand.documents().is_empty());
        store.settle(prepared).await.unwrap();
        assert!(store.latest_revision().await.unwrap() > 0);
        assert_eq!(
            store
                .query(FacetSetQuery::Doc(doc_id.clone()))
                .await
                .unwrap()
                .into_iter()
                .map(|record| (record.route.facet_key, record.doc_heads))
                .collect::<Vec<_>>(),
            [(key("a"), heads.clone()), (key("b"), heads)]
        );
        assert!(demand.documents().is_empty());
        let mut changed = Box::pin(demand.changed());
        assert!(futures::poll!(&mut changed).is_pending());
        drop(changed);
        let caller = repo
            .get_doc(&doc_id)
            .await
            .unwrap()
            .into_ready(doc_id.clone())
            .unwrap();
        assert_eq!(
            *demand.documents(),
            std::collections::HashSet::from([doc_id])
        );
        drop(caller);
        let repo_sql = repo.sql_ctx();
        drop(repo);
        stop.stop().await.unwrap();
        drop(store);
        repo_sql.read_pool.close().await;
        repo_sql.write_pool.close().await;
        sql.read_pool.close().await;
        sql.write_pool.close().await;
    }

    struct ScriptSource {
        receiver: std::sync::Mutex<
            Option<tokio::sync::mpsc::UnboundedReceiver<RevisionRead<u64, AutomergeFrontierEvent>>>,
        >,
        replay: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    struct ScriptReader {
        receiver: tokio::sync::mpsc::UnboundedReceiver<RevisionRead<u64, AutomergeFrontierEvent>>,
        replay: Option<tokio::sync::oneshot::Sender<()>>,
    }

    #[async_trait]
    impl RevisionedStore for ScriptSource {
        type Revision = u64;
        type Entry = AutomergeFrontierEvent;
        type Selector = ();
        type Error = utils_rs::prelude::eyre::Report;
        type Reader<'a> = ScriptReader;

        async fn latest_revision(&self) -> Res<u64> {
            Ok(0)
        }

        async fn open<'a>(&'a self, (): (), _after: u64) -> Res<ScriptReader> {
            Ok(ScriptReader {
                receiver: self.receiver.lock().unwrap().take().unwrap(),
                replay: self.replay.lock().unwrap().take(),
            })
        }
    }

    #[async_trait]
    impl RevisionedStoreReader<u64, AutomergeFrontierEvent, utils_rs::prelude::eyre::Report>
        for ScriptReader
    {
        async fn next(
            &mut self,
            _limits: RevisionReadLimits,
        ) -> Res<RevisionRead<u64, AutomergeFrontierEvent>> {
            let read = self
                .receiver
                .recv()
                .await
                .expect("script sender stays alive");
            if matches!(read, RevisionRead::ReplayComplete { .. })
                && let Some(replay) = self.replay.take()
            {
                replay
                    .send(())
                    .expect("script replay observer remains open");
            }
            Ok(read)
        }
    }

    fn changed(
        doc_id: &DocId,
        doc: &Automerge,
        revision: u64,
    ) -> RevisionRead<u64, AutomergeFrontierEvent> {
        RevisionRead::Entries {
            revision,
            entries: vec![AutomergeFrontierEvent::Changed {
                doc_id: doc_id.clone(),
                heads: doc.get_heads().into(),
                causal_epoch: None,
                routes: Vec::new(),
                revision,
            }],
        }
    }

    async fn boot_repo() -> (SharedBigRepo, big_repo::BigRepoStopToken) {
        big_repo::BigRepo::boot(big_repo::Config {
            node_identity_seed: [31; 32],
            storage: big_repo::StorageConfig::Memory,
            scope_key: "facet-set-test".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await
        .unwrap()
    }

    async fn start_script(
        store: FacetSetStore,
        repo: SharedBigRepo,
        source: ScriptSource,
    ) -> (service_utils_rs::StopToken, service_utils_rs::Spawner) {
        let (stop, tasks) = service_utils_rs::StopToken::new();
        let (ready, started) = tokio::sync::oneshot::channel();
        let owner = tasks.clone();
        tasks
            .spawn(async move {
                store
                    .drive(
                        &source,
                        (),
                        repo,
                        NonZeroUsize::new(2).unwrap(),
                        owner.cancelled(),
                        &mut Some(ready),
                    )
                    .await
                    .unwrap();
            })
            .unwrap();
        started.await.unwrap().unwrap();
        (stop, tasks)
    }

    #[tokio::test]
    async fn a_parked_doc_does_not_block_other_docs_and_newer_work_supersedes_it() {
        let (repo, stop) = boot_repo().await;
        let store = FacetSetStore::new(SqlCtx::memory().await.unwrap())
            .await
            .unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let source = ScriptSource {
            receiver: std::sync::Mutex::new(Some(receiver)),
            replay: std::sync::Mutex::new(None),
        };
        let (worker_stop, worker_tasks) =
            start_script(store.clone(), Arc::clone(&repo), source).await;
        let delayed_id = repo.allocate_id().await.unwrap();
        let (mut delayed, _, first, _) = fixture();
        sender.send(changed(&delayed_id, &delayed, 1)).unwrap();
        let other_id = repo.allocate_id().await.unwrap();
        let (other, _, _, _) = fixture();
        sender.send(changed(&other_id, &other, 2)).unwrap();
        let other = repo
            .commit_id(other_id.clone(), other, Vec::new(), &[])
            .await
            .unwrap();
        let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
        assert!(entries.iter().all(|entry| entry.key.doc_id == other_id));
        assert_eq!(store.source_revision().await.unwrap(), 0);
        let mut tx = delayed.transaction();
        tx.put(&first, "value", "superseding").unwrap();
        tx.commit();
        let latest_heads = delayed.get_heads();
        sender.send(changed(&delayed_id, &delayed, 3)).unwrap();
        let delayed = repo
            .commit_id(delayed_id.clone(), delayed, Vec::new(), &[])
            .await
            .unwrap();
        let mut after = revision;
        loop {
            let (revision, entries) = next_entries(&store, "org.example.note", after).await;
            after = revision;
            if entries.iter().any(|entry| {
                entry.key.doc_id == delayed_id
                    && matches!(&entry.value,
                Some(FacetMembership::Present { doc_heads, .. }) if *doc_heads == latest_heads)
            }) {
                break;
            }
        }
        assert_eq!(
            store.doc_state(&delayed_id).await.unwrap().unwrap().cursor,
            3
        );
        sender
            .send(RevisionRead::Entries {
                revision: 4,
                entries: vec![AutomergeFrontierEvent::Removed {
                    doc_id: delayed_id.clone(),
                    route: big_sync_core::PartKey::new(b"removed-route"),
                    revision: 4,
                }],
            })
            .unwrap();
        let (_, entries) = next_entries(&store, "org.example.note", after).await;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.key.doc_id == delayed_id
            && matches!(
                entry.value,
                Some(FacetMembership::Removed {
                    doc_heads: None,
                    ..
                })
            )));
        assert!(
            store
                .query(FacetSetQuery::Doc(delayed_id))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .query(FacetSetQuery::Doc(other_id))
                .await
                .unwrap()
                .len(),
            2
        );
        worker_stop.stop().await;
        assert_eq!(worker_tasks.live_tasks(), 0);
        drop(delayed);
        drop(other);
        drop(repo);
        stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn cooperative_stop_preserves_parked_work_and_replays_without_duplicate_output() {
        let (repo, stop) = boot_repo().await;
        let sql = SqlCtx::memory().await.unwrap();
        let store = FacetSetStore::new(sql.clone()).await.unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (worker_stop, worker_tasks) = start_script(
            store.clone(),
            Arc::clone(&repo),
            ScriptSource {
                receiver: std::sync::Mutex::new(Some(receiver)),
                replay: std::sync::Mutex::new(None),
            },
        )
        .await;
        let delayed_id = repo.allocate_id().await.unwrap();
        let (delayed, _, _, _) = fixture();
        sender.send(changed(&delayed_id, &delayed, 1)).unwrap();
        let other_id = repo.allocate_id().await.unwrap();
        let (other, _, _, _) = fixture();
        sender.send(changed(&other_id, &other, 2)).unwrap();
        let other = repo
            .commit_id(other_id.clone(), other, Vec::new(), &[])
            .await
            .unwrap();
        let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
        assert!(entries.iter().all(|entry| entry.key.doc_id == other_id));
        worker_stop.stop().await;
        assert_eq!(worker_tasks.live_tasks(), 0);
        assert_eq!(store.source_revision().await.unwrap(), 0);
        assert!(store.doc_state(&delayed_id).await.unwrap().is_none());
        let other_state = store.doc_state(&other_id).await.unwrap().unwrap();
        assert_eq!(other_state.cursor, 2);
        drop(sender);
        drop(store);

        let store = FacetSetStore::new(sql).await.unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        sender.send(changed(&delayed_id, &delayed, 1)).unwrap();
        let delayed = repo
            .commit_id(delayed_id.clone(), delayed, Vec::new(), &[])
            .await
            .unwrap();
        sender
            .send(RevisionRead::Entries {
                revision: 2,
                entries: vec![AutomergeFrontierEvent::Changed {
                    doc_id: other_id.clone(),
                    heads: other_state.heads.clone().unwrap().into(),
                    causal_epoch: None,
                    routes: Vec::new(),
                    revision: 2,
                }],
            })
            .unwrap();
        let (replay, completed) = tokio::sync::oneshot::channel();
        sender
            .send(RevisionRead::ReplayComplete { through: 2 })
            .unwrap();
        let (worker_stop, worker_tasks) = start_script(
            store.clone(),
            Arc::clone(&repo),
            ScriptSource {
                receiver: std::sync::Mutex::new(Some(receiver)),
                replay: std::sync::Mutex::new(Some(replay)),
            },
        )
        .await;
        completed.await.unwrap();
        let (_, entries) = next_entries(&store, "org.example.note", revision).await;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.key.doc_id == delayed_id));
        worker_stop.stop().await;
        assert_eq!(worker_tasks.live_tasks(), 0);
        assert_eq!(store.source_revision().await.unwrap(), 2);
        assert_eq!(
            store.doc_state(&delayed_id).await.unwrap().unwrap().cursor,
            1
        );
        assert_eq!(
            store.doc_state(&other_id).await.unwrap().unwrap(),
            other_state
        );
        drop(delayed);
        drop(other);
        drop(repo);
        stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn cooperative_stop_drains_an_inflight_preparation_before_acknowledging_it() {
        let (repo, stop) = boot_repo().await;
        let sql = SqlCtx::memory().await.unwrap();
        let store = FacetSetStore::new(sql.clone()).await.unwrap();
        let doc_id = repo.allocate_id().await.unwrap();
        let (doc, _, _, _) = fixture();
        let event = changed(&doc_id, &doc, 1);
        let handle = repo
            .commit_id(doc_id.clone(), doc, Vec::new(), &[])
            .await
            .unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (replay, completed) = tokio::sync::oneshot::channel();
        let (worker_stop, worker_tasks) = start_script(
            store.clone(),
            Arc::clone(&repo),
            ScriptSource {
                receiver: std::sync::Mutex::new(Some(receiver)),
                replay: std::sync::Mutex::new(Some(replay)),
            },
        )
        .await;
        let mut connections = Vec::new();
        for _ in 0..sql.read_pool.options().get_max_connections() {
            connections.push(sql.read_pool.acquire().await.unwrap());
        }
        sender.send(event).unwrap();
        sender
            .send(RevisionRead::ReplayComplete { through: 1 })
            .unwrap();
        completed.await.unwrap();
        let mut stopping = Box::pin(worker_stop.stop());
        assert!(futures::poll!(&mut stopping).is_pending());
        assert!(worker_tasks.is_cancelled());
        assert_eq!(worker_tasks.live_tasks(), 1);
        drop(connections);
        stopping.await;
        assert_eq!(worker_tasks.live_tasks(), 0);
        assert_eq!(store.doc_state(&doc_id).await.unwrap().unwrap().cursor, 1);
        assert_eq!(store.source_revision().await.unwrap(), 1);
        assert_eq!(
            store.query(FacetSetQuery::Doc(doc_id)).await.unwrap().len(),
            2
        );
        drop(handle);
        drop(repo);
        stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn live_afw_projection_needs_no_docs_service_and_replays_without_duplicate_output() {
        let (repo, stop) = boot_repo().await;
        let store = FacetSetStore::new(SqlCtx::memory().await.unwrap())
            .await
            .unwrap();
        let start_worker = || {
            let store = store.clone();
            let repo = Arc::clone(&repo);
            async move {
                let (stop, tasks) = service_utils_rs::StopToken::new();
                let (ready, started) = tokio::sync::oneshot::channel();
                let owner = tasks.clone();
                tasks
                    .spawn(async move {
                        store
                            .run_until(
                                repo,
                                NonZeroUsize::new(2).unwrap(),
                                owner.cancelled(),
                                &mut Some(ready),
                            )
                            .await
                            .unwrap();
                    })
                    .unwrap();
                started.await.unwrap().unwrap();
                (stop, tasks)
            }
        };
        let (worker_stop, worker_tasks) = start_worker().await;
        let doc_id = repo.allocate_id().await.unwrap();
        let (doc, _, _, _) = fixture();
        let handle = repo
            .commit_id(doc_id.clone(), doc, Vec::new(), &[])
            .await
            .unwrap();
        let (revision, entries) = next_entries(&store, "org.example.note", 0).await;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.key.doc_id == doc_id));
        let before = store.doc_state(&doc_id).await.unwrap().unwrap();
        worker_stop.stop().await;
        assert_eq!(worker_tasks.live_tasks(), 0);
        let work = Work {
            doc_id: doc_id.clone(),
            state: before.clone(),
        };
        store
            .settle(store.prepare(&repo, work).await.unwrap().unwrap())
            .await
            .unwrap();
        assert_eq!(store.latest_revision().await.unwrap(), revision);
        let (worker_stop, worker_tasks) = start_worker().await;
        handle
            .with_document(|doc| {
                let facets = doc.get(ROOT, "facets").unwrap().unwrap().1;
                let first = doc.get(&facets, key("a").to_string()).unwrap().unwrap().1;
                let mut tx = doc.transaction();
                tx.put(&first, "value", "remote-like raw edit").unwrap();
                tx.commit();
            })
            .await
            .unwrap();
        let (_, entries) = next_entries(&store, "org.example.note", revision).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.facet_key, key("a"));
        let FacetMembership::Present { facet_heads, .. } = entries[0].value.as_ref().unwrap()
        else {
            panic!("live facet")
        };
        assert_eq!(Some(facet_heads.clone()), before.heads);
        worker_stop.stop().await;
        assert_eq!(worker_tasks.live_tasks(), 0);
        drop(handle);
        drop(repo);
        stop.stop().await.unwrap();
    }
}
