use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjId;
use automerge::ObjType;
use automerge::Prop;
use automerge::ROOT;
use automerge::ReadDoc;
use automerge::Value;
use big_cabinet_core::DocId;
use big_cabinet_core::FacetKey;
use big_cabinet_core::FacetTag;
use big_repo::AutomergeFrontierEvent;
use big_repo::AutomergeFrontierRevisionStore;
use big_repo::AutomergeFrontierSelector;
use big_repo::AutomergeFrontierTarget;
use big_repo::BigRepoLocalFilter;
use big_repo::BigRepoLocalNotification;
use big_repo::DocLookup;
use big_repo::SharedBigRepo;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync::keyed_frontier::SqliteFrontierCodec;
use big_sync::keyed_frontier::SqliteFrontierSelector;
use big_sync::keyed_frontier::SqliteKeyedFrontier;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaRead;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaWalker;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::keyed_frontier::KeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontierError;
use big_sync_core::revisioned_store::KeyedFrontierRevisionReader;
use big_sync_core::revisioned_store::RevisionedStore;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::Deserialize;
use serde::Serialize;
use sqlite_rs::Database;
use sqlite_rs::Row;
use sqlite_rs::Sql;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::Res;
use utils_rs::prelude::async_trait;
use utils_rs::prelude::ferr;

mod queries {
    include!("facet_set/sqlite/gen.rs");
}

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

fn facet_record(row: Row) -> Res<FacetRecord> {
    Ok(FacetRecord {
        route: FacetRoute {
            doc_id: row.get::<String>("doc_id")?.parse()?,
            facet_key: row.get::<String>("facet_key")?.parse()?,
        },
        doc_heads: serde_json::from_slice(&row.get::<Vec<u8>>("doc_heads")?)?,
        facet_heads: serde_json::from_slice(&row.get::<Vec<u8>>("facet_heads")?)?,
        clock: u64::try_from(row.get::<i64>("clock")?)
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
    sql: Database,
    frontier: SqliteKeyedFrontier<FacetSetCodec>,
    input_state: SqliteDeltaWalkerStateRepo,
}

impl FacetSetStore {
    pub async fn new(sql: Database) -> Res<Self> {
        sql.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        let frontier = SqliteKeyedFrontier::new(
            sql.clone(),
            FACET_SET_ID,
            FacetSetCodec,
            Arc::new(Notify::new()),
        )
        .await?;
        let input_state =
            SqliteDeltaWalkerStateRepo::new(sql.clone(), FACET_SET_ID, "automerge-frontier")
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
        let bytes = queries::doc_state(&self.sql, doc_id.to_string()).await?;
        bytes
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    pub async fn query(&self, selector: FacetSetQuery) -> Res<Vec<FacetRecord>> {
        let mut query = Sql::new(
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
                query.push("doc_id = ").bind(doc.to_string());
            }
            FacetSetQuery::Tag(tag) => {
                query.push("facet_tag = ").bind(tag.to_string());
            }
            FacetSetQuery::Route(route) => {
                query
                    .push("doc_id = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND facet_key = ")
                    .bind(route.facet_key.to_string());
            }
            FacetSetQuery::Key(key) => {
                query.push("facet_key = ").bind(key.to_string());
            }
        }
        query.push(" ORDER BY doc_id, facet_key");
        query
            .query(&self.sql)
            .await?
            .rows
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
        let mut query = Sql::new(
            r#"SELECT doc_id
                 , facet_key
                 , doc_heads
                 , facet_heads
                 , clock
              FROM big_cabinet_facet_set_entries
              WHERE doc_id IN (SELECT value FROM json_each("#,
        );
        query.bind(serde_json::to_string(docs)?).push("))");
        if let Some(tag) = tag {
            query
                .push(" AND facet_tag = ")
                .bind(tag.as_str().to_owned());
        }
        if let Some(key) = key {
            query.push(" AND facet_key = ").bind(key.to_string());
        }
        query.push(" ORDER BY doc_id, facet_key");
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(facet_record)
            .collect()
    }

    async fn settle(&self, prepared: Prepared) -> Res<()> {
        let mut transaction = self.sql.begin_write().await?;
        let doc_id = prepared.work.doc_id.to_string();
        let stored = queries::doc_state(&transaction, doc_id.clone()).await?;
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
            let old = queries::doc_keys(&transaction, doc_id.clone()).await?;
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
                    queries::put_entry(
                        &transaction,
                        doc_id.clone(),
                        key.to_string(),
                        key.tag().to_string(),
                        serde_json::to_vec(&doc_heads)?,
                        serde_json::to_vec(&facet_heads)?,
                        i64::try_from(clock).map_err(|_| {
                            utils_rs::prelude::ferr!("facet-set clock is out of range")
                        })?,
                    )
                    .await?;
                }
                FacetMembership::Removed { .. } => {
                    queries::delete_entry(&transaction, doc_id.clone(), key.to_string()).await?;
                }
            }
        }
        queries::put_doc(
            &transaction,
            doc_id,
            serde_json::to_vec(&prepared.work.state)?,
        )
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
mod tests;
