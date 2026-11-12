use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjId;
use automerge::ObjType;
use automerge::PatchLog;
use automerge::Prop;
use automerge::ROOT;
use automerge::ReadDoc;
use automerge::Value;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::ReferenceTarget;
use big_cabinet_core::datum::declared_references;
use big_cabinet_core::datum::hydrate_value;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::query::Predicate;
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

use super::datum_ref::DatumRefEdge;
use super::datum_ref::DatumRefTarget;
use crate::doc::gate::Gate;
use crate::doc::gate::Verdict;
use crate::schema::SchemaRepo;

mod queries {
    include!("datum_set/sqlite/gen.rs");
}

const DATUM_SET_ID: &str = "big-cabinet-datum-set-v2";
const DMETA_KEY: &str = "org.example.daybook.dmeta/main";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DatumRoute {
    pub doc_id: DocId,
    pub datum_key: DatumKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatumMembership {
    Present {
        doc_heads: Vec<ChangeHash>,
        datum_heads: Vec<ChangeHash>,
        /// Lamport clock of `datum_heads`: the greatest `max_op` among the
        /// introducing changes. See `DatumVersion` in the DocsRepo protocol.
        clock: u64,
    },
    Removed {
        doc_heads: Option<Vec<ChangeHash>>,
        datum_heads: Vec<ChangeHash>,
        clock: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatumRecord {
    pub route: DatumRoute,
    pub doc_heads: Vec<ChangeHash>,
    pub datum_heads: Vec<ChangeHash>,
    pub clock: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatumSetQuery {
    Doc(DocId),
    Tag(DatumTag),
    Route(DatumRoute),
    /// Every live route with exactly this datum key, across documents.
    Key(DatumKey),
}

fn datum_record(row: Row) -> Res<DatumRecord> {
    Ok(DatumRecord {
        route: DatumRoute {
            doc_id: row.get::<String>("doc_id")?.parse()?,
            datum_key: row.get::<String>("datum_key")?.parse()?,
        },
        doc_heads: serde_json::from_slice(&row.get::<Vec<u8>>("doc_heads")?)?,
        datum_heads: serde_json::from_slice(&row.get::<Vec<u8>>("datum_heads")?)?,
        clock: u64::try_from(row.get::<i64>("clock")?)
            .map_err(|_| utils_rs::prelude::ferr!("datum-set clock is out of range"))?,
    })
}

#[derive(Clone)]
struct DatumSetCodec;

impl SqliteFrontierCodec for DatumSetCodec {
    type Key = DatumRoute;
    type Value = DatumMembership;

    fn encode_key(&self, key: &Self::Key) -> Vec<u8> {
        serde_json::to_vec(key).expect("datum routes serialize")
    }

    fn decode_key(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Key, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn encode_value(&self, value: &Self::Value) -> Vec<u8> {
        serde_json::to_vec(value).expect("datum membership serializes")
    }

    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn namespace(&self, key: &Self::Key) -> Option<Vec<u8>> {
        Some(key.datum_key.tag().as_str().as_bytes().to_vec())
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
    slots: BTreeMap<DatumKey, DatumMembership>,
    replace_all: bool,
    /// The declared references of every gate-accepted route this projection
    /// scanned, keyed by origin. A route whose tag declares no reference
    /// position is absent rather than empty, so `settle` replaces only the
    /// edges of routes the projection actually read.
    ref_edges: BTreeMap<DatumRoute, Vec<DatumRefEdge>>,
}

#[derive(Clone)]
pub struct DatumSetStore {
    sql: Database,
    frontier: SqliteKeyedFrontier<DatumSetCodec>,
    input_state: SqliteDeltaWalkerStateRepo,
    /// The compiled schemas that decide which datum tags declare references,
    /// and therefore which routes this worker extracts edges for.
    schemas: Arc<SchemaRepo>,
    /// Settlement counter tests watch instead of polling the index.
    #[cfg(test)]
    settled: Arc<tokio::sync::watch::Sender<u64>>,
}

impl DatumSetStore {
    pub async fn new(sql: Database, schemas: Arc<SchemaRepo>) -> Res<Self> {
        sql.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        let frontier = SqliteKeyedFrontier::new(
            sql.clone(),
            DATUM_SET_ID,
            DatumSetCodec,
            Arc::new(Notify::new()),
        )
        .await?;
        let input_state =
            SqliteDeltaWalkerStateRepo::new(sql.clone(), DATUM_SET_ID, "automerge-frontier")
                .await?;
        #[cfg(test)]
        let (settled, _) = tokio::sync::watch::channel(0);
        Ok(Self {
            sql,
            frontier,
            input_state,
            schemas,
            #[cfg(test)]
            settled: Arc::new(settled),
        })
    }

    /// Watch the number of settled projections. A test that has driven a change
    /// through the repository waits on this instead of polling the index for
    /// convergence; the entries and the reference edges of one revision land in
    /// the same settle, so one bump covers both.
    #[cfg(test)]
    pub(crate) fn subscribe_settled(&self) -> tokio::sync::watch::Receiver<u64> {
        self.settled.subscribe()
    }

    /// Open the frontier across every tag namespace, for consumers (such as
    /// doc-scoped observation) whose filter is not a single tag.
    pub async fn open_all(
        &self,
        after: u64,
    ) -> Result<KeyedFrontierRevisionReader<'_, DatumRoute, DatumMembership>, KeyedFrontierError>
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
        tags: impl IntoIterator<Item = (DatumTag, u64)>,
    ) -> Result<KeyedFrontierRevisionReader<'_, DatumRoute, DatumMembership>, KeyedFrontierError>
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

    pub async fn query(&self, selector: DatumSetQuery) -> Res<Vec<DatumRecord>> {
        let mut query = Sql::new(
            r#"SELECT doc_id
                 , datum_key
                 , doc_heads
                 , datum_heads
                 , clock
              FROM big_cabinet_datum_set_entries
             WHERE "#,
        );
        match selector {
            DatumSetQuery::Doc(doc) => {
                query.push("doc_id = ").bind(doc.to_string());
            }
            DatumSetQuery::Tag(tag) => {
                query.push("datum_tag = ").bind(tag.to_string());
            }
            DatumSetQuery::Route(route) => {
                query
                    .push("doc_id = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND datum_key = ")
                    .bind(route.datum_key.to_string());
            }
            DatumSetQuery::Key(key) => {
                query.push("datum_key = ").bind(key.to_string());
            }
        }
        query.push(" ORDER BY doc_id, datum_key");
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(datum_record)
            .collect()
    }

    /// Query every live route in `docs`, optionally narrowed to `tag` or `key`.
    pub async fn query_docs(
        &self,
        docs: &BTreeSet<DocId>,
        tag: Option<&DatumTag>,
        key: Option<&DatumKey>,
    ) -> Res<Vec<DatumRecord>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = Sql::new(
            r#"SELECT doc_id
                 , datum_key
                 , doc_heads
                 , datum_heads
                 , clock
              FROM big_cabinet_datum_set_entries
              WHERE doc_id IN (SELECT value FROM json_each("#,
        );
        query.bind(serde_json::to_string(docs)?).push("))");
        if let Some(tag) = tag {
            query
                .push(" AND datum_tag = ")
                .bind(tag.as_str().to_owned());
        }
        if let Some(key) = key {
            query.push(" AND datum_key = ").bind(key.to_string());
        }
        query.push(" ORDER BY doc_id, datum_key");
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(datum_record)
            .collect()
    }

    /// Every live route of the documents that satisfy `predicate`, ordered by route.
    ///
    /// The predicate compiles to one boolean expression over the candidate document id, and the
    /// entries are the outer scan: a document no route names cannot appear, which is what a datum
    /// query is asking for. No document is hydrated and no datum value is read.
    pub async fn query_predicate(&self, predicate: &Predicate) -> Res<Vec<DatumRecord>> {
        let mut query = Sql::new(
            r#"SELECT doc_id
                 , datum_key
                 , doc_heads
                 , datum_heads
                 , clock
              FROM big_cabinet_datum_set_entries AS entry
             WHERE "#,
        );
        push_predicate(&mut query, "entry.doc_id", predicate);
        query.push(" ORDER BY doc_id, datum_key");
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(datum_record)
            .collect()
    }

    /// The subset of `docs` whose document satisfies `predicate`.
    ///
    /// `docs` is the candidate universe, so the query does not scan the entries table at its top
    /// level: `IsDoc` and `InDrawer` can match a document that carries no live datum. That is also
    /// what makes a `Not` answerable here — the caller bound the candidates, so the negation never
    /// enumerates the node.
    pub async fn matching_docs(
        &self,
        predicate: &Predicate,
        docs: &BTreeSet<DocId>,
    ) -> Res<BTreeSet<DocId>> {
        if docs.is_empty() {
            return Ok(BTreeSet::new());
        }
        let mut query = Sql::new("SELECT candidate.value AS doc_id FROM json_each(");
        query
            .bind(serde_json::to_string(docs)?)
            .push(") AS candidate WHERE ");
        push_predicate(&mut query, "candidate.value", predicate);
        query.push(" ORDER BY candidate.value");
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(|row| Ok(row.get::<String>("doc_id")?.parse::<DocId>()?))
            .collect()
    }

    /// The documents a drawer reaches at `at_least`.
    ///
    /// A `childEntry` change can add or remove a whole subtree at once, and the page names only
    /// the parent drawer, so the documents to re-evaluate are the child's reachable set. The
    /// candidate universe is every document any drawer lists plus every drawer document, and the
    /// reach test is the same CTE `matching_docs` uses, so a roster change and a predicate
    /// re-evaluation cannot disagree about what a drawer reaches.
    pub async fn reachable_docs(
        &self,
        drawer_id: &DocId,
        transitive: bool,
        at_least: DrawerAccess,
    ) -> Res<BTreeSet<DocId>> {
        let mut query = Sql::new(
            r#"SELECT DISTINCT candidate.doc_id AS doc_id
                 FROM (
                     SELECT doc_id FROM big_cabinet_drawer_docs
                     UNION
                     SELECT drawer_id AS doc_id FROM big_cabinet_drawers
                 ) AS candidate
                WHERE "#,
        );
        push_in_drawer(
            &mut query,
            "candidate.doc_id",
            drawer_id,
            transitive,
            at_least,
        );
        query.push(" ORDER BY candidate.doc_id");
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(|row| Ok(row.get::<String>("doc_id")?.parse::<DocId>()?))
            .collect()
    }

    /// Whether `drawer_id` is projected as a self-doc drawer: its descriptor is its content
    /// doc, so under a non-transitive parent it counts as one of the parent's own docs
    /// (ADR 026 §6) rather than as a drawer whose docs are pulled in.
    pub async fn is_self_doc(&self, drawer_id: &DocId) -> Res<bool> {
        let mut query = Sql::new(
            "SELECT json_extract(descriptor_json, '$.layout.$type') AS layout FROM big_cabinet_drawers WHERE drawer_id = ",
        );
        query.bind(drawer_id.to_string());
        let rows = query.query(&self.sql).await?.rows;
        Ok(rows.first().is_some_and(|row| {
            row.get::<Option<String>>("layout")
                .ok()
                .flatten()
                .as_deref()
                == Some("org.example.daybook.drawer.selfDoc")
        }))
    }

    /// The drawers `drawer_id` is nested under, itself included.
    ///
    /// This is the `ancestors` CTE rooted at a drawer. A page can carry roster entries for every
    /// drawer on the node; a poll re-evaluates the affected documents only when the changed drawer
    /// is its anchor or a descendant of it, so an unrelated roster edit costs nothing.
    pub async fn drawer_ancestors(&self, drawer_id: &DocId) -> Res<BTreeSet<DocId>> {
        let mut query = Sql::new(
            r#"WITH RECURSIVE reached(drawer_id) AS (
                   SELECT "#,
        );
        query.bind(drawer_id.to_string()).push(
            r#"
                   UNION
                   SELECT links.parent_id
                     FROM reached
                     JOIN big_cabinet_drawer_children AS links ON links.child_id = reached.drawer_id
                     JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = links.parent_id
                    WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
               )
               SELECT drawer_id FROM reached ORDER BY drawer_id"#,
        );
        query
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(|row| Ok(row.get::<String>("drawer_id")?.parse::<DocId>()?))
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
                "datum-set document changed during preparation: {doc_id}"
            ));
        }
        let mut slots = prepared.slots;
        if prepared.replace_all {
            let old = queries::doc_keys(&transaction, doc_id.clone()).await?;
            for key in old {
                slots
                    .entry(key.parse()?)
                    .or_insert_with(|| DatumMembership::Removed {
                        doc_heads: prepared.work.state.heads.clone(),
                        datum_heads: Vec::new(),
                        clock: 0,
                    });
            }
        }
        let mutations = slots.iter().map(|(key, value)| {
            (
                DatumRoute {
                    doc_id: prepared.work.doc_id.clone(),
                    datum_key: key.clone(),
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
                DatumMembership::Present {
                    doc_heads,
                    datum_heads,
                    clock,
                } => {
                    queries::put_entry(
                        &transaction,
                        doc_id.clone(),
                        key.to_string(),
                        key.tag().to_string(),
                        serde_json::to_vec(&doc_heads)?,
                        serde_json::to_vec(&datum_heads)?,
                        i64::try_from(clock).map_err(|_| {
                            utils_rs::prelude::ferr!("datum-set clock is out of range")
                        })?,
                    )
                    .await?;
                }
                DatumMembership::Removed { .. } => {
                    queries::delete_entry(&transaction, doc_id.clone(), key.to_string()).await?;
                    // A removed datum states no references and was not scanned,
                    // so the edges its last accepted value published are stale.
                    if self.extracts_references(key.tag().as_str()) {
                        queries::delete_edges(&transaction, doc_id.clone(), key.to_string())
                            .await?;
                    }
                }
            }
        }
        // Replace the edges of every route this projection scanned. Doing it in
        // the entries' transaction is what lets a reader ask for the edges as of
        // one datum-set revision: both move with the same cursor.
        write_edges(&transaction, prepared.ref_edges).await?;
        queries::put_doc(
            &transaction,
            doc_id,
            serde_json::to_vec(&prepared.work.state)?,
        )
        .await?;
        transaction.commit().await?;
        #[cfg(test)]
        self.settled.send_modify(|settles| *settles += 1);
        self.frontier.notify_changed();
        Ok(())
    }

    /// Whether this worker extracts reference edges for the tag: its schema is
    /// known and declares at least one `reference` position.
    fn extracts_references(&self, datum_tag: &str) -> bool {
        self.schemas
            .resolve_for_read(datum_tag)
            .is_some_and(|schema| schema.compiled().declares_references())
    }
}

struct DocView<'a> {
    doc: &'a Automerge,
    heads: &'a [ChangeHash],
    current: bool,
    datums: Vec<ObjId>,
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
            datums: Vec::new(),
            metadata: Vec::new(),
        };
        view.datums = view.maps(&ROOT, "datums")?;
        for datums in &view.datums {
            for dmeta in view.maps(datums, DMETA_KEY)? {
                view.metadata.extend(view.maps(&dmeta, "datums")?);
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

    fn keys(&self) -> BTreeSet<DatumKey> {
        self.datums
            .iter()
            .chain(&self.metadata)
            .flat_map(|object| {
                let range = if self.current {
                    self.doc.map_range(object, ..)
                } else {
                    self.doc.map_range_at(object, .., self.heads)
                };
                range
                    .filter_map(|item| item.key.parse::<DatumKey>().ok())
                    .filter(|key| key.to_string() != DMETA_KEY)
            })
            .collect()
    }

    fn slot(&self, key: &DatumKey) -> Res<DatumMembership> {
        let text = key.to_string();
        let mut present = false;
        for datums in &self.datums {
            present |= !self.all(datums, text.as_str())?.is_empty();
        }
        let mut heads = BTreeSet::new();
        for metadata in &self.metadata {
            for datum in self.maps(metadata, &text)? {
                for (value, list) in self.all(&datum, "signatures")? {
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
            .filter_map(|head| self.doc.get_change_meta_by_hash(head))
            .map(|change| change.max_op)
            .max()
            .unwrap_or(0);
        let datum_heads = heads.into_iter().collect();
        Ok(if present {
            DatumMembership::Present {
                doc_heads: self.heads.to_vec(),
                datum_heads,
                clock,
            }
        } else {
            DatumMembership::Removed {
                doc_heads: Some(self.heads.to_vec()),
                datum_heads,
                clock,
            }
        })
    }

    fn project(
        &self,
        previous: Option<&DocState>,
    ) -> Res<(BTreeMap<DatumKey, DatumMembership>, bool)> {
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
                    // the op, not the current heads: a datum write followed
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
                    if name(0) != Some("datums") {
                        continue;
                    }
                    let key = match (name(1), name(2), name(3)) {
                        (None, _, _) => {
                            full = true;
                            None
                        }
                        (Some(DMETA_KEY), Some("datums"), Some(key)) => Some(key),
                        // Aliases, actors and timestamps do not change membership
                        // or signature heads; only the `datums` subtree does.
                        (Some(DMETA_KEY), Some(_), _) => None,
                        (Some(DMETA_KEY), _, _) => {
                            full = true;
                            None
                        }
                        (Some(key), _, _) => Some(key),
                    };
                    if let Some(key) = key.and_then(|key| key.parse::<DatumKey>().ok()) {
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

impl DatumSetStore {
    /// One read of the document, projected into the datum memberships it states
    /// and the declared references those memberships publish.
    ///
    /// `None` defers the delta instead of settling a partial view: the document
    /// is missing changes its heads name, so neither the membership nor the
    /// value a reference-declaring datum hydrates to is visible yet.
    fn project(
        &self,
        doc: &mut Automerge,
        work: Work,
        previous: Option<DocState>,
    ) -> Res<Option<Prepared>> {
        let Some(heads) = work.state.heads.as_deref() else {
            return Ok(None);
        };
        if !doc.get_missing_deps(heads).is_empty() {
            return Ok(None);
        }
        let (slots, replace_all) = {
            let baseline = previous
                .as_ref()
                .filter(|state| state.epoch == work.state.epoch);
            let view = DocView::new(doc, heads)?;
            view.project(baseline)?
        };
        let ref_edges = self.reference_edges(doc, &work.doc_id, heads, &slots)?;
        Ok(Some(Prepared {
            work,
            previous,
            slots,
            replace_all,
            ref_edges,
        }))
    }

    /// The declared references of every gate-accepted datum in this projection,
    /// keyed by the route they were read from.
    ///
    /// Only reference-declaring tags are opened: the gate verify and the
    /// hydration walk sit in the worker's hot path, and a tag whose schema
    /// declares no `reference` position can never publish an edge.
    fn reference_edges(
        &self,
        doc: &mut Automerge,
        doc_id: &DocId,
        heads: &[ChangeHash],
        slots: &BTreeMap<DatumKey, DatumMembership>,
    ) -> Res<BTreeMap<DatumRoute, Vec<DatumRefEdge>>> {
        let mut edges = BTreeMap::new();
        for (key, membership) in slots {
            let DatumMembership::Present { .. } = membership else {
                continue;
            };
            let Some(schema) = self.schemas.resolve_for_read(key.tag().as_str()) else {
                continue;
            };
            if !schema.compiled().declares_references() {
                continue;
            }
            let route = DatumRoute {
                doc_id: doc_id.clone(),
                datum_key: key.clone(),
            };
            edges.insert(
                route.clone(),
                edges_from(doc, &route, heads, schema.compiled())?,
            );
        }
        Ok(edges)
    }

    /// Catch the reference edges up with the tags this build newly declares references on.
    ///
    /// The worker extracts edges only for the revisions it projects, so a tag whose schema starts
    /// declaring a `reference` position has stored entries and no edges: the frontier has already
    /// moved past them. `big_cabinet_datum_set_ref_tags` records the tags caught up this way; at
    /// boot, before the worker consumes anything, every unseen reference tag re-extracts from its
    /// stored entries at the `doc_heads` each entry recorded — the revision that entry itself
    /// describes, so the edges agree with the membership beside them.
    ///
    /// A tag is marked seen only once every document holding its entries was read and every one of
    /// its routes extracted. One that is not local stays unseen and is retried at the next boot:
    /// the frontier does not re-emit a document that materializes at heads the index already
    /// settled, so skipping it here would leave its edges empty for good.
    pub(crate) async fn backfill_reference_tags(
        &self,
        repo: &SharedBigRepo,
        concurrency: NonZeroUsize,
    ) -> Res<()> {
        let seen: BTreeSet<String> = queries::ref_tags(&self.sql).await?.into_iter().collect();
        let unseen: BTreeSet<String> = self
            .schemas
            .reference_tags()
            .into_iter()
            .filter(|tag| !seen.contains(tag))
            .collect();
        if unseen.is_empty() {
            return Ok(());
        }
        let unseen = Arc::new(unseen);
        let mut docs = queries::ref_tag_docs(&self.sql, serde_json::to_string(&unseen)?)
            .await?
            .into_iter()
            .map(|doc_id| Ok(doc_id.parse::<DocId>()?))
            .collect::<Res<Vec<DocId>>>()?
            .into_iter();
        let tasks = FuturesUnordered::new();
        tokio::pin!(tasks);
        let mut complete = true;
        loop {
            while tasks.len() < concurrency.get() {
                let Some(doc_id) = docs.next() else {
                    break;
                };
                let store = self.clone();
                let repo = Arc::clone(repo);
                let unseen = Arc::clone(&unseen);
                tasks.push(async move { store.backfill_doc(&repo, &unseen, doc_id).await });
            }
            let Some(extracted) = tasks.next().await else {
                break;
            };
            complete &= extracted?;
        }
        if complete {
            let transaction = self.sql.begin_write().await?;
            for tag in unseen.iter() {
                queries::put_ref_tag(&transaction, tag.clone()).await?;
            }
            transaction.commit().await?;
        }
        Ok(())
    }

    /// Whether every one of `doc`'s stored routes in `tags` had its edges extracted.
    async fn backfill_doc(
        &self,
        repo: &SharedBigRepo,
        tags: &BTreeSet<String>,
        doc_id: DocId,
    ) -> Res<bool> {
        let handle = match repo.get_doc_for_projection(&doc_id).await? {
            DocLookup::Ready(handle) => handle,
            DocLookup::Missing | DocLookup::PendingMaterialization => return Ok(false),
        };
        let routes: Vec<(DatumRoute, Vec<ChangeHash>)> = self
            .query(DatumSetQuery::Doc(doc_id))
            .await?
            .into_iter()
            .filter(|record| tags.contains(record.route.datum_key.tag().as_str()))
            .map(|record| (record.route, record.doc_heads))
            .collect();
        let mut edges = BTreeMap::new();
        let mut extracted = true;
        handle
            .with_document(|doc| -> Res<()> {
                for (route, heads) in &routes {
                    let Some(schema) = self
                        .schemas
                        .resolve_for_read(route.datum_key.tag().as_str())
                    else {
                        continue;
                    };
                    // Stored heads the document no longer carries cannot be read; leave the tag
                    // unseen so a later boot retries once the changes are local.
                    if !doc.get_missing_deps(heads).is_empty() {
                        extracted = false;
                        continue;
                    }
                    edges.insert(
                        route.clone(),
                        edges_from(doc, route, heads, schema.compiled())?,
                    );
                }
                Ok(())
            })
            .await??;
        write_edges(&self.sql, edges).await?;
        Ok(extracted)
    }

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
                ref_edges: BTreeMap::new(),
            }));
        }
        if work.state.heads.is_none() {
            return Ok(Some(Prepared {
                work,
                previous,
                slots: BTreeMap::new(),
                replace_all: true,
                ref_edges: BTreeMap::new(),
            }));
        }
        if previous
            .as_ref()
            .is_some_and(|state| state.heads == work.state.heads && state.epoch == work.state.epoch)
        {
            return Ok(Some(Prepared {
                work,
                previous,
                slots: BTreeMap::new(),
                replace_all: false,
                ref_edges: BTreeMap::new(),
            }));
        }
        let handle = match repo.get_doc_for_projection(&work.doc_id).await? {
            DocLookup::Ready(handle) => handle,
            DocLookup::Missing | DocLookup::PendingMaterialization => return Ok(None),
        };
        // Extraction pins the doc at the delta's heads, which automerge only
        // offers through a transaction, so the projection takes the doc mutably
        // even though it writes nothing to it.
        handle
            .with_document(|doc| self.project(doc, work, previous))
            .await?
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
        // Before consuming anything: the frontier only carries revisions the worker has not settled
        // yet, so the edges of a tag that newly declares references would never be extracted from
        // the entries already stored for it.
        self.backfill_reference_tags(&repo, concurrency).await?;
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
                    for notification in notifications.ok_or_else(|| ferr!("datum-set materialization listener closed"))? {
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
impl RevisionedStore for DatumSetStore {
    type Revision = u64;
    type Entry = FrontierEntry<DatumRoute, DatumMembership>;
    type Selector = DatumTag;
    type Error = KeyedFrontierError;
    type Reader<'a> = KeyedFrontierRevisionReader<'a, DatumRoute, DatumMembership>;

    async fn latest_revision(&self) -> Result<u64, Self::Error> {
        self.frontier.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        tag: DatumTag,
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

/// The edges one route's value states at `heads`.
///
/// The caller has already established that the document carries those heads, so extraction is not
/// allowed to park here. Only a gate-accepted value publishes edges: a rejected or pending datum's
/// value is not what its signatures cover, and an accepted but undecodable value states nothing the
/// schema can read.
fn edges_from(
    doc: &mut Automerge,
    route: &DatumRoute,
    heads: &[ChangeHash],
    schema: &CompiledSchema,
) -> Res<Vec<DatumRefEdge>> {
    let Verdict::Accepted {
        native: Some(_), ..
    } = Gate::new(doc, heads).verify(&route.datum_key)
    else {
        return Ok(Vec::new());
    };
    let tx = doc
        .transaction_at(PatchLog::null(), heads)
        .expect("a null patch log matches any document");
    let edges = (|| -> Res<Vec<DatumRefEdge>> {
        let Some((Value::Object(ObjType::Map), datums)) = tx.get(ROOT, "datums")? else {
            return Ok(Vec::new());
        };
        let value = match hydrate_value(
            &tx,
            &datums,
            route.datum_key.to_string().as_str().into(),
            schema,
        ) {
            Ok(value) => value,
            Err(_) => return Ok(Vec::new()),
        };
        Ok(declared_references(schema, &value)
            .into_iter()
            .map(|found| DatumRefEdge {
                origin: route.clone(),
                path: found.path,
                target: match found.target {
                    ReferenceTarget::Datum { reference, heads } => DatumRefTarget::Route {
                        route: DatumRoute {
                            doc_id: reference.document().resolve(&route.doc_id),
                            datum_key: reference.key().clone(),
                        },
                        heads,
                    },
                    ReferenceTarget::Key(key) => DatumRefTarget::Key(key),
                },
            })
            .collect())
    })();
    tx.rollback();
    edges
}

/// Replace the stored edges of every route in `edges` with the extracted ones. Routes absent from
/// the map keep their edges, which is how a projection that only re-read some of a document's
/// datums leaves the others' references alone.
async fn write_edges(
    db: &impl crate::sqlite_rs_alias::Executor,
    edges: BTreeMap<DatumRoute, Vec<DatumRefEdge>>,
) -> Res<()> {
    for (route, edges) in edges {
        queries::delete_edges(db, route.doc_id.to_string(), route.datum_key.to_string()).await?;
        for edge in &edges {
            let (target_doc, target_datum, ref_heads) = edge.target.columns()?;
            queries::insert_edge(
                db,
                edge.origin.doc_id.to_string(),
                edge.origin.datum_key.to_string(),
                edge.path.clone(),
                target_doc,
                target_datum,
                ref_heads,
            )
            .await?;
        }
    }
    Ok(())
}

/// Append the SQL boolean for `predicate` over the candidate document column `doc`.
///
/// Every clause is one indexed question about the candidate document: `EXISTS` over the entries
/// table for tags and keys, the reference-edge join for `HasReferenceTo`, and a correlated reach
/// CTE for `InDrawer`. The combinators are the SQL boolean operators, so the caller gets one
/// statement and no document set crosses into Rust.
fn push_predicate(query: &mut Sql, doc: &str, predicate: &Predicate) {
    match predicate {
        Predicate::HasTag { tag } => {
            query
                .push("EXISTS (SELECT 1 FROM big_cabinet_datum_set_entries AS match WHERE match.doc_id = ")
                .push(doc)
                .push(" AND match.datum_tag = ")
                .bind(tag.to_string())
                .push(")");
        }
        Predicate::HasKey { key } => {
            query
                .push("EXISTS (SELECT 1 FROM big_cabinet_datum_set_entries AS match WHERE match.doc_id = ")
                .push(doc)
                .push(" AND match.datum_key = ")
                .bind(key.to_string())
                .push(")");
        }
        Predicate::IsDoc { doc_id } => {
            query.push(doc).push(" = ").bind(doc_id.to_string());
        }
        Predicate::InDrawer {
            drawer_id,
            transitive,
            at_least,
        } => push_in_drawer(query, doc, drawer_id, *transitive, *at_least),
        Predicate::HasReferenceTo {
            target_tag,
            target_doc,
        } => {
            query
                .push("EXISTS (SELECT 1 FROM big_cabinet_datum_ref_edges AS edge JOIN big_cabinet_datum_set_entries AS target ON target.doc_id = edge.target_doc AND target.datum_key = edge.target_datum WHERE edge.origin_doc = ")
                .push(doc)
                .push(" AND target.datum_tag = ")
                .bind(target_tag.to_string());
            if let Some(target_doc) = target_doc {
                query
                    .push(" AND target.doc_id = ")
                    .bind(target_doc.to_string());
            }
            query.push(")");
        }
        Predicate::And { clauses } => push_conjunction(query, clauses, " AND ", "1", doc),
        Predicate::Or { clauses } => push_conjunction(query, clauses, " OR ", "0", doc),
        Predicate::Not { inner } => {
            query.push("NOT (");
            push_predicate(query, doc, inner);
            query.push(")");
        }
    }
}

fn push_conjunction(
    query: &mut Sql,
    clauses: &[Predicate],
    operator: &str,
    empty: &str,
    doc: &str,
) {
    if clauses.is_empty() {
        query.push(empty);
        return;
    }
    query.push("(");
    for (index, clause) in clauses.iter().enumerate() {
        if index != 0 {
            query.push(operator);
        }
        push_predicate(query, doc, clause);
    }
    query.push(")");
}

/// The reach CTE from `drawer/sqlite/queries/scoped_docs.sql`, correlated on the candidate doc.
///
/// `transitive: false` keeps only the root reach row, which drops the child-link branch: the
/// drawer's own document entries, for a `selfDoc` layout the drawer document itself, and its
/// own self-doc children — a self-doc child is one of the drawer's own docs (ADR 026 §6),
/// while a single-group child's docs stay the child's. The root
/// level is the requested `at_least`, which is the caller's grant on the drawer as far as the
/// threshold matters: the caller was admitted at that level, and attenuating by a higher grant
/// cannot change whether a document clears the same threshold.
fn push_in_drawer(
    query: &mut Sql,
    doc: &str,
    drawer_id: &DocId,
    transitive: bool,
    at_least: DrawerAccess,
) {
    let level = drawer_level(at_least);
    query.push(
        r#"EXISTS (
            WITH RECURSIVE levels(access, level) AS (
                VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
            ), reachable(drawer_id, level) AS (
                SELECT "#,
    );
    query.bind(drawer_id.to_string()).push(", ").bind(level);
    if transitive {
        query.push(
            r#"
                UNION
                SELECT links.child_id
                     , MIN(reachable.level, levels.level)
                  FROM reachable
                  JOIN big_cabinet_drawer_children AS links ON links.parent_id = reachable.drawer_id
                  JOIN levels ON levels.access = links.access
                  JOIN big_cabinet_drawers AS parent ON parent.drawer_id = reachable.drawer_id
                 WHERE json_extract(parent.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'"#,
        );
    }
    query.push(
        r#"
            ), entries(doc_id, level) AS (
                SELECT docs.doc_id
                     , MIN(reachable.level, levels.level)
                  FROM reachable
                  JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
                  JOIN big_cabinet_drawer_docs AS docs ON docs.drawer_id = reachable.drawer_id
                  JOIN levels ON levels.access = docs.access
                 WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
                UNION ALL
                SELECT reachable.drawer_id
                     , reachable.level
                  FROM reachable
                  JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
                 WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'"#,
    );
    if !transitive {
        // `reachable` holds only the root here, so this adds exactly the root's own
        // self-doc children at the level their child entry confers.
        query.push(
            r#"
                UNION ALL
                SELECT links.child_id
                     , MIN(reachable.level, levels.level)
                  FROM reachable
                  JOIN big_cabinet_drawer_children AS links ON links.parent_id = reachable.drawer_id
                  JOIN levels ON levels.access = links.access
                  JOIN big_cabinet_drawers AS parent ON parent.drawer_id = reachable.drawer_id
                  JOIN big_cabinet_drawers AS child ON child.drawer_id = links.child_id
                 WHERE json_extract(parent.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
                   AND json_extract(child.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'"#,
        );
    }
    query.push(
        r#"
            )
            SELECT 1
              FROM entries
             WHERE entries.doc_id = "#,
    );
    query
        .push(doc)
        .push(" AND entries.level >= ")
        .bind(level)
        .push(")");
}

fn drawer_level(access: DrawerAccess) -> i64 {
    match access {
        DrawerAccess::Relay => 0,
        DrawerAccess::Read => 1,
        DrawerAccess::Write => 2,
        DrawerAccess::Admin => 3,
    }
}

#[cfg(test)]
mod tests;
