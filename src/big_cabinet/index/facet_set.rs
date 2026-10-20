use automerge::ChangeHash;
use big_cabinet_core::{DocumentId, FacetKey, FacetTag};
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync::keyed_frontier::{SqliteFrontierCodec, SqliteFrontierSelector, SqliteKeyedFrontier};
use big_sync_core::delta_walker_sparse_state::DeltaWalkerSparseStateRepo;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::keyed_frontier::{FrontierEntry, KeyedFrontier, KeyedFrontierError};
use big_sync_core::revisioned_store::{KeyedFrontierRevisionReader, RevisionRead, RevisionedStore};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Row, Sqlite, Transaction};
use sqlx_utils_rs::SqlCtx;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::{Res, async_trait, ferr};

const FACET_SET_ID: &str = "big-cabinet-facet-set";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FacetSetInput {
    Snapshot {
        document_id: DocumentId,
        heads: Vec<ChangeHash>,
        facet_keys: BTreeSet<FacetKey>,
    },
    Removed {
        document_id: DocumentId,
    },
}

impl FacetSetInput {
    fn document_id(&self) -> &DocumentId {
        match self {
            Self::Snapshot { document_id, .. } | Self::Removed { document_id } => document_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FacetRoute {
    pub facet_tag: FacetTag,
    pub document_id: DocumentId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FacetMembership {
    Present {
        heads: Vec<ChangeHash>,
        matching_keys: BTreeSet<FacetKey>,
    },
    Removed {
        prior_heads: Vec<ChangeHash>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetRecord {
    pub route: FacetRoute,
    pub heads: Vec<ChangeHash>,
    pub matching_keys: BTreeSet<FacetKey>,
    pub source_revision: u64,
    pub output_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FacetSetQuery {
    Document(DocumentId),
    Tag(FacetTag),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FacetSetProjection {
    Applied {
        source_revision: u64,
        output_revision: u64,
    },
    ReplayComplete {
        through: u64,
    },
}

#[derive(Clone)]
struct FacetSetCodec;

impl SqliteFrontierCodec for FacetSetCodec {
    type Key = FacetRoute;
    type Value = FacetMembership;

    fn encode_key(&self, key: &Self::Key) -> Vec<u8> {
        serde_json::to_vec(key).expect("facet route is serializable")
    }

    fn decode_key(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Key, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn encode_value(&self, value: &Self::Value) -> Vec<u8> {
        serde_json::to_vec(value).expect("facet membership is serializable")
    }

    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn namespace(&self, key: &Self::Key) -> Option<Vec<u8>> {
        Some(key.facet_tag.as_str().as_bytes().to_vec())
    }
}

#[derive(Clone)]
pub struct FacetSetStore {
    sql: SqlCtx,
    frontier: SqliteKeyedFrontier<FacetSetCodec>,
    input_state: SqliteDeltaWalkerStateRepo,
}

impl FacetSetStore {
    pub async fn new(sql: SqlCtx) -> Res<Self> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_routes (
                document_id TEXT NOT NULL
                , facet_tag TEXT NOT NULL
                , heads_json TEXT NOT NULL
                , matching_keys_json TEXT NOT NULL
                , source_revision INTEGER NOT NULL
                , output_revision INTEGER NOT NULL
                , PRIMARY KEY (document_id, facet_tag)
            ) STRICT"#,
        )
        .execute(&sql.write_pool)
        .await?;
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS big_cabinet_facet_set_routes_tag_idx
                 ON big_cabinet_facet_set_routes (facet_tag, document_id)"#,
        )
        .execute(&sql.write_pool)
        .await?;
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
            "accepted-documents",
        )
        .await?;
        Ok(Self {
            sql,
            frontier,
            input_state,
        })
    }

    pub async fn source_revision(&self) -> Res<u64> {
        Ok(self.input_state.progress().await?.upstream_revision)
    }

    pub async fn projector<'a, S>(
        &'a self,
        source: &'a S,
        selector: S::Selector,
    ) -> Res<FacetSetProjector<'a, S>>
    where
        S: RevisionedStore<Revision = u64, Entry = FacetSetInput>,
        S::Error: std::fmt::Debug,
    {
        let after = self.source_revision().await?;
        let reader = source
            .open(selector, after)
            .await
            .map_err(|error| ferr!("opening accepted document revisions: {error:?}"))?;
        let walker = SerialDeltaWalker::open(reader, &self.input_state).await?;
        Ok(FacetSetProjector {
            store: self,
            walker,
            pending: None,
        })
    }

    pub async fn query(&self, selector: FacetSetQuery) -> Res<Vec<FacetRecord>> {
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"SELECT document_id
                 , facet_tag
                 , heads_json
                 , matching_keys_json
                 , source_revision
                 , output_revision
              FROM big_cabinet_facet_set_routes
             WHERE "#,
        );
        match selector {
            FacetSetQuery::Document(document_id) => {
                query
                    .push("document_id = ")
                    .push_bind(document_id.to_string());
            }
            FacetSetQuery::Tag(tag) => {
                query.push("facet_tag = ").push_bind(tag.to_string());
            }
        }
        query.push(" ORDER BY facet_tag, document_id");
        query
            .build()
            .fetch_all(&self.sql.read_pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(FacetRecord {
                    route: FacetRoute {
                        facet_tag: FacetTag::new(row.get::<String, _>("facet_tag"))?,
                        document_id: row.get::<String, _>("document_id").parse()?,
                    },
                    heads: serde_json::from_str(row.get("heads_json"))?,
                    matching_keys: serde_json::from_str(row.get("matching_keys_json"))?,
                    source_revision: u64::try_from(row.get::<i64, _>("source_revision"))?,
                    output_revision: u64::try_from(row.get::<i64, _>("output_revision"))?,
                })
            })
            .collect()
    }

    async fn apply_in_context(
        &self,
        source_revision: u64,
        entries: &[FacetSetInput],
        previous_heads: &BTreeMap<Vec<u8>, Vec<ChangeHash>>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Res<u64> {
        let mut mutations = Vec::new();
        let mut records = Vec::new();
        for entry in entries {
            let document_id = entry.document_id();
            let document_key = document_id.to_string();
            let old_tags: Vec<String> = sqlx::query_scalar(
                r#"SELECT facet_tag
                     FROM big_cabinet_facet_set_routes
                    WHERE document_id = ?
                    ORDER BY facet_tag"#,
            )
            .bind(&document_key)
            .fetch_all(&mut **transaction)
            .await?;
            let mut current = BTreeMap::<FacetTag, BTreeSet<FacetKey>>::new();
            if let FacetSetInput::Snapshot { facet_keys, .. } = entry {
                for key in facet_keys {
                    current
                        .entry(key.tag().clone())
                        .or_default()
                        .insert(key.clone());
                }
            }
            for old_tag in old_tags {
                let facet_tag = FacetTag::new(old_tag)?;
                if !current.contains_key(&facet_tag) {
                    let prior_heads = previous_heads
                        .get(document_key.as_bytes())
                        .expect("indexed document has sparse accepted heads")
                        .clone();
                    mutations.push((
                        FacetRoute {
                            facet_tag,
                            document_id: document_id.clone(),
                        },
                        Some(FacetMembership::Removed { prior_heads }),
                    ));
                }
            }
            if let FacetSetInput::Snapshot { heads, .. } = entry {
                for (facet_tag, matching_keys) in current {
                    let route = FacetRoute {
                        facet_tag,
                        document_id: document_id.clone(),
                    };
                    mutations.push((
                        route.clone(),
                        Some(FacetMembership::Present {
                            heads: heads.clone(),
                            matching_keys: matching_keys.clone(),
                        }),
                    ));
                    records.push((route, heads, matching_keys));
                }
            }
        }
        let output_revision = self
            .frontier
            .apply_in_context(transaction, mutations)
            .await?;
        for entry in entries {
            sqlx::query(
                r#"DELETE FROM big_cabinet_facet_set_routes
                    WHERE document_id = ?"#,
            )
            .bind(entry.document_id().to_string())
            .execute(&mut **transaction)
            .await?;
        }
        for (route, heads, matching_keys) in records {
            sqlx::query(
                r#"INSERT INTO big_cabinet_facet_set_routes (
                    document_id
                    , facet_tag
                    , heads_json
                    , matching_keys_json
                    , source_revision
                    , output_revision
                ) VALUES (?, ?, ?, ?, ?, ?)"#,
            )
            .bind(route.document_id.to_string())
            .bind(route.facet_tag.as_str())
            .bind(serde_json::to_string(heads)?)
            .bind(serde_json::to_string(&matching_keys)?)
            .bind(i64::try_from(source_revision)?)
            .bind(i64::try_from(output_revision)?)
            .execute(&mut **transaction)
            .await?;
        }
        Ok(output_revision)
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
        let selector = SqliteFrontierSelector::Namespaces(BTreeMap::from([(
            tag.as_str().as_bytes().to_vec(),
            after,
        )]));
        Ok(KeyedFrontierRevisionReader::new(
            self.frontier.open(selector).await?,
        ))
    }
}

pub struct FacetSetProjector<'a, S>
where
    S: RevisionedStore<Revision = u64, Entry = FacetSetInput> + 'a,
{
    store: &'a FacetSetStore,
    walker: SerialDeltaWalker<'a, S, SqliteDeltaWalkerStateRepo>,
    pending: Option<(u64, Vec<FacetSetInput>)>,
}

impl<S> FacetSetProjector<'_, S>
where
    S: RevisionedStore<Revision = u64, Entry = FacetSetInput>,
    S::Error: std::fmt::Debug,
{
    pub async fn next(&mut self) -> Res<FacetSetProjection> {
        if self.pending.is_none() {
            match self.walker.next().await? {
                RevisionRead::ReplayComplete { through } => {
                    return Ok(FacetSetProjection::ReplayComplete { through });
                }
                RevisionRead::Entries {
                    revision,
                    mut entries,
                } => {
                    for entry in &mut entries {
                        if let FacetSetInput::Snapshot { heads, .. } = entry {
                            heads.sort_unstable();
                            heads.dedup();
                        }
                    }
                    self.pending = Some((revision, entries));
                }
            }
        }
        let (source_revision, entries) = self.pending.as_ref().expect("source revision is pending");
        let source_revision = *source_revision;
        let mut keys = BTreeSet::new();
        for entry in entries {
            let key = entry.document_id().to_string().into_bytes();
            if !keys.insert(key) {
                return Err(ferr!(
                    "duplicate document in accepted revision {source_revision}: {}",
                    entry.document_id()
                ));
            }
        }
        let previous_heads = self
            .store
            .input_state
            .get_many(&keys.into_iter().collect::<Vec<_>>())
            .await?
            .into_iter()
            .map(|(key, value)| Ok((key, serde_json::from_slice::<Vec<ChangeHash>>(&value)?)))
            .collect::<Res<BTreeMap<_, _>>>()?;
        let mut settlement = self.walker.begin_settlement(source_revision).await?;
        let output_revision = self
            .store
            .apply_in_context(
                source_revision,
                entries,
                &previous_heads,
                settlement.context_mut(),
            )
            .await?;
        for entry in entries {
            let key = entry.document_id().to_string().into_bytes();
            match entry {
                FacetSetInput::Snapshot { heads, .. } => {
                    settlement.put(key, serde_json::to_vec(heads)?).await?;
                }
                FacetSetInput::Removed { .. } => settlement.delete(&key).await?,
            }
        }
        settlement.settle().await?;
        self.store.frontier.notify_changed();
        self.pending = None;
        Ok(FacetSetProjection::Applied {
            source_revision,
            output_revision,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::revisioned_store::{RevisionReadLimits, RevisionedStoreReader};
    use big_sync_core::serial_delta_walker::SerialDeltaWalkerError;
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;

    struct Source {
        revisions: Vec<(u64, Vec<FacetSetInput>)>,
    }

    struct Reader {
        reads: VecDeque<RevisionRead<u64, FacetSetInput>>,
    }

    #[async_trait]
    impl RevisionedStoreReader<u64, FacetSetInput, utils_rs::prelude::eyre::Report> for Reader {
        async fn next(
            &mut self,
            _limits: RevisionReadLimits,
        ) -> Res<RevisionRead<u64, FacetSetInput>> {
            Ok(self.reads.pop_front().expect("scripted source exhausted"))
        }
    }

    #[async_trait]
    impl RevisionedStore for Source {
        type Revision = u64;
        type Entry = FacetSetInput;
        type Selector = ();
        type Error = utils_rs::prelude::eyre::Report;
        type Reader<'a> = Reader;

        async fn latest_revision(&self) -> Res<u64> {
            Ok(self
                .revisions
                .iter()
                .map(|(revision, _)| *revision)
                .max()
                .unwrap_or(0))
        }

        async fn open<'a>(&'a self, (): (), after: u64) -> Res<Reader> {
            let mut reads: VecDeque<_> = self
                .revisions
                .iter()
                .filter(|(revision, _)| *revision > after)
                .map(|(revision, entries)| RevisionRead::Entries {
                    revision: *revision,
                    entries: entries.clone(),
                })
                .collect();
            reads.push_back(RevisionRead::ReplayComplete {
                through: self.latest_revision().await?,
            });
            Ok(Reader { reads })
        }
    }

    fn document(index: u8) -> DocumentId {
        DocumentId::new([index; 32])
    }

    fn tag(name: &str) -> FacetTag {
        FacetTag::new(name).unwrap()
    }

    fn heads(indices: &[u8]) -> Vec<ChangeHash> {
        indices
            .iter()
            .map(|index| ChangeHash([*index; 32]))
            .collect()
    }

    fn keys(entries: &[(&str, &str)]) -> BTreeSet<FacetKey> {
        entries
            .iter()
            .map(|(tag, id)| FacetKey::from_text(*tag, *id).unwrap())
            .collect()
    }

    fn snapshot(index: u8, version: &[u8], facets: &[(&str, &str)]) -> FacetSetInput {
        FacetSetInput::Snapshot {
            document_id: document(index),
            heads: heads(version),
            facet_keys: keys(facets),
        }
    }

    fn removed(index: u8) -> FacetSetInput {
        FacetSetInput::Removed {
            document_id: document(index),
        }
    }

    fn route(index: u8, name: &str) -> FacetRoute {
        FacetRoute {
            facet_tag: tag(name),
            document_id: document(index),
        }
    }

    fn present(
        index: u8,
        name: &str,
        version: &[u8],
        facets: &[(&str, &str)],
        revision: u64,
    ) -> FrontierEntry<FacetRoute, FacetMembership> {
        FrontierEntry {
            key: route(index, name),
            revision,
            value: Some(FacetMembership::Present {
                heads: heads(version),
                matching_keys: keys(facets),
            }),
        }
    }

    fn tombstone(
        index: u8,
        name: &str,
        version: &[u8],
        revision: u64,
    ) -> FrontierEntry<FacetRoute, FacetMembership> {
        FrontierEntry {
            key: route(index, name),
            revision,
            value: Some(FacetMembership::Removed {
                prior_heads: heads(version),
            }),
        }
    }

    fn applied(source_revision: u64, output_revision: u64) -> FacetSetProjection {
        FacetSetProjection::Applied {
            source_revision,
            output_revision,
        }
    }

    async fn store() -> FacetSetStore {
        FacetSetStore::new(SqlCtx::memory().await.unwrap())
            .await
            .unwrap()
    }

    async fn sparse_heads(store: &FacetSetStore, index: u8) -> Option<Vec<ChangeHash>> {
        store
            .input_state
            .get(document(index).to_string().as_bytes())
            .await
            .unwrap()
            .map(|value| serde_json::from_slice(&value).unwrap())
    }

    #[tokio::test]
    async fn same_tag_instances_and_heads_are_canonical() -> Res<()> {
        let store = store().await;
        let binary_key = FacetKey::new("note", vec![0xff, b'/']).unwrap();
        let mut facet_keys = keys(&[("note", "a"), ("note", "b"), ("task", "c")]);
        facet_keys.insert(binary_key.clone());
        let source = Source {
            revisions: vec![(
                10,
                vec![FacetSetInput::Snapshot {
                    document_id: document(1),
                    heads: heads(&[2, 1, 2]),
                    facet_keys,
                }],
            )],
        };
        let mut projector = store.projector(&source, ()).await?;
        assert_eq!(projector.next().await?, applied(10, 1));
        let mut note_keys = keys(&[("note", "a"), ("note", "b")]);
        note_keys.insert(binary_key);
        let expected = vec![
            FacetRecord {
                route: route(1, "note"),
                heads: heads(&[1, 2]),
                matching_keys: note_keys.clone(),
                source_revision: 10,
                output_revision: 1,
            },
            FacetRecord {
                route: route(1, "task"),
                heads: heads(&[1, 2]),
                matching_keys: keys(&[("task", "c")]),
                source_revision: 10,
                output_revision: 1,
            },
        ];
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            expected
        );
        assert_eq!(
            store.query(FacetSetQuery::Tag(tag("note"))).await?,
            vec![expected[0].clone()]
        );
        assert_eq!(sparse_heads(&store, 1).await, Some(heads(&[1, 2])));
        let mut reader = store.open(tag("note"), 0).await?;
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 1,
                entries: vec![FrontierEntry {
                    key: route(1, "note"),
                    revision: 1,
                    value: Some(FacetMembership::Present {
                        heads: heads(&[1, 2]),
                        matching_keys: note_keys
                    }),
                }],
            }
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 1 }
        );
        assert_eq!(
            projector.next().await?,
            FacetSetProjection::ReplayComplete { through: 10 }
        );
        Ok(())
    }

    #[tokio::test]
    async fn content_only_updates_and_new_revision_reevaluations_touch_routes() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![
                (3, vec![snapshot(1, &[1], &[("note", "a")])]),
                (8, vec![snapshot(1, &[2], &[("note", "a")])]),
                (12, vec![snapshot(1, &[2], &[("note", "a")])]),
            ],
        };
        let mut projector = store.projector(&source, ()).await?;
        assert_eq!(projector.next().await?, applied(3, 1));
        let mut reader = store.open(tag("note"), 1).await?;
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 1,
                entries: vec![]
            }
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 1 }
        );
        let (read, projected) =
            tokio::join!(reader.next(RevisionReadLimits::default()), projector.next());
        assert_eq!(projected?, applied(8, 2));
        assert_eq!(
            read?,
            RevisionRead::Entries {
                revision: 2,
                entries: vec![present(1, "note", &[2], &[("note", "a")], 2)],
            }
        );
        assert_eq!(projector.next().await?, applied(12, 3));
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 3,
                entries: vec![present(1, "note", &[2], &[("note", "a")], 3)],
            }
        );
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            vec![FacetRecord {
                route: route(1, "note"),
                heads: heads(&[2]),
                matching_keys: keys(&[("note", "a")]),
                source_revision: 12,
                output_revision: 3,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn multi_document_tag_transition_and_removals_are_one_revision() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![
                (
                    1,
                    vec![
                        snapshot(1, &[1], &[("note", "a"), ("task", "b")]),
                        snapshot(2, &[7], &[("note", "c")]),
                    ],
                ),
                (
                    2,
                    vec![
                        snapshot(1, &[2], &[("archive", "a"), ("task", "b")]),
                        removed(2),
                    ],
                ),
                (3, vec![removed(1)]),
            ],
        };
        let mut projector = store.projector(&source, ()).await?;
        assert_eq!(projector.next().await?, applied(1, 1));
        assert_eq!(projector.next().await?, applied(2, 2));
        let mut reader = KeyedFrontierRevisionReader::new(
            store
                .frontier
                .open(SqliteFrontierSelector::All { after: 1 })
                .await?,
        );
        let limits = RevisionReadLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
        };
        let mut expected = vec![
            tombstone(1, "note", &[1], 2),
            tombstone(2, "note", &[7], 2),
            present(1, "archive", &[2], &[("archive", "a")], 2),
            present(1, "task", &[2], &[("task", "b")], 2),
        ];
        expected.sort_by(|left, right| left.key.cmp(&right.key));
        let RevisionRead::Entries {
            revision,
            mut entries,
        } = reader.next(limits).await?
        else {
            panic!("expected one atomic routing revision")
        };
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(
            RevisionRead::Entries { revision, entries },
            RevisionRead::Entries {
                revision: 2,
                entries: expected
            }
        );
        assert_eq!(
            reader.next(limits).await?,
            RevisionRead::ReplayComplete { through: 2 }
        );
        assert_eq!(
            store.query(FacetSetQuery::Document(document(2))).await?,
            vec![]
        );
        assert_eq!(sparse_heads(&store, 2).await, None);
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            vec![
                FacetRecord {
                    route: route(1, "archive"),
                    heads: heads(&[2]),
                    matching_keys: keys(&[("archive", "a")]),
                    source_revision: 2,
                    output_revision: 2
                },
                FacetRecord {
                    route: route(1, "task"),
                    heads: heads(&[2]),
                    matching_keys: keys(&[("task", "b")]),
                    source_revision: 2,
                    output_revision: 2
                },
            ]
        );
        assert_eq!(projector.next().await?, applied(3, 3));
        let mut reader = store.open(tag("task"), 2).await?;
        assert_eq!(
            reader.next(limits).await?,
            RevisionRead::Entries {
                revision: 3,
                entries: vec![tombstone(1, "task", &[2], 3)]
            }
        );
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            vec![]
        );
        assert_eq!(sparse_heads(&store, 1).await, None);
        Ok(())
    }

    #[tokio::test]
    async fn empty_progress_and_documents_without_facets_settle() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![
                (7, vec![]),
                (11, vec![snapshot(1, &[4], &[])]),
                (18, vec![removed(1)]),
            ],
        };
        let mut projector = store.projector(&source, ()).await?;
        assert_eq!(projector.next().await?, applied(7, 1));
        let mut reader = store.open(tag("note"), 0).await?;
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 1,
                entries: vec![]
            }
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 1 }
        );
        assert_eq!(projector.next().await?, applied(11, 2));
        assert_eq!(sparse_heads(&store, 1).await, Some(heads(&[4])));
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            vec![]
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 2,
                entries: vec![]
            }
        );
        assert_eq!(projector.next().await?, applied(18, 3));
        assert_eq!(sparse_heads(&store, 1).await, None);
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 3,
                entries: vec![]
            }
        );
        assert_eq!(store.source_revision().await?, 18);
        assert_eq!(
            projector.next().await?,
            FacetSetProjection::ReplayComplete { through: 18 }
        );
        Ok(())
    }

    #[tokio::test]
    async fn independent_tag_readers_and_late_readers_survive_reopen() -> Res<()> {
        let directory = tempfile::TempDir::new()?;
        let url = format!(
            "sqlite://{}",
            directory.path().join("routes.sqlite").display()
        );
        let source = Source {
            revisions: vec![
                (2, vec![snapshot(1, &[1], &[("note", "a"), ("task", "b")])]),
                (
                    5,
                    vec![
                        snapshot(1, &[2], &[("task", "b")]),
                        snapshot(2, &[3], &[("note", "c")]),
                    ],
                ),
                (9, vec![snapshot(2, &[4], &[("note", "c")])]),
            ],
        };
        {
            let sql = SqlCtx::url(&url).await?;
            let store = FacetSetStore::new(sql.clone()).await?;
            let mut projector = store.projector(&source, ()).await?;
            assert_eq!(projector.next().await?, applied(2, 1));
            let mut note = store.open(tag("note"), 0).await?;
            let mut task = store.open(tag("task"), 0).await?;
            assert_eq!(
                note.next(RevisionReadLimits::default()).await?,
                RevisionRead::Entries {
                    revision: 1,
                    entries: vec![present(1, "note", &[1], &[("note", "a")], 1)]
                }
            );
            assert_eq!(
                note.next(RevisionReadLimits::default()).await?,
                RevisionRead::ReplayComplete { through: 1 }
            );
            assert_eq!(projector.next().await?, applied(5, 2));
            assert_eq!(
                task.next(RevisionReadLimits::default()).await?,
                RevisionRead::Entries {
                    revision: 1,
                    entries: vec![]
                }
            );
            assert_eq!(
                task.next(RevisionReadLimits::default()).await?,
                RevisionRead::ReplayComplete { through: 1 }
            );
            assert_eq!(
                task.next(RevisionReadLimits::default()).await?,
                RevisionRead::Entries {
                    revision: 2,
                    entries: vec![present(1, "task", &[2], &[("task", "b")], 2)]
                }
            );
            assert_eq!(projector.next().await?, applied(9, 3));
            assert_eq!(
                task.next(RevisionReadLimits::default()).await?,
                RevisionRead::Entries {
                    revision: 3,
                    entries: vec![]
                }
            );
            drop(task);
            drop(note);
            drop(projector);
            drop(store);
            sql.read_pool.close().await;
            sql.write_pool.close().await;
        }
        let store = FacetSetStore::new(SqlCtx::url(&url).await?).await?;
        assert_eq!(store.source_revision().await?, 9);
        assert_eq!(store.latest_revision().await?, 3);
        assert_eq!(sparse_heads(&store, 1).await, Some(heads(&[2])));
        assert_eq!(
            store.query(FacetSetQuery::Tag(tag("note"))).await?,
            vec![FacetRecord {
                route: route(2, "note"),
                heads: heads(&[4]),
                matching_keys: keys(&[("note", "c")]),
                source_revision: 9,
                output_revision: 3,
            }]
        );
        let mut late = store.open(tag("note"), 0).await?;
        assert_eq!(
            late.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 2,
                entries: vec![tombstone(1, "note", &[1], 2)]
            }
        );
        assert_eq!(
            late.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 3,
                entries: vec![present(2, "note", &[4], &[("note", "c")], 3)]
            }
        );
        assert_eq!(
            late.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 3 }
        );
        let mut resumed = store.open(tag("note"), 2).await?;
        assert_eq!(
            resumed.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 3,
                entries: vec![present(2, "note", &[4], &[("note", "c")], 3)]
            }
        );
        assert_eq!(
            resumed.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 3 }
        );
        assert_eq!(
            store.projector(&source, ()).await?.next().await?,
            FacetSetProjection::ReplayComplete { through: 9 }
        );
        assert_eq!(store.latest_revision().await?, 3);
        Ok(())
    }

    #[tokio::test]
    async fn failed_source_cursor_settlement_rolls_back_every_projection_effect() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![
                (4, vec![snapshot(1, &[1], &[("note", "a")])]),
                (
                    8,
                    vec![
                        snapshot(1, &[2], &[("task", "b")]),
                        snapshot(2, &[3], &[("note", "c")]),
                    ],
                ),
            ],
        };
        let mut projector = store.projector(&source, ()).await?;
        assert_eq!(projector.next().await?, applied(4, 1));
        let before = store.query(FacetSetQuery::Document(document(1))).await?;
        sqlx::query(
            r#"CREATE TRIGGER fail_facet_set_cursor
                BEFORE UPDATE ON delta_walker_progress
                WHEN NEW.namespace = 'big-cabinet-facet-set'
                 AND NEW.consumer_id = 'accepted-documents'
                 AND NEW.upstream_revision = 8
                BEGIN
                    SELECT RAISE(ABORT, 'injected facet-set settlement failure');
                END"#,
        )
        .execute(&store.sql.write_pool)
        .await?;
        let error = projector.next().await.unwrap_err();
        assert!(format!("{error:?}").contains("injected facet-set settlement failure"));
        assert_eq!(store.source_revision().await?, 4);
        assert_eq!(store.latest_revision().await?, 1);
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            before
        );
        assert_eq!(
            store.query(FacetSetQuery::Document(document(2))).await?,
            vec![]
        );
        assert_eq!(sparse_heads(&store, 1).await, Some(heads(&[1])));
        assert_eq!(sparse_heads(&store, 2).await, None);
        let mut reader = store.open(tag("note"), 1).await?;
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision: 1,
                entries: vec![]
            }
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 1 }
        );
        sqlx::query("DROP TRIGGER fail_facet_set_cursor")
            .execute(&store.sql.write_pool)
            .await?;
        let (read, projected) =
            tokio::join!(reader.next(RevisionReadLimits::default()), projector.next());
        assert_eq!(projected?, applied(8, 2));
        assert_eq!(
            read?,
            RevisionRead::Entries {
                revision: 2,
                entries: vec![
                    tombstone(1, "note", &[1], 2),
                    present(2, "note", &[3], &[("note", "c")], 2),
                ]
            }
        );
        assert_eq!(store.source_revision().await?, 8);
        assert_eq!(sparse_heads(&store, 1).await, Some(heads(&[2])));
        assert_eq!(sparse_heads(&store, 2).await, Some(heads(&[3])));
        Ok(())
    }

    #[tokio::test]
    async fn serial_walker_rejects_wrong_settlement_and_out_of_order_source() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![
                (4, vec![snapshot(1, &[1], &[("note", "a")])]),
                (3, vec![snapshot(1, &[2], &[("task", "b")])]),
            ],
        };
        let reader = source.open((), 0).await?;
        let mut walker = SerialDeltaWalker::<Source, _>::open(reader, &store.input_state).await?;
        assert_eq!(
            walker.next().await?,
            RevisionRead::Entries {
                revision: 4,
                entries: vec![snapshot(1, &[1], &[("note", "a")])]
            }
        );
        assert!(matches!(
            walker.begin_settlement(3).await,
            Err(SerialDeltaWalkerError::WrongSettlementRevision {
                requested: 3,
                pending: 4
            })
        ));
        assert_eq!(store.source_revision().await?, 0);
        assert_eq!(store.latest_revision().await?, 0);
        drop(walker);
        let mut projector = store.projector(&source, ()).await?;
        assert_eq!(projector.next().await?, applied(4, 1));
        let before = store.query(FacetSetQuery::Document(document(1))).await?;
        let error = projector.next().await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<SerialDeltaWalkerError<utils_rs::prelude::eyre::Report>>(),
            Some(SerialDeltaWalkerError::NonAdvancingRevision)
        ));
        assert_eq!(store.source_revision().await?, 4);
        assert_eq!(store.latest_revision().await?, 1);
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            before
        );
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_document_states_do_not_advance_projection() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![(
                5,
                vec![
                    snapshot(1, &[1], &[("note", "a")]),
                    snapshot(1, &[1], &[("task", "b")]),
                ],
            )],
        };
        let mut projector = store.projector(&source, ()).await?;
        let error = projector.next().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate document in accepted revision 5")
        );
        assert_eq!(store.source_revision().await?, 0);
        assert_eq!(store.latest_revision().await?, 0);
        assert_eq!(
            store.query(FacetSetQuery::Document(document(1))).await?,
            vec![]
        );
        assert_eq!(sparse_heads(&store, 1).await, None);
        Ok(())
    }

    #[tokio::test]
    async fn stale_projector_cannot_overwrite_another_settlement() -> Res<()> {
        let store = store().await;
        let source = Source {
            revisions: vec![(5, vec![snapshot(1, &[1], &[("note", "a")])])],
        };
        let mut first = store.projector(&source, ()).await?;
        let mut stale = store.projector(&source, ()).await?;
        assert_eq!(first.next().await?, applied(5, 1));
        let error = stale.next().await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<SerialDeltaWalkerError<utils_rs::prelude::eyre::Report>>(),
            Some(SerialDeltaWalkerError::State(
                big_sync_core::delta_walker_state::DeltaWalkerStateError::StaleProgress {
                    expected: 0
                }
            ))
        ));
        assert_eq!(store.source_revision().await?, 5);
        assert_eq!(store.latest_revision().await?, 1);
        Ok(())
    }
}
