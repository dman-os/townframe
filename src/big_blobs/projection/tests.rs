use super::*;
use automerge::ChangeHash;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsMessage;
use futures::future::BoxFuture;
use service_utils_rs::InlineRoute;
use sqlite_rs::Sql;
use sqlite_rs::SqliteRuntime;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use tokio::sync::oneshot;

fn route(document: u8, name: &str) -> DatumRoute {
    DatumRoute {
        doc_id: DocId::new([document; 32]),
        datum_key: format!("{}/{name}", Blob::ID).parse().unwrap(),
    }
}

fn accepted(digest: String, urls: Vec<String>, length_octets: u64) -> DatumRead {
    DatumRead::Accepted {
        value: serde_json::json!({
            "mime": "application/octet-stream",
            "digest": digest,
            "lengthOctets": length_octets,
            "urls": urls,
        }),
        version: DatumVersion::default(),
    }
}

fn reference(route: &DatumRoute, blob_id: BlobId) -> BlobReference {
    BlobReference {
        route: route.clone(),
        blob_id,
        length_octets: 42,
    }
}

fn entry(route: &DatumRoute, revision: u64) -> FrontierEntry<DatumRoute, DatumMembership> {
    FrontierEntry {
        revision,
        key: route.clone(),
        value: Some(DatumMembership::Present {
            doc_heads: vec![ChangeHash([9; 32])],
            datum_heads: vec![ChangeHash([8; 32])],
            clock: 3,
        }),
    }
}

struct DocsStub {
    expected: ReadDatums,
    response: Result<DatumsRead, DocsError>,
}

impl InlineRoute<DocsProtocol> for DocsStub {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let DocsMessage::ReadDatums(message) = message else {
                panic!("projection may only request typed reads");
            };
            assert_eq!(message.inner.params, self.expected);
            message
                .tx
                .send(self.response.clone().map(Into::into))
                .await
                .unwrap();
        })
    }
}

impl DocsStub {
    fn new(route: &DatumRoute, at: At, response: Result<DatumRead, DocsError>) -> Self {
        let capability = CapId::from_bytes([7; 16]);
        let heads = match &at {
            At::Heads(heads) => heads.clone(),
            At::Current => vec![ChangeHash([10; 32])],
        };
        let response = response.map(|datum| DatumsRead {
            heads: heads.clone(),
            datums: BTreeMap::from([(route.datum_key.clone(), datum)]),
        });
        Self {
            expected: ReadDatums {
                capability,
                doc_id: route.doc_id.clone(),
                at,
                datums: Some([route.datum_key.clone()].into()),
                optimistic: false,
            },
            response,
        }
    }
}

fn docs(
    route: &DatumRoute,
    response: Result<DatumRead, DocsError>,
) -> (Client<DocsProtocol>, CapId) {
    let stub = DocsStub::new(route, At::Heads(vec![ChangeHash([9; 32])]), response);
    let capability = stub.expected.capability;
    (Client::direct(Arc::new(stub)), capability)
}

struct ScriptedDocs {
    responses: std::sync::Mutex<VecDeque<DocsStub>>,
}

impl ScriptedDocs {
    fn new(
        route: &DatumRoute,
        responses: impl IntoIterator<Item = (At, Result<DatumRead, DocsError>)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            responses: std::sync::Mutex::new(
                responses
                    .into_iter()
                    .map(|(at, response)| DocsStub::new(route, at, response))
                    .collect(),
            ),
        })
    }
}

impl InlineRoute<DocsProtocol> for ScriptedDocs {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        let stub = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected Docs retry");
        Arc::new(stub).route_inline(message)
    }
}

struct ScriptedReader {
    reads: VecDeque<RevisionRead<u64, FrontierEntry<DatumRoute, DatumMembership>>>,
    exhausted: Option<oneshot::Sender<()>>,
}

impl ScriptedReader {
    fn new(
        reads: impl IntoIterator<Item = RevisionRead<u64, FrontierEntry<DatumRoute, DatumMembership>>>,
    ) -> (Self, impl Future<Output = ()> + Send) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                reads: reads.into_iter().collect(),
                exhausted: Some(sender),
            },
            async move { receiver.await.unwrap() },
        )
    }
}

#[async_trait]
impl RevisionedStoreReader<u64, FrontierEntry<DatumRoute, DatumMembership>, KeyedFrontierError>
    for ScriptedReader
{
    async fn next(
        &mut self,
        _limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, FrontierEntry<DatumRoute, DatumMembership>>, KeyedFrontierError>
    {
        if let Some(read) = self.reads.pop_front() {
            return Ok(read);
        }
        if let Some(exhausted) = self.exhausted.take() {
            exhausted.send(()).unwrap();
        }
        std::future::pending().await
    }
}

struct BlockingDocs {
    started: Arc<Notify>,
}

impl InlineRoute<DocsProtocol> for BlockingDocs {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let DocsMessage::ReadDatums(_message) = message else {
                panic!("projection may only request typed reads");
            };
            self.started.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn value_digest_not_main_id_and_both_spellings_deduplicate() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([11; 32]);
    for (index, spelling) in [blob_id.digest_str(), blob_id.to_string()]
        .into_iter()
        .enumerate()
    {
        let revision = u64::try_from(index)? + 1;
        let Prepared::Ready(projected) =
            reference_from_read(&route, accepted(spelling, Vec::new(), u64::MAX))
        else {
            panic!("accepted value must settle");
        };
        projection
            .settle(&route, projected, revision, Progress::Complete(revision))
            .await?;
    }
    assert_eq!(
        projection.query_digest(&blob_id).await?,
        vec![route.clone()]
    );
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![BlobReference {
            route,
            blob_id,
            length_octets: u64::MAX,
        }]
    );
    assert_eq!(projection.source_revision().await?, 2);
    Ok(())
}

#[test]
fn digest_is_authoritative_with_legacy_url_fallback_only_when_invalid() -> Res<()> {
    let route = route(1, "main");
    let primary = BlobId::new([1; 32]);
    let fallback = BlobId::new([2; 32]);
    assert_eq!(
        reference_from_read(
            &route,
            accepted(
                primary.digest_str(),
                vec![format!("db+blob:///{fallback}")],
                42,
            ),
        ),
        Prepared::Ready(Some(reference(&route, primary)))
    );
    for scheme in ["db+blob", "daybook-blob"] {
        for spelling in [fallback.digest_str(), fallback.to_string()] {
            assert_eq!(
                reference_from_read(
                    &route,
                    accepted(
                        "invalid".into(),
                        vec![
                            "not a url".into(),
                            format!("https://example.invalid/{primary}"),
                            format!("{scheme}:///{spelling}?via=mirror"),
                        ],
                        42,
                    ),
                ),
                Prepared::Ready(Some(reference(&route, fallback)))
            );
        }
    }
    assert_eq!(
        reference_from_read(&route, accepted("invalid".into(), Vec::new(), 42)),
        Prepared::Ready(None)
    );
    Ok(())
}

#[tokio::test]
async fn removed_route_without_a_document_preserves_other_references() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let blob_id = BlobId::new([3; 32]);
    let first = route(1, "main");
    let sibling = route(1, "other");
    let other_doc = route(2, "main");
    for (index, route) in [&first, &sibling, &other_doc].into_iter().enumerate() {
        let revision = u64::try_from(index)? + 1;
        projection
            .settle(
                route,
                Some(reference(route, blob_id)),
                revision,
                Progress::Complete(revision),
            )
            .await?;
    }
    let (docs, capability) = docs(&first, Err(DocsError::DocNotFound));
    for (index, value) in [
        Some(DatumMembership::Removed {
            doc_heads: None,
            datum_heads: Vec::new(),
            clock: 0,
        }),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let revision = u64::try_from(index)? * 2 + 4;
        projection
            .settle(
                &first,
                Some(reference(&first, blob_id)),
                revision,
                Progress::Complete(revision),
            )
            .await?;
        let entry = FrontierEntry {
            revision: revision + 1,
            key: first.clone(),
            value,
        };
        let projected = projection
            .prepare(&docs, &capability, &entry, ReadSource::Frontier)
            .await?;
        assert_eq!(projected, Prepared::Ready(None));
        projection
            .settle(&first, None, revision + 1, Progress::Complete(revision + 1))
            .await?;
        assert_eq!(
            projection.query_digest(&blob_id).await?,
            vec![sibling.clone(), other_doc.clone()]
        );
    }
    assert_eq!(projection.source_revision().await?, 7);
    assert_eq!(
        projection.query_doc(&first.doc_id).await?,
        vec![reference(&sibling, blob_id)]
    );
    Ok(())
}

#[tokio::test]
async fn rollback_and_stale_revisions_cannot_replace_later_rows() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let current = BlobId::new([4; 32]);
    let stale = BlobId::new([5; 32]);
    projection
        .settle(
            &route,
            Some(reference(&route, current)),
            8,
            Progress::Complete(8),
        )
        .await?;
    let before = projection.latest_revision().await?;
    let mut transaction = projection.sql.begin_write().await?;
    assert!(
        projection
            .settle_in_context(
                &mut transaction,
                &route,
                Some(reference(&route, stale)),
                9,
                Progress::Complete(9),
            )
            .await?
    );
    transaction.rollback().await?;
    for revision in [7, 8] {
        projection
            .settle(
                &route,
                Some(reference(&route, stale)),
                revision,
                Progress::Complete(revision),
            )
            .await?;
    }
    assert_eq!(projection.latest_revision().await?, before);
    assert_eq!(projection.source_revision().await?, 8);
    assert_eq!(
        BlobProjection::projected_revision(&projection.sql, &route).await?,
        Some(8)
    );
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, current)]
    );
    assert!(projection.query_digest(&stale).await?.is_empty());
    projection
        .settle(&route, None, 10, Progress::Complete(10))
        .await?;
    projection
        .settle(
            &route,
            Some(reference(&route, stale)),
            9,
            Progress::Complete(9),
        )
        .await?;
    assert!(projection.query_doc(&route.doc_id).await?.is_empty());
    assert_eq!(projection.source_revision().await?, 10);
    Ok(())
}

#[tokio::test]
async fn restart_replays_partial_source_revision_without_losing_routes() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[])?;
    let projection = BlobProjection::new(sql.clone()).await?;
    let first = route(1, "main");
    let second = route(2, "main");
    let blob_id = BlobId::new([6; 32]);
    projection
        .settle(
            &first,
            Some(reference(&first, blob_id)),
            7,
            Progress::Incomplete,
        )
        .await?;
    assert_eq!(projection.source_revision().await?, 0);
    drop(projection);
    let restarted = BlobProjection::new(sql.clone()).await?;
    let (docs, capability) = docs(&first, Err(DocsError::DocNotFound));
    let replayed = restarted
        .prepare(&docs, &capability, &entry(&first, 7), ReadSource::Frontier)
        .await?;
    assert_eq!(replayed, Prepared::Ready(None));
    restarted
        .settle(&first, None, 7, Progress::Incomplete)
        .await?;
    assert_eq!(restarted.source_revision().await?, 0);
    restarted
        .settle(
            &second,
            Some(reference(&second, blob_id)),
            7,
            Progress::Complete(7),
        )
        .await?;
    drop(restarted);
    let restarted = BlobProjection::new(sql).await?;
    assert_eq!(restarted.source_revision().await?, 7);
    assert_eq!(
        restarted.query_digest(&blob_id).await?,
        vec![first.clone(), second.clone()]
    );
    let mut reader = restarted.open(0).await?;
    for (revision, route) in [(1, first), (2, second)] {
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision,
                entries: vec![FrontierEntry {
                    revision,
                    key: route.clone(),
                    value: Some(reference(&route, blob_id)),
                }],
            }
        );
    }
    assert_eq!(
        reader.next(RevisionReadLimits::default()).await?,
        RevisionRead::ReplayComplete { through: 2 }
    );
    Ok(())
}

#[tokio::test]
async fn consumer_parks_unavailable_work_and_retries_it_after_restart() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.ephemeral(&[])?;
    let projection = BlobProjection::new(sql.clone()).await?;
    let first = route(1, "main");
    let second = route(2, "main");
    let previous = BlobId::new([12; 32]);
    let replacement = BlobId::new([13; 32]);
    for (index, route) in [&first, &second].into_iter().enumerate() {
        let revision = u64::try_from(index)? + 1;
        projection
            .settle(
                route,
                Some(reference(route, previous)),
                revision,
                Progress::Complete(revision),
            )
            .await?;
    }
    let reads = [
        RevisionRead::Entries {
            revision: 7,
            entries: vec![
                FrontierEntry {
                    revision: 7,
                    key: first.clone(),
                    value: Some(DatumMembership::Removed {
                        doc_heads: None,
                        datum_heads: Vec::new(),
                        clock: 0,
                    }),
                },
                entry(&second, 7),
            ],
        },
        RevisionRead::Entries {
            revision: 8,
            entries: Vec::new(),
        },
        RevisionRead::ReplayComplete { through: 9 },
    ];
    let unavailable = ScriptedDocs::new(
        &second,
        [
            (
                At::Heads(vec![ChangeHash([9; 32])]),
                Err(DocsError::DocNotFound),
            ),
            (At::Current, Err(DocsError::DocNotFound)),
            (At::Current, Err(DocsError::DocNotFound)),
        ],
    );
    let capability = CapId::from_bytes([7; 16]);
    let (mut reader, shutdown) = ScriptedReader::new(reads);
    projection
        .consume(
            &mut reader,
            &Client::direct(Arc::clone(&unavailable)),
            &capability,
            shutdown,
        )
        .await?;
    assert!(unavailable.responses.lock().unwrap().is_empty());
    assert_eq!(projection.source_revision().await?, 9);
    assert_eq!(projection.parked(None).await?, vec![entry(&second, 7)]);
    assert_eq!(projection.latest_revision().await?, 3);
    assert!(projection.query_doc(&first.doc_id).await?.is_empty());
    assert_eq!(
        projection.query_digest(&previous).await?,
        vec![second.clone()]
    );
    assert_eq!(
        BlobProjection::projected_revision(&sql, &first).await?,
        Some(7)
    );
    assert_eq!(
        BlobProjection::projected_revision(&sql, &second).await?,
        Some(2)
    );
    drop(projection);
    let restarted = BlobProjection::new(sql).await?;
    let available = Client::direct(Arc::new(DocsStub::new(
        &second,
        At::Current,
        Ok(accepted(replacement.digest_str(), Vec::new(), 42)),
    )));
    assert_eq!(restarted.parked(None).await?, vec![entry(&second, 7)]);
    let (mut reader, shutdown) = ScriptedReader::new([RevisionRead::ReplayComplete { through: 9 }]);
    restarted
        .consume(&mut reader, &available, &capability, shutdown)
        .await?;
    assert!(restarted.parked(None).await?.is_empty());
    assert_eq!(restarted.source_revision().await?, 9);
    assert_eq!(restarted.latest_revision().await?, 4);
    assert!(restarted.query_digest(&previous).await?.is_empty());
    assert_eq!(
        restarted.query_digest(&replacement).await?,
        vec![second.clone()]
    );
    let mut frontier = restarted.open(2).await?;
    for (revision, key, value) in [
        (3, first, None),
        (4, second.clone(), Some(reference(&second, replacement))),
    ] {
        assert_eq!(
            frontier.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries {
                revision,
                entries: vec![FrontierEntry {
                    revision,
                    key,
                    value,
                }],
            }
        );
    }
    assert_eq!(
        frontier.next(RevisionReadLimits::default()).await?,
        RevisionRead::ReplayComplete { through: 4 }
    );
    Ok(())
}

#[tokio::test]
async fn rearm_retries_unavailable_work_without_stopping_or_blocking_other_routes() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let blocked = route(1, "main");
    let other = route(2, "main");
    let previous = BlobId::new([17; 32]);
    let replacement = BlobId::new([18; 32]);
    for unavailable in [
        Err(DocsError::DocNotFound),
        Err(DocsError::Denied),
        Ok(DatumRead::Pending {
            reason: "awaiting eligibility".into(),
            version: DatumVersion::default(),
            value: Some(serde_json::json!({ "digest": replacement.digest_str() })),
        }),
    ] {
        let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
        for route in [&blocked, &other] {
            projection
                .settle(
                    route,
                    Some(reference(route, previous)),
                    1,
                    Progress::Complete(1),
                )
                .await?;
        }
        let scripted = ScriptedDocs::new(
            &blocked,
            [
                (At::Heads(vec![ChangeHash([9; 32])]), unavailable),
                (
                    At::Current,
                    Ok(accepted(replacement.digest_str(), Vec::new(), 42)),
                ),
            ],
        );
        let docs = Client::direct(Arc::clone(&scripted));
        let capability = CapId::from_bytes([7; 16]);
        let (mut reader, exhausted) = ScriptedReader::new([RevisionRead::Entries {
            revision: 2,
            entries: vec![
                entry(&blocked, 2),
                FrontierEntry {
                    revision: 2,
                    key: other.clone(),
                    value: None,
                },
            ],
        }]);
        let (stop, stopped) = oneshot::channel();
        let wake = projection.clone();
        let drive = async {
            exhausted.await;
            assert_eq!(projection.source_revision().await?, 2);
            assert_eq!(projection.latest_revision().await?, 3);
            assert_eq!(projection.parked(None).await?, vec![entry(&blocked, 2)]);
            assert_eq!(
                projection.query_digest(&previous).await?,
                vec![blocked.clone()]
            );
            assert!(projection.query_doc(&other.doc_id).await?.is_empty());
            let mut frontier = projection.open(3).await?;
            assert_eq!(
                frontier.next(RevisionReadLimits::default()).await?,
                RevisionRead::ReplayComplete { through: 3 }
            );
            wake.rearm();
            wake.rearm();
            assert_eq!(
                frontier.next(RevisionReadLimits::default()).await?,
                RevisionRead::Entries {
                    revision: 4,
                    entries: vec![FrontierEntry {
                        revision: 4,
                        key: blocked.clone(),
                        value: Some(reference(&blocked, replacement))
                    }],
                }
            );
            assert!(projection.parked(None).await?.is_empty());
            assert_eq!(projection.source_revision().await?, 2);
            assert_eq!(
                projection.query_digest(&replacement).await?,
                vec![blocked.clone()]
            );
            stop.send(()).unwrap();
            Ok::<_, utils_rs::prelude::eyre::Report>(())
        };
        futures::try_join!(
            projection.consume(&mut reader, &docs, &capability, async {
                stopped.await.unwrap()
            }),
            drive,
        )?;
        assert!(scripted.responses.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn a_later_source_revision_retries_parked_current_signed_state() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([19; 32]);
    let scripted = ScriptedDocs::new(
        &route,
        [
            (
                At::Heads(vec![ChangeHash([9; 32])]),
                Err(DocsError::UnknownHeads {
                    heads: vec![ChangeHash([9; 32])],
                }),
            ),
            (
                At::Current,
                Ok(accepted(blob_id.digest_str(), Vec::new(), 42)),
            ),
        ],
    );
    let docs = Client::direct(Arc::clone(&scripted));
    let capability = CapId::from_bytes([7; 16]);
    let (mut reader, shutdown) = ScriptedReader::new([
        RevisionRead::Entries {
            revision: 2,
            entries: vec![entry(&route, 2)],
        },
        RevisionRead::Entries {
            revision: 5,
            entries: Vec::new(),
        },
        RevisionRead::ReplayComplete { through: 5 },
    ]);
    projection
        .consume(&mut reader, &docs, &capability, shutdown)
        .await?;
    assert_eq!(projection.source_revision().await?, 5);
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, blob_id)]
    );
    assert_eq!(
        BlobProjection::projected_revision(&projection.sql, &route).await?,
        Some(2)
    );
    assert!(projection.parked(None).await?.is_empty());
    assert!(scripted.responses.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn newer_parked_settled_and_removed_revisions_outrank_late_results() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let previous = BlobId::new([20; 32]);
    let replacement = BlobId::new([21; 32]);
    projection
        .settle(
            &route,
            Some(reference(&route, previous)),
            1,
            Progress::Complete(1),
        )
        .await?;
    let (docs, capability) = docs(
        &route,
        Ok(accepted(replacement.digest_str(), Vec::new(), 42)),
    );
    let Prepared::Ready(late) = projection
        .prepare(&docs, &capability, &entry(&route, 2), ReadSource::Frontier)
        .await?
    else {
        panic!("available value must settle");
    };
    projection
        .park(&entry(&route, 3), Progress::Complete(3))
        .await?;
    projection
        .settle(&route, late, 2, Progress::Incomplete)
        .await?;
    projection
        .park(&entry(&route, 2), Progress::Incomplete)
        .await?;
    projection
        .settle(&route, None, 2, Progress::Incomplete)
        .await?;
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, previous)]
    );
    assert_eq!(projection.parked(None).await?, vec![entry(&route, 3)]);
    assert_eq!(projection.latest_revision().await?, 1);
    let mut newest = entry(&route, 4);
    newest.value = Some(DatumMembership::Present {
        doc_heads: vec![ChangeHash([22; 32])],
        datum_heads: vec![ChangeHash([23; 32])],
        clock: 4,
    });
    projection.park(&newest, Progress::Complete(4)).await?;
    projection
        .park(&entry(&route, 3), Progress::Incomplete)
        .await?;
    projection
        .settle(
            &route,
            Some(reference(&route, previous)),
            3,
            Progress::Incomplete,
        )
        .await?;
    assert_eq!(projection.parked(None).await?, vec![newest]);
    projection
        .settle(
            &route,
            Some(reference(&route, replacement)),
            4,
            Progress::Incomplete,
        )
        .await?;
    projection
        .park(&entry(&route, 4), Progress::Incomplete)
        .await?;
    assert!(projection.parked(None).await?.is_empty());
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, replacement)]
    );
    projection
        .park(&entry(&route, 5), Progress::Complete(5))
        .await?;
    projection
        .settle(&route, None, 6, Progress::Complete(6))
        .await?;
    projection
        .settle(
            &route,
            Some(reference(&route, previous)),
            5,
            Progress::Incomplete,
        )
        .await?;
    projection
        .park(&entry(&route, 5), Progress::Incomplete)
        .await?;
    assert!(projection.parked(None).await?.is_empty());
    assert!(projection.query_doc(&route.doc_id).await?.is_empty());
    assert_eq!(projection.source_revision().await?, 6);
    assert_eq!(projection.latest_revision().await?, 3);
    Ok(())
}

#[tokio::test]
async fn invalid_docs_requests_fail_without_parking_or_advancing() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    for error in [
        DocsError::InvalidCommand,
        DocsError::Authority(AuthorityServiceError::StateCorrupt(
            "invalid authority state".into(),
        )),
        DocsError::Authority(AuthorityServiceError::Storage(
            "authority storage failed".into(),
        )),
        DocsError::MalformedEvidence {
            reason: "invalid proof".into(),
        },
        DocsError::Unsupported {
            reason: "unsupported proof".into(),
        },
    ] {
        let (docs, capability) = docs(&route, Err(error.clone()));
        let (mut reader, shutdown) = ScriptedReader::new([RevisionRead::Entries {
            revision: 1,
            entries: vec![entry(&route, 1)],
        }]);
        let failure = projection
            .consume(&mut reader, &docs, &capability, shutdown)
            .await
            .unwrap_err();
        assert!(failure.to_string().contains(&error.to_string()));
        assert!(projection.parked(None).await?.is_empty());
        assert!(projection.query_doc(&route.doc_id).await?.is_empty());
        assert_eq!(projection.source_revision().await?, 0);
        assert_eq!(projection.latest_revision().await?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn failed_cursor_write_rolls_back_parking_rows_frontier_and_route_revision() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let previous = BlobId::new([14; 32]);
    let replacement = BlobId::new([15; 32]);
    projection
        .settle(
            &route,
            Some(reference(&route, previous)),
            1,
            Progress::Complete(1),
        )
        .await?;
    projection
        .park(&entry(&route, 2), Progress::Incomplete)
        .await?;
    Sql::new(
        r#"CREATE TRIGGER big_blobs_projection_fail_progress
             BEFORE UPDATE ON big_blobs_projection_progress
           BEGIN
             SELECT RAISE(ABORT, 'injected blob projection failure');
           END"#,
    )
    .execute(&projection.sql)
    .await?;
    let error = projection
        .park(&entry(&route, 3), Progress::Complete(3))
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("injected blob projection failure"));
    assert_eq!(projection.source_revision().await?, 1);
    assert_eq!(projection.parked(None).await?, vec![entry(&route, 2)]);
    let error = projection
        .settle(
            &route,
            Some(reference(&route, replacement)),
            2,
            Progress::Complete(2),
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("injected blob projection failure"));
    assert_eq!(projection.source_revision().await?, 1);
    assert_eq!(projection.parked(None).await?, vec![entry(&route, 2)]);
    assert_eq!(projection.latest_revision().await?, 1);
    assert_eq!(
        BlobProjection::projected_revision(&projection.sql, &route).await?,
        Some(1)
    );
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, previous)]
    );
    assert_eq!(
        projection.query_digest(&previous).await?,
        vec![route.clone()]
    );
    assert!(projection.query_digest(&replacement).await?.is_empty());
    Sql::new("DROP TRIGGER big_blobs_projection_fail_progress")
        .execute(&projection.sql)
        .await?;
    projection
        .settle(
            &route,
            Some(reference(&route, replacement)),
            2,
            Progress::Complete(2),
        )
        .await?;
    assert_eq!(projection.source_revision().await?, 2);
    assert!(projection.parked(None).await?.is_empty());
    assert_eq!(projection.latest_revision().await?, 2);
    assert!(projection.query_digest(&previous).await?.is_empty());
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, replacement)]
    );
    let mut frontier = projection.open(1).await?;
    assert_eq!(
        frontier.next(RevisionReadLimits::default()).await?,
        RevisionRead::Entries {
            revision: 2,
            entries: vec![FrontierEntry {
                revision: 2,
                key: route.clone(),
                value: Some(reference(&route, replacement)),
            }],
        }
    );
    Ok(())
}

#[tokio::test]
async fn failed_parked_retry_rolls_back_rows_frontier_revision_and_parked_deletion() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let previous = BlobId::new([24; 32]);
    let replacement = BlobId::new([25; 32]);
    projection
        .settle(
            &route,
            Some(reference(&route, previous)),
            1,
            Progress::Complete(1),
        )
        .await?;
    projection
        .park(&entry(&route, 2), Progress::Complete(2))
        .await?;
    let docs = Client::direct(Arc::new(DocsStub::new(
        &route,
        At::Current,
        Ok(accepted(replacement.digest_str(), Vec::new(), 42)),
    )));
    let capability = CapId::from_bytes([7; 16]);
    let mut shutdown = std::future::pending();
    let mut frontier = projection.open(1).await?;
    assert_eq!(
        frontier.next(RevisionReadLimits::default()).await?,
        RevisionRead::ReplayComplete { through: 1 }
    );
    let mut waiting = Box::pin(frontier.next(RevisionReadLimits::default()));
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    Sql::new(
        r#"CREATE TRIGGER big_blobs_projection_fail_frontier
             BEFORE INSERT ON generic_frontier_entries
             WHEN NEW.frontier_id = 'big-blobs-projection-v1'
           BEGIN
             SELECT RAISE(ABORT, 'injected blob frontier failure');
           END"#,
    )
    .execute(&projection.sql)
    .await?;
    let error = projection
        .retry_parked(&docs, &capability, None, &mut shutdown)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("injected blob frontier failure"));
    assert_eq!(projection.source_revision().await?, 2);
    assert_eq!(projection.latest_revision().await?, 1);
    assert_eq!(projection.parked(None).await?, vec![entry(&route, 2)]);
    assert_eq!(
        BlobProjection::projected_revision(&projection.sql, &route).await?,
        Some(1)
    );
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, previous)]
    );
    assert_eq!(
        projection.query_digest(&previous).await?,
        vec![route.clone()]
    );
    assert!(projection.query_digest(&replacement).await?.is_empty());
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    Sql::new("DROP TRIGGER big_blobs_projection_fail_frontier")
        .execute(&projection.sql)
        .await?;
    assert!(
        projection
            .retry_parked(&docs, &capability, None, &mut shutdown)
            .await?
            .is_continue()
    );
    assert_eq!(
        waiting.await?,
        RevisionRead::Entries {
            revision: 2,
            entries: vec![FrontierEntry {
                revision: 2,
                key: route.clone(),
                value: Some(reference(&route, replacement)),
            }],
        }
    );
    assert_eq!(projection.source_revision().await?, 2);
    assert_eq!(projection.latest_revision().await?, 2);
    assert!(projection.parked(None).await?.is_empty());
    assert!(projection.query_digest(&previous).await?.is_empty());
    assert_eq!(projection.query_digest(&replacement).await?, vec![route]);
    Ok(())
}

#[tokio::test]
async fn cancellation_during_typed_read_leaves_projection_unchanged() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([16; 32]);
    projection
        .settle(
            &route,
            Some(reference(&route, blob_id)),
            1,
            Progress::Complete(1),
        )
        .await?;
    let started = Arc::new(Notify::new());
    let docs = Client::direct(Arc::new(BlockingDocs {
        started: Arc::clone(&started),
    }));
    let capability = CapId::from_bytes([7; 16]);
    let (mut reader, _exhausted) = ScriptedReader::new([RevisionRead::Entries {
        revision: 2,
        entries: vec![entry(&route, 2)],
    }]);
    projection
        .consume(&mut reader, &docs, &capability, started.notified())
        .await?;
    assert_eq!(projection.source_revision().await?, 1);
    assert_eq!(projection.latest_revision().await?, 1);
    assert_eq!(
        BlobProjection::projected_revision(&projection.sql, &route).await?,
        Some(1)
    );
    assert_eq!(
        projection.query_doc(&route.doc_id).await?,
        vec![reference(&route, blob_id)]
    );
    assert_eq!(projection.query_digest(&blob_id).await?, vec![route]);
    Ok(())
}

#[tokio::test]
async fn frontier_handoff_observes_commits_before_and_during_wait() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([7; 32]);
    let mut reader = projection.open(0).await?;
    assert_eq!(
        reader.next(RevisionReadLimits::default()).await?,
        RevisionRead::ReplayComplete { through: 0 }
    );
    projection
        .settle(
            &route,
            Some(reference(&route, blob_id)),
            1,
            Progress::Complete(1),
        )
        .await?;
    assert_eq!(
        reader.next(RevisionReadLimits::default()).await?,
        RevisionRead::Entries {
            revision: 1,
            entries: vec![FrontierEntry {
                revision: 1,
                key: route.clone(),
                value: Some(reference(&route, blob_id)),
            }],
        }
    );
    let mut waiting = Box::pin(reader.next(RevisionReadLimits::default()));
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    projection
        .settle(&route, None, 2, Progress::Complete(2))
        .await?;
    assert_eq!(
        waiting.await?,
        RevisionRead::Entries {
            revision: 2,
            entries: vec![FrontierEntry {
                revision: 2,
                key: route,
                value: None,
            }],
        }
    );
    Ok(())
}

#[tokio::test]
async fn typed_reads_use_worker_capability_and_exact_membership_heads() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([8; 32]);
    let (docs, capability) = docs(&route, Ok(accepted(blob_id.digest_str(), Vec::new(), 42)));
    assert_eq!(
        projection
            .prepare(&docs, &capability, &entry(&route, 1), ReadSource::Frontier)
            .await?,
        Prepared::Ready(Some(reference(&route, blob_id)))
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_and_pending_reads_park_without_replacing_prior_rows() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([9; 32]);
    projection
        .settle(
            &route,
            Some(reference(&route, blob_id)),
            1,
            Progress::Complete(1),
        )
        .await?;
    for response in [
        Err(DocsError::DocNotFound),
        Err(DocsError::UnknownHeads {
            heads: vec![ChangeHash([9; 32])],
        }),
        Err(DocsError::Repository("unavailable".into())),
        Err(DocsError::Denied),
        Ok(DatumRead::Pending {
            reason: "awaiting claim".into(),
            version: DatumVersion::default(),
            value: Some(serde_json::json!({ "digest": BlobId::new([10; 32]).digest_str() })),
        }),
    ] {
        let (docs, capability) = docs(&route, response);
        assert_eq!(
            projection
                .prepare(&docs, &capability, &entry(&route, 2), ReadSource::Frontier)
                .await?,
            Prepared::Parked
        );
        projection
            .park(&entry(&route, 2), Progress::Complete(2))
            .await?;
        assert_eq!(projection.parked(None).await?, vec![entry(&route, 2)]);
        assert_eq!(projection.source_revision().await?, 2);
        assert_eq!(projection.latest_revision().await?, 1);
        assert_eq!(
            projection.query_doc(&route.doc_id).await?,
            vec![reference(&route, blob_id)]
        );
    }
    Ok(())
}

#[tokio::test]
async fn definitive_nonvalues_delete_the_exact_route() -> Res<()> {
    let runtime = SqliteRuntime::new(Default::default());
    let projection = BlobProjection::new(runtime.ephemeral(&[])?).await?;
    let route = route(1, "main");
    let blob_id = BlobId::new([10; 32]);
    for (index, datum) in [
        DatumRead::Absent {
            version: DatumVersion::default(),
        },
        DatumRead::Rejected {
            reason: "bad signature".into(),
        },
        DatumRead::Undecodable {
            reason: "wrong shape".into(),
            version: DatumVersion::default(),
        },
        DatumRead::Accepted {
            value: serde_json::json!({ "digest": blob_id.digest_str() }),
            version: DatumVersion::default(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let revision = u64::try_from(index)? * 2 + 1;
        projection
            .settle(
                &route,
                Some(reference(&route, blob_id)),
                revision,
                Progress::Complete(revision),
            )
            .await?;
        let (docs, capability) = docs(&route, Ok(datum));
        let projected = projection
            .prepare(
                &docs,
                &capability,
                &entry(&route, revision + 1),
                ReadSource::Frontier,
            )
            .await?;
        assert_eq!(projected, Prepared::Ready(None));
        projection
            .settle(&route, None, revision + 1, Progress::Complete(revision + 1))
            .await?;
        assert!(projection.query_digest(&blob_id).await?.is_empty());
    }
    Ok(())
}
