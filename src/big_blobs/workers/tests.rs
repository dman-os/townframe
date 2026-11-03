use super::*;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsMessage;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::query::Anchor;
use futures::future::BoxFuture;
use service_utils_rs::InlineRoute;
use service_utils_rs::Spawner;
use sqlite_rs::SqliteRuntime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

struct PollCall {
    request: PollDatums,
    reply: oneshot::Sender<Result<DatumPage, DocsError>>,
}

struct DocsStub(mpsc::Sender<PollCall>);

impl InlineRoute<DocsProtocol> for DocsStub {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let DocsMessage::PollDatums(message) = message else {
                panic!("poll workers only issue PollDatums");
            };
            let (reply, response) = oneshot::channel();
            self.0
                .send(PollCall {
                    request: message.inner.params,
                    reply,
                })
                .await
                .unwrap();
            message
                .tx
                .send(response.await.unwrap().map(Into::into))
                .await
                .unwrap();
        })
    }
}

struct MintCall {
    document: DocId,
    write: bool,
    reply: oneshot::Sender<Result<CapId, ClientError<DocsError>>>,
}

struct Polls {
    queue: WorkQueue,
    docs: Client<DocsProtocol>,
    requests: mpsc::Receiver<PollCall>,
    mints: mpsc::UnboundedReceiver<MintCall>,
    minted: Vec<(DocId, bool)>,
    metadata: MetadataCapabilities,
    policy: watch::Sender<u64>,
    tasks: Spawner,
    stop: StopToken,
}

impl Polls {
    async fn new(sql: Database) -> Res<Self> {
        let (send, requests) = mpsc::channel(8);
        let (policy, revision) = watch::channel(0);
        let (mint, mints) = mpsc::unbounded_channel();
        let metadata = MetadataCapabilities::new(
            Arc::new(move |scope| {
                let document = scope
                    .read
                    .iter()
                    .next()
                    .expect("metadata poll names one document")
                    .clone();
                assert_eq!(scope, metadata_scope(document.clone(), false));
                let (reply, response) = oneshot::channel();
                mint.send(MintCall {
                    document,
                    write: false,
                    reply,
                })
                .unwrap();
                Box::pin(async move { response.await.unwrap() })
            }),
            revision,
        );
        let (stop, tasks) = StopToken::new();
        Ok(Self {
            queue: WorkQueue::new(sql).await?,
            docs: Client::direct(Arc::new(DocsStub(send))),
            requests,
            mints,
            minted: Vec::new(),
            metadata,
            policy,
            tasks,
            stop,
        })
    }

    async fn next(&mut self) -> PollCall {
        loop {
            tokio::select! {
                call = self.requests.recv() => return call.unwrap(),
                mint = self.mints.recv() => {
                    let mint = mint.unwrap();
                    assert!(!mint.write);
                    mint.reply.send(Ok(metadata_capability(&mint.document))).unwrap();
                    self.minted.push((mint.document, mint.write));
                }
            }
        }
    }

    fn start(&self, document: DocId, content: bool) {
        let docs = self.docs.clone();
        let queue = self.queue.clone();
        let policy = self.policy.subscribe();
        let metadata = self.metadata.clone();
        let cancel = self.tasks.clone();
        self.tasks
            .spawn(async move {
                let capability = CapId::from_bytes([7; 16]);
                if content {
                    poll_drawer(docs, capability, queue, document, cancel.cancelled())
                        .await
                        .unwrap();
                } else {
                    poll_document(docs, metadata, queue, document, policy, cancel.cancelled())
                        .await
                        .unwrap();
                }
            })
            .unwrap();
    }
}

fn metadata_scope(document: DocId, write: bool) -> DocScope {
    DocScope {
        read: BTreeSet::from([document.clone()]),
        write: if write {
            BTreeSet::from([document])
        } else {
            BTreeSet::new()
        },
        ..Default::default()
    }
}

fn metadata_capability(document: &DocId) -> CapId {
    let hash = blake3::hash(document.to_string().as_bytes());
    CapId::from_bytes(hash.as_bytes()[..16].try_into().unwrap())
}

fn assert_request(request: &PollDatums, document: &DocId, content: bool, after: u64) {
    assert_eq!(
        request.capability,
        if content {
            CapId::from_bytes([7; 16])
        } else {
            metadata_capability(document)
        }
    );
    assert_eq!(request.after, after);
    if content {
        assert_eq!(
            request.predicate.anchors().unwrap(),
            vec![Anchor::Drawer {
                drawer_id: document.clone(),
                at_least: DrawerAccess::Read,
            }]
        );
        assert_eq!(
            request.predicate,
            Predicate::And {
                clauses: vec![
                    Predicate::InDrawer {
                        drawer_id: document.clone(),
                        transitive: false,
                        at_least: DrawerAccess::Read,
                    },
                    Predicate::Or {
                        clauses: blob_tags()
                            .into_iter()
                            .map(|tag| Predicate::HasTag { tag })
                            .collect(),
                    },
                ],
            }
        );
        assert_eq!(
            request.datums,
            DatumSelection::Of {
                tags: blob_tags(),
                keys: BTreeSet::new(),
            }
        );
    } else {
        assert_eq!(
            request.predicate.anchors().unwrap(),
            vec![Anchor::Doc {
                doc_id: document.clone(),
            }]
        );
        assert_eq!(
            request.predicate,
            Predicate::IsDoc {
                doc_id: document.clone(),
            }
        );
        assert_eq!(request.datums, DatumSelection::All);
    }
}

#[tokio::test]
async fn metadata_capabilities_cache_exact_scopes_and_refresh_after_denial_or_policy_change()
-> Res<()> {
    let scopes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let minted = Arc::clone(&scopes);
    let (policy, revision) = watch::channel(0);
    let mut metadata = MetadataCapabilities::new(
        Arc::new(move |scope| {
            let mut minted = minted.lock().unwrap();
            minted.push(scope);
            let capability = CapId::from_bytes((minted.len() as u128).to_be_bytes());
            Box::pin(async move { Ok(capability) })
        }),
        revision,
    );
    let document = DocId::new([3; 32]);
    let read = metadata
        .call(&document, false, |capability| async move { Ok(capability) })
        .await?;
    assert_eq!(
        metadata
            .call(&document, false, |capability| async move { Ok(capability) })
            .await?,
        read
    );
    let write = metadata
        .call(&document, true, |capability| async move { Ok(capability) })
        .await?;
    assert_ne!(read, write);
    let denied: Result<(), _> = metadata
        .call(&document, false, |capability| async move {
            assert_eq!(capability, read);
            Err(ClientError::Operation(DocsError::Denied))
        })
        .await;
    assert!(matches!(
        denied,
        Err(ClientError::Operation(DocsError::Denied))
    ));
    assert_ne!(
        metadata
            .call(&document, false, |capability| async move { Ok(capability) })
            .await?,
        read
    );
    policy.send_modify(|revision| *revision += 1);
    assert_ne!(
        metadata
            .call(&document, true, |capability| async move { Ok(capability) })
            .await?,
        write
    );
    assert_eq!(
        *scopes.lock().unwrap(),
        vec![
            metadata_scope(document.clone(), false),
            metadata_scope(document.clone(), true),
            metadata_scope(document.clone(), false),
            metadata_scope(document, true),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn content_roots_and_inventory_polls_have_independent_durable_cursors() -> Res<()> {
    let sql = SqliteRuntime::new(Default::default()).ephemeral(&[])?;
    let mut polls = Polls::new(sql.clone()).await?;
    let documents: Vec<_> = (1..=6).map(|byte| DocId::new([byte; 32])).collect();
    let cases = [
        (documents[0].clone(), true),
        (documents[0].clone(), false),
        (documents[1].clone(), false),
        (documents[2].clone(), false),
    ];
    let mut held = Vec::new();
    let mut progress = BTreeMap::new();
    for (index, (document, content)) in cases.iter().enumerate() {
        let source = if *content {
            format!("poll:{document}")
        } else {
            format!("poll-document:{document}")
        };
        let after = (index as u64 + 1) * 10;
        let through = after + 3;
        polls
            .queue
            .ingest(&source, after, BTreeSet::new(), false)
            .await?;
        polls.start(document.clone(), *content);
        let call = polls.next().await;
        assert_request(&call.request, document, *content, after);
        let events = if *content {
            vec![
                DatumEvent::Enter {
                    revision: after + 1,
                    doc_id: documents[3].clone(),
                    datums: BTreeMap::new(),
                },
                DatumEvent::Datum {
                    revision: after + 2,
                    doc_id: documents[4].clone(),
                    datum_key: DatumKey::from_text(Jwk::ID, "main")?,
                    datum: ObservedDatum::Present {
                        doc_heads: Vec::new(),
                        version: DatumVersion::default(),
                    },
                },
                DatumEvent::Leave {
                    revision: through,
                    doc_id: documents[5].clone(),
                },
            ]
        } else {
            vec![DatumEvent::Enter {
                revision: through,
                doc_id: document.clone(),
                datums: BTreeMap::new(),
            }]
        };
        call.reply
            .send(Ok(DatumPage {
                events,
                through,
                caught_up: true,
            }))
            .unwrap();
        let call = polls.next().await;
        assert_request(&call.request, document, *content, through);
        held.push(call);
        progress.insert(source, through);
    }
    assert_eq!(polls.minted.len(), 3);
    polls.stop.stop().await;
    drop(held);
    let (_, _, path) = sql
        .query("PRAGMA database_list", ())
        .await?
        .one::<(i64, String, String)>()?;
    drop(polls.queue);
    let runtime = SqliteRuntime::new(Default::default());
    let mut resumed = Polls::new(runtime.database(path, &[], "_reopened_polls")).await?;
    let mut pending = BTreeSet::new();
    while let Some((document, generation)) = resumed.queue.next().await? {
        assert!(pending.insert(document.clone()));
        resumed.queue.finish(&document, generation, false).await?;
    }
    assert_eq!(pending, documents.into_iter().collect());
    for (source, through) in &progress {
        assert_eq!(resumed.queue.progress(source).await?, *through);
    }
    let mut held = Vec::new();
    for (document, content) in cases {
        resumed.start(document.clone(), content);
        let call = resumed.next().await;
        let source = if content {
            format!("poll:{document}")
        } else {
            format!("poll-document:{document}")
        };
        assert_request(&call.request, &document, content, progress[&source]);
        held.push(call);
    }
    resumed.stop.stop().await;
    drop(held);
    Ok(())
}

#[tokio::test]
async fn uncertain_document_polls_retry_without_advancing_or_losing_work() -> Res<()> {
    let sql = SqliteRuntime::new(Default::default()).ephemeral(&[])?;
    let mut polls = Polls::new(sql).await?;
    let document = DocId::new([1; 32]);
    let source = format!("poll-document:{document}");
    polls
        .queue
        .ingest(&source, 17, BTreeSet::new(), false)
        .await?;
    polls.start(document.clone(), false);
    let mut call = polls.next().await;
    for error in [
        DocsError::Denied,
        DocsError::DocNotFound,
        DocsError::Pending {
            reason: "awaiting signatures".into(),
        },
        DocsError::AuthorityUnavailable("awaiting authority".into()),
        DocsError::Access("awaiting document access".into()),
    ] {
        assert_request(&call.request, &document, false, 17);
        call.reply.send(Err(error)).unwrap();
        polls.policy.send_modify(|revision| *revision += 1);
        call = polls.next().await;
        assert_eq!(polls.queue.progress(&source).await?, 17);
        assert!(polls.queue.next().await?.is_none());
    }
    call.reply
        .send(Ok(DatumPage {
            events: vec![DatumEvent::Leave {
                revision: 23,
                doc_id: document.clone(),
            }],
            through: 23,
            caught_up: true,
        }))
        .unwrap();
    let call = polls.next().await;
    assert_request(&call.request, &document, false, 23);
    let (queued, generation) = polls.queue.next().await?.unwrap();
    assert_eq!(queued, document);
    polls.queue.finish(&queued, generation, false).await?;
    call.reply
        .send(Ok(DatumPage {
            events: Vec::new(),
            through: 24,
            caught_up: true,
        }))
        .unwrap();
    let held = polls.next().await;
    assert_request(&held.request, &document, false, 24);
    assert_eq!(polls.queue.progress(&source).await?, 24);
    assert!(polls.queue.next().await?.is_none());
    assert_eq!(polls.minted.len(), 6);
    polls.stop.stop().await;
    drop(held);
    Ok(())
}

#[tokio::test]
async fn refused_metadata_mint_preserves_cursor_and_retries_on_policy_change() -> Res<()> {
    let sql = SqliteRuntime::new(Default::default()).ephemeral(&[])?;
    let mut polls = Polls::new(sql).await?;
    let document = DocId::new([2; 32]);
    let source = format!("poll-document:{document}");
    polls
        .queue
        .ingest(&source, 31, BTreeSet::new(), false)
        .await?;
    polls.start(document.clone(), false);
    let mint = polls.mints.recv().await.unwrap();
    assert_eq!(mint.document, document);
    assert!(!mint.write);
    mint.reply
        .send(Err(ClientError::Operation(DocsError::Access(
            "read grant unavailable".into(),
        ))))
        .unwrap();
    polls.policy.send_modify(|revision| *revision += 1);
    let held = polls.next().await;
    assert_request(&held.request, &document, false, 31);
    assert_eq!(polls.queue.progress(&source).await?, 31);
    assert!(polls.queue.next().await?.is_none());
    assert_eq!(polls.minted.len(), 1);
    polls.stop.stop().await;
    drop(held);
    Ok(())
}
