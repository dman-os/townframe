use super::*;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsMessage;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_cabinet_core::query::Anchor;
use futures::future::BoxFuture;
use service_client_native_rs::InlineRoute;
use service_client_native_rs::Transport;
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
            docs: Client::new(Transport::direct(Arc::new(DocsStub(send)))),
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
            std::collections::BTreeMap::from([(document, big_cabinet_core::doc::DatumKinds::All)])
        } else {
            std::collections::BTreeMap::new()
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
            Err(ClientError::Method(DocsError::Denied))
        })
        .await;
    assert!(matches!(
        denied,
        Err(ClientError::Method(DocsError::Denied))
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
                DatumEvent::Datum {
                    revision: after + 1,
                    doc_id: documents[3].clone(),
                    datum_key: DatumKey::from_text(Jwk::ID, "main")?,
                    datum: ObservedDatum::Present {
                        doc_heads: Vec::new(),
                        version: DatumVersion::default(),
                    },
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
            vec![DatumEvent::Datum {
                revision: through,
                doc_id: document.clone(),
                datum_key: DatumKey::from_text(Drawer::ID, "main")?,
                datum: ObservedDatum::Present {
                    doc_heads: Vec::new(),
                    version: DatumVersion::default(),
                },
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
        .send(Err(ClientError::Method(DocsError::Access(
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

/// A roster write on a drawer doc names the one member whose membership moved, so its poll
/// enqueues that member alone. Enqueueing the drawer instead fanned out to every member
/// (`enqueue_root_members`), and each note created re-reconciled every note already in the
/// drawer. A page carries only the rows that changed, so each `drawerDocEntry` row enqueues the
/// member it names and any row no entry localizes (here the descriptor) enqueues the drawer
/// itself. A parked drawer stays parked through member-only pages, so its next rearm reaches
/// everyone.
#[tokio::test]
async fn roster_entries_on_a_polled_drawer_enqueue_only_the_member_they_name() -> Res<()> {
    let sql = SqliteRuntime::new(Default::default()).ephemeral(&[])?;
    let mut polls = Polls::new(sql).await?;
    let drawer = DocId::new([1; 32]);
    let settled = DocId::new([2; 32]);
    let entered = DocId::new([3; 32]);
    let descriptor = DatumKey::from_text(Drawer::ID, "main")?;
    let entry = |doc: &DocId| DatumKey::from_text(DrawerDocEntry::ID, doc.to_string());
    let present = ObservedDatum::Present {
        doc_heads: Vec::new(),
        version: DatumVersion::default(),
    };
    let removed = ObservedDatum::Removed {
        doc_heads: None,
        version: DatumVersion::default(),
    };
    let cases = [
        // The first rows of a fresh task: the descriptor stales the drawer, the entry its member.
        (
            vec![
                (descriptor.clone(), present.clone()),
                (entry(&settled)?, present.clone()),
            ],
            BTreeSet::from([drawer.clone(), settled.clone()]),
        ),
        // A doc enters: only it, not the settled member nor the parked drawer.
        (
            vec![(entry(&entered)?, present.clone())],
            BTreeSet::from([entered.clone()]),
        ),
        // A doc leaves: only it.
        (
            vec![(entry(&settled)?, removed)],
            BTreeSet::from([settled.clone()]),
        ),
        // The descriptor moves every member's target: the drawer, to fan out.
        (
            vec![(descriptor.clone(), present)],
            BTreeSet::from([drawer.clone()]),
        ),
    ];
    polls.start(drawer.clone(), false);
    let mut call = polls.next().await;
    for (through, (rows, stale)) in (1..).zip(cases) {
        assert_request(&call.request, &drawer, false, through - 1);
        call.reply
            .send(Ok(DatumPage {
                events: rows
                    .into_iter()
                    .map(|(datum_key, datum)| DatumEvent::Datum {
                        revision: through,
                        doc_id: drawer.clone(),
                        datum_key,
                        datum,
                    })
                    .collect(),
                through,
                caught_up: true,
            }))
            .unwrap();
        call = polls.next().await;
        let mut queued_docs = BTreeSet::new();
        while let Some((queued, generation)) = polls.queue.next().await? {
            polls
                .queue
                .finish(&queued, generation, queued == drawer)
                .await?;
            queued_docs.insert(queued);
        }
        assert_eq!(queued_docs, stale);
    }
    polls
        .queue
        .ingest("availability", 1, BTreeSet::new(), true)
        .await?;
    assert_eq!(polls.queue.next().await?.unwrap().0, drawer);
    polls.stop.stop().await;
    drop(call);
    Ok(())
}

/// Serves `ReadDatums` for the member doc: `Denied` until `arrived` is set (the roster names the
/// member before its content syncs in), then an empty datum set. `denied` and `served` are
/// notified for each read of the matching outcome.
struct MemberDocs {
    arrived: std::sync::atomic::AtomicBool,
    denied: tokio::sync::Notify,
    served: tokio::sync::Notify,
}

impl InlineRoute<DocsProtocol> for MemberDocs {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let DocsMessage::ReadDatums(message) = message else {
                panic!("a member doc without blobs is only read");
            };
            let reply = if self.arrived.load(std::sync::atomic::Ordering::SeqCst) {
                Ok(big_cabinet_core::doc::DatumsRead {
                    heads: Vec::new(),
                    datums: BTreeMap::new(),
                })
            } else {
                Err(DocsError::Denied)
            };
            let served = reply.is_ok();
            message.tx.send(reply.map(Into::into)).await.unwrap();
            if served {
                self.served.notify_one();
            } else {
                self.denied.notify_one();
            }
        })
    }
}

/// A drawer roster names a member doc whose content has not synced in: the worker reads it
/// `Denied` and parks it. Nothing the roster poll or the policy stream publishes changes when
/// the content lands, only the repo's Automerge frontier does, so the worker must wake the
/// parked member from that stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_parked_before_its_content_arrives_is_read_again_when_it_materializes() -> Res<()>
{
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (repo, repo_stop) = big_repo::BigRepo::boot(big_repo::Config {
        node_identity_seed: [126; 32],
        storage: big_repo::Storage::memory(&runtime).unwrap(),
        scope_key: "workers-materialization-wake".into(),
        hidden_parts: Default::default(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
    })
    .await?;
    let sql = runtime.ephemeral(&[])?;
    // The worker consults the drawer tables to tell a root drawer from a member doc.
    sql.apply_migrations(
        "test_drawers",
        &[
            sqlite_rs::Migration {
                version: 1,
                name: "drawers",
                sql: include_str!("../../big_cabinet/drawer/sqlite/migrations/001_drawers.sql"),
            },
            sqlite_rs::Migration {
                version: 2,
                name: "metadata",
                sql: include_str!(
                    "../../big_cabinet/drawer/sqlite/migrations/003_meta_doc_roots.sql"
                ),
            },
        ],
    )
    .await?;
    let (blobs, blobs_stop) = crate::boot(crate::Config {
        storage: crate::Storage::memory().unwrap(),
        sql: sql.clone(),
        authority: Client::new(Transport::direct(Arc::new(UnusedAuthority))),
        operator: big_cabinet_core::dmeta::Uuid7::new(),
        namespace: big_cabinet_core::authority::Namespace {
            namespace_id: big_cabinet_core::dmeta::Uuid7::new(),
            capability: CapId::from_bytes([127; 16]),
        },
    })
    .await?;
    let docs_stub = Arc::new(MemberDocs {
        arrived: false.into(),
        denied: tokio::sync::Notify::new(),
        served: tokio::sync::Notify::new(),
    });
    let docs = Client::new(Transport::<DocsProtocol>::direct(Arc::clone(&docs_stub)));
    let parts: SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
    let queue = WorkQueue::new(sql.clone()).await?;
    sql.apply_migrations(
        sqlite::queries::MIGRATION_TABLE,
        sqlite::queries::MIGRATIONS,
    )
    .await?;
    let (policy, policy_rx) = watch::channel(0);
    let metadata = MetadataCapabilities::new(
        Arc::new(|_| panic!("a member doc is read with the worker's own capability")),
        policy_rx.clone(),
    );
    let retention = Retention::new(
        sql.clone(),
        Arc::clone(&blobs.service),
        docs.clone(),
        metadata.clone(),
        Arc::new(|_| Box::pin(async { Ok(false) })),
        Arc::clone(&parts),
    )
    .await?;
    let capability = CapId::from_bytes([7; 16]);
    let adapter = DocumentBlobs::new(
        docs.clone(),
        capability,
        blobs.service.store().clone(),
        Arc::clone(&blobs.service.provider),
        blobs.service.roots.clone(),
    );
    let (mirrors, mirrors_stop) = crate::admission::InventoryMirrors::boot(
        sql.clone(),
        Arc::clone(&blobs.service),
        Arc::clone(&parts),
        Arc::clone(&repo),
        queue.clone(),
    )
    .await?;
    let projection = Arc::new(BlobProjection::new(sql.clone()).await?);
    let (stop, tasks) = StopToken::new();
    spawn_worker(
        &tasks,
        WorkerParts {
            repo: Arc::clone(&repo),
            sql: sql.clone(),
            service: Arc::clone(&blobs.service),
            docs,
            capability,
            metadata,
            mirrors,
            observe_metadata: Arc::new(|_| panic!("the member is not a root drawer")),
            policy: policy_rx,
        },
        queue.clone(),
        retention,
        adapter,
        projection,
    );

    // The member doc exists in the repo (the roster names it) but its datums are not readable.
    let mut member = automerge::Automerge::new();
    let mut transaction = member.transaction();
    automerge::transaction::Transactable::put(&mut transaction, automerge::ROOT, "seed", true)?;
    transaction.commit();
    let handle = repo.create_doc(member).await?;
    let member = big_repo::automerge_doc_obj_id(handle.document_id());
    queue.enqueue(&member).await?;

    // Only once the worker has been refused the read does the content land, so the member is
    // parked (or about to be) and cannot read the content on its first pass.
    docs_stub.denied.notified().await;
    // The content lands: the stub starts serving, and the doc changes in the repo, which is
    // what publishes a frontier revision for it.
    docs_stub
        .arrived
        .store(true, std::sync::atomic::Ordering::SeqCst);
    handle
        .with_document(|doc| {
            let mut transaction = doc.transaction();
            automerge::transaction::Transactable::put(
                &mut transaction,
                automerge::ROOT,
                "content",
                true,
            )
            .unwrap();
            transaction.commit();
        })
        .await?;
    docs_stub.served.notified().await;

    stop.stop().await;
    mirrors_stop.stop().await;
    blobs_stop.stop().await;
    repo_stop.stop().await?;
    drop(policy);
    Ok(())
}

struct UnusedAuthority;

impl InlineRoute<big_cabinet_core::authority::AuthorityProtocol> for UnusedAuthority {
    fn route_inline(
        self: Arc<Self>,
        _: big_cabinet_core::authority::AuthorityMessage,
    ) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("the worker test does not mint client capabilities") })
    }
}
