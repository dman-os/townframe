use super::WcmpPackage;
use super::WcmpRuntime;
use clay_collection::WriteBasis;
use clay_guest_session::Limits as DocumentLimits;
use clay_guest_session::capabilities::RowAttachment;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_rt_wcmp::InitialContext;
use clay_rt_wcmp::WcmpPlacement;
use clay_rt_wcmp::test_utils::WcmpStoreState;
use clay_rt_wcmp::test_utils::convert_mutation;
use clay_rt_wcmp::test_utils::resolve_component;
use clay_rt_wcmp::test_utils::wit_presentation;
use clay_surface::Event;
use clay_surface::EventResponse;
use clay_surface::Mutation;
use clay_surface::NodeId;
use clay_surface::PresentationError;
use clay_surface::SurfaceSink;
use clay_surface::SurfaceTree;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use wcmp::Resource;

pub(super) struct RecordingSink {
    tree: Mutex<Option<SurfaceTree<ComponentRef>>>,
    changed: tokio::sync::Notify,
}

impl RecordingSink {
    pub(super) fn new() -> Self {
        Self {
            tree: Mutex::new(Some(SurfaceTree::new())),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn tree<R>(&self, read: impl FnOnce(&SurfaceTree<ComponentRef>) -> R) -> R {
        read(self.tree.lock().unwrap().as_ref().expect("surface is live"))
    }

    async fn wait_for(&self, predicate: impl Fn(&str) -> bool) -> String {
        loop {
            let changed = self.changed.notified();
            let text = self.tree(SurfaceTree::text_content);
            if predicate(&text) {
                return text;
            }
            changed.await;
        }
    }
}

impl SurfaceSink<ComponentRef> for RecordingSink {
    fn is_live(&self) -> bool {
        self.tree.lock().unwrap().is_some()
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        let result = self
            .tree
            .lock()
            .unwrap()
            .as_mut()
            .ok_or(PresentationError::Revoked)?
            .apply(edits);
        self.changed.notify_waiters();
        result
    }

    fn withdraw(&self) {
        self.tree.lock().unwrap().take();
        self.changed.notify_waiters();
    }
}

fn mouse_data_json() -> String {
    serde_json::json!({
        "alt_key": false,
        "button": 0,
        "buttons": 0,
        "client_x": 0.0,
        "client_y": 0.0,
        "ctrl_key": false,
        "meta_key": false,
        "offset_x": 0.0,
        "offset_y": 0.0,
        "page_x": 0.0,
        "page_y": 0.0,
        "screen_x": 0.0,
        "screen_y": 0.0,
        "shift_key": false,
    })
    .to_string()
}

/// The counter publishes the presentations `view` and `badge` and the call `count`, which its
/// manifest has to declare.
fn counter_context() -> InitialContext {
    use clay_collection::Nsid;
    use clay_guest_session::capabilities::DeclaredPublication;
    use clay_guest_session::capabilities::PublicationKind;
    use clay_guest_session::capabilities::presentation_component;
    let presentation = || DeclaredPublication {
        kind: PublicationKind::Presentation,
        contract: presentation_component(),
    };
    InitialContext {
        publications: std::collections::BTreeMap::from([
            ("view".to_owned(), presentation()),
            ("badge".to_owned(), presentation()),
            (
                "count".to_owned(),
                DeclaredPublication {
                    kind: PublicationKind::Call,
                    contract: Nsid::literal("org.example.dayframe.counter.count"),
                },
            ),
            (
                "increment".to_owned(),
                DeclaredPublication {
                    kind: PublicationKind::Call,
                    contract: Nsid::literal("org.example.dayframe.toolbar.action"),
                },
            ),
        ]),
        ..InitialContext::default()
    }
}

#[tokio::test]
async fn counter_activates_and_mount_dispatch_unmount_complete_on_first_poll() {
    let package = WcmpPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid");
    let host = Host::new();
    let owner = host.start("counter");
    let (runtime, _driver) =
        WcmpRuntime::initialize(&host, owner.clone(), package.as_shared(), counter_context())
            .await
            .unwrap();

    let sink = Arc::new(RecordingSink::new());
    let session = runtime.mount("view", Arc::clone(&sink) as _).unwrap();
    assert_eq!(sink.tree(SurfaceTree::text_content), "0");

    let dispatch = session.handler();
    for expected in ["1", "2"] {
        let (revision, element) = sink.tree(|tree| {
            (
                tree.revision(),
                tree.listener_id("button", "click")
                    .expect("counter button listens for clicks"),
            )
        });
        let response = dispatch(Event {
            revision,
            element: NodeId::try_from(element.raw()).unwrap(),
            name: "click".into(),
            bubbles: true,
            data_json: mouse_data_json(),
        });
        assert_eq!(response, EventResponse::default());
        assert_eq!(sink.tree(SurfaceTree::text_content), expected);
    }

    drop(session);
    assert!(!sink.is_live(), "unmount withdraws the surface");
    assert_eq!(
        host.check(&owner),
        Ok(()),
        "a clean unmount keeps the instance"
    );
    host.close();
}

/// The wcmp counterpart of the wasmtime spike: a guest whose sync-typed `dispatch` blocks via
/// `std::thread::sleep` (lifted to the async `monotonic-clock.wait-until` import) must be handled
/// as a trap — default response, surface withdrawn,
/// incarnation closed — never as a host panic or a hang. This pins *our adapter's* behavior on
/// the polyfill engine (the one the browser backend shares), which upstream `wcmp`'s own
/// `CannotBlock` test does not cover. Pinned by `sleeper_fixture`.
#[tokio::test]
async fn guest_blocking_in_dispatch_fails_as_trap_on_the_wcmp_engine() {
    struct LiveSink(AtomicBool);

    impl SurfaceSink<ComponentRef> for LiveSink {
        fn is_live(&self) -> bool {
            !self.0.load(Ordering::Acquire)
        }

        fn apply(&self, _edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
            Ok(0)
        }

        fn withdraw(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let host = Host::new();
    let owner = host.start("sleeper");
    let ended = host.observe_instance(&owner).unwrap();
    let package = WcmpPackage::from_env("sleeper")
        .await
        .expect("prepared sleeper component is valid");
    let (runtime, driver) = WcmpRuntime::initialize(
        &host,
        owner.clone(),
        package.as_shared(),
        InitialContext::default(),
    )
    .await
    .unwrap();
    let writer = Arc::new(LiveSink(AtomicBool::new(false)));
    let session = runtime
        .mount("view", Arc::clone(&writer) as _)
        .expect("mount must complete before dispatch blocks");
    assert!(writer.is_live());

    let response = session.handler()(Event {
        revision: 1,
        element: NodeId::try_from(1_u32).unwrap(),
        name: "click".into(),
        bubbles: true,
        data_json: "{}".into(),
    });
    assert_eq!(response, EventResponse::default());
    assert!(
        !writer.is_live(),
        "a blocked dispatch withdraws the surface"
    );
    assert_eq!(
        host.check(&owner),
        Err(clay_host::AccessError::Revoked),
        "a blocked dispatch closes the incarnation"
    );
    assert_eq!(
        *ended.borrow(),
        Some(clay_host::CloseReason::Requested),
        "closure is reported to the terminal observer"
    );
    drop(session);
    drop(driver);
    host.close();
}

#[tokio::test]
async fn close_before_mount_revokes_the_incarnation() {
    let package = WcmpPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid");
    let host = Host::new();
    let owner = host.start("close-before-mount");
    let (runtime, driver) =
        WcmpRuntime::initialize(&host, owner.clone(), package.as_shared(), counter_context())
            .await
            .unwrap();
    host.close_instance(&owner).unwrap();
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    assert_eq!(
        runtime.mount("view", Arc::clone(&writer)).err(),
        Some(PresentationError::Revoked)
    );
    assert!(writer.is_live());
    drop(runtime);
    drop(driver);
    host.close();
}

#[tokio::test]
async fn document_driver_delivers_neutral_updates() {
    use clay_docs::DocumentGrant;
    use clay_docs::DocumentService;
    use clay_docs::FacetRoute;
    use std::future::Future;
    use std::task::Context;
    use std::task::Waker;

    let host = Host::new();
    let owner = host.start("counter");
    let service = DocumentService::new(32);
    let node = service.mock_documents();
    let route = FacetRoute {
        document: "counter-config".into(),
        branch: "main".into(),
        tag: "org.example.dayframe.counter.config".into(),
        key_id: "count".into(),
    };
    node.seed(route.clone(), serde_json::json!(3));
    let access = service
        .grant_documents(
            &host,
            &owner,
            vec![DocumentGrant {
                document: route.document.clone(),
                branch: route.branch.clone(),
                facets: vec![daybook_types::manifest::RoutineFacetAccess {
                    tag: route.tag.clone().into(),
                    key_id: Some(route.key_id.clone()),
                    read: true,
                    write: true,
                    ..Default::default()
                }],
            }],
        )
        .unwrap();
    let document = access
        .document(route.document.clone(), route.branch.clone())
        .unwrap();
    let package = WcmpPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid");
    let (runtime, driver) = WcmpRuntime::initialize(
        &host,
        owner.clone(),
        package.as_shared(),
        InitialContext {
            configs: [("counter".into(), document)].into(),
            ..counter_context()
        },
    )
    .await
    .unwrap();
    let writer = Arc::new(RecordingSink::new());
    let surface = runtime.mount("view", Arc::clone(&writer) as _).unwrap();
    assert_eq!(
        writer.tree(SurfaceTree::text_content),
        "3Pending counter writes: 0Refresh counter"
    );
    let mut driver = std::pin::pin!(driver);
    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    node.external_change(route.clone(), Some(serde_json::json!(7)));
    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        writer.tree(SurfaceTree::text_content),
        "7Pending counter writes: 0Refresh counter"
    );
    drop(surface);
    host.close_instance(&owner).unwrap();
    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    host.close();
}

#[tokio::test]
async fn collection_probe_bootstraps_polls_replays_resumes_resets_and_revokes() {
    use clay_collection::Change;
    use clay_collection::RowEvidence;
    use clay_collection::Version;
    use clay_guest_session::collections::BytesProjection;
    use clay_guest_session::collections::BytesSource;
    use std::num::NonZeroUsize;

    fn cbor(value: &str) -> Vec<u8> {
        let mut encoded = Vec::new();
        ciborium::into_writer(value, &mut encoded).unwrap();
        encoded
    }

    fn upsert(
        key: &str,
        value: &str,
        sequence: u64,
    ) -> Change<
        Vec<u8>,
        clay_collection::Stamped<Option<clay_guest_session::collections::CollectionValue>>,
    > {
        Change::Upsert {
            key: cbor(key),
            row_version: Version::new(1, sequence),
            value: stamped(clay_guest_session::collections::CollectionValue::plain(
                cbor(value),
            )),
            evidence: RowEvidence::Authoritative,
            write_context: None,
        }
    }

    let host = Host::new();
    let owner = host.start("collection-probe");
    let package = WcmpPackage::from_env("collection-probe")
        .await
        .expect("prepared collection probe is valid");
    let projection =
        BytesProjection::with_source(BytesSource, NonZeroUsize::new(8).unwrap(), [11, 12]);
    projection.publish(vec![upsert("alpha", "one", 1), upsert("beta", "two", 2)]);
    let (runtime, driver) = WcmpRuntime::initialize(
        &host,
        owner.clone(),
        package.as_shared(),
        InitialContext {
            collections: [("rows".into(), projection.clone())].into(),
            ..InitialContext::default()
        },
    )
    .await
    .unwrap();
    let scenario = async move {
        let writer = Arc::new(RecordingSink::new());
        let session = runtime
            .mount("view", Arc::<RecordingSink>::clone(&writer))
            .unwrap();
        let text = writer
            .wait_for(|text| text.contains("status=live") && text.contains("beta=two"))
            .await;
        assert!(text.contains("alpha=one"), "{text}");

        writer
            .wait_for(|text| {
                text.lines()
                    .find_map(|line| line.strip_prefix("empty-reads="))
                    .is_some_and(|count| count.parse::<u64>().unwrap() >= 1)
            })
            .await;
        projection.publish(vec![upsert("gamma", "three", 3)]);
        writer.wait_for(|text| text.contains("gamma=three")).await;

        let command = |element: u32| {
            let revision = writer.tree(|tree| tree.revision());
            session.handler()(Event {
                revision,
                element: NodeId::try_from(element).unwrap(),
                name: "click".into(),
                bubbles: true,
                data_json: "{}".into(),
            });
        };

        command(2);
        projection.publish(vec![upsert("delta", "four", 4)]);
        let text = writer
            .wait_for(|text| text.contains("replays=1") && text.contains("delta=four"))
            .await;
        assert!(text.contains("gamma=three"), "{text}");

        command(3);
        writer.wait_for(|text| text.contains("resumes=1")).await;
        command(4);
        writer
            .wait_for(|text| text.contains("status=stopped"))
            .await;
        projection.publish(vec![upsert("epsilon", "five", 5)]);
        command(3);
        writer
            .wait_for(|text| text.contains("resumes=2") && text.contains("epsilon=five"))
            .await;

        projection.reset();
        projection.publish(vec![upsert("fresh", "six", 6)]);
        let text = writer
            .wait_for(|text| {
                text.contains("resets=1")
                    && text.contains("status=live")
                    && text.contains("fresh=six")
            })
            .await;
        assert!(!text.contains("alpha=one"), "{text}");

        projection.revoke();
        writer
            .wait_for(|text| text.contains("status=revoked"))
            .await;
        drop(session);
        host.close();
    };
    tokio::join!(driver, scenario);
}

#[tokio::test]
async fn collection_probe_registration_writers_are_owner_scoped_compare_and_set_and_withdrawn() {
    use clay_collection::Change;
    use clay_collection::ReadLimits;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    fn removals(page: &clay_guest_session::collections::BytesPage) -> Vec<usize> {
        page.transactions
            .iter()
            .map(|transaction| {
                transaction
                    .changes
                    .iter()
                    .filter(|change| matches!(change, Change::Remove { .. }))
                    .count()
            })
            .collect()
    }

    let host = Host::new();
    let owner = host.start("collection-probe");
    let package = WcmpPackage::from_env("collection-probe")
        .await
        .expect("prepared collection probe is valid");
    let registrations = bytes_registrations(NonZeroUsize::new(64).unwrap(), [21, 22]);
    let observer = registrations.projection();
    let mut lease = observer.acquire(Vec::new(), None).unwrap();
    let bootstrap = lease.read(None, ReadLimits::default()).await.unwrap();
    assert!(bootstrap.bootstrap_complete);
    let (runtime, driver) = WcmpRuntime::initialize(
        &host,
        owner.clone(),
        package.as_shared(),
        InitialContext {
            collections: [("rows".into(), registrations.projection())].into(),
            registrations: [
                ("a".into(), full_grant(&registrations, "probe")),
                ("b".into(), full_grant(&registrations, "probe")),
            ]
            .into(),
            ..InitialContext::default()
        },
    )
    .await
    .unwrap();
    let scenario = async move {
        let writer = Arc::new(RecordingSink::new());
        let session = runtime
            .mount("view", Arc::<RecordingSink>::clone(&writer))
            .unwrap();
        writer.wait_for(|text| text.contains("status=live")).await;
        let command = |element: u32| {
            let revision = writer.tree(|tree| tree.revision());
            session.handler()(Event {
                revision,
                element: NodeId::try_from(element).unwrap(),
                name: "click".into(),
                bubbles: true,
                data_json: "{}".into(),
            });
        };

        command(5);
        let text = writer
            .wait_for(|text| text.contains("scenario=done") && text.contains("rows=2"))
            .await;
        let log = text
            .lines()
            .skip_while(|line| !line.starts_with("a-put-x="))
            .collect::<Vec<_>>();
        assert_eq!(
            log,
            [
                "a-put-x=ok@1",
                "b-put-x=ok@2",
                "a-stale-cas-x=conflict@2",
                "a-put-x-again=ok@3",
                "b-delete-y=ok@1",
                // Blind: concurrent writes lose to the tombstone they never saw.
                "a-blind-put-y=ok@1",
                // Default basis: the replica holds the tombstone's stamp, so the write wins.
                "a-recreate-y=ok@2",
                "a-put-big-key=key-too-large",
                "a-put-big-value-y=value-too-large",
                "b-delete-x=ok@4",
                "b-put-x-again=ok@5",
            ],
            "{text}"
        );
        assert!(
            text.contains("\nx=b2\ny=a3\n"),
            "both writers contest the same logical keys: {text}"
        );
        let writes = lease
            .read(Some(bootstrap.through), ReadLimits::default())
            .await
            .unwrap();
        // Tombstones replicate as rows, so no write publishes a removal.
        assert_eq!(removals(&writes), [0, 0, 0, 0, 0, 0, 0]);

        command(6);
        let text = writer
            .wait_for(|text| text.contains("a-writer-drop=ok"))
            .await;
        assert!(text.contains("\nx=b2\ny=a3\n"), "{text}");
        let unchanged = lease
            .read(Some(writes.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(unchanged.transactions.is_empty());

        host.close_instance(&owner).unwrap();
        let revoked = lease
            .read(Some(writes.through), ReadLimits::default())
            .await
            .unwrap();
        // Closing the owner retracts both scopes: x and y have no candidates left.
        assert_eq!(removals(&revoked), [2]);
        assert!(registrations.snapshot().is_empty());
        drop(session);
        host.close();
    };
    tokio::join!(driver, scenario);
}

#[tokio::test]
async fn collection_probe_capability_publication_redemption_and_closure_cross_the_boundary() {
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let owner = host.start("collection-probe");
    let package = WcmpPackage::from_env("collection-probe")
        .await
        .expect("prepared collection probe is valid");
    let registrations = bytes_registrations(NonZeroUsize::new(64).unwrap(), [41, 42]);
    let (runtime, driver) = WcmpRuntime::initialize(
        &host,
        owner.clone(),
        package.as_shared(),
        InitialContext {
            collections: [("rows".into(), registrations.projection())].into(),
            registrations: [("cap".into(), full_grant(&registrations, "probe"))].into(),
            ..InitialContext::default()
        },
    )
    .await
    .unwrap();
    let scenario = async move {
        let writer = Arc::new(RecordingSink::new());
        let session = runtime
            .mount("view", Arc::<RecordingSink>::clone(&writer))
            .unwrap();
        writer.wait_for(|text| text.contains("status=live")).await;
        let command = |element: u32| {
            let revision = writer.tree(|tree| tree.revision());
            session.handler()(Event {
                revision,
                element: NodeId::try_from(element).unwrap(),
                name: "click".into(),
                bubbles: true,
                data_json: "{}".into(),
            });
        };

        command(5);
        let text = writer
            .wait_for(|text| text.contains("scenario=done") && text.contains("cap-put=ok@1"))
            .await;
        assert!(text.contains("cap-publish=ok"), "{text}");
        assert!(
            text.contains("cap-fabricated-put=invalid-attachment"),
            "a tagged body without attachments conveys no authority: {text}"
        );
        assert!(text.contains("cap@0"), "{text}");
        let stored = registrations
            .snapshot()
            .into_values()
            .next()
            .unwrap()
            .value
            .unwrap();
        assert_eq!(stored.attachments.len(), 1);

        command(7);
        let text = writer.wait_for(|text| text.contains("cap-redeem=")).await;
        assert!(text.contains("cap-redeem=mounted"), "{text}");

        command(6);
        let text = writer
            .wait_for(|text| text.contains("cap-proxy-drop=ok"))
            .await;
        assert!(text.contains("cap-remove=ok@2"), "{text}");
        assert!(text.contains("cap-close=ok"), "{text}");
        assert!(
            text.contains("cap-proxy=revoked"),
            "provider closure leaves the retained proxy inert but droppable: {text}"
        );
        assert!(bodies(&registrations).is_empty());
        drop(session);
        host.close();
    };
    tokio::join!(driver, scenario);
}

#[test]
fn component_resource_borrows_resolve_only_granted_handles_and_release_capacity() {
    use clay_guest_session::PresentationHost;
    use wit_presentation::HostComponentRef;
    use wit_presentation::Mutation as Wire;
    let host = Host::new();
    let owner = host.start("layout");
    let component = host.publish_component(&owner).unwrap();
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner, DocumentLimits::default()).unwrap();
    let mut resources = state
        .grant_components(&vec![component; PresentationHost::MAX_COMPONENT_HANDLES])
        .unwrap();
    let borrowed = Resource::new_borrow(resources[0].rep());
    assert_eq!(
        clay_guest_session::presentation::map_mutations(
            vec![convert_mutation(Wire::MountComponent(borrowed))],
            |reference| resolve_component(&state.table, &reference),
        )
        .unwrap(),
        [Mutation::MountComponent(component)]
    );
    assert!(matches!(
        state.grant_components(&[component]),
        Err(PresentationError::Unsupported(_))
    ));
    let removed = resources.pop().unwrap();
    let stale_rep = removed.rep();
    HostComponentRef::drop(&mut state, removed).unwrap();
    assert!(
        clay_guest_session::presentation::map_mutations(
            vec![convert_mutation(Wire::MountComponent(
                Resource::new_borrow(stale_rep)
            ))],
            |reference| resolve_component(&state.table, &reference),
        )
        .is_err()
    );
    let replacement = state.grant_components(&[component]).unwrap().pop().unwrap();
    let wrong_type = state.table.push(17_u32).unwrap();
    assert!(
        clay_guest_session::presentation::map_mutations(
            vec![convert_mutation(Wire::MountComponent(
                Resource::new_borrow(wrong_type.rep())
            ))],
            |reference| resolve_component(&state.table, &reference),
        )
        .is_err()
    );
    state.table.delete(wrong_type).unwrap();
    HostComponentRef::drop(&mut state, replacement).unwrap();
    for resource in resources {
        HostComponentRef::drop(&mut state, resource).unwrap();
    }
    assert_eq!(state.presentation.component_handles(), 0);
    host.close();
}

/// A grant of `micro` over `registrations`.
fn full_grant(
    registrations: &clay_guest_session::collections::BytesRegistrations,
    micro: &str,
) -> clay_host::Grant<clay_guest_session::collections::BytesRegistrations> {
    clay_host::Grant {
        collection: registrations.clone(),
        micro: clay_host::MicroId::new(micro),
        writer: clay_collection::WriterId::new(
            &clay_collection::ReplicaId::from_bytes([1; clay_collection::ReplicaId::LEN]),
            micro,
        ),
    }
}

/// The bodies of the registrations' live rows; tombstones are skipped.
fn bodies(registrations: &clay_guest_session::collections::BytesRegistrations) -> Vec<Vec<u8>> {
    registrations
        .snapshot()
        .into_values()
        .filter_map(|row| Some(row.value?.body))
        .collect()
}

/// A projected row as a fixed writer's first write.
fn stamped(
    value: clay_guest_session::collections::CollectionValue,
) -> clay_collection::Stamped<Option<clay_guest_session::collections::CollectionValue>> {
    clay_collection::Stamped {
        stamp: clay_collection::Stamp {
            lamport: 1,
            writer: clay_collection::WriterId::from_encoded("test"),
        },
        value: Some(value),
    }
}

#[test]
fn registration_writer_resources_release_budget_and_reuse_table_slots() {
    use clay_collection::WriteBasis;
    use clay_guest_session::collections::CollectionLimits;
    use clay_guest_session::collections::CollectionSession;
    use clay_guest_session::collections::RegistrationWriter;
    use clay_guest_session::collections::WriteFailure;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;
    use wcmp::ResourceAny;

    let host = Host::new();
    let owner = host.start("writer-budget");
    let limits = CollectionLimits {
        writers: 2,
        ..CollectionLimits::default()
    };
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [31, 32]);
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    state.collections = CollectionSession::new(host.clone(), owner, limits);
    state.table.set_max_capacity(limits.writers);
    let first = state
        .table
        .push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    let other = state
        .table
        .push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(state.collections.writer_count(), limits.writers);
    assert_eq!(
        state
            .collections
            .grant_writer(full_grant(&registrations, "writer"))
            .err(),
        Some(clay_host::AccessError::Denied)
    );
    let first_stamp = state
        .table
        .get(&first)
        .unwrap()
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    let other_stamp = state
        .table
        .get(&other)
        .unwrap()
        .put(vec![2], WriteBasis::Concurrent, vec![2])
        .unwrap();
    let first_rep = first.rep();
    let engine = wcmp::Engine::with_backend(wcmp_wasm_core_wasmi::Wasmi::new()).unwrap();
    let mut store = wcmp::Store::new(&engine, state).unwrap();
    let stale = ResourceAny::try_from_resource(first, &mut store.as_context_mut()).unwrap();
    let first = stale
        .try_into_resource::<RegistrationWriter>(&mut store.as_context_mut())
        .unwrap();
    drop(store.data_mut().table.delete(first).unwrap());
    assert_eq!(store.data().collections.writer_count(), 1);
    assert!(
        store
            .data()
            .table
            .get(&Resource::<RegistrationWriter>::new_borrow(first_rep))
            .is_err()
    );
    assert!(
        store
            .data_mut()
            .table
            .delete(Resource::<RegistrationWriter>::new_own(first_rep))
            .is_err()
    );
    assert_eq!(store.data().collections.writer_count(), 1);
    assert_eq!(bodies(&registrations), [vec![1], vec![2]]);
    let state = store.data_mut();
    let replacement = state
        .table
        .push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(replacement.rep(), first_rep);
    assert_eq!(state.collections.writer_count(), limits.writers);
    let current = ResourceAny::try_from_resource(replacement, &mut store.as_context_mut()).unwrap();
    assert_ne!(stale, current);
    assert!(
        stale
            .try_into_resource::<RegistrationWriter>(&mut store.as_context_mut())
            .is_err()
    );
    let replacement = current
        .try_into_resource::<RegistrationWriter>(&mut store.as_context_mut())
        .unwrap();
    let state = store.data_mut();
    let writer = state.table.get(&replacement).unwrap();
    // The replacement scope holds nothing, but the dropped writer's row is still visible.
    assert_eq!(
        writer.put(vec![1], WriteBasis::CompareAndSet(None), vec![3]),
        Err(WriteFailure::Conflict {
            current: Some(first_stamp.clone())
        })
    );
    writer
        .put(vec![1], WriteBasis::Observed(first_stamp), vec![3])
        .unwrap();
    state
        .table
        .get(&other)
        .unwrap()
        .delete(vec![2], WriteBasis::Observed(other_stamp))
        .unwrap();
    assert_eq!(bodies(&registrations), [vec![3]]);
    drop(state.table.delete(replacement).unwrap());
    drop(state.table.delete(other).unwrap());
    assert_eq!(state.collections.writer_count(), 0);
    assert!(state.table.is_empty());
    assert_eq!(registrations.snapshot().len(), 2);
    host.close();
    assert!(registrations.snapshot().is_empty());
}

#[test]
fn registration_writer_table_capacity_failure_releases_its_permit() {
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;
    use wcmp::ResourceTableError;

    let host = Host::new();
    let owner = host.start("writer-table-capacity");
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [33, 34]);
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner, DocumentLimits::default()).unwrap();
    state.table.set_max_capacity(1);
    let first = state
        .table
        .push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    state
        .table
        .get(&first)
        .unwrap()
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    assert!(matches!(
        state.table.push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap()
        ),
        Err(ResourceTableError::Full)
    ));
    assert_eq!(state.collections.writer_count(), 1);
    assert_eq!(registrations.snapshot().len(), 1);
    let first_rep = first.rep();
    drop(state.table.delete(first).unwrap());
    assert_eq!(state.collections.writer_count(), 0);
    assert_eq!(registrations.snapshot().len(), 1);
    let replacement = state
        .table
        .push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(replacement.rep(), first_rep);
    assert_eq!(state.collections.writer_count(), 1);
    drop(state.table.delete(replacement).unwrap());
    assert_eq!(state.collections.writer_count(), 0);
    assert!(state.table.is_empty());
    host.close();
}

#[test]
fn registration_writer_table_entries_revoke_without_affecting_other_instances() {
    use clay_guest_session::collections::CollectionLimits;
    use clay_guest_session::collections::CollectionSession;
    use clay_guest_session::collections::RegistrationWriter;
    use clay_guest_session::collections::WriteFailure;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let owner = host.start("revoked-writer");
    let other_session = CollectionSession::new(
        host.clone(),
        host.start("live-writer"),
        CollectionLimits::default(),
    );
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [35, 36]);
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    let writer = state
        .table
        .push(
            state
                .collections
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    let other = state
        .table
        .push(
            other_session
                .grant_writer(full_grant(&registrations, "writer"))
                .unwrap(),
        )
        .unwrap();
    let accepted = state
        .table
        .get(&writer)
        .unwrap()
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    let other_accepted = state
        .table
        .get(&other)
        .unwrap()
        .put(vec![1], WriteBasis::Concurrent, vec![7])
        .unwrap();
    host.close_instance(&owner).unwrap();
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .filter_map(|entry| Some(entry.value?.body))
            .collect::<Vec<_>>(),
        [vec![7]]
    );
    assert_eq!(
        state
            .table
            .get(&writer)
            .unwrap()
            .put(vec![1], WriteBasis::Concurrent, vec![2]),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(
        state
            .table
            .get(&writer)
            .unwrap()
            .delete(vec![1], WriteBasis::Observed(accepted)),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(state.collections.writer_count(), 1);
    assert_eq!(
        state
            .collections
            .grant_writer(full_grant(&registrations, "writer"))
            .err(),
        Some(clay_host::AccessError::Revoked)
    );
    let writer_rep = writer.rep();
    drop(state.table.delete(writer).unwrap());
    assert_eq!(state.collections.writer_count(), 0);
    assert!(
        state
            .table
            .get(&Resource::<RegistrationWriter>::new_borrow(writer_rep))
            .is_err()
    );
    let updated = state
        .table
        .get(&other)
        .unwrap()
        .put(vec![1], WriteBasis::Observed(other_accepted), vec![9])
        .unwrap();
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .filter_map(|entry| Some(entry.value?.body))
            .collect::<Vec<_>>(),
        [vec![9]]
    );
    state
        .table
        .get(&other)
        .unwrap()
        .delete(vec![1], WriteBasis::Observed(updated))
        .unwrap();
    drop(state.table.delete(other).unwrap());
    assert_eq!(other_session.writer_count(), 0);
    assert!(bodies(&registrations).is_empty());
    assert!(state.table.is_empty());
    host.close();
}

#[derive(Default)]
struct TestSink(AtomicBool);

impl SurfaceSink<ComponentRef> for TestSink {
    fn is_live(&self) -> bool {
        !self.0.load(Ordering::Acquire)
    }

    fn apply(&self, _edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        unreachable!("handle-budget test does not render")
    }

    fn withdraw(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn oversized_adapter_batch_withdraws_before_resolving_component_handles() {
    use wit_presentation::HostMountPoint;
    use wit_presentation::HostSurface;

    let host = Host::new();
    let owner = host.start("ingress-budget");
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner, DocumentLimits::default()).unwrap();
    let placement = state
        .table
        .push(WcmpPlacement::new(
            Arc::clone(&writer),
            Arc::new(AtomicBool::new(false)),
        ))
        .unwrap();
    let surface = HostMountPoint::open(&mut state, Resource::new_borrow(placement.rep()))
        .unwrap()
        .unwrap();
    let mut edits = std::iter::repeat_with(|| wit_presentation::Mutation::Pop)
        .take(SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS + 1)
        .collect::<Vec<_>>();
    *edits.last_mut().unwrap() =
        wit_presentation::Mutation::MountComponent(Resource::new_borrow(u32::MAX));
    assert!(matches!(
        HostSurface::apply(&mut state, Resource::new_borrow(surface.rep()), edits).unwrap(),
        Err(wit_presentation::PresentationError::Unsupported(message))
            if message == "batch mutation limit"
    ));
    assert!(!writer.is_live());
    HostSurface::drop(&mut state, surface).unwrap();
    HostMountPoint::drop(&mut state, placement).unwrap();
    host.close();
}

#[test]
fn retained_surface_handles_are_bounded_and_release_capacity() {
    use clay_guest_session::PresentationHost;
    use wit_presentation::HostMountPoint;
    use wit_presentation::HostSurface;

    let host = Host::new();
    let owner = host.start("surface-budget");
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    let placement = state
        .table
        .push(WcmpPlacement::new(
            Arc::clone(&writer),
            Arc::new(AtomicBool::new(false)),
        ))
        .unwrap();
    let surface = HostMountPoint::open(&mut state, Resource::new_borrow(placement.rep()))
        .unwrap()
        .unwrap();
    let identity = HostSurface::identity(&mut state, Resource::new_borrow(surface.rep())).unwrap();
    let mut retained = Vec::new();
    for _ in 1..PresentationHost::MAX_SURFACE_HANDLES {
        retained.push(
            HostSurface::retain(&mut state, Resource::new_borrow(surface.rep()))
                .unwrap()
                .unwrap(),
        );
    }
    assert!(matches!(
        HostSurface::retain(&mut state, Resource::new_borrow(surface.rep())).unwrap(),
        Err(wit_presentation::PresentationError::Unsupported(_))
    ));
    assert!(writer.is_live());
    HostSurface::drop(&mut state, retained.pop().unwrap()).unwrap();
    let replacement = HostSurface::retain(&mut state, Resource::new_borrow(surface.rep()))
        .unwrap()
        .unwrap();
    assert_eq!(
        HostSurface::identity(&mut state, Resource::new_borrow(replacement.rep())).unwrap(),
        identity
    );
    let supplier = host.start("supplier");
    let reference = host.publish_component(&supplier).unwrap();
    let component = state.grant_components(&[reference]).unwrap().pop().unwrap();
    host.close_instance(&supplier).unwrap();
    assert!(matches!(
        HostSurface::apply(
            &mut state,
            Resource::new_borrow(replacement.rep()),
            vec![wit_presentation::Mutation::MountComponent(
                Resource::new_borrow(component.rep())
            )],
        )
        .unwrap(),
        Err(wit_presentation::PresentationError::Revoked)
    ));
    wit_presentation::HostComponentRef::drop(&mut state, component).unwrap();
    let oversized = std::iter::repeat_with(|| wit_presentation::Mutation::Pop)
        .take(SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS + 1)
        .collect::<Vec<_>>();
    assert!(matches!(
        HostSurface::apply(
            &mut state,
            Resource::new_borrow(replacement.rep()),
            oversized
        )
        .unwrap(),
        Err(wit_presentation::PresentationError::Unsupported(_))
    ));
    assert!(
        !writer.is_live(),
        "a budget violation withdraws the surface"
    );
    host.close_instance(&owner).unwrap();
    assert!(matches!(
        HostMountPoint::open(&mut state, Resource::new_borrow(placement.rep())).unwrap(),
        Err(wit_presentation::PresentationError::Revoked)
    ));
    assert!(matches!(
        HostSurface::retain(&mut state, Resource::new_borrow(replacement.rep())).unwrap(),
        Err(wit_presentation::PresentationError::Revoked)
    ));
    assert!(matches!(
        HostSurface::apply(
            &mut state,
            Resource::new_borrow(replacement.rep()),
            Vec::new()
        )
        .unwrap(),
        Err(wit_presentation::PresentationError::Revoked)
    ));
    assert!(HostSurface::identity(&mut state, Resource::new_borrow(replacement.rep())).is_err());
    HostSurface::drop(&mut state, surface).unwrap();
    for resource in retained {
        HostSurface::drop(&mut state, resource).unwrap();
    }
    HostSurface::drop(&mut state, replacement).unwrap();
    assert_eq!(state.presentation.surface_handles(), 0);
    HostMountPoint::drop(&mut state, placement).unwrap();
    host.close();
}

#[tokio::test]
async fn capability_row_mints_distinct_recipient_local_refs_for_two_wcmp_readers() {
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilitySession;
    use clay_guest_session::collections::CollectionValue;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let provider_instance = host.start("provider");
    let component = host.publish_component(&provider_instance).unwrap();
    let provider =
        CapabilitySession::new(host.clone(), provider_instance, CapabilityLimits::default());
    let lease = provider.publish_component("view", component).unwrap();
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [41, 42]);
    let (mut writer_state, _writer_incoming) = WcmpStoreState::detached(
        host.clone(),
        host.start("writer"),
        DocumentLimits::default(),
    )
    .unwrap();
    let granted = writer_state
        .collections
        .grant_writer(full_grant(&registrations, "writer"))
        .unwrap();
    let writer = writer_state.table.push(granted).unwrap();
    writer_state
        .table
        .get(&writer)
        .unwrap()
        .put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue {
                body: vec![0xd9, 0xa7, 0xf8, 0],
                attachments: vec![RowAttachment::Bound(lease.publication())],
            },
        )
        .unwrap();
    let stored = registrations
        .snapshot()
        .into_values()
        .next()
        .unwrap()
        .value
        .unwrap();
    assert_eq!(stored.body, vec![0xd9, 0xa7, 0xf8, 0]);
    assert_eq!(stored.attachments.len(), 1);
    assert_eq!(
        stored.attachments[0],
        RowAttachment::Bound(lease.publication())
    );

    let (mut reader_a, _reader_a_incoming) = WcmpStoreState::detached(
        host.clone(),
        host.start("reader-a"),
        DocumentLimits::default(),
    )
    .unwrap();
    let (mut reader_b, _reader_b_incoming) = WcmpStoreState::detached(
        host.clone(),
        host.start("reader-b"),
        DocumentLimits::default(),
    )
    .unwrap();
    let minted_a = reader_a
        .capabilities
        .mint_attachment(&stored.attachments[0])
        .unwrap();
    let ref_a = reader_a.table.push(minted_a).unwrap();
    let minted_b = reader_b
        .capabilities
        .mint_attachment(&stored.attachments[0])
        .unwrap();
    let ref_b = reader_b.table.push(minted_b).unwrap();
    assert_eq!(
        reader_a.table.get(&ref_a).unwrap().attachment(),
        RowAttachment::Bound(lease.publication())
    );
    assert_eq!(
        reader_b.table.get(&ref_b).unwrap().attachment(),
        RowAttachment::Bound(lease.publication())
    );
    assert_ne!(
        reader_a.table.get(&ref_a).unwrap().recipient_scope(),
        reader_b.table.get(&ref_b).unwrap().recipient_scope()
    );
    assert!(
        reader_a
            .capabilities
            .same_recipient(reader_a.table.get(&ref_a).unwrap())
    );
    assert!(
        !reader_a
            .capabilities
            .same_recipient(reader_b.table.get(&ref_b).unwrap())
    );
    assert_eq!(reader_a.capabilities.reference_count(), 1);
    assert_eq!(reader_b.capabilities.reference_count(), 1);

    let redeemed_a = reader_a
        .table
        .get(&ref_a)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    let proxy_a = reader_a.table.push(redeemed_a).unwrap();
    let redeemed_b = reader_b
        .table
        .get(&ref_b)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    let proxy_b = reader_b.table.push(redeemed_b).unwrap();
    assert_eq!(
        reader_a.table.get(&proxy_a).unwrap().component().unwrap(),
        component
    );
    assert_eq!(
        reader_b.table.get(&proxy_b).unwrap().component().unwrap(),
        component
    );
    assert_eq!(reader_a.capabilities.proxy_count(), 1);
    assert_eq!(reader_b.capabilities.proxy_count(), 1);

    drop(reader_a.table.delete(proxy_a).unwrap());
    assert_eq!(reader_a.capabilities.proxy_count(), 0);
    assert_eq!(
        reader_b.table.get(&proxy_b).unwrap().component().unwrap(),
        component
    );
    let redeemed_b_again = reader_b
        .table
        .get(&ref_b)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    assert_eq!(redeemed_b_again.component().unwrap(), component);
    drop(redeemed_b_again);
    assert_eq!(reader_b.capabilities.proxy_count(), 1);

    drop(reader_a.table.delete(ref_a).unwrap());
    assert_eq!(reader_a.capabilities.reference_count(), 0);
    assert_eq!(reader_b.capabilities.reference_count(), 1);
    drop(reader_b.table.delete(proxy_b).unwrap());
    drop(reader_b.table.delete(ref_b).unwrap());
    assert_eq!(reader_b.capabilities.proxy_count(), 0);
    assert_eq!(reader_b.capabilities.reference_count(), 0);
    assert!(reader_a.table.is_empty());
    assert!(reader_b.table.is_empty());

    drop(writer_state.table.delete(writer).unwrap());
    assert_eq!(writer_state.collections.writer_count(), 0);
    assert_eq!(registrations.snapshot().len(), 1);
    drop(lease);
    assert_eq!(provider.publication_count(), 0);
    host.close();
    assert!(registrations.snapshot().is_empty());
}

#[tokio::test]
async fn retained_presentation_proxy_survives_row_removal_but_goes_inert_on_lease_drop_and_provider_closure()
 {
    use clay_guest_session::capabilities::CapabilityFailure;
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilitySession;
    use clay_guest_session::collections::CollectionValue;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let provider_instance = host.start("provider");
    let component = host.publish_component(&provider_instance).unwrap();
    let provider = CapabilitySession::new(
        host.clone(),
        provider_instance.clone(),
        CapabilityLimits::default(),
    );
    let lease = provider.publish_component("view", component).unwrap();
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [43, 44]);
    let (mut writer_state, _writer_incoming) = WcmpStoreState::detached(
        host.clone(),
        host.start("writer"),
        DocumentLimits::default(),
    )
    .unwrap();
    let granted = writer_state
        .collections
        .grant_writer(full_grant(&registrations, "writer"))
        .unwrap();
    let writer = writer_state.table.push(granted).unwrap();
    let version = writer_state
        .table
        .get(&writer)
        .unwrap()
        .put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue {
                body: vec![0xd9, 0xa7, 0xf8, 0],
                attachments: vec![RowAttachment::Bound(lease.publication())],
            },
        )
        .unwrap();
    let stored = registrations
        .snapshot()
        .into_values()
        .next()
        .unwrap()
        .value
        .unwrap();

    let (mut reader, _reader_incoming) = WcmpStoreState::detached(
        host.clone(),
        host.start("reader"),
        DocumentLimits::default(),
    )
    .unwrap();
    let minted = reader
        .capabilities
        .mint_attachment(&stored.attachments[0])
        .unwrap();
    let reference = reader.table.push(minted).unwrap();
    let redeemed = reader
        .table
        .get(&reference)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    let proxy = reader.table.push(redeemed).unwrap();
    assert_eq!(
        reader.table.get(&proxy).unwrap().component().unwrap(),
        component
    );

    let version = writer_state
        .table
        .get(&writer)
        .unwrap()
        .put_value(
            vec![1],
            WriteBasis::Observed(version),
            CollectionValue::plain(vec![0x80]),
        )
        .unwrap();
    writer_state
        .table
        .get(&writer)
        .unwrap()
        .delete(vec![1], WriteBasis::Observed(version))
        .unwrap();
    assert!(bodies(&registrations).is_empty());
    assert_eq!(
        reader.table.get(&proxy).unwrap().component().unwrap(),
        component,
        "removing the row drops the stored attachment, not a proxy already redeemed from it"
    );

    drop(lease);
    assert_eq!(provider.publication_count(), 0);
    assert_eq!(
        reader.table.get(&proxy).unwrap().component().err(),
        Some(CapabilityFailure::Revoked)
    );
    assert!(reader.table.get(&proxy).unwrap().is_revoked());
    assert_eq!(reader.capabilities.proxy_count(), 1);
    drop(reader.table.delete(proxy).unwrap());
    assert_eq!(reader.capabilities.proxy_count(), 0);
    drop(reader.table.delete(reference).unwrap());
    assert_eq!(reader.capabilities.reference_count(), 0);

    let lease = provider.publish_component("view", component).unwrap();
    let minted = reader.capabilities.mint(&lease.publication()).unwrap();
    let reference = reader.table.push(minted).unwrap();
    let redeemed = reader
        .table
        .get(&reference)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    let proxy = reader.table.push(redeemed).unwrap();
    assert_eq!(
        reader.table.get(&proxy).unwrap().component().unwrap(),
        component
    );
    host.close_instance(&provider_instance).unwrap();
    assert_eq!(
        reader.table.get(&proxy).unwrap().component().err(),
        Some(CapabilityFailure::Revoked)
    );
    assert!(reader.table.get(&proxy).unwrap().is_revoked());
    drop(reader.table.delete(proxy).unwrap());
    drop(reader.table.delete(reference).unwrap());
    assert_eq!(reader.capabilities.proxy_count(), 0);
    assert_eq!(reader.capabilities.reference_count(), 0);
    assert!(reader.table.is_empty());
    drop(lease);
    drop(writer_state.table.delete(writer).unwrap());
    host.close();
}

#[tokio::test]
async fn writer_rejects_fabricated_attachment_indices_and_accepts_only_attached_publications() {
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilityRef;
    use clay_guest_session::capabilities::CapabilitySession;
    use clay_guest_session::collections::CollectionValue;
    use clay_guest_session::collections::WriteFailure;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let provider_instance = host.start("provider");
    let provider =
        CapabilitySession::new(host.clone(), provider_instance, CapabilityLimits::default());
    let lease = provider.publish_presentation("view").unwrap();
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [45, 46]);
    let (mut state, _incoming) = WcmpStoreState::detached(
        host.clone(),
        host.start("writer"),
        DocumentLimits::default(),
    )
    .unwrap();
    let granted = state
        .collections
        .grant_writer(full_grant(&registrations, "writer"))
        .unwrap();
    let writer = state.table.push(granted).unwrap();
    let writer = state.table.get(&writer).unwrap();

    assert_eq!(
        writer.put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue::plain(vec![0xd9, 0xa7, 0xf8, 0])
        ),
        Err(WriteFailure::InvalidAttachment)
    );
    assert_eq!(
        writer.put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue {
                body: vec![0xd9, 0xa7, 0xf8, 0],
                attachments: Vec::new(),
            },
        ),
        Err(WriteFailure::InvalidAttachment)
    );
    assert_eq!(
        writer.put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue {
                body: vec![0xd9, 0xa7, 0xf8, 1],
                attachments: vec![RowAttachment::Bound(lease.publication())],
            },
        ),
        Err(WriteFailure::InvalidAttachment)
    );
    assert!(
        state
            .table
            .get(&Resource::<CapabilityRef>::new_borrow(u32::MAX))
            .is_err(),
        "copied body bytes cannot be exchanged for a capability resource the table never issued"
    );
    assert!(registrations.snapshot().is_empty());

    let version = writer
        .put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue {
                body: vec![0xd9, 0xa7, 0xf8, 0],
                attachments: vec![RowAttachment::Bound(lease.publication())],
            },
        )
        .unwrap();
    let stored = registrations
        .snapshot()
        .into_values()
        .next()
        .unwrap()
        .value
        .unwrap();
    assert_eq!(stored.body, vec![0xd9, 0xa7, 0xf8, 0]);
    assert_eq!(stored.attachments.len(), 1);
    assert_eq!(
        stored.attachments[0],
        RowAttachment::Bound(lease.publication())
    );
    let RowAttachment::Bound(attached) = &stored.attachments[0] else {
        panic!("the row attaches the live publication");
    };
    assert_eq!(
        attached.contract(),
        &clay_guest_session::capabilities::presentation_component()
    );
    writer
        .delete(vec![1], WriteBasis::Observed(version))
        .unwrap();
    assert!(bodies(&registrations).is_empty());
    drop(lease);
    host.close();
}

#[tokio::test]
async fn capability_table_resources_release_session_limits_on_drop() {
    use clay_guest_session::capabilities::CapabilityFailure;
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilitySession;

    let host = Host::new();
    let owner = host.start("capability-limits");
    let (mut state, _incoming) =
        WcmpStoreState::detached(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    state.capabilities = CapabilitySession::new(
        host.clone(),
        owner,
        CapabilityLimits {
            publications: 1,
            references: 1,
            proxies: 1,
        },
    );

    let published = state.capabilities.publish_presentation("view").unwrap();
    let lease = state.table.push(published).unwrap();
    assert_eq!(
        state.capabilities.publish_presentation("view").err(),
        Some(CapabilityFailure::LimitExceeded)
    );
    let minted = state.table.get(&lease).unwrap().reference().unwrap();
    let reference = state.table.push(minted).unwrap();
    assert_eq!(
        state.table.get(&lease).unwrap().reference().err(),
        Some(CapabilityFailure::LimitExceeded)
    );
    let redeemed = state
        .table
        .get(&reference)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    let proxy = state.table.push(redeemed).unwrap();
    assert_eq!(
        state
            .table
            .get(&reference)
            .unwrap()
            .redeem_presentation()
            .await
            .err(),
        Some(CapabilityFailure::LimitExceeded)
    );
    assert_eq!(state.capabilities.publication_count(), 1);
    assert_eq!(state.capabilities.reference_count(), 1);
    assert_eq!(state.capabilities.proxy_count(), 1);

    drop(state.table.delete(proxy).unwrap());
    assert_eq!(state.capabilities.proxy_count(), 0);
    let redeemed = state
        .table
        .get(&reference)
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    let proxy = state.table.push(redeemed).unwrap();
    drop(state.table.delete(proxy).unwrap());
    drop(state.table.delete(reference).unwrap());
    assert_eq!(state.capabilities.proxy_count(), 0);
    assert_eq!(state.capabilities.reference_count(), 0);

    let minted = state.table.get(&lease).unwrap().reference().unwrap();
    let reference = state.table.push(minted).unwrap();
    assert_eq!(state.capabilities.reference_count(), 1);
    drop(state.table.delete(reference).unwrap());
    drop(state.table.delete(lease).unwrap());
    assert_eq!(state.capabilities.reference_count(), 0);
    assert_eq!(state.capabilities.publication_count(), 0);
    assert!(state.table.is_empty());

    let lease = state.capabilities.publish_presentation("view").unwrap();
    assert_eq!(state.capabilities.publication_count(), 1);
    drop(lease);
    assert_eq!(state.capabilities.publication_count(), 0);
    host.close();
}
