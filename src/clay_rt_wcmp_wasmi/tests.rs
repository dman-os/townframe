use super::WcmpPackage;
use super::WcmpRuntime;
use clay_guest_session::Limits as DocumentLimits;
use clay_host::ComponentRef;
use clay_host::Host;
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
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use wcmp::Resource;

struct RecordingSink {
    tree: Mutex<Option<SurfaceTree<ComponentRef>>>,
    changed: tokio::sync::Notify,
}

impl RecordingSink {
    fn new() -> Self {
        Self {
            tree: Mutex::new(Some(SurfaceTree::new())),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn tree<R>(&self, read: impl FnOnce(&SurfaceTree<ComponentRef>) -> R) -> R {
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

#[tokio::test]
async fn counter_activates_and_mount_dispatch_unmount_complete_on_first_poll() {
    let package = WcmpPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid");
    let host = Host::new();
    let owner = host.start("counter");
    let (runtime, _driver) = WcmpRuntime::activate(
        &host,
        owner.clone(),
        package.as_shared(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .await
    .unwrap();

    let sink = Arc::new(RecordingSink::new());
    let session = runtime.mount(Arc::clone(&sink) as _, &[]).unwrap();
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
    let (runtime, driver) = WcmpRuntime::activate(
        &host,
        owner.clone(),
        package.as_shared(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let writer = Arc::new(LiveSink(AtomicBool::new(false)));
    let session = runtime
        .mount(Arc::clone(&writer) as _, &[])
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
    let (runtime, driver) = WcmpRuntime::activate(
        &host,
        owner.clone(),
        package.as_shared(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    host.close_instance(&owner).unwrap();
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    assert_eq!(
        runtime.mount(Arc::clone(&writer), &[]).err(),
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
        tag: "org.dayframe.counter.config".into(),
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
    let (runtime, driver) = WcmpRuntime::activate(
        &host,
        owner.clone(),
        package.as_shared(),
        [("counter".into(), document)].into(),
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let writer = Arc::new(RecordingSink::new());
    let surface = runtime.mount(Arc::clone(&writer) as _, &[]).unwrap();
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

    fn upsert(key: &str, value: &str, sequence: u64) -> Change<Vec<u8>, Vec<u8>> {
        Change::Upsert {
            key: cbor(key),
            row_version: Version::new(1, sequence),
            value: cbor(value),
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
    let (runtime, driver) = WcmpRuntime::activate(
        &host,
        owner.clone(),
        package.as_shared(),
        BTreeMap::new(),
        [("rows".into(), projection.clone())].into(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let scenario = async move {
        let writer = Arc::new(RecordingSink::new());
        let session = runtime
            .mount(Arc::<RecordingSink>::clone(&writer), &[])
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
    let (runtime, driver) = WcmpRuntime::activate(
        &host,
        owner.clone(),
        package.as_shared(),
        BTreeMap::new(),
        [("rows".into(), registrations.projection())].into(),
        [
            ("a".into(), registrations.clone()),
            ("b".into(), registrations.clone()),
        ]
        .into(),
    )
    .await
    .unwrap();
    let scenario = async move {
        let writer = Arc::new(RecordingSink::new());
        let session = runtime
            .mount(Arc::<RecordingSink>::clone(&writer), &[])
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
            .wait_for(|text| text.contains("scenario=done") && text.contains("rows=3"))
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
                "a-blind-put-x=conflict@1",
                "a-cas-put-x=ok@3",
                "a-stale-remove-x=conflict@3",
                "b-put-with-a-version-x=conflict@2",
                "a-put-y=ok@4",
                "b-remove-y=absent",
                "b-remove-with-a-version-y=conflict@none",
                "a-put-big-key=key-too-large",
                "a-put-big-value-y=value-too-large",
                "b-remove-x=ok@5",
                "b-put-x-again=ok@6",
            ],
            "{text}"
        );
        assert!(
            text.contains("\n1:x=a2\n1:y=a3\n2:x=b2\n"),
            "both writers hold rows under the same local key: {text}"
        );
        let writes = lease
            .read(Some(bootstrap.through), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(removals(&writes), [0, 0, 0, 0, 1, 0]);

        command(6);
        let withdrawn = lease
            .read(Some(writes.through), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(
            removals(&withdrawn),
            [2],
            "dropping writer a withdraws both of its rows in one transaction"
        );
        let text = writer.wait_for(|text| text.contains("rows=1")).await;
        assert!(text.contains("\n2:x=b2\n"), "{text}");

        host.close_instance(&owner).unwrap();
        let revoked = lease
            .read(Some(withdrawn.through), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(
            removals(&revoked),
            [1],
            "closing the instance withdraws the writer the store still holds"
        );
        assert!(registrations.snapshot().is_empty());
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
        WcmpStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
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

#[test]
fn registration_writer_resources_release_budget_and_reuse_table_slots() {
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
        WcmpStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    state.collections = CollectionSession::new(host.clone(), owner, limits);
    state.table.set_max_capacity(limits.writers);
    let first = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    let other = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    assert_eq!(state.collections.writer_count(), limits.writers);
    assert_eq!(
        state.collections.grant_writer(&registrations).err(),
        Some(clay_host::AccessError::Denied)
    );
    let first_version = state
        .table
        .get(&first)
        .unwrap()
        .put(vec![1], None, vec![1])
        .unwrap();
    let other_version = state
        .table
        .get(&other)
        .unwrap()
        .put(vec![1], None, vec![2])
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
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        [vec![2]]
    );
    let state = store.data_mut();
    let replacement = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
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
    assert_eq!(
        writer.put(vec![1], Some(first_version), vec![3]),
        Err(WriteFailure::Conflict { current: None })
    );
    writer.put(vec![1], None, vec![3]).unwrap();
    assert!(
        state
            .table
            .get(&other)
            .unwrap()
            .remove(vec![1], Some(other_version))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        [vec![3]]
    );
    drop(state.table.delete(replacement).unwrap());
    drop(state.table.delete(other).unwrap());
    assert_eq!(state.collections.writer_count(), 0);
    assert!(state.table.is_empty());
    assert!(registrations.snapshot().is_empty());
    host.close();
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
        WcmpStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
    state.table.set_max_capacity(1);
    let first = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    state
        .table
        .get(&first)
        .unwrap()
        .put(vec![1], None, vec![1])
        .unwrap();
    assert!(matches!(
        state
            .table
            .push(state.collections.grant_writer(&registrations).unwrap()),
        Err(ResourceTableError::Full)
    ));
    assert_eq!(state.collections.writer_count(), 1);
    assert_eq!(registrations.snapshot().len(), 1);
    let first_rep = first.rep();
    drop(state.table.delete(first).unwrap());
    assert_eq!(state.collections.writer_count(), 0);
    assert!(registrations.snapshot().is_empty());
    let replacement = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
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
        WcmpStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    let writer = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    let other = state
        .table
        .push(other_session.grant_writer(&registrations).unwrap())
        .unwrap();
    let accepted = state
        .table
        .get(&writer)
        .unwrap()
        .put(vec![1], None, vec![1])
        .unwrap();
    let other_accepted = state
        .table
        .get(&other)
        .unwrap()
        .put(vec![1], None, vec![7])
        .unwrap();
    host.close_instance(&owner).unwrap();
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        [vec![7]]
    );
    assert_eq!(
        state
            .table
            .get(&writer)
            .unwrap()
            .put(vec![1], None, vec![2]),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(
        state
            .table
            .get(&writer)
            .unwrap()
            .remove(vec![1], Some(accepted)),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(state.collections.writer_count(), 1);
    assert_eq!(
        state.collections.grant_writer(&registrations).err(),
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
        .put(vec![1], Some(other_accepted), vec![9])
        .unwrap();
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        [vec![9]]
    );
    assert!(
        state
            .table
            .get(&other)
            .unwrap()
            .remove(vec![1], Some(updated))
            .unwrap()
            .is_some()
    );
    drop(state.table.delete(other).unwrap());
    assert_eq!(other_session.writer_count(), 0);
    assert!(registrations.snapshot().is_empty());
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
        WcmpStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
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
        WcmpStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
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
