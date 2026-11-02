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
