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

struct RecordingSink(Mutex<Option<SurfaceTree<ComponentRef>>>);

impl RecordingSink {
    fn tree<R>(&self, read: impl FnOnce(&SurfaceTree<ComponentRef>) -> R) -> R {
        read(self.0.lock().unwrap().as_ref().expect("surface is live"))
    }
}

impl SurfaceSink<ComponentRef> for RecordingSink {
    fn is_live(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        self.0
            .lock()
            .unwrap()
            .as_mut()
            .ok_or(PresentationError::Revoked)?
            .apply(edits)
    }

    fn withdraw(&self) {
        self.0.lock().unwrap().take();
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
    let (runtime, _driver) =
        WcmpRuntime::activate(&host, owner.clone(), package.as_shared(), BTreeMap::new())
            .await
            .unwrap();

    let sink = Arc::new(RecordingSink(Mutex::new(Some(SurfaceTree::new()))));
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

#[tokio::test]
async fn close_before_mount_revokes_the_incarnation() {
    let package = WcmpPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid");
    let host = Host::new();
    let owner = host.start("close-before-mount");
    let (runtime, driver) =
        WcmpRuntime::activate(&host, owner.clone(), package.as_shared(), BTreeMap::new())
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
    )
    .await
    .unwrap();
    let writer = Arc::new(RecordingSink(Mutex::new(Some(SurfaceTree::new()))));
    let surface = runtime.mount(Arc::clone(&writer) as _, &[]).unwrap();
    assert_eq!(
        writer.0.lock().unwrap().as_ref().unwrap().text_content(),
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
        writer.0.lock().unwrap().as_ref().unwrap().text_content(),
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
