use super::*;
use wasmtime::component::Resource;

fn counter_package() -> WasmPackage {
    WasmPackage::from_env("counter").expect("prepared counter component is valid")
}

#[test]
fn component_resource_borrows_resolve_only_granted_handles_and_release_capacity() {
    use wit_presentation::HostComponentRef;
    use wit_presentation::Mutation as Wire;
    let host = Host::new();
    let owner = host.start("layout");
    let component = host.publish_component(&owner).unwrap();
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
    let mut resources = state
        .grant_components(&vec![component; WasmStoreState::MAX_COMPONENT_HANDLES])
        .unwrap();
    let borrowed = Resource::new_borrow(resources[0].rep());
    assert_eq!(
        convert_mutation(Wire::MountComponent(borrowed), &state.table).unwrap(),
        Mutation::MountComponent(component)
    );
    assert!(matches!(
        state.grant_components(&[component]),
        Err(PresentationError::Unsupported(_))
    ));
    let removed = resources.pop().unwrap();
    let stale_rep = removed.rep();
    HostComponentRef::drop(&mut state, removed).unwrap();
    assert!(
        convert_mutation(
            Wire::MountComponent(Resource::new_borrow(stale_rep)),
            &state.table
        )
        .is_err()
    );
    let replacement = state.grant_components(&[component]).unwrap().pop().unwrap();
    let wrong_type = state.table.push(17_u32).unwrap();
    assert!(
        convert_mutation(
            Wire::MountComponent(Resource::new_borrow(wrong_type.rep())),
            &state.table
        )
        .is_err()
    );
    state.table.delete(wrong_type).unwrap();
    HostComponentRef::drop(&mut state, replacement).unwrap();
    for resource in resources {
        HostComponentRef::drop(&mut state, resource).unwrap();
    }
    assert_eq!(state.component_handles, 0);
    host.close();
}

#[test]
fn ingress_budget_rejects_oversized_batches_before_conversion() {
    use wit_presentation::Mutation as Wire;
    assert_eq!(
        ingress_budget(&[Wire::Pop, Wire::CreateText("ok".into())]),
        Ok(())
    );
    assert_eq!(
        ingress_budget(
            &std::iter::repeat_with(|| Wire::Pop)
                .take(SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS + 1)
                .collect::<Vec<_>>()
        ),
        Err("batch mutation limit")
    );
    assert_eq!(
        ingress_budget(&[Wire::SetAttribute((
            "title".into(),
            None,
            wit_presentation::AttributeValue::Text(
                "x".repeat(SurfaceTree::<ComponentRef>::MAX_STRING_BYTES + 1)
            ),
        ))]),
        Err("string byte limit")
    );
    let text = "x".repeat(SurfaceTree::<ComponentRef>::MAX_STRING_BYTES);
    let batch = std::iter::repeat_with(|| Wire::CreateText(text.clone()))
        .take(
            SurfaceTree::<ComponentRef>::MAX_BATCH_BYTES
                / SurfaceTree::<ComponentRef>::MAX_STRING_BYTES
                + 1,
        )
        .collect::<Vec<_>>();
    assert_eq!(ingress_budget(&batch), Err("batch byte limit"));
}

#[test]
fn execution_and_linear_memory_are_bounded_per_incarnation() {
    let host = Host::new();
    let package = counter_package();
    let (mut active, _incoming) = package.instantiate(&host, host.start("bounded")).unwrap();
    let module = wasmtime::Module::new(
        &package.engine,
        r#"(module
        (memory (export "memory") 1)
        (func (export "loop") (loop br 0))
    )"#,
    )
    .unwrap();
    let instance = wasmtime::Instance::new(&mut active.store, &module, &[]).unwrap();
    let memory = instance.get_memory(&mut active.store, "memory").unwrap();
    assert!(memory.grow(&mut active.store, 1024).is_err());
    let looping = instance
        .get_typed_func::<(), ()>(&mut active.store, "loop")
        .unwrap();
    active.store.set_fuel(16).unwrap();
    let trap = looping.call(&mut active.store, ()).unwrap_err();
    assert_eq!(
        trap.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::OutOfFuel)
    );
    active.refuel();
    assert_eq!(active.store.get_fuel().unwrap(), 50_000_000);
    host.close();
}

#[tokio::test]
async fn document_driver_delivers_neutral_updates_and_closes_on_guest_trap() {
    use clay_docs::DocumentGrant;
    use clay_docs::DocumentService;
    use clay_docs::FacetRoute;
    use std::future::Future;
    use std::task::Context;
    use std::task::Waker;

    struct RecordingSink(Mutex<Option<SurfaceTree<ComponentRef>>>);

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

    let host = Host::new();
    let owner = host.start("counter");
    let ended = host.observe_instance(&owner).unwrap();
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
    let (runtime, driver) = WasmRuntime::activate(
        &host,
        owner.clone(),
        &counter_package(),
        [("counter".into(), document)].into(),
    )
    .unwrap();
    let writer = Arc::new(RecordingSink(Mutex::new(Some(SurfaceTree::new()))));
    let surface = runtime.mount(writer.clone(), &[]).unwrap();
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
    runtime.exhaust_fuel();
    node.external_change(route, Some(serde_json::json!(8)));
    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(*ended.borrow(), Some(clay_host::CloseReason::Requested));
    assert_eq!(host.check(&owner), Err(clay_host::AccessError::Revoked));
    assert!(!writer.is_live());
    drop(surface);
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
fn retained_surface_handles_are_bounded_and_release_capacity() {
    use wit_presentation::HostMountPoint;
    use wit_presentation::HostSurface;

    let host = Host::new();
    let owner = host.start("surface-budget");
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    let placement = state
        .table
        .push(WasmPlacement {
            writer: writer.clone(),
            revoked: Arc::new(AtomicBool::new(false)),
            surface_identity: None,
        })
        .unwrap();
    let surface = HostMountPoint::open(&mut state, Resource::new_borrow(placement.rep()))
        .unwrap()
        .unwrap();
    let identity = HostSurface::identity(&mut state, Resource::new_borrow(surface.rep())).unwrap();
    let mut retained = Vec::new();
    for _ in 1..WasmStoreState::MAX_SURFACE_HANDLES {
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
    assert_eq!(
        HostSurface::identity(&mut state, Resource::new_borrow(replacement.rep()))
            .unwrap_err()
            .downcast::<clay_host::AccessError>()
            .unwrap(),
        clay_host::AccessError::Revoked
    );
    HostSurface::drop(&mut state, surface).unwrap();
    for resource in retained {
        HostSurface::drop(&mut state, resource).unwrap();
    }
    assert!(writer.is_live());
    HostSurface::drop(&mut state, replacement).unwrap();
    assert!(!writer.is_live());
    assert_eq!(state.surface_handles, 0);
    HostMountPoint::drop(&mut state, placement).unwrap();
    host.close();
}
