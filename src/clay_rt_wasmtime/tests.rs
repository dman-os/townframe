use super::*;
use clay_surface::SurfaceTree;
use wasmtime::component::Resource;

async fn counter_package() -> WasmPackage {
    WasmPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid")
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
        .grant_components(&vec![component; PresentationHost::MAX_COMPONENT_HANDLES])
        .unwrap();
    let borrowed = Resource::new_borrow(resources[0].rep());
    assert_eq!(
        map_mutations(
            vec![convert_mutation(Wire::MountComponent(borrowed))],
            |reference| state.table.get(&reference).map(|component| component.0),
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
        map_mutations(
            vec![convert_mutation(Wire::MountComponent(
                Resource::new_borrow(stale_rep,)
            ))],
            |reference| state.table.get(&reference).map(|component| component.0),
        )
        .is_err()
    );
    let replacement = state.grant_components(&[component]).unwrap().pop().unwrap();
    let wrong_type = state.table.push(17_u32).unwrap();
    assert!(
        map_mutations(
            vec![convert_mutation(Wire::MountComponent(
                Resource::new_borrow(wrong_type.rep(),)
            ))],
            |reference| state.table.get(&reference).map(|component| component.0),
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
fn ingress_budget_rejects_oversized_batches_before_conversion() {
    use clay_guest_session::ingress_budget;
    use clay_surface::AttributeValue;
    assert_eq!(
        ingress_budget::<ComponentRef>(&[Mutation::Pop, Mutation::CreateText("ok".into())]),
        Ok(())
    );
    assert_eq!(
        ingress_budget::<ComponentRef>(
            &std::iter::repeat_with(|| Mutation::Pop)
                .take(clay_surface::SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS + 1)
                .collect::<Vec<_>>()
        ),
        Err("batch mutation limit")
    );
    assert_eq!(
        ingress_budget::<ComponentRef>(&[Mutation::SetAttribute((
            "title".into(),
            None,
            AttributeValue::Text(
                "x".repeat(clay_surface::SurfaceTree::<ComponentRef>::MAX_STRING_BYTES + 1)
            ),
        ))]),
        Err("string byte limit")
    );
    let text = "x".repeat(clay_surface::SurfaceTree::<ComponentRef>::MAX_STRING_BYTES);
    let batch = std::iter::repeat_with(|| Mutation::CreateText(text.clone()))
        .take(
            clay_surface::SurfaceTree::<ComponentRef>::MAX_BATCH_BYTES
                / clay_surface::SurfaceTree::<ComponentRef>::MAX_STRING_BYTES
                + 1,
        )
        .collect::<Vec<_>>();
    assert_eq!(
        ingress_budget::<ComponentRef>(&batch),
        Err("batch byte limit")
    );

    let final_text_bytes = clay_surface::SurfaceTree::<ComponentRef>::MAX_BATCH_BYTES
        - 3 * clay_surface::SurfaceTree::<ComponentRef>::MAX_STRING_BYTES
        - 4 * std::mem::size_of::<Mutation<ComponentRef>>()
        + 1;
    let texts = [
        text.clone(),
        text.clone(),
        text,
        "x".repeat(final_text_bytes),
    ];
    let resolved = texts
        .iter()
        .cloned()
        .map(Mutation::CreateText)
        .collect::<Vec<Mutation<ComponentRef>>>();
    let wire = texts
        .into_iter()
        .map(Mutation::CreateText)
        .collect::<Vec<Mutation<Resource<WasmComponentRef>>>>();
    assert_eq!(
        ingress_budget::<ComponentRef>(&resolved),
        Err("batch byte limit")
    );
    assert_eq!(
        ingress_budget::<Resource<WasmComponentRef>>(&wire),
        ingress_budget::<ComponentRef>(&resolved)
    );
}

#[test]
fn oversized_adapter_batch_withdraws_before_resolving_component_handles() {
    use wit_presentation::HostMountPoint;
    use wit_presentation::HostSurface;

    let host = Host::new();
    let owner = host.start("ingress-budget");
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
    let placement = state
        .table
        .push(Placement::new(
            writer.clone(),
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

#[tokio::test]
async fn execution_and_linear_memory_are_bounded_per_incarnation() {
    let host = Host::new();
    let package = counter_package().await;
    let (mut active, _incoming) = package
        .instantiate(&host, host.start("bounded"))
        .await
        .unwrap();
    let module = wasmtime::Module::new(
        &package.engine,
        r#"(module
        (memory (export "memory") 1)
        (func (export "loop") (loop br 0))
    )"#,
    )
    .unwrap();
    // The component's async imports set `async_required` on the store, so core-module
    // entrypoints must use the async variants (`Instance::new_async`,
    // `Memory::grow_async`, `TypedFunc::call_async`).
    let instance = wasmtime::Instance::new_async(&mut active.store, &module, &[])
        .await
        .unwrap();
    let memory = instance.get_memory(&mut active.store, "memory").unwrap();
    assert!(memory.grow_async(&mut active.store, 1024).await.is_err());
    let looping = instance
        .get_typed_func::<(), ()>(&mut active.store, "loop")
        .unwrap();
    active.store.set_fuel(16).unwrap();
    let trap = looping.call_async(&mut active.store, ()).await.unwrap_err();
    assert_eq!(
        trap.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::OutOfFuel)
    );
    active.refuel();
    assert_eq!(active.store.get_fuel().unwrap(), 50_000_000);
    host.close();
}

#[tokio::test]
async fn close_before_mount_revokes_and_releases_incarnation_owners() {
    let host = Host::new();
    let owner = host.start("close-before-mount");
    let (runtime, driver) = WasmRuntime::activate(
        &host,
        owner.clone(),
        &counter_package().await,
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let fuel = runtime.fuel_budget();
    host.close_instance(&owner).unwrap();
    let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
    assert_eq!(
        runtime.mount(writer.clone(), &[]).err(),
        Some(PresentationError::Revoked)
    );
    assert!(writer.is_live());
    drop(runtime);
    drop(driver);
    assert_eq!(Arc::strong_count(&fuel), 1);
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
        &counter_package().await,
        [("counter".into(), document)].into(),
    )
    .await
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
        .push(Placement::new(
            writer.clone(),
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
    assert_eq!(state.presentation.surface_handles(), 0);
    HostMountPoint::drop(&mut state, placement).unwrap();
    host.close();
}
