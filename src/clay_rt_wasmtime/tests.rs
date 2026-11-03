use super::*;
use clay_surface::SurfaceTree;
use wasmtime::component::Resource;

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

async fn counter_package() -> WasmPackage {
    WasmPackage::from_env("counter")
        .await
        .expect("prepared counter component is valid")
}

async fn read_capability_page(
    store: &mut Store<WasmStoreState>,
    lease: u32,
    committed: Option<Vec<u8>>,
) -> bindings::townframe::dayframe::collections::Page {
    use bindings::townframe::dayframe::collections as wit_collections;
    use wit_collections::HostProjectionLeaseWithStore;

    store
        .run_concurrent(async |accessor| {
            HasSelf::<WasmStoreState>::read(
                accessor,
                Resource::new_borrow(lease),
                committed,
                wit_collections::ReadLimits {
                    max_transactions: None,
                    max_changes: None,
                    poll_timeout_ms: 1_000,
                },
            )
            .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn capability_delivery(
    host: &Host,
    name: &str,
    projection: &clay_guest_session::collections::BytesProjection,
) -> (
    Store<WasmStoreState>,
    mpsc::Receiver<Work>,
    u32,
    u32,
    Vec<u8>,
) {
    use bindings::townframe::dayframe::collections as wit_collections;
    use wit_collections::HostCollectionReaderWithStore;

    let owner = host.start(name);
    let (mut state, incoming) =
        WasmStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
    let reader = state
        .table
        .push(state.collections.grant(projection.clone()).unwrap())
        .unwrap();
    let reader_rep = reader.rep();
    let mut store = Store::new(&engine().unwrap(), state);
    let lease_rep = store
        .run_concurrent(async |accessor| {
            HasSelf::<WasmStoreState>::acquire(
                accessor,
                Resource::new_borrow(reader_rep),
                Vec::new(),
                None,
            )
            .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .rep();
    let page = read_capability_page(&mut store, lease_rep, None).await;
    let wit_collections::Change::Upsert(upsert) = &page.transactions[0].changes[0] else {
        panic!("capability delivery must start with the published upsert");
    };
    assert_eq!(upsert.value.body, [0xd9, 0xa7, 0xf8, 0]);
    assert_eq!(upsert.value.capabilities.len(), 1);
    (
        store,
        incoming,
        lease_rep,
        upsert.value.capabilities[0].rep(),
        page.through,
    )
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
fn registration_writer_resources_release_budget_and_reuse_table_slots() {
    use bindings::townframe::dayframe::collection_registration::HostRegistrationWriter;
    use clay_guest_session::collections::CollectionLimits;
    use clay_guest_session::collections::CollectionSession;
    use clay_guest_session::collections::RegistrationWriter;
    use clay_guest_session::collections::WriteFailure;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;
    use wasmtime::component::ResourceAny;

    let host = Host::new();
    let owner = host.start("writer-budget");
    let limits = CollectionLimits {
        writers: 2,
        ..CollectionLimits::default()
    };
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [31, 32]);
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
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
    let mut store = Store::new(&engine().unwrap(), state);
    let stale = ResourceAny::try_from_resource(first, &mut store).unwrap();
    let first = stale
        .try_into_resource::<RegistrationWriter>(&mut store)
        .unwrap();
    HostRegistrationWriter::drop(store.data_mut(), first).unwrap();
    assert_eq!(store.data().collections.writer_count(), 1);
    assert!(
        store
            .data()
            .table
            .get(&Resource::<RegistrationWriter>::new_borrow(first_rep))
            .is_err()
    );
    assert!(HostRegistrationWriter::drop(store.data_mut(), Resource::new_own(first_rep)).is_err());
    assert_eq!(store.data().collections.writer_count(), 1);
    assert_eq!(
        registrations
            .snapshot()
            .into_values()
            .map(|entry| entry.value.body)
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
    let current = ResourceAny::try_from_resource(replacement, &mut store).unwrap();
    assert_ne!(stale, current);
    assert!(
        stale
            .try_into_resource::<RegistrationWriter>(&mut store)
            .is_err()
    );
    let replacement = current
        .try_into_resource::<RegistrationWriter>(&mut store)
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
            .map(|entry| entry.value.body)
            .collect::<Vec<_>>(),
        [vec![3]]
    );
    HostRegistrationWriter::drop(state, replacement).unwrap();
    HostRegistrationWriter::drop(state, other).unwrap();
    assert_eq!(state.collections.writer_count(), 0);
    assert!(state.table.is_empty());
    assert!(registrations.snapshot().is_empty());
    host.close();
}

#[test]
fn registration_writer_table_capacity_failure_releases_its_permit() {
    use bindings::townframe::dayframe::collection_registration::HostRegistrationWriter;
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;
    use wasmtime::component::ResourceTableError;

    let host = Host::new();
    let owner = host.start("writer-table-capacity");
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [33, 34]);
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
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
    HostRegistrationWriter::drop(&mut state, first).unwrap();
    assert_eq!(state.collections.writer_count(), 0);
    assert!(registrations.snapshot().is_empty());
    let replacement = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    assert_eq!(replacement.rep(), first_rep);
    assert_eq!(state.collections.writer_count(), 1);
    HostRegistrationWriter::drop(&mut state, replacement).unwrap();
    assert_eq!(state.collections.writer_count(), 0);
    assert!(state.table.is_empty());
    host.close();
}

#[tokio::test]
async fn registration_writer_adapter_revokes_without_affecting_other_instances() {
    use bindings::townframe::dayframe::collection_registration as registration_wit;
    use clay_guest_session::collections::CollectionLimits;
    use clay_guest_session::collections::CollectionSession;
    use clay_guest_session::collections::bytes_registrations;
    use registration_wit::HostRegistrationWriter;
    use registration_wit::HostRegistrationWriterWithStore;
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
        WasmStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    let writer = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    let other = state
        .table
        .push(other_session.grant_writer(&registrations).unwrap())
        .unwrap();
    let writer_rep = writer.rep();
    let other_rep = other.rep();
    let mut store = Store::new(&engine().unwrap(), state);
    store
        .run_concurrent(async |accessor| {
            let put = async |rep, based_on, body| {
                HasSelf::<WasmStoreState>::put(
                    accessor,
                    Resource::new_borrow(rep),
                    vec![1],
                    based_on,
                    registration_wit::EncodedValue {
                        body: vec![body],
                        capabilities: Vec::new(),
                    },
                )
                .await
            };
            let remove = async |rep, based_on| {
                HasSelf::<WasmStoreState>::remove(
                    accessor,
                    Resource::new_borrow(rep),
                    vec![1],
                    based_on,
                )
                .await
            };
            let accepted = put(writer_rep, None, 1).await.unwrap().unwrap();
            let other_accepted = put(other_rep, None, 7).await.unwrap().unwrap();
            host.close_instance(&owner).unwrap();
            assert_eq!(
                registrations
                    .snapshot()
                    .into_values()
                    .map(|entry| entry.value.body)
                    .collect::<Vec<_>>(),
                [vec![7]]
            );
            assert!(matches!(
                put(writer_rep, None, 2).await.unwrap(),
                Err(registration_wit::MutationError::Revoked)
            ));
            assert!(matches!(
                remove(writer_rep, Some(accepted.row_version))
                    .await
                    .unwrap(),
                Err(registration_wit::MutationError::Revoked)
            ));
            accessor.with(|mut access| {
                let state = access.get();
                assert_eq!(state.collections.writer_count(), 1);
                assert_eq!(
                    state.collections.grant_writer(&registrations).err(),
                    Some(clay_host::AccessError::Revoked)
                );
                HostRegistrationWriter::drop(state, writer).unwrap();
                assert_eq!(state.collections.writer_count(), 0);
            });
            assert!(put(writer_rep, None, 3).await.is_err());
            assert!(remove(writer_rep, None).await.is_err());
            let updated = put(other_rep, Some(other_accepted.row_version), 9)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                registrations
                    .snapshot()
                    .into_values()
                    .map(|entry| entry.value.body)
                    .collect::<Vec<_>>(),
                [vec![9]]
            );
            assert!(
                remove(other_rep, Some(updated.row_version))
                    .await
                    .unwrap()
                    .unwrap()
                    .is_some()
            );
            accessor.with(|mut access| {
                let state = access.get();
                HostRegistrationWriter::drop(state, other).unwrap();
                assert!(state.table.is_empty());
            });
        })
        .await
        .unwrap();
    assert_eq!(other_session.writer_count(), 0);
    assert!(registrations.snapshot().is_empty());
    host.close();
}

#[tokio::test]
async fn capability_delivery_mints_distinct_recipient_refs_that_redeem_independently() {
    use bindings::townframe::dayframe::capability_bank as cap_wit;
    use cap_wit::HostCapabilityRef;
    use cap_wit::HostPresentationComponent;
    use clay_collection::Change;
    use clay_collection::RowEvidence;
    use clay_collection::Version;
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilityRef;
    use clay_guest_session::capabilities::CapabilitySession;
    use clay_guest_session::collections::BytesProjection;
    use clay_guest_session::collections::BytesSource;
    use clay_guest_session::collections::CollectionValue;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let provider = CapabilitySession::new(
        host.clone(),
        host.start("capability-provider"),
        CapabilityLimits::default(),
    );
    let lease = provider.publish_presentation_component().unwrap();
    let projection =
        BytesProjection::with_source(BytesSource, NonZeroUsize::new(8).unwrap(), [41, 42]);
    projection.publish(vec![Change::Upsert {
        key: vec![1],
        row_version: Version::new(1, 1),
        value: CollectionValue {
            body: vec![0xd9, 0xa7, 0xf8, 0],
            capabilities: vec![lease.publication()],
        },
        evidence: RowEvidence::Authoritative,
        write_context: None,
    }]);

    let (mut store_a, _incoming_a, _lease_a, reference_a, _through_a) =
        capability_delivery(&host, "capability-reader-a", &projection).await;
    let (mut store_b, _incoming_b, _lease_b, reference_b, _through_b) =
        capability_delivery(&host, "capability-reader-b", &projection).await;

    let (publication_a, scope_a) = {
        let state = store_a.data_mut();
        let reference = state
            .table
            .get(&Resource::<CapabilityRef>::new_borrow(reference_a))
            .unwrap();
        assert!(state.capabilities.same_recipient(reference));
        assert_eq!(state.capabilities.reference_count(), 1);
        (reference.publication_id(), reference.recipient_scope())
    };
    let (publication_b, scope_b) = {
        let state = store_b.data_mut();
        let reference = state
            .table
            .get(&Resource::<CapabilityRef>::new_borrow(reference_b))
            .unwrap();
        assert!(state.capabilities.same_recipient(reference));
        assert_eq!(state.capabilities.reference_count(), 1);
        (reference.publication_id(), reference.recipient_scope())
    };
    assert_eq!(publication_a, publication_b);
    assert_eq!(publication_a, lease.publication().id());
    assert_ne!(scope_a, scope_b);

    let proxy_a = {
        let state = store_a.data_mut();
        let proxy =
            HostCapabilityRef::as_presentation_component(state, Resource::new_borrow(reference_a))
                .unwrap()
                .unwrap();
        assert_eq!(state.capabilities.proxy_count(), 1);
        proxy
    };
    let proxy_b = {
        let state = store_b.data_mut();
        let proxy =
            HostCapabilityRef::as_presentation_component(state, Resource::new_borrow(reference_b))
                .unwrap()
                .unwrap();
        assert_eq!(state.capabilities.proxy_count(), 1);
        proxy
    };

    let component_a = {
        let state = store_a.data_mut();
        let resource =
            HostPresentationComponent::component(state, Resource::new_borrow(proxy_a.rep()))
                .unwrap()
                .unwrap();
        let component = state.table.get(&resource).unwrap().0;
        wit_presentation::HostComponentRef::drop(state, resource).unwrap();
        component
    };
    let component_b = {
        let state = store_b.data_mut();
        let resource =
            HostPresentationComponent::component(state, Resource::new_borrow(proxy_b.rep()))
                .unwrap()
                .unwrap();
        let component = state.table.get(&resource).unwrap().0;
        wit_presentation::HostComponentRef::drop(state, resource).unwrap();
        component
    };
    assert_eq!(component_a, component_b);

    // Releasing reader A's proxy and reference leaves reader B fully usable.
    {
        let state = store_a.data_mut();
        HostPresentationComponent::drop(state, proxy_a).unwrap();
        assert_eq!(state.capabilities.proxy_count(), 0);
        HostCapabilityRef::drop(state, Resource::new_own(reference_a)).unwrap();
        assert_eq!(state.capabilities.reference_count(), 0);
    }
    {
        let state = store_b.data_mut();
        assert_eq!(state.capabilities.proxy_count(), 1);
        assert_eq!(state.capabilities.reference_count(), 1);
        let resource =
            HostPresentationComponent::component(state, Resource::new_borrow(proxy_b.rep()))
                .unwrap()
                .unwrap();
        assert_eq!(state.table.get(&resource).unwrap().0, component_b);
        wit_presentation::HostComponentRef::drop(state, resource).unwrap();
        HostPresentationComponent::drop(state, proxy_b).unwrap();
        HostCapabilityRef::drop(state, Resource::new_own(reference_b)).unwrap();
        assert_eq!(state.capabilities.proxy_count(), 0);
        assert_eq!(state.capabilities.reference_count(), 0);
    }
    drop(lease);
    host.close();
}

#[tokio::test]
async fn retained_presentation_proxy_survives_row_removal_but_turns_inert_when_lease_drops() {
    use bindings::townframe::dayframe::capability_bank as cap_wit;
    use bindings::townframe::dayframe::collections as wit_collections;
    use cap_wit::HostCapabilityRef;
    use cap_wit::HostPresentationComponent;
    use clay_collection::Change;
    use clay_collection::RowEvidence;
    use clay_collection::Version;
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilitySession;
    use clay_guest_session::collections::BytesProjection;
    use clay_guest_session::collections::BytesSource;
    use clay_guest_session::collections::CollectionValue;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let provider = CapabilitySession::new(
        host.clone(),
        host.start("retained-provider"),
        CapabilityLimits::default(),
    );
    let lease = provider.publish_presentation_component().unwrap();
    let projection =
        BytesProjection::with_source(BytesSource, NonZeroUsize::new(8).unwrap(), [43, 44]);
    projection.publish(vec![Change::Upsert {
        key: vec![1],
        row_version: Version::new(1, 1),
        value: CollectionValue {
            body: vec![0xd9, 0xa7, 0xf8, 0],
            capabilities: vec![lease.publication()],
        },
        evidence: RowEvidence::Authoritative,
        write_context: None,
    }]);
    let (mut store, _incoming, lease_rep, reference, through) =
        capability_delivery(&host, "retained-reader", &projection).await;
    let proxy = {
        let state = store.data_mut();
        HostCapabilityRef::as_presentation_component(state, Resource::new_borrow(reference))
            .unwrap()
            .unwrap()
    };
    {
        let state = store.data_mut();
        let resource =
            HostPresentationComponent::component(state, Resource::new_borrow(proxy.rep()))
                .unwrap()
                .unwrap();
        wit_presentation::HostComponentRef::drop(state, resource).unwrap();
    }

    projection.publish(vec![Change::Remove {
        key: vec![1],
        row_version: Version::new(1, 2),
    }]);
    let page = read_capability_page(&mut store, lease_rep, Some(through)).await;
    assert!(matches!(
        page.transactions[0].changes[0],
        wit_collections::Change::Remove(_)
    ));
    {
        let state = store.data_mut();
        let resource =
            HostPresentationComponent::component(state, Resource::new_borrow(proxy.rep()))
                .unwrap()
                .unwrap();
        wit_presentation::HostComponentRef::drop(state, resource).unwrap();
        assert_eq!(state.capabilities.proxy_count(), 1);
    }

    drop(lease);
    {
        let state = store.data_mut();
        assert!(matches!(
            HostPresentationComponent::component(state, Resource::new_borrow(proxy.rep()))
                .unwrap()
                .err(),
            Some(cap_wit::CapabilityError::Revoked)
        ));
        assert_eq!(state.capabilities.proxy_count(), 1);
        HostPresentationComponent::drop(state, proxy).unwrap();
        assert_eq!(state.capabilities.proxy_count(), 0);
        HostCapabilityRef::drop(state, Resource::new_own(reference)).unwrap();
        assert_eq!(state.capabilities.reference_count(), 0);
    }
    host.close();
}

#[tokio::test]
async fn registration_writer_surfaces_invalid_attachment_for_fabricated_capability_index() {
    use bindings::townframe::dayframe::capability_bank as cap_wit;
    use bindings::townframe::dayframe::collection_registration as registration_wit;
    use cap_wit::HostPublishedCapability;
    use clay_guest_session::capabilities::CapabilityRef;
    use clay_guest_session::collections::bytes_registrations;
    use registration_wit::HostRegistrationWriterWithStore;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let owner = host.start("capability-writer");
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [45, 46]);
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
    let writer = state
        .table
        .push(state.collections.grant_writer(&registrations).unwrap())
        .unwrap();
    let writer_rep = writer.rep();
    let lease = cap_wit::Host::publish_presentation_component(&mut state)
        .unwrap()
        .unwrap();
    let publication_id = state.table.get(&lease).unwrap().publication().id();
    let lease_rep = lease.rep();
    let reference = HostPublishedCapability::reference(&mut state, Resource::new_borrow(lease_rep))
        .unwrap()
        .unwrap();
    let reference_rep = reference.rep();
    let mut store = Store::new(&engine().unwrap(), state);

    let fabricated = store
        .run_concurrent(async |accessor| {
            HasSelf::<WasmStoreState>::put(
                accessor,
                Resource::new_borrow(writer_rep),
                vec![1],
                None,
                registration_wit::EncodedValue {
                    body: vec![0xd9, 0xa7, 0xf8, 0],
                    capabilities: Vec::new(),
                },
            )
            .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        fabricated.err(),
        Some(registration_wit::MutationError::InvalidAttachment)
    ));
    assert!(registrations.snapshot().is_empty());

    let accepted = store
        .run_concurrent(async |accessor| {
            HasSelf::<WasmStoreState>::put(
                accessor,
                Resource::new_borrow(writer_rep),
                vec![1],
                None,
                registration_wit::EncodedValue {
                    body: vec![0xd9, 0xa7, 0xf8, 0],
                    capabilities: vec![Resource::<CapabilityRef>::new_own(reference_rep)],
                },
            )
            .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(accepted.is_ok());
    let stored = registrations.snapshot().into_values().next().unwrap().value;
    assert_eq!(stored.body, [0xd9, 0xa7, 0xf8, 0]);
    assert_eq!(stored.capabilities.len(), 1);
    assert_eq!(stored.capabilities[0].id(), publication_id);

    let state = store.data_mut();
    HostPublishedCapability::drop(state, Resource::new_own(lease_rep)).unwrap();
    registration_wit::HostRegistrationWriter::drop(state, Resource::new_own(writer_rep)).unwrap();
    host.close();
}

#[test]
fn capability_bank_table_drops_release_reference_and_proxy_capacity() {
    use bindings::townframe::dayframe::capability_bank as cap_wit;
    use cap_wit::HostCapabilityRef;
    use cap_wit::HostPresentationComponent;
    use cap_wit::HostPublishedCapability;
    use clay_guest_session::capabilities::CapabilityLimits;
    use clay_guest_session::capabilities::CapabilitySession;

    let host = Host::new();
    let owner = host.start("capability-budget");
    let (mut state, _incoming) =
        WasmStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
    state.capabilities = CapabilitySession::new(
        host.clone(),
        owner,
        CapabilityLimits {
            publications: 1,
            references: 1,
            proxies: 1,
        },
    );
    let lease = cap_wit::Host::publish_presentation_component(&mut state)
        .unwrap()
        .unwrap();
    assert_eq!(state.capabilities.publication_count(), 1);

    let reference =
        HostPublishedCapability::reference(&mut state, Resource::new_borrow(lease.rep()))
            .unwrap()
            .unwrap();
    assert_eq!(state.capabilities.reference_count(), 1);
    assert!(matches!(
        HostPublishedCapability::reference(&mut state, Resource::new_borrow(lease.rep()))
            .unwrap()
            .err(),
        Some(cap_wit::CapabilityError::LimitExceeded)
    ));
    HostCapabilityRef::drop(&mut state, reference).unwrap();
    assert_eq!(state.capabilities.reference_count(), 0);
    let reference =
        HostPublishedCapability::reference(&mut state, Resource::new_borrow(lease.rep()))
            .unwrap()
            .unwrap();

    let proxy = HostCapabilityRef::as_presentation_component(
        &mut state,
        Resource::new_borrow(reference.rep()),
    )
    .unwrap()
    .unwrap();
    assert_eq!(state.capabilities.proxy_count(), 1);
    assert!(matches!(
        HostCapabilityRef::as_presentation_component(
            &mut state,
            Resource::new_borrow(reference.rep()),
        )
        .unwrap()
        .err(),
        Some(cap_wit::CapabilityError::LimitExceeded)
    ));
    HostPresentationComponent::drop(&mut state, proxy).unwrap();
    assert_eq!(state.capabilities.proxy_count(), 0);
    let proxy = HostCapabilityRef::as_presentation_component(
        &mut state,
        Resource::new_borrow(reference.rep()),
    )
    .unwrap()
    .unwrap();

    HostPresentationComponent::drop(&mut state, proxy).unwrap();
    HostCapabilityRef::drop(&mut state, reference).unwrap();
    HostPublishedCapability::drop(&mut state, lease).unwrap();
    assert_eq!(state.capabilities.proxy_count(), 0);
    assert_eq!(state.capabilities.reference_count(), 0);
    assert_eq!(state.capabilities.publication_count(), 0);
    assert!(state.table.is_empty());
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
    let wasm = wat::parse_str(
        r#"(module
        (memory (export "memory") 1)
        (func (export "loop") (loop br 0))
    )"#,
    )
    .unwrap();
    let module = wasmtime::Module::new(&package.engine, wasm).unwrap();
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
    let (runtime, driver) = WasmRuntime::initialize(
        &host,
        owner.clone(),
        &counter_package().await,
        InitialContext::default(),
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

/// W2 spike: a guest whose sync-typed `dispatch` blocks via `std::thread::sleep` (which lifts
/// to the async `monotonic-clock.wait-until` import).
/// The component model's may-not-suspend rule must surface the block as `Err` (CannotBlock)
/// on the first poll — not `Pending`, not a host panic — and the host must handle it as a
/// trap: withdraw the surface, close the instance. Pinned by `sleeper_fixture`.
#[tokio::test]
async fn guest_blocking_in_dispatch_fails_as_trap_not_panic_or_pending() {
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
    let package = WasmPackage::from_env("sleeper")
        .await
        .expect("prepared sleeper component is valid");
    let (runtime, driver) =
        WasmRuntime::initialize(&host, owner.clone(), &package, InitialContext::default())
            .await
            .unwrap();
    let writer = Arc::new(LiveSink(AtomicBool::new(false)));
    let session = runtime
        .mount(Arc::clone(&writer) as _, &[])
        .expect("mount must complete before dispatch blocks");
    assert!(writer.is_live());

    let suspended_before = suspended_count();
    // A real event so the guest's dispatch runs; the sleeper sleeps inside it.
    let response = session.handler()(Event {
        revision: 1,
        element: clay_surface::NodeId::try_from(1_u32).unwrap(),
        name: "click".into(),
        bubbles: true,
        data_json: "{}".into(),
    });
    // Trap-equivalent: default response, surface withdrawn, instance closed.
    assert_eq!(response, EventResponse::default());
    assert!(
        !writer.is_live(),
        "a blocked dispatch withdraws the surface"
    );
    assert_eq!(
        host.check(&owner),
        Err(clay_host::AccessError::Revoked),
        "a blocked dispatch closes the instance"
    );
    assert_eq!(
        *ended.borrow(),
        Some(clay_host::CloseReason::Requested),
        "closure is reported to the terminal observer"
    );
    // The block must surface as Ready(Err) (CannotBlock), never as Pending/Suspended.
    assert_eq!(
        suspended_count(),
        suspended_before,
        "wasmtime must fail a blocking sync-typed call with CannotBlock on the first poll, \
         not return Pending"
    );
    drop(session);
    drop(driver);
    host.close();
}

#[test]
fn first_poll_maps_pending_to_suspended_and_ready_to_output() {
    use std::task::Poll;

    struct AlwaysPending;
    impl std::future::Future for AlwaysPending {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    assert!(matches!(
        first_poll(AlwaysPending, Waker::noop()),
        Err(Suspended)
    ));
    assert!(matches!(
        first_poll(std::future::ready(()), Waker::noop()),
        Ok(())
    ));
}

#[tokio::test]
async fn document_driver_delivers_neutral_updates_and_closes_on_guest_trap() {
    use clay_docs::DocumentGrant;
    use clay_docs::DocumentService;
    use clay_docs::FacetRoute;
    use std::future::Future;
    use std::task::Context;
    use std::task::Waker;

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
    let (runtime, driver) = WasmRuntime::initialize(
        &host,
        owner.clone(),
        &counter_package().await,
        InitialContext {
            configs: [("counter".into(), document)].into(),
            ..InitialContext::default()
        },
    )
    .await
    .unwrap();
    let writer = Arc::new(RecordingSink::new());
    let surface = runtime.mount(writer.clone(), &[]).unwrap();
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
    ) -> Change<Vec<u8>, clay_guest_session::collections::CollectionValue> {
        Change::Upsert {
            key: cbor(key),
            row_version: Version::new(1, sequence),
            value: clay_guest_session::collections::CollectionValue::plain(cbor(value)),
            evidence: RowEvidence::Authoritative,
            write_context: None,
        }
    }

    let host = Host::new();
    let owner = host.start("collection-probe");
    let package = WasmPackage::from_env("collection-probe")
        .await
        .expect("prepared collection probe is valid");
    let projection =
        BytesProjection::with_source(BytesSource, NonZeroUsize::new(8).unwrap(), [11, 12]);
    projection.publish(vec![upsert("alpha", "one", 1), upsert("beta", "two", 2)]);
    let (runtime, driver) = WasmRuntime::initialize(
        &host,
        owner.clone(),
        &package,
        InitialContext {
            collections: [("rows".into(), projection.clone())].into(),
            ..InitialContext::default()
        },
    )
    .await
    .unwrap();
    let scenario = async move {
        let writer = Arc::new(RecordingSink::new());
        let session = runtime.mount(writer.clone(), &[]).unwrap();
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
                element: clay_surface::NodeId::try_from(element).unwrap(),
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
    let package = WasmPackage::from_env("collection-probe")
        .await
        .expect("prepared collection probe is valid");
    let registrations = bytes_registrations(NonZeroUsize::new(64).unwrap(), [21, 22]);
    let observer = registrations.projection();
    let mut lease = observer.acquire(Vec::new(), None).unwrap();
    let bootstrap = lease.read(None, ReadLimits::default()).await.unwrap();
    assert!(bootstrap.bootstrap_complete);
    let (runtime, driver) = WasmRuntime::initialize(
        &host,
        owner.clone(),
        &package,
        InitialContext {
            collections: [("rows".into(), registrations.projection())].into(),
            registrations: [
                ("a".into(), registrations.clone()),
                ("b".into(), registrations.clone()),
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
            .mount(Arc::<RecordingSink>::clone(&writer), &[])
            .unwrap();
        writer.wait_for(|text| text.contains("status=live")).await;
        let command = |element: u32| {
            let revision = writer.tree(|tree| tree.revision());
            session.handler()(Event {
                revision,
                element: clay_surface::NodeId::try_from(element).unwrap(),
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

#[tokio::test]
async fn collection_probe_capability_publication_redemption_and_closure_cross_the_boundary() {
    use clay_guest_session::collections::bytes_registrations;
    use std::num::NonZeroUsize;

    let host = Host::new();
    let owner = host.start("collection-probe");
    let package = WasmPackage::from_env("collection-probe")
        .await
        .expect("prepared collection probe is valid");
    let registrations = bytes_registrations(NonZeroUsize::new(64).unwrap(), [41, 42]);
    let (runtime, driver) = WasmRuntime::initialize(
        &host,
        owner.clone(),
        &package,
        InitialContext {
            collections: [("rows".into(), registrations.projection())].into(),
            registrations: [("cap".into(), registrations.clone())].into(),
            ..InitialContext::default()
        },
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
                element: clay_surface::NodeId::try_from(element).unwrap(),
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
        let stored = registrations.snapshot().into_values().next().unwrap().value;
        assert_eq!(stored.capabilities.len(), 1);

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
        assert!(registrations.snapshot().is_empty());
        drop(session);
        host.close();
    };
    tokio::join!(driver, scenario);
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

// ── One engine per process, and a precompiled artifact cache (F10/W6) ────

#[tokio::test]
async fn packages_share_one_engine_and_one_linker() {
    let first = counter_package().await;
    let second = counter_package().await;
    assert!(Engine::same(&first.engine, &second.engine));
    assert!(Arc::ptr_eq(&first.linker, &second.linker));
    assert!(Engine::same(&first.engine, &engine().unwrap()));
}

#[tokio::test]
async fn precompiled_artifacts_are_reused_by_content() {
    let path = std::env::var("DAYFRAME_COUNTER_COMPONENT")
        .expect("DAYFRAME_COUNTER_COMPONENT must name a prepared counter component");
    let bytes = std::fs::read(path).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "clay-component-cache-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let artifact = {
        let engine = engine().unwrap();
        // The cache directory is process-global configuration; this test owns it for its duration.
        let previous = std::env::var_os("DAYFRAME_COMPONENT_CACHE");
        // SAFETY: tests in this binary do not read this variable concurrently except through
        // this test, which restores it before returning.
        unsafe { std::env::set_var("DAYFRAME_COMPONENT_CACHE", &directory) };
        let first = compile_component(&engine, &bytes).unwrap();
        let artifact = cache_path(&engine, &bytes).unwrap();
        assert!(artifact.exists(), "a cold compile publishes its artifact");
        let modified = std::fs::metadata(&artifact).unwrap().modified().unwrap();
        let second = compile_component(&engine, &bytes).unwrap();
        assert_eq!(
            std::fs::metadata(&artifact).unwrap().modified().unwrap(),
            modified,
            "a warm load must not recompile"
        );
        drop((first, second));
        match previous {
            Some(previous) => unsafe { std::env::set_var("DAYFRAME_COMPONENT_CACHE", previous) },
            None => unsafe { std::env::remove_var("DAYFRAME_COMPONENT_CACHE") },
        }
        artifact
    };
    // A warm load produces a component on the shared engine, from the published artifact.
    let package = WasmPackage::from_bytes(&bytes).await.unwrap();
    assert!(Engine::same(&package.engine, &engine().unwrap()));
    assert!(artifact.exists());
    std::fs::remove_dir_all(directory).ok();
}
