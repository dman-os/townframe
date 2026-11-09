//! What an incarnation's registrations publish and retract: the staging of a scope until its
//! initialization commits, and its removal, alone, when the incarnation closes. The micro
//! writes one row from `activate` (`<name>/guest`, the `name` row's value being the prefix);
//! the explicit-basis writer sequence (compare-and-set, observed supersession, tombstones) is
//! the registration writer's own, covered at its layer (`collection_probe_registration_writers_*`),
//! because the authoring `Registration` derives every basis from its mirror.
use super::*;
use clay_collection::Change;
use clay_collection::ReadLimits;
use std::collections::BTreeSet;

/// The host's row `key` with the text `value`, written by `seeder`.
fn seed(
    seeder: &crate::collections::RegistrationWriter,
    key: &str,
    value: &str,
    basis: clay_collection::WriteBasis,
) -> clay_collection::Stamp {
    seeder
        .put_value(
            cbor_text(key),
            basis,
            crate::collections::CollectionValue {
                body: cbor_text(value),
                attachments: Vec::new(),
            },
        )
        .unwrap()
}

/// `rows` is exactly the one row `micro` writes, as a live value of its own writer.
fn assert_owns_one_row(
    micro: &str,
    rows: &BTreeMap<Vec<u8>, clay_collection::Stamped<Option<crate::collections::CollectionValue>>>,
) {
    assert_eq!(
        rows.keys()
            .map(|key| ciborium::from_reader::<String, _>(key.as_slice()).unwrap())
            .collect::<Vec<_>>(),
        [format!("{micro}/guest")]
    );
    for entry in rows.values() {
        let value = entry
            .value
            .as_ref()
            .expect("the micro leaves no tombstone winner");
        assert!(value.attachments.is_empty());
        assert_eq!(entry.stamp.writer.as_bytes(), writer_of(micro).as_bytes());
    }
}

/// The version each key a page changed was published at.
fn versions_of(
    page: &crate::collections::BytesPage,
) -> BTreeMap<Vec<u8>, clay_collection::Version> {
    page.transactions
        .iter()
        .flat_map(|transaction| &transaction.changes)
        .map(|change| (change.key().clone(), change.row_version()))
        .collect()
}

/// Two incarnations of the micro share one collection and write one row each, named by the
/// host's `name` row at the moment each starts. While an incarnation initializes, its row is a
/// staged candidate: nothing is published until the host commits the initialization, and then
/// the net row is one transaction. Closing the first incarnation's guest half removes nothing;
/// the host closing its instance removes exactly its row, in one transaction, and the other's
/// stays until its own close. Run once for incarnations that are still initializing when they
/// write and once for ones that are already active, which publish the row as they write it.
pub async fn registration_scope_ownership<E: Incarnations>(engine: E) {
    let engine = Rc::new(engine);
    for initial_state in [Lifecycle::Initializing, Lifecycle::Active] {
        let host = Host::new();
        let registrations = bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), [81, 82]);
        let seeder = seeder(&host, "seeder", &registrations);
        let supervisor = Supervisor::<CapabilityPublication>::new(host.clone(), []);
        let mut observer = registrations
            .projection()
            .acquire(Vec::new(), None)
            .unwrap();
        let bootstrap = observer.read(None, ReadLimits::default()).await.unwrap();
        assert!(bootstrap.bootstrap_complete);
        assert!(bootstrap.transactions.is_empty());
        let mut through = bootstrap.through;
        let mut name = None;
        let mut incarnations = Vec::new();
        let mut owned_rows = Vec::new();
        let mut published_versions = Vec::new();
        for micro in ["first", "other"] {
            // The host's `name` row is what the micro prefixes its row with; it is replaced
            // before each start, and each replacement is a transaction the observer reads.
            name = Some(seed(
                &seeder,
                "name",
                micro,
                name.map_or(clay_collection::WriteBasis::Concurrent, |stamp| {
                    clay_collection::WriteBasis::Observed(stamp)
                }),
            ));
            let seeded = observer
                .read(Some(through), ReadLimits::default())
                .await
                .unwrap();
            through = seeded.through;
            let instance = host.create(micro).unwrap();
            host.begin_initialization(&instance).unwrap();
            if initial_state == Lifecycle::Active {
                host.commit_initialization(&instance).unwrap();
            }
            let before = registrations.snapshot();
            let (runtime, driver) = Rc::clone(&engine)
                .initialize(
                    host.clone(),
                    instance.clone(),
                    ContextWorld::granted(micro, &registrations, supervisor.activator()),
                )
                .await
                .unwrap();
            let driver = tokio::task::spawn_local(driver);
            assert_eq!(E::inspect(&runtime).writers, 1);
            if initial_state == Lifecycle::Initializing {
                assert_eq!(registrations.snapshot(), before);
                let staged = observer
                    .read(Some(through.clone()), ReadLimits::default())
                    .complete_timeout()
                    .unwrap();
                assert!(staged.transactions.is_empty());
                assert_eq!(staged.through, through);
                host.commit_initialization(&instance).unwrap();
            }
            let rows = registrations
                .snapshot()
                .into_iter()
                .filter(|(key, _)| !before.contains_key(key))
                .collect::<BTreeMap<_, _>>();
            assert_owns_one_row(micro, &rows);
            // Staged or active, the row is published once, as one transaction.
            let published = observer
                .read(Some(through), ReadLimits::default())
                .await
                .unwrap();
            assert_eq!(published.transactions.len(), 1);
            assert_eq!(published.transactions[0].changes.len(), rows.len());
            assert!(published.transactions[0].changes.iter().all(|change| {
                matches!(change, Change::Upsert { key, value, .. } if rows.get(key) == Some(value))
            }));
            published_versions.push(versions_of(&published));
            through = published.through;
            incarnations.push((instance, runtime, driver));
            owned_rows.push(rows);
        }
        let [
            (first, runtime, driver),
            (other, other_runtime, other_driver),
        ]: [_; 2] = incarnations.try_into().ok().unwrap();
        let [first_rows, other_rows]: [_; 2] = owned_rows.try_into().unwrap();
        let [first_versions, _]: [_; 2] = published_versions.try_into().unwrap();
        // What the incarnations own, without the host's `name` row.
        let owned = || {
            let mut rows = registrations.snapshot();
            rows.remove(&cbor_text("name"));
            rows
        };
        assert_eq!(owned().len(), 2);
        assert!(host.begin_close(&first).unwrap());
        assert_eq!(owned().len(), 2);
        let closing = observer
            .read(Some(through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(closing.transactions.is_empty());
        let (closed, residue) = E::close(runtime).await;
        closed.unwrap();
        assert_eq!(residue, GuestResidue::NONE);
        driver.await.unwrap();
        assert_eq!(owned().len(), 2, "the guest half's close retracts nothing");
        host.close_instance(&first).unwrap();
        let removed = observer
            .read(
                Some(through),
                ReadLimits {
                    max_transactions: Some(1),
                    max_changes: Some(1),
                },
            )
            .await
            .unwrap();
        assert_eq!(removed.transactions.len(), 1);
        assert_eq!(removed.transactions[0].changes.len(), first_rows.len());
        let removed_keys = removed.transactions[0]
            .changes
            .iter()
            .map(|change| {
                let Change::Remove { key, row_version } = change else {
                    panic!("incarnation closure published an upsert");
                };
                assert!(*row_version > first_versions[key]);
                key.clone()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(removed_keys, first_rows.into_keys().collect());
        assert_eq!(owned(), other_rows);
        let quiet = observer
            .read(Some(removed.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(quiet.transactions.is_empty());
        let (closed, residue) = E::close(other_runtime).await;
        closed.unwrap();
        assert_eq!(residue, GuestResidue::NONE);
        other_driver.await.unwrap();
        assert_eq!(owned(), other_rows);
        host.close_instance(&other).unwrap();
        let removed = observer
            .read(Some(removed.through), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(removed.transactions.len(), 1);
        assert_eq!(removed.transactions[0].changes.len(), other_rows.len());
        assert!(owned().is_empty());
        host.close();
    }
}

/// An initialization the micro fails after it wrote its row publishes nothing: the staged row
/// never leaves the incarnation's scope, which the host closes without committing.
pub async fn failed_registration_initialization_publishes_nothing<E: Incarnations>(engine: E) {
    let host = Host::new();
    let registrations = bytes_registrations(std::num::NonZeroUsize::new(16).unwrap(), [83, 84]);
    let seeder = seeder(&host, "seeder", &registrations);
    // The `reject` row makes the micro fail after it wrote and published everything.
    seed(
        &seeder,
        "reject",
        "reject",
        clay_collection::WriteBasis::Concurrent,
    );
    let supervisor = Supervisor::<CapabilityPublication>::new(host.clone(), []);
    let mut observer = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = observer.read(None, ReadLimits::default()).await.unwrap();
    let instance = host.create("failing").unwrap();
    host.begin_initialization(&instance).unwrap();
    let result = Rc::new(engine)
        .initialize(
            host.clone(),
            instance.clone(),
            ContextWorld::granted("failing", &registrations, supervisor.activator()),
        )
        .await;
    let refusal = result.err().expect("the micro refuses its initialization");
    assert!(
        refusal.contains("failing provider rejects its initialization"),
        "{refusal}"
    );
    host.close_instance(&instance).unwrap();
    assert_eq!(
        registrations.snapshot().len(),
        1,
        "only the host's seed row is left"
    );
    // The seed was part of the bootstrap; nothing the micro staged follows it.
    let unseen = observer
        .read(Some(bootstrap.through), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    assert!(unseen.transactions.is_empty());
    host.close();
}
