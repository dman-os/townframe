//! A world for the driver tests and the simulation: a memory db holding the wants table beside
//! the backend's own tables, a scripted backend with a content store, and the checks the laws
//! turn into at that boundary.
//!
//! The backend keeps a naive model of the table's live rows, written by the same calls the
//! test makes, and judges every landing against it (law 12) and every start against the
//! one-fetch and cap laws (laws 5 and 8). Violations are recorded, not panicked: they happen
//! inside the driver's task, so the test asserts them from outside.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use futures::executor::block_on;

use super::table::Table;
use super::*;
use crate::okv::OkvStore;
use crate::okv::memory::MemoryDb;

/// What the next fetch does.
#[derive(Debug, Clone, Copy)]
pub(super) enum Fate {
    /// Succeeds after this many milliseconds, if the provider is still up.
    Ok(u64),
    Fail(u64),
    /// Never ends: only a cancel or a crash stops it.
    Hang,
}

/// What the model says the table holds for an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Model {
    pub version: Version,
    pub queued_at: u64,
    pub priority: u8,
    pub identity: u8,
    pub providers: BTreeSet<u8>,
}

#[derive(Default)]
pub(super) struct Shared {
    /// Identities whose content is durable, per object: the content store that survives
    /// crashes.
    pub durable: BTreeMap<u8, BTreeSet<u8>>,
    /// The peers whose fetches succeed.
    pub up: BTreeSet<u8>,
    pub fates: Vec<Fate>,
    pub fate_at: usize,
    /// Every fetch ends in `Ok(10)`, whatever the script says.
    pub reliable: bool,
    /// Fetches whose futures are alive: object to provider.
    pub in_flight: BTreeMap<u8, u8>,
    pub cap: usize,
    /// Every fetch started: object, provider, version.
    pub started: Vec<(u8, u8, Version)>,
    /// Every landing: object, version, whether it deleted.
    pub landings: Vec<(u8, Version, bool)>,
    pub violations: Vec<String>,
    pub model: BTreeMap<u8, Model>,
    pub next_version: u64,
    /// How long a landing takes.
    pub land_delay: Duration,
    /// The content lands in the wants table's own transaction (law 12).
    pub shared_store: bool,
    /// While set, the next fetch announces its content durable and hangs: a crash point
    /// between the fetch and the landing.
    pub crash_after_content: Option<Arc<tokio::sync::Notify>>,
    /// While set, a landing fails as a store error would.
    pub fail_landings: bool,
}

#[derive(Clone)]
pub(super) struct World {
    pub db: MemoryDb,
    pub wants: WantsTable<Table>,
    /// The backend's own table, written in the transaction that records a want (law 1).
    pub membership: Table,
    pub shared: Arc<Mutex<Shared>>,
}

impl World {
    pub(super) fn new(cap: usize, shared_store: bool) -> Self {
        let db = MemoryDb::default();
        let wants = WantsTable::new(db.table());
        let membership = db.table();
        let shared = Shared {
            cap,
            shared_store,
            next_version: 1,
            ..default()
        };
        Self {
            db,
            wants,
            membership,
            shared: Arc::new(Mutex::new(shared)),
        }
    }

    pub(super) fn with<R>(&self, f: impl FnOnce(&mut Shared) -> R) -> R {
        f(&mut self.shared.lock().unwrap())
    }

    pub(super) fn violations(&self) -> Vec<String> {
        self.with(|shared| shared.violations.clone())
    }

    /// Record a want and the backend's membership in one transaction, as a backend's `record`
    /// does, and apply it to the model written independently of [`Want::record`].
    pub(super) fn record(&self, n: u8, provider: u8, priority: u8, id: u8, now_ms: u64) {
        let version = self.with(|shared| {
            shared.next_version += 1;
            shared.next_version
        });
        let mut tx = self.db.begin();
        let advert = Advert {
            provider: peer(provider),
            priority,
            identity: identity(id),
            version,
            parts: default(),
        };
        block_on(self.wants.record(&mut tx, &obj(n), advert, now_ms)).unwrap();
        block_on(self.membership.apply(
            &mut tx,
            vec![crate::okv::Mutation::Put {
                key: vec![n],
                value: vec![id],
            }],
        ))
        .unwrap();
        tx.commit().unwrap();
        self.with(|shared| match shared.model.get_mut(&n) {
            None => {
                shared.model.insert(
                    n,
                    Model {
                        version,
                        queued_at: now_ms,
                        priority,
                        identity: id,
                        providers: [provider].into(),
                    },
                );
            }
            Some(held) if held.identity == id => {
                held.priority = priority;
                held.providers.insert(provider);
            }
            Some(held) => {
                held.version = version;
                held.identity = id;
                held.priority = priority;
                held.providers = [provider].into();
            }
        });
    }

    pub(super) fn withdraw(&self, n: u8) {
        let mut tx = self.db.begin();
        let withdrawn = block_on(self.wants.withdraw(&mut tx, &obj(n))).unwrap();
        tx.commit().unwrap();
        let live = self.with(|shared| shared.model.remove(&n).is_some());
        assert_eq!(
            withdrawn, live,
            "a withdrawal tombstones exactly a live want"
        );
    }

    /// The table's live rows, as the model says they are.
    pub(super) fn check_table(&self) {
        let mut tx = self.db.begin();
        for n in 0..8u8 {
            let row = block_on(self.wants.store().get(&mut tx, &vec![n]))
                .unwrap()
                .map(|row| Want::decode(&row));
            let model = self.with(|shared| shared.model.get(&n).cloned());
            let row = row.map(|row| Model {
                version: row.version,
                queued_at: row.queued_at,
                priority: row.priority,
                identity: row.identity.as_bytes()[0],
                providers: row.providers.iter().map(|p| p.as_bytes()[0]).collect(),
            });
            assert_eq!(row, model, "the table's row of object {n}");
        }
    }

    /// Judge a landing against the model and apply it (law 12).
    fn landed(&self, n: u8, version: Version, deleted: bool) {
        self.with(|shared| {
            shared.landings.push((n, version, deleted));
            let held = shared.model.get(&n).cloned();
            let expected = held.as_ref().is_some_and(|held| held.version == version);
            if deleted != expected {
                shared.violations.push(format!(
                    "landing {n}@{version} deleted={deleted}, model {held:?}"
                ));
            }
            if deleted {
                let held = held.unwrap_or_else(|| panic!("landing deleted a want the model lacks"));
                let durable = shared
                    .durable
                    .get(&n)
                    .is_some_and(|ids| ids.contains(&held.identity));
                if !durable {
                    shared.violations.push(format!(
                        "want {n}@{version} landed before its content was durable"
                    ));
                }
                shared.model.remove(&n);
            }
        });
    }
}

/// The fetch futures alive, for the one-per-object and cap checks. Dropping the future (a cancel,
/// a crash) ends the flight.
struct Flight {
    shared: Arc<Mutex<Shared>>,
    obj: u8,
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.shared.lock().unwrap().in_flight.remove(&self.obj);
    }
}

#[derive(Clone)]
pub(super) struct WorldBackend(pub World);

impl WorldBackend {
    /// One transaction: `work` on it, then commit; a conflict runs it again.
    fn transact<R>(&self, mut work: impl FnMut(&mut crate::okv::memory::MemoryTx) -> R) -> R {
        loop {
            let mut tx = self.0.db.begin();
            let result = work(&mut tx);
            if tx.commit().is_ok() {
                return result;
            }
        }
    }
}

impl OutboxBackend for WorldBackend {
    async fn fetch(&self, job: &Job) -> Res<Fetched> {
        let world = &self.0;
        let (n, provider) = (job.obj.as_bytes()[0], job.provider.as_bytes()[0]);
        let id = job.identity.as_bytes()[0];
        let fate = world.with(|shared| {
            if shared.in_flight.insert(n, provider).is_some() {
                shared
                    .violations
                    .push(format!("a second fetch of object {n} beside a live one"));
            }
            if shared.in_flight.len() > shared.cap {
                shared.violations.push(format!(
                    "{} fetches alive, cap {}",
                    shared.in_flight.len(),
                    shared.cap
                ));
            }
            shared.started.push((n, provider, job.version));
            if shared.reliable || shared.fates.is_empty() {
                return Fate::Ok(10);
            }
            let fate = shared.fates[shared.fate_at % shared.fates.len()];
            shared.fate_at += 1;
            fate
        });
        let _flight = Flight {
            shared: Arc::clone(&world.shared),
            obj: n,
        };
        match fate {
            Fate::Hang => std::future::pending().await,
            Fate::Fail(ms) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                eyre::bail!("scripted failure");
            }
            Fate::Ok(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
        if !world.with(|shared| shared.up.contains(&provider)) {
            eyre::bail!("the provider is down");
        }
        if world.with(|shared| shared.shared_store) {
            // the content and the landing in one transaction
            let deleted = self.transact(|tx| {
                let deleted = block_on(world.wants.land(tx, &job.obj, job.version)).unwrap();
                block_on(world.membership.apply(
                    tx,
                    vec![crate::okv::Mutation::Put {
                        key: vec![200 + n],
                        value: vec![id],
                    }],
                ))
                .unwrap();
                deleted
            });
            world.with(|shared| shared.durable.entry(n).or_default().insert(id));
            world.landed(n, job.version, deleted);
            return Ok(Fetched::Landed { deleted });
        }
        world.with(|shared| shared.durable.entry(n).or_default().insert(id));
        if let Some(armed) = world.with(|shared| shared.crash_after_content.clone()) {
            armed.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(Fetched::Durable)
    }

    async fn land(&self, obj: &ObjKey, version: Version) -> Res<bool> {
        let world = &self.0;
        let delay = world.with(|shared| shared.land_delay);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if world.with(|shared| shared.fail_landings) {
            eyre::bail!("the table failed a landing");
        }
        let deleted = self.transact(|tx| block_on(world.wants.land(tx, obj, version)).unwrap());
        world.landed(obj.as_bytes()[0], version, deleted);
        Ok(deleted)
    }
}
