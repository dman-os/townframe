use crate::Authority;
use crate::DatumFeeds;
use crate::DatumLease;
use crate::DatumSelector;
use crate::FeedError;
use crate::RowKey;
use crate::fake::Grant;
use crate::fake::MemoryCabinet;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task::JoinHandle;

fn doc(byte: u8) -> DocId {
    DocId::new([byte; 32])
}

fn key(tag: &str) -> DatumKey {
    DatumKey::from_text(tag, "main").unwrap()
}

const DRAWER: u8 = 0xd0;

/// Notes tagged `org.example.note` in one drawer.
fn notes() -> DatumSelector {
    DatumSelector {
        predicate: Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: doc(DRAWER),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasTag {
                    tag: DatumTag::new("org.example.note").unwrap(),
                },
            ],
        },
        datums: DatumSelection::All,
    }
}

struct Rig {
    cabinet: MemoryCabinet,
    feeds: DatumFeeds,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Rig {
    fn new() -> Self {
        let tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let spawned = Arc::clone(&tasks);
        let feeds = DatumFeeds::new(
            Arc::new(move |task| spawned.lock().unwrap().push(tokio::spawn(task))),
            64,
            15_000,
        );
        Self {
            cabinet: MemoryCabinet::default(),
            feeds,
            tasks,
        }
    }

    /// A micro granted Read on `drawers`.
    fn micro(&self, name: &str, drawers: &[u8]) -> Authority {
        let capability = CapId::new_random();
        self.cabinet.grant(
            capability,
            Grant::Drawers(drawers.iter().copied().map(doc).collect()),
        );
        Authority {
            id: format!("micros/notes/{name}"),
            node: Arc::new(self.cabinet.clone()),
            capability,
        }
    }

    fn note(&self, byte: u8) {
        self.cabinet
            .write_datum(&doc(byte), &key("org.example.note"));
        self.cabinet.add_to_drawer(&doc(DRAWER), &doc(byte));
    }

    async fn acquire(&self, authority: &Authority) -> Result<DatumLease, FeedError> {
        self.feeds.acquire(authority, notes()).await
    }

    /// Awaits every feed loop, which must each have ended cleanly.
    async fn ended(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            task.await.unwrap();
        }
    }
}

/// Reads one page and applies it to `rows`, returning the cursor to commit next.
async fn read(
    lease: &mut DatumLease,
    committed: Option<Cursor>,
    rows: &mut BTreeSet<RowKey>,
) -> Result<(Cursor, bool), ReadError> {
    let page = lease.lease.read(committed, ReadLimits::default()).await?;
    for transaction in page.transactions {
        for change in transaction.changes {
            match change {
                Change::Upsert { key, .. } => rows.insert(key),
                Change::Remove { key, .. } => rows.remove(&key),
            };
        }
    }
    Ok((page.through, page.bootstrap_complete))
}

fn note_rows(bytes: &[u8]) -> BTreeSet<RowKey> {
    bytes
        .iter()
        .map(|byte| (doc(*byte), key("org.example.note")))
        .collect()
}

#[tokio::test]
async fn a_lease_bootstraps_then_follows_move_ins_and_move_outs() {
    let rig = Rig::new();
    let micro = rig.micro("list", &[DRAWER]);
    rig.note(1);
    rig.note(2);
    // Tagged but outside the drawer, so outside the query.
    rig.cabinet.write_datum(&doc(3), &key("org.example.note"));

    let mut lease = rig.acquire(&micro).await.unwrap();
    let mut rows = BTreeSet::new();
    let (cursor, bootstrapped) = read(&mut lease, None, &mut rows).await.unwrap();
    assert!(bootstrapped);
    assert_eq!(rows, note_rows(&[1, 2]));

    // Entering the drawer moves doc 3 in; leaving it moves doc 1 out.
    rig.cabinet.add_to_drawer(&doc(DRAWER), &doc(3));
    let (cursor, _) = read(&mut lease, Some(cursor), &mut rows).await.unwrap();
    assert_eq!(rows, note_rows(&[1, 2, 3]));
    rig.cabinet.remove_from_drawer(&doc(DRAWER), &doc(1));
    let (cursor, _) = read(&mut lease, Some(cursor), &mut rows).await.unwrap();
    assert_eq!(rows, note_rows(&[2, 3]));

    // Losing the tag the predicate names is a move-out too.
    rig.cabinet.remove_datum(&doc(2), &key("org.example.note"));
    read(&mut lease, Some(cursor), &mut rows).await.unwrap();
    assert_eq!(rows, note_rows(&[3]));
}

#[tokio::test]
async fn leases_of_one_authority_share_a_feed_and_authorities_never_do() {
    let rig = Rig::new();
    let first = rig.micro("first", &[DRAWER]);
    let second = rig.micro("second", &[DRAWER]);
    rig.note(1);

    let mut a = rig.acquire(&first).await.unwrap();
    let mut b = rig.acquire(&first).await.unwrap();
    assert_eq!(rig.feeds.live_feeds(), 1);
    // The other micro reaches the same rows, through its own feed polled with its own capability.
    let mut c = rig.acquire(&second).await.unwrap();
    assert_eq!(rig.feeds.live_feeds(), 2);
    assert!(rig.cabinet.polls(second.capability) > 0);

    for lease in [&mut a, &mut b, &mut c] {
        let mut rows = BTreeSet::new();
        read(lease, None, &mut rows).await.unwrap();
        assert_eq!(rows, note_rows(&[1]));
    }
}

#[tokio::test]
async fn the_last_lease_dropping_stops_the_feed() {
    let rig = Rig::new();
    let micro = rig.micro("list", &[DRAWER]);
    rig.note(1);
    let first = rig.acquire(&micro).await.unwrap();
    let second = rig.acquire(&micro).await.unwrap();
    drop(first);
    assert_eq!(rig.feeds.live_feeds(), 1);
    drop(second);
    assert_eq!(rig.feeds.live_feeds(), 0);
    // The loop was parked in a held poll; the drop ends it without waiting out the hold.
    rig.ended().await;
    let polls = rig.cabinet.polls(micro.capability);
    rig.note(2);
    tokio::task::yield_now().await;
    assert_eq!(rig.cabinet.polls(micro.capability), polls);
}

#[tokio::test]
async fn a_query_outside_the_authority_is_refused_and_leaves_no_feed() {
    let rig = Rig::new();
    let stranger = rig.micro("stranger", &[0xee]);
    rig.note(1);
    assert!(matches!(
        rig.acquire(&stranger).await,
        Err(FeedError::Denied)
    ));
    assert_eq!(rig.feeds.live_feeds(), 0);
    rig.ended().await;
}

#[tokio::test]
async fn a_withdrawn_grant_revokes_held_leases() {
    let rig = Rig::new();
    let micro = rig.micro("list", &[DRAWER]);
    rig.note(1);
    let mut lease = rig.acquire(&micro).await.unwrap();
    let mut rows = BTreeSet::new();
    let (cursor, _) = read(&mut lease, None, &mut rows).await.unwrap();

    // The feed is parked in a held poll; the withdrawal wakes it rather than waiting out the
    // hold, and the node's refusal ends the lease for good.
    rig.cabinet.revoke(micro.capability);
    assert_eq!(
        read(&mut lease, Some(cursor), &mut rows).await,
        Err(ReadError::Revoked)
    );
    rig.ended().await;
}
