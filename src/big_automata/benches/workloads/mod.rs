//! What the instruction-count bench (`instr`) and the wall-clock bench (`wall`) both measure:
//! the setup of each workload, built the same way for both, and the measured bodies.
//! Only the crate's public API is reachable from `benches/`; checks that need its internals are
//! the allocation-count tests in `scaling`.

use big_automata::ObjKey;
use big_automata::PartKey;
use big_automata::PeerKey;
use big_automata::durable_outbox::Command;
use big_automata::durable_outbox::Config;
use big_automata::durable_outbox::Ended;
use big_automata::durable_outbox::Fetched;
use big_automata::durable_outbox::Outbox;
use big_automata::durable_outbox::Want;
use big_automata::okv::Mutation;
use big_automata::okv::OkvStore;
use big_automata::okv::Selector;
use big_automata::okv::memory::MemoryDb;
use big_automata::okv::memory::MemoryOkv;
use big_automata::part_store::AccessLevel;
use big_automata::part_store::AuthorizedPage;
use big_automata::part_store::LogRoutes;
use big_automata::part_store::PartStoreServe;
use big_automata::part_store::PartStoreWrite;
use big_automata::part_store::memory::MemoryPartStore;
use big_automata::replay::server::ReplayServer;
use big_automata::replay::wire::Route;
use big_automata::replay::wire::UpdateSession;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::RevSource;
use big_automata::rev::RevStream;
use futures::executor::block_on;
use web_time::Instant;

pub type Table = MemoryOkv<Vec<u8>, Vec<u8>>;

fn limits(max_entries: usize) -> Limits {
    Limits {
        max_entries: max_entries.try_into().expect("non-zero"),
        max_bytes: None,
    }
}

fn key(row: usize) -> Vec<u8> {
    u32::try_from(row)
        .expect("rows fit u32")
        .to_be_bytes()
        .to_vec()
}

fn put(row: usize) -> Mutation<Vec<u8>, Vec<u8>> {
    Mutation::Put {
        key: key(row),
        value: vec![7; 64],
    }
}

// # okv

pub fn empty_table(rows: usize) -> (usize, (MemoryDb, Table)) {
    let db = MemoryDb::default();
    let table = db.table();
    (rows, (db, table))
}

pub fn filled_table(rows: usize) -> (MemoryDb, Table) {
    let (_, (db, table)) = empty_table(rows);
    let mut tx = db.begin();
    for row in 0..rows {
        block_on(table.apply(&mut tx, vec![put(row)])).expect("apply");
    }
    tx.commit().expect("commit");
    (db, table)
}

/// One batch per row in one transaction, then the commit.
pub fn okv_write((rows, (db, table)): (usize, (MemoryDb, Table))) -> usize {
    let mut tx = db.begin();
    for row in 0..rows {
        block_on(table.apply(&mut tx, vec![put(row)])).expect("apply");
    }
    tx.commit().expect("commit");
    rows
}

/// The whole log, 256 rows a page.
pub fn okv_page_the_log((_db, table): (MemoryDb, Table)) -> u64 {
    let latest = block_on(table.latest_rev()).expect("latest");
    let mut page = Page::default();
    let mut after = 0;
    while after < latest {
        block_on(table.read(&Selector::All, after, latest, limits(256), &mut page)).expect("read");
        after = page.through;
    }
    after
}

pub fn rev_stream_replay((_db, table): (MemoryDb, Table)) -> u64 {
    let mut stream = RevStream::new(table, Selector::All, 0);
    let mut page = Page::default();
    while !stream.caught_up() {
        block_on(stream.next_into(&mut page, limits(256))).expect("page");
    }
    page.through
}

// # part store

fn part() -> PartKey {
    PartKey::new(&[1])
}

fn reader() -> PeerKey {
    PeerKey::new(&[2])
}

/// A part of `members` live members, readable by the reader.
pub fn filled_part(members: usize) -> MemoryPartStore {
    let store = MemoryPartStore::default();
    block_on(async {
        let mut tx = store.begin();
        store
            .grant_access(&mut tx, &[(part(), reader(), AccessLevel::Read)])
            .await
            .expect("grant");
        tx.commit();
        for start in (0..members).step_by(100) {
            let chunk = start..(start + 100).min(members);
            let obj = |row: usize| ObjKey::new(&key(row));
            let mut tx = store.begin();
            store
                .set_payloads(
                    &mut tx,
                    chunk
                        .clone()
                        .map(|row| (obj(row), serde_json::json!({ "row": row })))
                        .collect(),
                )
                .await
                .expect("payloads");
            store
                .add_members(
                    &mut tx,
                    &chunk.map(|row| (obj(row), part())).collect::<Vec<_>>(),
                )
                .await
                .expect("members");
            tx.commit();
        }
    });
    store
}

/// The part's log through the authorized read, 256 rows a page.
pub fn part_store_page_the_log(store: MemoryPartStore) -> u64 {
    let latest = block_on(PartStoreServe::latest_rev(&store)).expect("latest");
    let routes = LogRoutes {
        parts: [(part(), 0)].into(),
        objects: Default::default(),
    };
    let mut page = AuthorizedPage::default();
    let mut after = 0;
    while after < latest {
        block_on(store.read_authorized(&reader(), &routes, after, latest, limits(256), &mut page))
            .expect("read");
        after = page.rows.through;
    }
    after
}

// # replay server

fn route(row: usize) -> (u32, Route) {
    (
        u32::try_from(row).expect("rows fit u32"),
        Route::Part {
            part_key: PartKey::new(&key(row)),
        },
    )
}

/// A server holding one session of `targets` part targets.
pub fn opened(targets: usize) -> (usize, ReplayServer) {
    let mut server = ReplayServer::default();
    let open = UpdateSession {
        session_id: uuid::Uuid::from_u128(1),
        generation: 0,
        additions: (0..targets).map(route).collect(),
        removals: vec![],
    };
    server
        .update(&reader(), &open, Instant::now(), |_| true)
        .expect("opens");
    (targets, server)
}

/// Every target replaced in one update: near the server's 16,384 cap, where the id-reuse check
/// was once quadratic.
pub fn replay_replace_every_target((targets, mut server): (usize, ReplayServer)) -> u64 {
    let replace = UpdateSession {
        session_id: uuid::Uuid::from_u128(1),
        generation: 1,
        additions: (targets..2 * targets).map(route).collect(),
        removals: (0..targets).map(|row| route(row).0).collect(),
    };
    server
        .update(&reader(), &replace, Instant::now(), |_| true)
        .expect("applies")
        .generation
}

// # durable outbox machine

/// `rows` wants advertised by one connected provider.
pub fn wants(rows: usize) -> (usize, Outbox) {
    let provider = PeerKey::new(&[1]);
    let mut outbox = Outbox::new(Config::default());
    outbox.rows((0..rows).map(|row| {
        let obj = ObjKey::new(&key(row));
        let want = Want {
            version: row as u64 + 1,
            queued_at: row as u64,
            priority: (row % 5) as u8,
            identity: obj.0.clone(),
            providers: [provider.clone()].into(),
            named: Default::default(),
        };
        (obj, Some(want))
    }));
    outbox.peer_connected(provider);
    (rows, outbox)
}

/// Every want fetched and landed with `C` and `M` at their defaults: a flush per completion.
pub fn outbox_machine_fetches_every_want((rows, mut outbox): (usize, Outbox)) -> usize {
    let now = Instant::now();
    let mut commands = vec![];
    let mut landed = 0;
    outbox.flush(now, &mut commands);
    while landed < rows {
        for command in std::mem::take(&mut commands) {
            match command {
                Command::Fetch { call, .. } => {
                    outbox.fetch_ended(now, call, Ended::Fetched(Fetched::Durable));
                }
                Command::Land { obj, version } => {
                    outbox.landed(&obj, version, true);
                    landed += 1;
                }
                Command::Cancel { .. } => unreachable!("nothing is superseded"),
            }
        }
        outbox.flush(now, &mut commands);
    }
    landed
}
