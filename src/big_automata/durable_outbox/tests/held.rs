//! The held wants' laws (16 to 20) over a memory table, and the table against a naive model.

use futures::executor::block_on;
use proptest::prelude::*;

use super::*;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;

type Table = MemoryOkv<Vec<u8>, Vec<u8>>;

const SECOND: u64 = 1000;

struct Fixture {
    db: MemoryDb,
    held: HeldTable<Table>,
    /// A wants table beside it, the one the machine follows.
    wants: WantsTable<Table>,
}

impl Fixture {
    fn new(max_per_peer: usize, ttl_secs: u64) -> Self {
        let db = MemoryDb::default();
        let config = HeldConfig {
            max_per_peer,
            ttl: Duration::from_secs(ttl_secs),
        };
        Self {
            held: HeldTable::new(db.table(), config),
            wants: WantsTable::new(db.table()),
            db,
        }
    }

    /// Another handle on the same rows, as a restart makes.
    fn reopened(&self) -> HeldTable<Table> {
        HeldTable::new(self.held.store().clone(), self.held.config())
    }

    fn hold(&self, n: u8, provider: u8, identity_n: u8, now_ms: u64) -> Held {
        self.hold_at(&self.held, n, provider, identity_n, now_ms)
    }

    fn hold_at(
        &self,
        held: &HeldTable<Table>,
        n: u8,
        provider: u8,
        identity_n: u8,
        now_ms: u64,
    ) -> Held {
        let mut tx = self.db.begin();
        let advert = Advert::new(peer(provider), 0, identity(identity_n));
        let outcome = block_on(held.hold(&mut tx, &obj(n), advert, now_ms)).unwrap();
        tx.commit().unwrap();
        outcome
    }

    fn release(&self, n: u8) -> Option<Want> {
        let mut tx = self.db.begin();
        let want = block_on(self.held.release(&mut tx, &obj(n))).unwrap();
        tx.commit().unwrap();
        want
    }

    fn expire(&self, now_ms: u64) -> Vec<Dropped> {
        let mut tx = self.db.begin();
        let dropped = block_on(self.held.expire(&mut tx, now_ms)).unwrap();
        tx.commit().unwrap();
        dropped
    }

    fn row(&self, n: u8) -> Option<Want> {
        block_on(self.held.get(&mut self.db.begin(), &obj(n))).unwrap()
    }

    fn held_by(&self, provider: u8) -> usize {
        block_on(self.held.held_by(&peer(provider))).unwrap()
    }

    /// The objects among `1..=max` that hold a row, with their providers.
    fn rows(&self, max: u8) -> Vec<(u8, Vec<u8>)> {
        (1..=max)
            .filter_map(|n| {
                let want = self.row(n)?;
                Some((n, want.providers.iter().map(|p| p.as_bytes()[0]).collect()))
            })
            .collect()
    }
}

#[test]
fn law_16_a_held_want_is_a_want_the_machine_never_sees() {
    let fixture = Fixture::new(8, 60);
    let held = fixture.hold(1, 1, 7, 0);
    assert_eq!(held.recorded, Recorded::Created);
    let want = fixture.row(1).unwrap();
    assert_eq!(want.queued_at, 0);
    assert_eq!(want.identity, identity(7));
    assert_eq!(want.providers, [peer(1)].into());
    // the wants table, which the machine follows, has nothing
    assert_eq!(
        block_on(crate::rev::RevSource::latest_rev(fixture.wants.store())).unwrap(),
        0
    );

    // a replay of the parked notification is the same row and writes nothing (law 2)
    let rev = block_on(crate::rev::RevSource::latest_rev(fixture.held.store())).unwrap();
    assert_eq!(
        fixture.hold(1, 1, 7, 5 * SECOND).recorded,
        Recorded::Unchanged
    );
    assert_eq!(
        block_on(crate::rev::RevSource::latest_rev(fixture.held.store())).unwrap(),
        rev
    );
    assert_eq!(fixture.row(1).unwrap(), want, "a replay moved the age");

    // another peer advertising it joins the row; a new identity supersedes it
    assert_eq!(
        fixture.hold(1, 2, 7, 6 * SECOND).recorded,
        Recorded::Updated
    );
    assert_eq!(fixture.row(1).unwrap().providers.len(), 2);
    assert_eq!(
        fixture.hold(1, 3, 8, 7 * SECOND).recorded,
        Recorded::Superseded
    );
    let want = fixture.row(1).unwrap();
    assert_eq!(want.providers, [peer(3)].into());
    assert_eq!(
        want.queued_at, 0,
        "a supersession keeps the first-seen time"
    );
    assert_eq!(fixture.held_by(1), 0);
    assert_eq!(fixture.held_by(3), 1);
}

#[test]
fn law_17_a_peer_over_the_cap_loses_its_oldest_and_no_other_peer_loses_anything() {
    let fixture = Fixture::new(3, 3600);
    for n in 1..=3 {
        assert!(
            fixture
                .hold(n, 1, n, u64::from(n) * SECOND)
                .dropped
                .is_empty()
        );
    }
    // peer 2's wants are its own, and under the cap
    for n in 11..=12 {
        assert!(
            fixture
                .hold(n, 2, n, u64::from(n) * SECOND)
                .dropped
                .is_empty()
        );
    }
    // peer 1's fourth takes its first
    let held = fixture.hold(4, 1, 4, 20 * SECOND);
    assert_eq!(
        held.dropped,
        vec![Dropped::Cap {
            obj: obj(1),
            peer: peer(1)
        }]
    );
    assert_eq!(fixture.row(1), None, "a sole provider's row is deleted");
    assert_eq!(
        fixture.rows(12).iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        [2, 3, 4, 11, 12]
    );
    assert_eq!((fixture.held_by(1), fixture.held_by(2)), (3, 2));

    // a want that two peers advertise survives the drop of one of them
    fixture.hold(5, 1, 5, 21 * SECOND);
    assert_eq!(fixture.row(2), None);
    fixture.hold(3, 2, 3, 22 * SECOND);
    assert_eq!(fixture.row(3).unwrap().providers.len(), 2);
    let held = fixture.hold(6, 1, 6, 23 * SECOND);
    assert_eq!(
        held.dropped,
        vec![Dropped::Cap {
            obj: obj(3),
            peer: peer(1)
        }]
    );
    assert_eq!(
        fixture.row(3).unwrap().providers,
        [peer(2)].into(),
        "peer 2's advert of the same want survives peer 1's drop"
    );
    assert_eq!((fixture.held_by(1), fixture.held_by(2)), (3, 3));
}

#[test]
fn law_17_ties_in_age_drop_the_smaller_object_first() {
    let fixture = Fixture::new(2, 3600);
    for n in [3, 1, 2] {
        fixture.hold(n, 1, n, 0);
    }
    assert_eq!(
        fixture.rows(3).iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        [2, 3]
    );
}

#[test]
fn law_18_a_want_outlives_its_ttl_by_nothing_whatever_re_advertises_it() {
    let fixture = Fixture::new(8, 15);
    fixture.hold(1, 1, 1, 0);
    fixture.hold(2, 1, 2, 10 * SECOND);
    assert_eq!(fixture.expire(15 * SECOND - 1), vec![]);
    assert_eq!(
        fixture.expire(15 * SECOND),
        vec![Dropped::Ttl { obj: obj(1) }]
    );
    fixture.hold(3, 1, 3, 20 * SECOND);
    assert_eq!(
        fixture.rows(3).iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(fixture.held_by(1), 2);

    assert_eq!(
        fixture.expire(25 * SECOND),
        vec![Dropped::Ttl { obj: obj(2) }]
    );

    // another peer re-advertising object 3 does not move its first-seen time
    fixture.hold(3, 2, 3, 34 * SECOND);
    assert_eq!(fixture.row(3).unwrap().queued_at, 20 * SECOND);
    // and every hold expires too: this one finds object 3 (first seen at 20s) due at 35s
    let held = fixture.hold(4, 1, 4, 35 * SECOND);
    assert_eq!(held.dropped, vec![Dropped::Ttl { obj: obj(3) }]);
    assert_eq!(
        fixture.rows(4).iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        [4]
    );
    assert_eq!((fixture.held_by(1), fixture.held_by(2)), (1, 0));
}

#[test]
fn law_19_a_release_hands_the_want_over_once_and_frees_the_peers_room() {
    let fixture = Fixture::new(2, 3600);
    fixture.hold(1, 1, 1, 0);
    fixture.hold(2, 1, 2, SECOND);
    let held = fixture.row(1).unwrap();
    assert_eq!(fixture.release(1), Some(held));
    assert_eq!(fixture.release(1), None, "a second release finds nothing");
    assert_eq!(fixture.row(1), None);
    assert_eq!(fixture.held_by(1), 1);
    // the room is the peer's again: no drop for a want that fits
    assert!(fixture.hold(3, 1, 3, 2 * SECOND).dropped.is_empty());
    // the released want is handed to the wants table in the caller's transaction
    let mut tx = fixture.db.begin();
    let want = block_on(fixture.held.release(&mut tx, &obj(2)))
        .unwrap()
        .unwrap();
    for provider in &want.providers {
        let advert = Advert {
            provider: provider.clone(),
            priority: want.priority,
            identity: want.identity.clone(),
            version: want.version,
        };
        block_on(
            fixture
                .wants
                .record(&mut tx, &obj(2), advert, want.queued_at),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(fixture.row(2), None);
    let live = block_on(
        fixture
            .wants
            .store()
            .get(&mut fixture.db.begin(), &obj(2).as_bytes().to_vec()),
    )
    .unwrap()
    .map(|row| Want::decode(&row))
    .unwrap();
    assert_eq!(live.providers, [peer(1)].into());
    assert_eq!(live.queued_at, SECOND, "the want keeps its age");
}

#[test]
fn law_20_a_dropped_want_comes_back_only_by_another_advert() {
    let fixture = Fixture::new(1, 3600);
    fixture.hold(1, 1, 1, 0);
    fixture.hold(2, 1, 2, SECOND);
    assert_eq!(fixture.row(1), None);
    // nothing remembers object 1: its next advert is a new want with a new age
    assert_eq!(
        fixture.hold(1, 2, 1, 5 * SECOND).recorded,
        Recorded::Created
    );
    assert_eq!(fixture.row(1).unwrap().queued_at, 5 * SECOND);
}

#[test]
fn a_restart_rebuilds_the_index_from_the_table() {
    let fixture = Fixture::new(2, 15);
    fixture.hold(1, 1, 1, 0);
    fixture.hold(2, 1, 2, 10 * SECOND);
    let reopened = fixture.reopened();
    assert_eq!(block_on(reopened.held_by(&peer(1))).unwrap(), 2);
    // the reopened table enforces the cap and the TTL over what it found
    let held = fixture.hold_at(&reopened, 3, 1, 3, 12 * SECOND);
    assert_eq!(
        held.dropped,
        vec![Dropped::Cap {
            obj: obj(1),
            peer: peer(1)
        }]
    );
    let held = fixture.hold_at(&reopened, 4, 2, 4, 25 * SECOND);
    assert_eq!(held.dropped, vec![Dropped::Ttl { obj: obj(2) }]);
}

#[test]
fn a_rolled_back_hold_leaves_a_stale_entry_that_never_costs_a_real_want() {
    let fixture = Fixture::new(2, 3600);
    {
        let mut tx = fixture.db.begin();
        let advert = Advert::new(peer(1), 0, identity(1));
        block_on(fixture.held.hold(&mut tx, &obj(1), advert, 0)).unwrap();
        // dropped without a commit
    }
    fixture.hold(2, 1, 2, SECOND);
    let held = fixture.hold(3, 1, 3, 2 * SECOND);
    assert!(
        held.dropped.is_empty(),
        "the stale entry was dropped, not a want"
    );
    assert_eq!(
        fixture.rows(3).iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        [2, 3]
    );
}

// # The naive model

const CAP: usize = 2;
const TTL_SECS: u64 = 20;

#[derive(Debug, Clone)]
enum Op {
    Hold { obj: u8, peer: u8, identity: u8 },
    Release { obj: u8 },
    Expire,
}

/// Every decision recomputed from the whole state: no index, no incremental count.
#[derive(Default)]
struct Model {
    rows: BTreeMap<ObjKey, Want>,
}

impl Model {
    fn expire(&mut self, now_ms: u64) {
        self.rows
            .retain(|_, want| want.queued_at + TTL_SECS * SECOND > now_ms);
    }

    fn hold(&mut self, obj: &ObjKey, advert: Advert, now_ms: u64) {
        self.expire(now_ms);
        let provider = advert.provider.clone();
        if let Some((want, _)) = Want::record(self.rows.get(obj), advert, now_ms) {
            self.rows.insert(obj.clone(), want);
        }
        loop {
            let oldest = self
                .rows
                .iter()
                .filter(|(_, want)| want.providers.contains(&provider))
                .map(|(obj, want)| (want.queued_at, obj.clone()))
                .collect::<BTreeSet<_>>();
            if oldest.len() <= CAP {
                break;
            }
            let (_, victim) = oldest.first().cloned().unwrap();
            let want = self.rows.get_mut(&victim).unwrap();
            want.providers.remove(&provider);
            if want.providers.is_empty() {
                self.rows.remove(&victim);
            }
        }
    }

    fn held_by(&self, provider: &PeerKey) -> usize {
        self.rows
            .values()
            .filter(|want| want.providers.contains(provider))
            .count()
    }
}

fn ops() -> impl Strategy<Value = Vec<(Op, u64)>> {
    let op = prop_oneof![
        8 => (0..6u8, 0..3u8, 0..2u8)
            .prop_map(|(obj, peer, identity)| Op::Hold { obj, peer, identity }),
        2 => (0..6u8).prop_map(|obj| Op::Release { obj }),
        1 => Just(Op::Expire),
    ];
    proptest::collection::vec((op, 0..8u64), 1..60)
}

proptest! {
    #[test]
    fn the_held_table_matches_the_naive_model(ops in ops()) {
        let fixture = Fixture::new(CAP, TTL_SECS);
        let mut model = Model::default();
        let mut now_ms = 0;
        for (step, (op, advance)) in ops.into_iter().enumerate() {
            now_ms += advance * SECOND;
            match op {
                Op::Hold { obj: n, peer: p, identity: i } => {
                    // the version the want gets if the advert changes its identity
                    let version = step as u64 + 1;
                    let advert = Advert {
                        provider: peer(p),
                        priority: 0,
                        identity: identity(i),
                        version,
                    };
                    model.hold(&obj(n), advert.clone(), now_ms);
                    let mut tx = fixture.db.begin();
                    block_on(fixture.held.hold(&mut tx, &obj(n), advert, now_ms)).unwrap();
                    tx.commit().unwrap();
                }
                Op::Release { obj: n } => {
                    let expected = model.rows.remove(&obj(n));
                    prop_assert_eq!(fixture.release(n), expected);
                }
                Op::Expire => {
                    model.expire(now_ms);
                    fixture.expire(now_ms);
                }
            }
            for n in 0..6 {
                prop_assert_eq!(fixture.row(n), model.rows.get(&obj(n)).cloned(), "object {}", n);
            }
            for p in 0..3 {
                prop_assert_eq!(fixture.held_by(p), model.held_by(&peer(p)), "peer {}", p);
            }
        }
    }
}
