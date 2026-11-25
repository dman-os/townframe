//! The table's laws (1 to 3 and 12) over a memory okv table.

use futures::executor::block_on;

use super::*;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;
use crate::rev::RevSource;

pub(in crate::durable_outbox) type Table = MemoryOkv<Vec<u8>, Vec<u8>>;

pub(in crate::durable_outbox) struct Fixture {
    pub db: MemoryDb,
    pub wants: WantsTable<Table>,
}

impl Fixture {
    pub(in crate::durable_outbox) fn new() -> Self {
        let db = MemoryDb::default();
        let wants = WantsTable::new(db.table());
        Self { db, wants }
    }

    pub(in crate::durable_outbox) fn record(
        &self,
        n: u8,
        provider: u8,
        priority: u8,
        identity_n: u8,
        version: u64,
        now_ms: u64,
    ) -> Recorded {
        let mut tx = self.db.begin();
        let advert = Advert {
            provider: peer(provider),
            priority,
            identity: identity(identity_n),
            version,
        };
        let recorded = block_on(self.wants.record(&mut tx, &obj(n), advert, now_ms)).unwrap();
        tx.commit().unwrap();
        recorded
    }

    pub(in crate::durable_outbox) fn row(&self, n: u8) -> Option<Want> {
        let mut tx = self.db.begin();
        let row = block_on(self.wants.store().get(&mut tx, &obj(n).as_bytes().to_vec())).unwrap();
        row.map(|row| Want::decode(&row))
    }

    pub(in crate::durable_outbox) fn latest_rev(&self) -> u64 {
        block_on(self.wants.store().latest_rev()).unwrap()
    }

    pub(in crate::durable_outbox) fn land(&self, n: u8, version: u64) -> bool {
        let mut tx = self.db.begin();
        let landed = block_on(self.wants.land(&mut tx, &obj(n), version)).unwrap();
        tx.commit().unwrap();
        landed
    }

    pub(in crate::durable_outbox) fn withdraw(&self, n: u8) -> bool {
        let mut tx = self.db.begin();
        let withdrawn = block_on(self.wants.withdraw(&mut tx, &obj(n))).unwrap();
        tx.commit().unwrap();
        withdrawn
    }
}

/// A want and the backend's own write are one commit: a rollback keeps neither, a commit both.
#[test]
fn law_1_a_want_is_atomic_with_the_callers_writes() {
    let fixture = Fixture::new();
    let membership: Table = fixture.db.table();
    let write = |commit: bool| {
        let mut tx = fixture.db.begin();
        let advert = Advert {
            provider: peer(1),
            priority: 0,
            identity: identity(1),
            version: 1,
        };
        block_on(fixture.wants.record(&mut tx, &obj(1), advert, 0)).unwrap();
        block_on(crate::okv::OkvStore::apply(
            &membership,
            &mut tx,
            vec![crate::okv::Mutation::Put {
                key: vec![1],
                value: vec![1],
            }],
        ))
        .unwrap();
        if commit {
            tx.commit().unwrap();
        }
    };
    write(false);
    assert_eq!(fixture.row(1), None, "a rolled back record leaves no want");
    assert_eq!(block_on(membership.latest_rev()).unwrap(), 0);
    write(true);
    assert!(fixture.row(1).is_some());
    assert_eq!(block_on(membership.latest_rev()).unwrap(), 1);
}

#[test]
fn law_2_the_first_seen_time_survives_every_overwrite() {
    let fixture = Fixture::new();
    assert_eq!(fixture.record(1, 1, 0, 1, 10, 1_000), Recorded::Created);
    assert_eq!(fixture.record(1, 2, 3, 1, 11, 5_000), Recorded::Updated);
    assert_eq!(fixture.record(1, 2, 3, 2, 12, 9_000), Recorded::Superseded);
    assert_eq!(fixture.row(1).unwrap().queued_at, 1_000);
}

#[test]
fn law_2_the_version_changes_exactly_when_the_identity_does() {
    let fixture = Fixture::new();
    fixture.record(1, 1, 0, 1, 10, 0);
    fixture.record(1, 2, 5, 1, 11, 0);
    assert_eq!(
        fixture.row(1).unwrap().version,
        10,
        "an equal identity keeps it"
    );
    fixture.record(1, 2, 5, 2, 12, 0);
    assert_eq!(fixture.row(1).unwrap().version, 12);
}

#[test]
fn law_2_providers_union_under_an_identity_and_reset_under_a_new_one() {
    let fixture = Fixture::new();
    fixture.record(1, 1, 0, 1, 10, 0);
    fixture.record(1, 2, 0, 1, 11, 0);
    let providers = |n| fixture.row(n).unwrap().providers;
    assert_eq!(providers(1), [peer(1), peer(2)].into());
    fixture.record(1, 3, 0, 2, 12, 0);
    assert_eq!(
        providers(1),
        [peer(3)].into(),
        "the others advertised the old one"
    );
}

#[test]
fn law_2_the_newest_priority_wins() {
    let fixture = Fixture::new();
    fixture.record(1, 1, 5, 1, 10, 0);
    fixture.record(1, 1, 1, 1, 11, 0);
    assert_eq!(fixture.row(1).unwrap().priority, 1);
}

/// A record that changes nothing writes nothing: no rev, so nobody wakes.
#[test]
fn law_2_a_record_that_changes_nothing_writes_nothing() {
    let fixture = Fixture::new();
    fixture.record(1, 1, 2, 1, 10, 0);
    let rev = fixture.latest_rev();
    assert_eq!(fixture.record(1, 1, 2, 1, 99, 500), Recorded::Unchanged);
    assert_eq!(fixture.latest_rev(), rev);
}

#[test]
#[should_panic(expected = "a version is never reused")]
fn law_2_a_reused_version_for_a_new_identity_crashes() {
    let fixture = Fixture::new();
    fixture.record(1, 1, 0, 1, 10, 0);
    fixture.record(1, 1, 0, 2, 10, 0);
}

#[test]
fn law_3_a_withdrawal_is_a_tombstone_written_only_for_a_live_want() {
    let fixture = Fixture::new();
    assert!(!fixture.withdraw(1), "nothing to withdraw");
    assert_eq!(fixture.latest_rev(), 0, "and nothing written");
    fixture.record(1, 1, 0, 1, 10, 0);
    let rev = fixture.latest_rev();
    assert!(fixture.withdraw(1));
    assert_eq!(fixture.latest_rev(), rev + 1);
    assert_eq!(fixture.row(1), None);
    assert!(!fixture.withdraw(1), "a tombstone is not a want");
}

#[test]
fn law_12_a_landing_deletes_only_the_version_it_fetched() {
    let fixture = Fixture::new();
    fixture.record(1, 1, 0, 1, 10, 0);
    fixture.record(1, 1, 0, 2, 11, 0);
    assert!(!fixture.land(1, 10), "the want moved on to version 11");
    assert_eq!(fixture.row(1).unwrap().version, 11);
    assert!(fixture.land(1, 11));
    assert_eq!(fixture.row(1), None);
    assert!(!fixture.land(1, 11), "already gone");
    assert!(!fixture.land(9, 1), "never there");
}
