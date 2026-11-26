//! The same fixture and bodies as `benches/workloads/mod.rs` (a module cannot be shared between
//! a bench target and the crate's tests without `#[path]`); keep the two in step.

use std::sync::LazyLock;

use crate::MIGRATIONS;
use crate::SqliteKeyline;
use ed25519_dalek::SigningKey;
use future_form::Local;
use futures::executor::block_on;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::identifier::Identifier;
use keyhive_crypto::verified::Verified;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;
use keyline::contract::Keyline;
use keyline::delegation::Delegation;
use keyline::power::Power;
use keyline::revocation::Revocation;
use keyline::test_utils::assume_verified;
use keyline::test_utils::delegation;
use keyline::test_utils::revocation;
use sqlite_rs::Database;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;

pub type Store = SqliteKeyline<(), Database>;

static RUNTIME: LazyLock<SqliteRuntime> = LazyLock::new(|| {
    SqliteRuntime::new(RuntimeConfig {
        workers: 1,
        ..RuntimeConfig::default()
    })
});

pub struct Fixture {
    pub doc: Identifier,
    pub admin: Identifier,
    pub certs: Vec<VerifiedCertificate<()>>,
}

fn key(index: usize) -> SigningKey {
    let mut seed = [0xA5; 32];
    seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
    SigningKey::from(seed)
}

/// The star with `members` members. `signed` certificates carry real signatures, which a store
/// that rebuilds from its rows verifies; unsigned ones carry zeroed signatures, which a store
/// that was told of every insert never checks.
pub fn star(members: usize, signed: bool) -> Fixture {
    let keys: Vec<SigningKey> = (0..members + 2).map(key).collect();
    let ids: Vec<Identifier> = keys.iter().map(Identifier::from).collect();
    let grant = |issuer: usize, audience: usize, power: Power| -> VerifiedCertificate<()> {
        if signed {
            Verified::sign_sync(
                &Delegation::new(ids[audience], ids[0], power),
                &keys[issuer],
            )
            .expect("a key signs")
            .into()
        } else {
            assume_verified(delegation(ids[issuer], ids[audience], ids[0], power))
        }
    };
    let revoke = |issuer: usize, target: Digest<Delegation>| -> VerifiedCertificate<()> {
        if signed {
            Verified::sign_sync(&Revocation::<()>::new(target), &keys[issuer])
                .expect("a key signs")
                .into()
        } else {
            assume_verified(revocation::<()>(ids[issuer], target))
        }
    };
    let mut certs = vec![grant(0, 1, Power::Admin)];
    for member in 2..members + 2 {
        let cert = grant(1, member, Power::Read);
        let CertificateId::Delegation(digest) = cert.id() else {
            unreachable!("a grant is a delegation")
        };
        certs.push(cert);
        if member % 10 == 0 {
            certs.push(revoke(1, digest));
        }
        if member % 7 == 0 && member + 1 < members + 2 {
            certs.push(grant(member, member + 1, Power::Read));
        }
    }
    Fixture {
        doc: ids[0],
        admin: ids[1],
        certs,
    }
}

pub fn empty_store() -> Store {
    Store::new(
        RUNTIME
            .ephemeral(MIGRATIONS)
            .expect("an ephemeral database opens"),
    )
}

/// Setup of the insert: an empty store and the batch.
pub fn to_insert(members: usize) -> (Store, Vec<VerifiedCertificate<()>>) {
    (empty_store(), star(members, false).certs)
}

/// One batch of `members` grants into an empty store, in one transaction.
pub fn insert(state: (Store, Vec<VerifiedCertificate<()>>)) -> u64 {
    let (store, certs) = state;
    let version = block_on(Keyline::<Local>::insert(&store, certs)).expect("an insert commits");
    format!("{version:?}").len() as u64
}

/// Setup of the queries: a store that inserted the star, so its cache holds the set and no
/// evaluation yet.
pub fn inserted(members: usize) -> (Store, Fixture) {
    let (store, certs) = to_insert(members);
    let fixture = star(0, false);
    block_on(Keyline::<Local>::insert(&store, certs)).expect("an insert commits");
    (
        store,
        Fixture {
            certs: Vec::new(),
            ..fixture
        },
    )
}

/// The first `members` after an insert: both strata over the whole set, then the subject's row.
pub fn members_after_insert(state: (Store, Fixture)) -> usize {
    let (store, fixture) = state;
    block_on(Keyline::<Local>::members(&store, fixture.doc))
        .expect("members answers")
        .value
        .len()
}

/// A point read through an index: the delegations the admin holds, whatever the set's size.
pub fn point_read(state: (Store, Fixture)) -> usize {
    let (store, fixture) = state;
    block_on(Keyline::<Local>::delegations_to(
        &store,
        fixture.doc,
        fixture.admin,
    ))
    .expect("delegations_to answers")
    .value
    .len()
}

/// Setup of the cold query: a database holding the signed star, and no store attached to it.
pub fn stored(members: usize) -> (Database, Identifier) {
    let db = RUNTIME
        .ephemeral(MIGRATIONS)
        .expect("an ephemeral database opens");
    let fixture = star(members, true);
    block_on(Keyline::<Local>::insert(
        &Store::new(db.clone()),
        fixture.certs,
    ))
    .expect("an insert commits");
    (db, fixture.doc)
}

/// `members` on a store that has just attached: every row read, decoded and verified again,
/// then both strata, then the row.
pub fn cold_members(state: (Database, Identifier)) -> usize {
    let (db, doc) = state;
    block_on(Keyline::<Local>::members(&Store::new(db), doc))
        .expect("members answers")
        .value
        .len()
}
