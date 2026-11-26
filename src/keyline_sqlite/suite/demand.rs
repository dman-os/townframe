//! What on-demand evaluation promises, beyond giving the reference's answers: a query reads the
//! closure of its subject and nothing else, in a bounded number of statements, and the checks
//! that say so fail when the loader is made wrong.
//!
//! Rows and statements are counted at the database wrapper (`recording.rs`), so the counts are
//! what the database was asked, not what the code believes it asked.

use std::collections::BTreeMap;

use ed25519_dalek::SigningKey;
use future_form::Local;
use futures::executor::block_on;
use keyhive_crypto::identifier::Identifier;
use keyhive_crypto::verified::Verified;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;
use keyline::contract::Keyline;
use keyline::delegation::Delegation;
use keyline::power::Power;
use keyline::revocation::Revocation;
use keyline::test_utils::conformance::ALICE;
use keyline::test_utils::conformance::DOC;
use keyline::test_utils::conformance::OWNERS;
use keyline::test_utils::conformance::d;
use keyline::test_utils::id;
use keyline_memory::MemoryKeyline;
use sqlite_rs::SqliteDb;

use super::fresh_db;
use super::recording::Recording;
use super::sign;
use crate::SqliteKeyline;
use crate::closure::PLANTED;
use crate::closure::Planted;

type Store = SqliteKeyline<(), Recording>;

fn key(index: usize) -> SigningKey {
    let mut seed = [0x3C; 32];
    seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
    SigningKey::from(seed)
}

fn identifier(key: &SigningKey) -> Identifier {
    Identifier::from(key)
}

fn grant(
    issuer: &SigningKey,
    audience: Identifier,
    subject: Identifier,
    power: Power,
) -> VerifiedCertificate<()> {
    Verified::sign_sync(&Delegation::new(audience, subject, power), issuer)
        .expect("a key signs")
        .into()
}

/// The small document every test asks about: owners, a member of the owners, and a grant revoked.
fn small_doc() -> Vec<VerifiedCertificate<()>> {
    let bob = d(ALICE, id(3), DOC, Power::Edit);
    vec![
        sign(d(DOC, OWNERS, DOC, Power::Admin)),
        sign(d(OWNERS, ALICE, OWNERS, Power::Admin)),
        sign(bob),
        sign(keyline::test_utils::conformance::r::<()>(ALICE, &bob)),
    ]
}

/// A document of its own with `members` members, in keys the small document does not use, so it is
/// unrelated to it: `n` rows the small document's queries must not read.
fn unrelated(members: usize) -> Vec<VerifiedCertificate<()>> {
    let doc = key(0);
    let admin = key(1);
    let (doc_id, admin_id) = (identifier(&doc), identifier(&admin));
    let mut certs = vec![grant(&doc, admin_id, doc_id, Power::Admin)];
    for member in 2..members + 2 {
        let member = identifier(&key(member));
        let cert = grant(&admin, member, doc_id, Power::Read);
        if member.as_bytes()[0] % 10 == 0 {
            let CertificateId::Delegation(digest) = cert.id() else {
                unreachable!("a grant is a delegation")
            };
            certs.push(
                Verified::sign_sync(&Revocation::<()>::new(digest), &admin)
                    .expect("a key signs")
                    .into(),
            );
        }
        certs.push(cert);
    }
    certs
}

fn store_with(certs: Vec<VerifiedCertificate<()>>) -> (Store, Recording) {
    let recording = Recording::new(fresh_db());
    let store = Store::new(recording.clone());
    block_on(Keyline::<Local>::insert(&store, certs)).expect("an insert commits");
    recording.counts.take();
    (store, recording)
}

/// Rows read and statements run by every evaluated query about the small document, in a store
/// holding it beside `others` unrelated members.
fn small_doc_cost(others: usize) -> (usize, usize) {
    let mut certs = small_doc();
    certs.extend(unrelated(others));
    let (store, recording) = store_with(certs);
    let version = block_on(Keyline::<Local>::version(&store)).unwrap();
    recording.counts.take();
    block_on(async {
        Keyline::<Local>::members(&store, id(DOC)).await.unwrap();
        Keyline::<Local>::effective_power(&store, id(DOC), id(3))
            .await
            .unwrap();
        Keyline::<Local>::positive_power(&store, id(DOC), id(3))
            .await
            .unwrap();
        Keyline::<Local>::closure(&store, id(DOC)).await.unwrap();
    });
    let (statements, rows) = recording.counts.take();
    assert!(version > keyline::contract::Version::INITIAL);
    (statements, rows)
}

/// The point of the redesign: the cost of a query about one document does not depend on how many
/// unrelated documents the set holds.
#[test]
fn a_query_does_not_read_what_is_not_in_its_closure() {
    let (small_statements, small_rows) = small_doc_cost(50);
    let (large_statements, large_rows) = small_doc_cost(500);
    println!(
        "DEMAND small document beside 50 unrelated members: {small_statements} statements, \
         {small_rows} rows; beside 500: {large_statements} statements, {large_rows} rows"
    );
    assert_eq!(
        (small_statements, small_rows),
        (large_statements, large_rows),
        "a query about a document of four certificates changed its cost with the unrelated set"
    );
    // Four queries, each: begin read, version, rounds of two statements, end. Bounded by the
    // document's derivation depth, not by anything's size.
    assert!(small_statements <= 4 * 12, "{small_statements} statements");
    assert!(small_rows <= 4 * 6, "{small_rows} rows for a four-certificate closure");
}

/// What a query does read: the closure, in full. The inherent cost of `members` of a large
/// document is its output.
#[test]
fn a_query_reads_the_whole_closure() {
    let mut certs = small_doc();
    certs.extend(unrelated(200));
    let (store, recording) = store_with(certs);
    let doc = identifier(&key(0));
    let rows = block_on(async {
        let snapshot = SqliteDb::begin_read(&recording).await.unwrap();
        store.evaluate_in(&snapshot, doc).await.unwrap().rows_read()
    });
    assert!(rows >= 201, "{rows}");
    assert!(rows < 300, "{rows}: only the document's own certificates");
}

/// A delegation that names a node but is signed by a key without standing over the queried
/// subject costs its own row, however large the structure it points at: the ladder behind it is
/// never explored (evaluation-notes section 7, obligation 3).
fn ladder_rows(rungs: usize) -> usize {
    let mut certs = small_doc();
    let eve = key(900);
    let ladder: Vec<SigningKey> = (0..rungs).map(|n| key(1000 + n)).collect();
    for window in ladder.windows(2) {
        // Each rung is its own root and supplies the next, with a member of its own.
        let (rung, next) = (identifier(&window[0]), identifier(&window[1]));
        certs.push(grant(&window[0], next, rung, Power::Admin));
        certs.push(grant(
            &window[0],
            identifier(&key(5000 + certs.len())),
            rung,
            Power::Read,
        ));
    }
    // The gift: the ladder's top over the document, from a key that has no standing over it.
    let top = identifier(&ladder[0]);
    certs.push(grant(&eve, top, id(DOC), Power::Admin));
    let (store, recording) = store_with(certs);
    block_on(async {
        let snapshot = SqliteDb::begin_read(&recording).await.unwrap();
        store.evaluate_in(&snapshot, id(DOC)).await.unwrap().rows_read()
    })
}

#[test]
fn a_gift_certificate_costs_its_row_and_not_the_ladder() {
    let (short, tall) = (ladder_rows(10), ladder_rows(100));
    println!("DEMAND gift certificate: closure rows {short} (10 rungs), {tall} (100 rungs)");
    assert_eq!(short, tall, "the ladder behind a gift was explored");
    let without_gift = {
        let (store, recording) = store_with(small_doc());
        block_on(async {
            let snapshot = SqliteDb::begin_read(&recording).await.unwrap();
            store.evaluate_in(&snapshot, id(DOC)).await.unwrap().rows_read()
        })
    };
    assert_eq!(short, without_gift + 1, "exactly the gift's own row");
}

/// The answers the same sets give through a reference.
fn members_of(
    certs: &[VerifiedCertificate<()>],
    subject: Identifier,
) -> (BTreeMap<Identifier, Power>, BTreeMap<Identifier, Power>) {
    let (store, _) = store_with(certs.to_vec());
    let memory = MemoryKeyline::<()>::new();
    block_on(Keyline::<Local>::insert(&memory, certs.to_vec())).unwrap();
    (
        block_on(Keyline::<Local>::members(&store, subject))
            .unwrap()
            .value,
        block_on(Keyline::<Local>::members(&memory, subject))
            .unwrap()
            .value,
    )
}

/// A role (`OWNERS`) holds a member who stands over the document only through the role: the
/// loader must compose membership to find the member's own certificates.
fn role_with_member() -> Vec<VerifiedCertificate<()>> {
    vec![
        sign(d(DOC, OWNERS, DOC, Power::Admin)),
        sign(d(OWNERS, ALICE, OWNERS, Power::Admin)),
        sign(d(ALICE, id(3), DOC, Power::Edit)),
    ]
}

#[test]
fn the_loader_agrees_with_the_reference_on_a_role() {
    let (sqlite, memory) = members_of(&role_with_member(), id(DOC));
    assert_eq!(sqlite, memory);
    assert!(sqlite.contains_key(&id(3)), "{sqlite:?}");
}

/// The checks above can fail: with membership composition removed from the loader, the answer
/// about a role changes; with grounding removed, the ladder is explored. A suite that passed both
/// of these would be checking nothing.
#[test]
fn the_checks_fail_when_the_loader_is_wrong() {
    PLANTED.set(Planted::NoComposition);
    let (sqlite, memory) = members_of(&role_with_member(), id(DOC));
    PLANTED.set(Planted::None);
    assert_ne!(
        sqlite, memory,
        "a loader without membership composition still agreed with the reference"
    );

    PLANTED.set(Planted::Ungrounded);
    let (short, tall) = (ladder_rows(10), ladder_rows(30));
    PLANTED.set(Planted::None);
    assert_ne!(
        short, tall,
        "a loader that explores ungrounded nodes read the same rows for different ladders"
    );
}
