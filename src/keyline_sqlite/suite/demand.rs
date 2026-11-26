//! What the materialized evaluation promises beyond giving the reference's answers: a point
//! query reads a handful of rows however large the set, an insert pays for the facts it derives,
//! a certificate that names a node without standing derives nothing, and the checks that say so
//! fail when the evaluation is made wrong.
//!
//! Rows and statements are counted at the database wrapper (`recording.rs`), so the counts are
//! what the database was asked, not what the code believes it asked.


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
use keyline::test_utils::conformance::r;
use keyline::test_utils::id;
use keyline_memory::MemoryKeyline;
use sqlite_rs::SqliteDb;

use super::fresh_db;
use super::recording::Recording;
use super::sign;
use crate::SqliteKeyline;
use crate::planted::PLANTED;
use crate::planted::Planted;

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
    let bob = d(ALICE, 3, DOC, Power::Edit);
    vec![
        sign(d(DOC, OWNERS, DOC, Power::Admin)),
        sign(d(OWNERS, ALICE, OWNERS, Power::Admin)),
        sign(bob),
        sign(r::<()>(ALICE, &bob)),
    ]
}

/// A document of its own with `members` members, in keys the small document does not use: `n`
/// rows the small document's queries must not read. Every tenth grant is revoked by the admin.
fn unrelated(members: usize) -> Vec<VerifiedCertificate<()>> {
    let doc = key(0);
    let admin = key(1);
    let (doc_id, admin_id) = (identifier(&doc), identifier(&admin));
    let mut certs = vec![grant(&doc, admin_id, doc_id, Power::Admin)];
    for member in 2..members + 2 {
        let cert = grant(&admin, identifier(&key(member)), doc_id, Power::Read);
        if member % 10 == 0 {
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

/// A store over a recording database holding `certs`, and its counts reset.
fn store_with(certs: Vec<VerifiedCertificate<()>>) -> (Store, Recording) {
    let recording = Recording::new(fresh_db());
    let store = Store::new(recording.clone());
    block_on(Keyline::<Local>::insert(&store, certs)).expect("an insert commits");
    recording.counts.take();
    (store, recording)
}

/// Statements and rows of every point query about the small document, in a store holding it
/// beside `others` unrelated members.
fn small_doc_cost(others: usize) -> (usize, usize) {
    let mut certs = small_doc();
    certs.extend(unrelated(others));
    let (store, recording) = store_with(certs);
    // The revocation disputes the document: the first evaluated question computes its live set,
    // which is paid once and is not what is measured here.
    block_on(Keyline::<Local>::members(&store, id(DOC))).unwrap();
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
    recording.counts.take()
}

/// The point of the design: the cost of a query about one document does not depend on how many
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
        "a query about a small document changed its cost with the unrelated set"
    );
    assert!(small_rows <= 24, "{small_rows} rows");
}

/// A point query about a member of a large document is a few indexed reads however many members
/// the document has; `members` is as large as its answer. A document some revocation disputes pays
/// once, at the first question after an insert, to compute its live set.
#[test]
fn a_point_query_is_constant_and_members_is_its_output() {
    let measure = |members: usize, disputed: bool| {
        let mut certs = unrelated(members);
        if !disputed {
            certs.retain(|cert| matches!(cert.id(), CertificateId::Delegation(_)));
        }
        certs.extend(small_doc());
        let (store, recording) = store_with(certs);
        let doc = identifier(&key(0));
        let member = identifier(&key(3));
        block_on(Keyline::<Local>::effective_power(&store, doc, member)).unwrap();
        let first = recording.counts.take();
        block_on(Keyline::<Local>::effective_power(&store, doc, member)).unwrap();
        let point = recording.counts.take();
        let answer = block_on(Keyline::<Local>::members(&store, doc)).unwrap().value;
        let all = recording.counts.take();
        (first, point, all, answer.len())
    };
    for disputed in [false, true] {
        let (first_small, point_small, all_small, size_small) = measure(200, disputed);
        let (first_large, point_large, all_large, size_large) = measure(2000, disputed);
        println!(
            "DEMAND large document, disputed={disputed}: first question {first_small:?} / {first_large:?}, \
             then a point query {point_small:?} / {point_large:?} (statements, rows) at 200 / 2000 members; \
             members {all_small:?} / {all_large:?} for {size_small} / {size_large} members"
        );
        assert_eq!(point_small, point_large, "a point query grew with the document");
        assert!(point_small.0 <= 5 && point_small.1 <= 5, "{point_small:?}");
        if !disputed {
            assert_eq!(first_small, first_large, "an undisputed document's first question was not a point query");
        }
        assert!(all_large.1 >= size_large, "members reads at least its answer");
        assert!(all_large.1 <= 2 * size_large, "{all_large:?} for {size_large} members");
    }
}

/// What inserting costs: statements per batch (rounds of the fixpoint, not certificates) and
/// facts derived.
#[test]
fn an_insert_costs_its_rounds_and_its_facts() {
    let cost = |members: usize| {
        let certs = unrelated(members);
        let count = certs.len();
        let recording = Recording::new(fresh_db());
        let store = Store::new(recording.clone());
        let db = recording.clone();
        let (staged, statements) = block_on(async {
            let tx = SqliteDb::begin_write(&db).await.unwrap();
            let staged = store.insert_in(&tx, certs).await.unwrap();
            <Recording as SqliteDb>::commit(tx).await.unwrap();
            (staged, recording.counts.take().0)
        });
        let extension = staged.extension();
        store.committed(staged);
        (count, statements, extension)
    };
    let (n1, statements1, small) = cost(100);
    let (n2, statements2, large) = cost(1000);
    println!(
        "DEMAND insert of {n1} certificates: {statements1} statements, {} rounds, {} facts; of {n2}: \
         {statements2} statements, {} rounds, {} facts",
        small.rounds, small.facts, large.rounds, large.facts
    );
    assert_eq!(statements1, statements2, "statements follow rounds, not certificates");
    assert_eq!(small.rounds, large.rounds);
    assert!(large.facts < 12 * small.facts, "{small:?} {large:?}");
}

/// The ladder: rung `i` is the root of its own graph and supplies rung `i + 1`, which has a member.
fn ladder(rungs: usize) -> Vec<VerifiedCertificate<()>> {
    let keys: Vec<SigningKey> = (0..rungs).map(|n| key(1000 + n)).collect();
    let mut certs = Vec::new();
    for (position, window) in keys.windows(2).enumerate() {
        let (rung, next) = (identifier(&window[0]), identifier(&window[1]));
        certs.push(grant(&window[0], next, rung, Power::Admin));
        certs.push(grant(
            &window[0],
            identifier(&key(5000 + position)),
            rung,
            Power::Read,
        ));
    }
    certs
}

/// A certificate that names a node without standing derives nothing: a key with no standing over
/// the document gifts it the top of a ladder, and the stored reach does not move.
fn gift_facts(rungs: usize) -> (u64, usize) {
    let mut certs = small_doc();
    certs.extend(ladder(rungs));
    let recording = Recording::new(fresh_db());
    let store = Store::new(recording.clone());
    block_on(Keyline::<Local>::insert(&store, certs)).unwrap();
    let eve = key(900);
    let top = identifier(&key(1000));
    let gift = grant(&eve, top, id(DOC), Power::Admin);
    let facts = block_on(async {
        let db = recording.clone();
        let tx = SqliteDb::begin_write(&db).await.unwrap();
        let staged = store.insert_in(&tx, vec![gift]).await.unwrap();
        <Recording as SqliteDb>::commit(tx).await.unwrap();
        staged.extension().facts
    });
    recording.counts.take();
    let members = block_on(Keyline::<Local>::members(&store, id(DOC)))
        .unwrap()
        .value
        .len();
    (facts, members)
}

#[test]
fn a_gift_certificate_derives_nothing_and_the_ladder_is_not_walked() {
    let (short, tall) = (gift_facts(10), gift_facts(60));
    println!("DEMAND gift certificate: (facts, members of the document) {short:?} (10 rungs), {tall:?} (60 rungs)");
    assert_eq!(short, (0, short.1), "the gift derived facts");
    assert_eq!(short, tall, "the ladder behind the gift mattered");
}

/// The answers of the reference for the same set.
fn agree_on_members(certs: &[VerifiedCertificate<()>], subject: Identifier) -> bool {
    let (store, _) = store_with(certs.to_vec());
    let memory = MemoryKeyline::<()>::new();
    block_on(Keyline::<Local>::insert(&memory, certs.to_vec())).unwrap();
    let sqlite = block_on(Keyline::<Local>::members(&store, subject)).unwrap().value;
    let reference = block_on(Keyline::<Local>::members(&memory, subject))
        .unwrap()
        .value;
    sqlite == reference
}

/// Roles nested two deep: `ALICE` stands over the document only as a member of `OWNERS`, and is a
/// role herself, whose member (4) delegates to a fifth (5).
fn role_with_member() -> Vec<VerifiedCertificate<()>> {
    vec![
        sign(d(DOC, OWNERS, DOC, Power::Admin)),
        sign(d(OWNERS, ALICE, OWNERS, Power::Admin)),
        sign(d(ALICE, 4, ALICE, Power::Admin)),
        sign(d(4, 5, DOC, Power::Read)),
    ]
}

/// The document's own example of a deep revocation (implementation.md, "Covered edges are
/// clamped"): Dan administers the role `MODS`, which is supplied into `DOC` at Edit; Eve is a Mod
/// and holds a direct Read from the owners; she delegates Admin over `DOC` to Frank, and Dan
/// revokes that delegation. Frank is clamped to Read: Eve's standing through the role Dan
/// administers does not flow through the edge he revoked, her independent Read does.
fn deep_revocation() -> Vec<VerifiedCertificate<()>> {
    const MODS: u8 = 3;
    const DAN: u8 = 4;
    const EVE: u8 = 5;
    const FRANK: u8 = 6;
    let frank = d(EVE, FRANK, DOC, Power::Admin);
    // The role itself issues a delegation, which Dan revokes: its route to its own issuer runs
    // through the node the revocation covers, and so it must not conduct.
    let by_role = d(MODS, 7, DOC, Power::Edit);
    vec![
        sign(by_role),
        sign(r::<()>(DAN, &by_role)),
        sign(d(DOC, OWNERS, DOC, Power::Admin)),
        sign(d(MODS, DAN, MODS, Power::Admin)),
        sign(d(DOC, MODS, DOC, Power::Edit)),
        sign(d(MODS, EVE, MODS, Power::Edit)),
        sign(d(OWNERS, EVE, DOC, Power::Read)),
        sign(frank),
        sign(r::<()>(DAN, &frank)),
    ]
}

#[test]
fn the_loader_agrees_with_the_reference_on_roles_and_on_a_deep_revocation() {
    assert!(agree_on_members(&role_with_member(), id(DOC)));
    assert!(agree_on_members(&deep_revocation(), id(DOC)));
    let (store, _) = store_with(deep_revocation());
    let members = block_on(Keyline::<Local>::members(&store, id(DOC))).unwrap().value;
    assert_eq!(members.get(&id(6)), Some(&Power::Read), "{members:?}");
    assert_eq!(members.get(&id(7)), None, "{members:?}");
}

/// The checks above can fail: plant each bug in the evaluation's statements and the answer
/// changes. A suite that passed these would be checking nothing.
#[test]
fn the_checks_fail_when_the_evaluation_is_wrong() {
    let plant = |bug: Planted, run: &dyn Fn() -> bool| {
        PLANTED.set(bug);
        let agreed = run();
        PLANTED.set(Planted::None);
        assert!(!agreed, "{bug:?}: a wrong evaluation still agreed with the reference");
    };
    plant(Planted::NoComposition, &|| agree_on_members(&role_with_member(), id(DOC)));
    plant(Planted::IgnoreOwnContext, &|| agree_on_members(&deep_revocation(), id(DOC)));
    plant(Planted::IgnoreExclusion, &|| agree_on_members(&deep_revocation(), id(DOC)));
    // Optimistic seeding: the gift derives facts it must not.
    PLANTED.set(Planted::Ungrounded);
    let (facts, _) = gift_facts(10);
    PLANTED.set(Planted::None);
    assert_ne!(facts, 0, "an ungrounded evaluation derived nothing from a gift");
}

