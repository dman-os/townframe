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
use crate::Extension;
use crate::Refresh;
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
/// the document has, whether or not a revocation disputes it: the live set was computed by the
/// insert, so the first question is already the cached read. `members` is as large as its answer.
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
             second {point_small:?} / {point_large:?} (statements, rows) at 200 / 2000 members; \
             members {all_small:?} / {all_large:?} for {size_small} / {size_large} members"
        );
        assert_eq!(point_small, point_large, "a point query grew with the document");
        assert!(point_small.0 <= 5 && point_small.1 <= 5, "{point_small:?}");
        assert_eq!(first_small, point_small, "the first question paid to compute something");
        assert_eq!(first_large, point_large, "the first question paid to compute something");
        assert!(all_large.1 >= size_large, "members reads at least its answer");
        assert!(all_large.1 <= 2 * size_large, "{all_large:?} for {size_large} members");
    }
}

/// Run `batch` through `insert_in` on `store` and return what it cost: statements, extension.
fn insert_cost(
    store: &Store,
    recording: &Recording,
    batch: Vec<VerifiedCertificate<()>>,
) -> (usize, Extension) {
    block_on(async {
        let db = recording.clone();
        let tx = SqliteDb::begin_write(&db).await.unwrap();
        let staged = store.insert_in(&tx, batch).await.unwrap();
        <Recording as SqliteDb>::commit(tx).await.unwrap();
        let extension = staged.extension();
        store.committed(staged);
        (recording.counts.take().0, extension)
    })
}

/// What inserting costs. A batch of grants with no revocation: statements follow rounds, not
/// certificates, and the live sets are not touched. A batch with revocations disputes the
/// document and computes its live set. A revocation into a large closure recomputes it.
#[test]
fn an_insert_costs_its_rounds_and_its_facts() {
    let grants = |members: usize| -> Vec<VerifiedCertificate<()>> {
        unrelated(members)
            .into_iter()
            .filter(|cert| matches!(cert.id(), CertificateId::Delegation(_)))
            .collect()
    };
    let fresh = || {
        let recording = Recording::new(fresh_db());
        (Store::new(recording.clone()), recording)
    };

    // No revocation.
    let (store, recording) = fresh();
    let (statements1, small) = insert_cost(&store, &recording, grants(100));
    let (store, recording) = fresh();
    let (statements2, large) = insert_cost(&store, &recording, grants(1000));
    println!(
        "DEMAND insert, no revocation: 101 grants: {statements1} statements, {} rounds, {} facts, \
         {} live subjects; 1001 grants: {statements2} statements, {} rounds, {} facts, {} live subjects",
        small.rounds, small.facts, small.refresh.subjects, large.rounds, large.facts, large.refresh.subjects
    );
    assert_eq!(statements1, statements2, "statements follow rounds, not certificates");
    assert_eq!(small.rounds, large.rounds);
    assert_eq!(small.refresh, Refresh::default(), "nothing is disputed, nothing to refresh");
    assert!(large.facts < 12 * small.facts, "{small:?} {large:?}");

    // Revocations in the batch: the document is disputed and its live set computed.
    let (store, recording) = fresh();
    let (statements_r1, small) = insert_cost(&store, &recording, unrelated(100));
    let (store, recording) = fresh();
    let (statements_r2, large) = insert_cost(&store, &recording, unrelated(1000));
    println!(
        "DEMAND insert with revocations (every tenth grant): 111 certificates: {statements_r1} statements, \
         {} rounds, {} facts, live {:?}; 1101: {statements_r2} statements, {} rounds, {} facts, live {:?}",
        small.rounds, small.facts, small.refresh, large.rounds, large.facts, large.refresh
    );
    assert_eq!(small.refresh.subjects, 1);
    assert_eq!(statements_r1, statements_r2, "statements follow rounds, not certificates");

    // One revocation into a large, already disputed closure, and into an undisputed one.
    for (label, base) in [("undisputed", grants(1000)), ("disputed", unrelated(1000))] {
        let (store, recording) = fresh();
        let _: (usize, Extension) = insert_cost(&store, &recording, base.clone());
        let admin = key(1);
        let target = base
            .iter()
            .find_map(|cert| match cert.id() {
                CertificateId::Delegation(digest) if cert.issuer() == identifier(&admin) => Some(digest),
                _ => None,
            })
            .expect("the admin issued a grant");
        let revocation = Verified::sign_sync(&Revocation::<()>::new(target), &admin)
            .expect("a key signs")
            .into();
        let (statements, one) = insert_cost(&store, &recording, vec![revocation]);
        println!(
            "DEMAND one revocation into a 1001-member {label} closure: {statements} statements, {} rounds, \
             {} reach facts, live {:?}",
            one.rounds, one.facts, one.refresh
        );
        assert_eq!(one.refresh.subjects, 1, "the touched disputed subject is refreshed");
    }

    // One grant to an unrelated document, with the large one disputed: nothing recomputed.
    let (store, recording) = fresh();
    let _: (usize, Extension) = insert_cost(&store, &recording, unrelated(1000));
    let (statements, unrelated_grant) = insert_cost(&store, &recording, small_doc().into_iter().take(2).collect());
    println!(
        "DEMAND an unrelated grant beside a disputed 1001-member document: {statements} statements, \
         live {:?}",
        unrelated_grant.refresh
    );
    assert_eq!(unrelated_grant.refresh, Refresh::default());
}

/// Another handle over the same database answers a disputed subject right after an insert it did
/// not make, from a read-only snapshot: no write transaction, and the reference's answers.
#[test]
fn a_read_only_handle_answers_a_disputed_subject_right_after_another_handles_insert() {
    let recording = Recording::new(fresh_db());
    let writer = Store::new(recording.clone());
    let reader = Store::new(recording.clone());
    let memory = MemoryKeyline::<()>::new();
    let certs = deep_revocation();
    let (before, after) = certs.split_at(certs.len() / 2);
    for batch in [before, after] {
        block_on(Keyline::<Local>::insert(&writer, batch.to_vec())).unwrap();
        block_on(Keyline::<Local>::insert(&memory, batch.to_vec())).unwrap();
    }
    recording.counts.take();
    let writes_before = recording
        .counts
        .write_transactions
        .load(std::sync::atomic::Ordering::SeqCst);

    let sqlite = block_on(Keyline::<Local>::members(&reader, id(DOC))).unwrap().value;
    let reference = block_on(Keyline::<Local>::members(&memory, id(DOC))).unwrap().value;
    assert_eq!(sqlite, reference);
    for cert in &certs {
        if let CertificateId::Delegation(digest) = cert.id() {
            assert_eq!(
                block_on(Keyline::<Local>::is_live(&reader, digest)).unwrap().value,
                block_on(Keyline::<Local>::is_live(&memory, digest)).unwrap().value,
            );
            assert_eq!(
                block_on(Keyline::<Local>::conveyed_power(&reader, digest)).unwrap().value,
                block_on(Keyline::<Local>::conveyed_power(&memory, digest)).unwrap().value,
            );
        }
    }
    for audience in 1..=8 {
        assert_eq!(
            block_on(Keyline::<Local>::effective_power(&reader, id(DOC), id(audience))).unwrap().value,
            block_on(Keyline::<Local>::effective_power(&memory, id(DOC), id(audience))).unwrap().value,
        );
    }
    let writes = recording
        .counts
        .write_transactions
        .load(std::sync::atomic::Ordering::SeqCst)
        - writes_before;
    // The writer's inserts begin write transactions; the reader's questions do not.
    assert_eq!(writes, 0, "answering a disputed subject began a write transaction");
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

/// As [`agree_on_members`], with the certificates inserted in these batches, in order.
fn agree_in_batches(batches: &[Vec<VerifiedCertificate<()>>], subject: Identifier) -> bool {
    let recording = Recording::new(fresh_db());
    let store = Store::new(recording);
    let memory = MemoryKeyline::<()>::new();
    for batch in batches {
        block_on(Keyline::<Local>::insert(&store, batch.clone())).unwrap();
        block_on(Keyline::<Local>::insert(&memory, batch.clone())).unwrap();
    }
    block_on(Keyline::<Local>::members(&store, subject)).unwrap().value
        == block_on(Keyline::<Local>::members(&memory, subject)).unwrap().value
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
    // An insert that does not refresh the live set of a disputed subject it touched: once when a
    // revocation newly disputes the document, once when a later grant touches the disputed one.
    let (delegations, revocations): (Vec<_>, Vec<_>) = deep_revocation()
        .into_iter()
        .partition(|cert| matches!(cert.id(), CertificateId::Delegation(_)));
    plant(Planted::SkipRefresh, &|| {
        agree_in_batches(&[delegations.clone(), revocations.clone()], id(DOC))
    });
    plant(Planted::SkipRefresh, &|| {
        agree_in_batches(
            &[deep_revocation(), vec![sign(d(OWNERS, 8, DOC, Power::Read))]],
            id(DOC),
        )
    });
    // Optimistic seeding: the gift derives facts it must not.
    PLANTED.set(Planted::Ungrounded);
    let (facts, _) = gift_facts(10);
    PLANTED.set(Planted::None);
    assert_ne!(facts, 0, "an ungrounded evaluation derived nothing from a gift");
}

