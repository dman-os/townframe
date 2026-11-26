//! `SqliteKeyline` against `MemoryKeyline` on random certificate sets: every query, over the
//! whole pool, after every batch, through the store that inserted and through a store that
//! reopened the database and rebuilt its evaluation from the rows.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use arbitrary::Arbitrary;
use arbitrary::Unstructured;
use future_form::Local;
use futures::executor::block_on;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::identifier::Identifier;
use keyline::certificate::Certificate;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;
use keyline::contract::CertificateSet;
use keyline::contract::Keyline;
use keyline::contract::Version;
use keyline::delegation::Delegation;
use keyline::power::Power;
use keyline::revocation::RevocationId;
use keyline::test_utils::conformance::r#gen::CertSet;
use keyline::test_utils::conformance::r#gen::ids;
use keyline_memory::MemoryKeyline;
use proptest::prelude::*;

use super::fresh_db;
use super::sign;
use crate::SqliteKeyline;

/// Every observable answer of a store holding `certs`, each read at one version.
#[derive(Debug, PartialEq)]
struct Answers {
    version: Version,
    certificates: Vec<Certificate<()>>,
    subjects: BTreeSet<Identifier>,
    digest: Digest<CertificateSet>,
    powers: BTreeMap<(Identifier, Identifier), (Option<Power>, Option<Power>)>,
    members: BTreeMap<Identifier, BTreeMap<Identifier, Power>>,
    closures: BTreeMap<Identifier, Vec<Certificate<()>>>,
    delegations_to:
        BTreeMap<(Identifier, Identifier), BTreeMap<Digest<Delegation>, (Identifier, Delegation)>>,
    per_certificate: BTreeMap<CertificateId, (bool, Option<Certificate<()>>)>,
    per_delegation:
        BTreeMap<Digest<Delegation>, (bool, Option<Power>, BTreeSet<Digest<RevocationId>>)>,
    subset_members: BTreeMap<Identifier, BTreeMap<Identifier, Power>>,
}

/// Read everything, asserting every query reports the version the store is at.
fn answers<K: Keyline<Local, RetentionWatermark = ()>>(
    store: &K,
    certs: &[VerifiedCertificate<()>],
) -> Answers {
    let version = block_on(store.version()).expect("a store answers");
    let at = |read: Version| assert_eq!(read, version, "an answer carries the store's version");
    macro_rules! ask {
        ($query:expr) => {{
            let answer = block_on($query).expect("a store answers");
            at(answer.version);
            answer.value
        }};
    }

    let pool: Vec<Identifier> = ids().collect();
    let delegations: Vec<Digest<Delegation>> = certs
        .iter()
        .filter_map(|cert| match cert.id() {
            CertificateId::Delegation(digest) => Some(digest),
            CertificateId::Revocation(_) => None,
        })
        .collect();
    // Every other certificate, and an identity the set does not hold.
    let subset: BTreeSet<CertificateId> = certs
        .iter()
        .step_by(2)
        .map(VerifiedCertificate::id)
        .chain([CertificateId::Delegation(Digest::from([7; 32]))])
        .collect();

    let mut powers = BTreeMap::new();
    let mut delegations_to = BTreeMap::new();
    for &subject in &pool {
        for &audience in &pool {
            powers.insert(
                (subject, audience),
                (
                    ask!(store.effective_power(subject, audience)),
                    ask!(store.positive_power(subject, audience)),
                ),
            );
            delegations_to.insert(
                (subject, audience),
                ask!(store.delegations_to(subject, audience)),
            );
        }
    }
    Answers {
        version,
        certificates: ask!(store.certificates()),
        subjects: ask!(store.subjects()),
        digest: ask!(store.digest()),
        powers,
        members: pool.iter().map(|&s| (s, ask!(store.members(s)))).collect(),
        closures: pool.iter().map(|&s| (s, ask!(store.closure(s)))).collect(),
        delegations_to,
        per_certificate: certs
            .iter()
            .map(|cert| {
                let id = cert.id();
                (id, (ask!(store.contains(id)), ask!(store.get(id))))
            })
            .chain([(
                CertificateId::Revocation(Digest::from([9; 32])),
                (false, None),
            )])
            .collect(),
        per_delegation: delegations
            .iter()
            .map(|&h| {
                (
                    h,
                    (
                        ask!(store.is_live(h)),
                        ask!(store.conveyed_power(h)),
                        ask!(store.revocations_of(h)),
                    ),
                )
            })
            .collect(),
        subset_members: pool
            .iter()
            .map(|&s| (s, ask!(store.members_for_subset(subset.clone(), s))))
            .collect(),
    }
}

/// Insert `certs` in batches of `batch` into both stores, comparing the versions each insert
/// returns, then compare every answer of the reference with the store that inserted and with a
/// second store over the same database that has never seen the inserts.
fn agree(certs: &[VerifiedCertificate<()>], batch: usize) {
    let db = fresh_db();
    let sqlite = SqliteKeyline::<(), _>::new(db.clone());
    let memory = MemoryKeyline::<()>::new();
    for chunk in certs.chunks(batch) {
        let expected = block_on(Keyline::<Local>::insert(&memory, chunk.to_vec())).unwrap();
        let version = block_on(Keyline::<Local>::insert(&sqlite, chunk.to_vec())).unwrap();
        assert_eq!(
            version, expected,
            "an insert returns the reference's version"
        );
    }
    let expected = answers(&memory, certs);
    assert_eq!(answers(&sqlite, certs), expected, "the store that inserted");
    let reopened = SqliteKeyline::<(), _>::new(db);
    assert_eq!(
        answers(&reopened, certs),
        expected,
        "a store that rebuilt from the rows"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn sqlite_matches_memory(
        bytes in proptest::collection::vec(any::<u8>(), 256..3072),
        batch in 1_usize..8,
    ) {
        let Ok(set) = CertSet::<()>::arbitrary(&mut Unstructured::new(&bytes)) else {
            return Err(TestCaseError::reject("not enough bytes for a set"));
        };
        let certs: Vec<_> = set.certs.into_iter().map(sign).collect();
        agree(&certs, batch);
    }
}
