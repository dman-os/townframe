//! The store held to `MemoryKeyhiveStore` (a differential over random operation sequences, with
//! reopening), then what SQLite adds: transactions the caller owns, secrets at rest, query plans.

use std::sync::LazyLock;

use arbitrary::Arbitrary;
use arbitrary::Unstructured;
use beekem::id::TreeId;
use beekem::operation::CgkaOperation;
use ed25519_dalek::SigningKey;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId;
use keyhive_core::principal::individual::id::IndividualId;
use keyhive_core::principal::individual::op::KeyOp;
use keyhive_core::principal::individual::op::add_key::AddKeyOp;
use keyhive_core::principal::individual::op::rotate_key::RotateKeyOp;
use keyhive_crypto::identifier::Identifier;
use keyhive_crypto::share_key::ShareKey;
use keyhive_crypto::share_key::ShareSecretKey;
use keyhive_crypto::symmetric_key::SymmetricKey;
use keyhive_crypto::verified::Verified;
use sqlite_rs::Database;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;
use std::sync::Arc;

use crate::Kek;
use crate::KekSource;
use crate::MIGRATIONS;
use crate::SqliteKeyhiveStore;

mod differential;
mod plans;
mod secrets;
mod transactions;

pub(crate) type Ref = [u8; 32];
pub(crate) type Store = SqliteKeyhiveStore<Ref, Database>;

pub(crate) static RUNTIME: LazyLock<SqliteRuntime> = LazyLock::new(|| {
    SqliteRuntime::new(RuntimeConfig {
        execution: sqlite_rs::test_support::execution(),
        workers: 2,
        ..RuntimeConfig::default()
    })
});

/// Keys with a history: new wraps use the last, an older DEK row names the one it was wrapped by.
pub(crate) struct Keks(pub Vec<Kek>);

impl KekSource for Keks {
    fn current(&self) -> Kek {
        self.0.last().expect("at least one key").clone()
    }

    fn get(&self, version: u64) -> Option<Kek> {
        self.0.iter().find(|kek| kek.version == version).cloned()
    }
}

pub(crate) fn kek() -> Kek {
    Kek::new(1, [7; 32])
}

/// A new, empty, migrated database on its own ephemeral file.
pub(crate) fn fresh_db() -> Database {
    RUNTIME
        .ephemeral(MIGRATIONS)
        .expect("an ephemeral database opens")
}

pub(crate) fn fresh_store() -> (Database, Store) {
    let db = fresh_db();
    (db.clone(), Store::new(db, kek()))
}

/// Everything the differential draws from: a few individuals and documents, and a fixed set of
/// signed values over them, so that random sequences repeat, overlap and re-add what they hold.
pub(crate) struct Pool {
    pub individuals: Vec<IndividualId>,
    pub docs: Vec<DocumentId>,
    /// Prekey operations, with the index of the individual that signed each.
    pub key_ops: Vec<(usize, Arc<KeyOp>)>,
    /// CGKA operations, with the index of their document. Each names earlier ones as predecessors.
    pub cgka_ops: Vec<(usize, Arc<Verified<CgkaOperation>>)>,
    pub key_pairs: Vec<(ShareKey, ShareSecretKey)>,
    pub refs: Vec<Ref>,
    pub events: Vec<StaticEvent<Ref>>,
}

/// Deterministic bytes: the fixture is the same on every run, so a failure replays.
struct Bytes(u64);

impl Bytes {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0; N];
        for byte in &mut out {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            *byte = (self.0 >> 24) as u8;
        }
        out
    }
}

/// The content key of reference `r` in document `d`: a function of the pair, as a real one is.
pub(crate) fn content_key(d: usize, r: usize) -> SymmetricKey {
    let mut bytes = [0; 32];
    bytes[0] = d as u8;
    bytes[1] = r as u8;
    bytes[2] = 0xC0;
    SymmetricKey::from(bytes)
}

pub(crate) static POOL: LazyLock<Pool> = LazyLock::new(|| {
    let mut rng = Bytes(0x9E37_79B9_7F4A_7C15);
    let signers: Vec<SigningKey> = (0..3)
        .map(|_| SigningKey::from_bytes(&rng.take()))
        .collect();
    let individuals: Vec<_> = signers
        .iter()
        .map(|key| IndividualId::from(Identifier::from(key)))
        .collect();
    let doc_keys: Vec<SigningKey> = (0..3)
        .map(|_| SigningKey::from_bytes(&rng.take()))
        .collect();
    let docs: Vec<_> = doc_keys
        .iter()
        .map(|key| DocumentId::from(Identifier::from(key)))
        .collect();

    let share_keys: Vec<_> = (0..12)
        .map(|_| ShareSecretKey::force_from_bytes(rng.take()))
        .collect();
    let key_pairs: Vec<_> = share_keys
        .iter()
        .map(|secret| (secret.share_key(), secret.clone()))
        .collect();

    let mut key_ops = Vec::new();
    for (index, signer) in signers.iter().enumerate() {
        for pair in 0..4 {
            let new = key_pairs[(index * 4 + pair) % key_pairs.len()].0;
            let op = if pair == 0 {
                KeyOp::Add(Verified::sign_sync(&AddKeyOp { share_key: new }, signer).unwrap())
            } else {
                let old = key_pairs[(index * 4 + pair - 1) % key_pairs.len()].0;
                KeyOp::Rotate(Verified::sign_sync(&RotateKeyOp { old, new }, signer).unwrap())
            };
            key_ops.push((index, Arc::new(op)));
        }
    }

    let mut cgka_ops: Vec<(usize, Arc<Verified<CgkaOperation>>)> = Vec::new();
    for round in 0..24 {
        let doc = round % docs.len();
        let bytes: [u8; 512] = rng.take();
        let mut op = CgkaOperation::arbitrary(&mut Unstructured::new(&bytes))
            .expect("512 bytes make an operation");
        op.doc_id = TreeId::from(Identifier::from(docs[doc]));
        // Predecessors: up to two earlier operations of the same document, none for the first.
        let earlier: Vec<_> = cgka_ops.iter().filter(|(d, _)| *d == doc).collect();
        let picks = if earlier.is_empty() {
            Vec::new()
        } else {
            let a = earlier[usize::from(rng.take::<1>()[0]) % earlier.len()];
            let b = earlier[usize::from(rng.take::<1>()[0]) % earlier.len()];
            vec![a.1.digest(), b.1.digest()]
        };
        op.predecessors = picks.into_iter().collect();
        let signer = &signers[round % signers.len()];
        cgka_ops.push((doc, Arc::new(Verified::sign_sync(&op, signer).unwrap())));
    }

    let events = (0..16)
        .map(|_| {
            let bytes: [u8; 1024] = rng.take();
            StaticEvent::<Ref>::arbitrary(&mut Unstructured::new(&bytes))
                .expect("1024 bytes make an event")
        })
        .collect();

    Pool {
        individuals,
        docs,
        key_ops,
        cgka_ops,
        key_pairs,
        refs: (0..5).map(|_| rng.take()).collect(),
        events,
    }
});
