//! The upstream conformance suite and a differential against `MemoryKeyline`, over SQLite; then
//! what SQLite adds: transactions the caller owns, reopening, query plans, scaling.

use std::marker::PhantomData;
use std::sync::LazyLock;

use future_form::FutureForm;
use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyhive_crypto::identifier::Identifier;
use keyhive_crypto::verified::Verified;
use keyline::certificate::VerifiedCertificate;
use keyline::contract::Keyline;
use keyline::test_utils::Statement;
use keyline::test_utils::conformance::Backend;
use keyline::test_utils::conformance::r#gen::POOL;
use keyline::test_utils::signing_key;
use sqlite_rs::Database;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;

use crate::MIGRATIONS;
use crate::SqliteKeyline;

mod conformance;
mod demand;
mod differential;
mod plans;
mod recording;
mod scaling;
mod transactions;

pub(crate) static RUNTIME: LazyLock<SqliteRuntime> = LazyLock::new(|| {
    SqliteRuntime::new(RuntimeConfig {
        execution: sqlite_rs::test_support::execution(),
        workers: 2,
        ..RuntimeConfig::default()
    })
});

/// A new, empty database on its own ephemeral file, migrated.
pub(crate) fn fresh_db() -> Database {
    RUNTIME
        .ephemeral(MIGRATIONS)
        .expect("an ephemeral database opens")
}

/// What the conformance suite builds its stores from: an empty set on a database of its own.
impl<W: Encode + Decode> Default for SqliteKeyline<W, Database> {
    fn default() -> Self {
        Self::new(fresh_db())
    }
}

/// The conformance [`Backend`] for SQLite: a store per harness, futures driven by an executor.
pub(crate) struct Sqlite<W, F>(PhantomData<(W, F)>);

impl<W: Encode + Decode, F: FutureForm> Backend for Sqlite<W, F>
where
    SqliteKeyline<W, Database>: Keyline<F, RetentionWatermark = W>,
{
    type Form = F;
    type Watermark = W;
    type Store = SqliteKeyline<W, Database>;

    fn block_on<T, Fut: std::future::Future<Output = T>>(future: Fut) -> T {
        futures::executor::block_on(future)
    }
}

/// The pool's signing keys, so signing a fixture does not search all 256 of them.
static POOL_KEYS: LazyLock<Vec<(Identifier, ed25519_dalek::SigningKey)>> = LazyLock::new(|| {
    (1..=POOL)
        .map(signing_key)
        .map(|key| (Identifier::from(&key), key))
        .collect()
});

/// A fixture signed for real, by the pool identity that issues it: what a reopened store
/// verifies again. The conformance fixtures' zeroed signatures are enough while the cache is
/// warm, and not after.
pub(crate) fn sign(statement: impl Into<Statement<()>>) -> VerifiedCertificate<()> {
    fn key_of(issuer: Identifier) -> &'static ed25519_dalek::SigningKey {
        &POOL_KEYS
            .iter()
            .find(|(id, _)| *id == issuer)
            .expect("the issuer is a pool identity")
            .1
    }
    match statement.into() {
        Statement::Delegation(d) => Verified::sign_sync(&d.payload, key_of(d.issuer))
            .expect("a pool key signs")
            .into(),
        Statement::Revocation(r) => Verified::sign_sync(&r.payload, key_of(r.issuer))
            .expect("a pool key signs")
            .into(),
    }
}
