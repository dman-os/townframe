//! [`SqliteKeyline`] and its [`Keyline`] implementation.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use future_form::FutureForm;
use future_form::Local;
use future_form::Sendable;
use future_form::future_form;
use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::identifier::Identifier;
use keyline::certificate::Certificate;
use keyline::certificate::CertificateId;
use keyline::certificate::VerifiedCertificate;
use keyline::contract::CertificateSet;
use keyline::contract::Keyline;
use keyline::contract::Query;
use keyline::contract::Version;
use keyline::contract::Versioned;
use keyline::contract::set_digest;
use keyline::delegation::Delegation;
use keyline::power::Power;
use keyline::revocation::RevocationId;
use keyline_memory::state::State;
use sqlite_rs::Executor;

use crate::closure;
use crate::closure::Evaluated;
use crate::memo::Memo;
use crate::error::Error;
use crate::rows;
use crate::sqlite::queries;
use crate::write;
use crate::write::Staged;
use sqlite_rs::SqliteDb;

/// The Keyline certificate set in a SQLite database.
///
/// Clones share the database handle and the verification memo. The database's own transactions
/// are the concurrency control: a query reads one snapshot, an insert holds the writer. Nothing
/// else is held between calls: an evaluated query loads the closure of its subject from the
/// tables (see [`closure`](crate::closure)).
pub struct SqliteKeyline<W, D: SqliteDb> {
    db: D,
    memo: Arc<Memo<W>>,
}

impl<W, D: SqliteDb> Clone for SqliteKeyline<W, D> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            memo: Arc::clone(&self.memo),
        }
    }
}

/// A read snapshot and the version it was read at.
struct Snapshot<D: SqliteDb> {
    tx: D::Read,
    version: i64,
}

impl<D: SqliteDb> Snapshot<D> {
    /// End the snapshot, and return the version its answers carry. An error before this drops
    /// the snapshot, which releases it.
    async fn finish(self) -> Result<Version, Error> {
        D::end_read(self.tx).await?;
        Ok(version_from_sql(self.version))
    }
}

fn version_from_sql(version: i64) -> Version {
    Version::new(u64::try_from(version).expect("a stored version is not negative"))
}

impl<W: Encode + Decode, D: SqliteDb> SqliteKeyline<W, D> {
    /// Attach to `db`, which must have been registered with [`MIGRATIONS`](crate::MIGRATIONS)
    /// under [`MIGRATION_TABLE`](crate::MIGRATION_TABLE).
    pub fn new(db: D) -> Self {
        Self {
            db,
            memo: Arc::default(),
        }
    }

    /// Add `batch` through `tx`, a write transaction the caller owns and shares with other
    /// stores of the same database: the grant joins that transaction's commit or rollback.
    ///
    /// The certificates are in the tables, and the version has moved, for whoever reads through
    /// `tx`, and for everyone else only once `tx` commits. A rollback leaves no row and no
    /// version bump. After a commit, pass the result to [`committed`](Self::committed); after a
    /// rollback, drop it.
    pub async fn insert_in(
        &self,
        tx: &impl Executor,
        batch: Vec<VerifiedCertificate<W>>,
    ) -> Result<Staged<W>, Error> {
        write::insert_in(tx, batch).await
    }

    /// The commit half of [`insert_in`](Self::insert_in): hand the verification memo the batch the
    /// transaction committed, so the first read of those rows does not verify them again. It is
    /// only a memo: a store that is never told answers correctly, verifying on first read.
    pub fn committed(&self, staged: Staged<W>) {
        self.memo.warm(staged.warm);
    }

    /// The closure of `subject` as `snapshot` sees it, with the reference evaluator over it: for a
    /// caller that answers several questions, or combines them with reads of other stores, in one
    /// snapshot it owns. `snapshot` must be a read snapshot or a write transaction of the database
    /// this store is attached to.
    pub async fn evaluate_in(
        &self,
        snapshot: &impl sqlite_rs::QueryExecutor,
        subject: Identifier,
    ) -> Result<Evaluated<W>, Error> {
        closure::load(snapshot, subject, &self.memo).await
    }

    async fn snapshot(&self) -> Result<Snapshot<D>, Error> {
        let tx = self.db.begin_read().await?;
        let version = queries::meta_version(&tx).await?;
        Ok(Snapshot { tx, version })
    }

    /// Answer `query` from the closure of `subject`, at the version one snapshot read.
    async fn evaluated<T>(
        &self,
        subject: Identifier,
        query: impl FnOnce(&State<W>) -> T,
    ) -> Result<Versioned<T>, Error> {
        let snapshot = self.snapshot().await?;
        let evaluated = closure::load(&snapshot.tx, subject, &self.memo).await?;
        let version = snapshot.finish().await?;
        Ok(Versioned {
            version,
            value: query(evaluated.state()),
        })
    }

    /// Answer `query` from the closure of the subject of `delegation`, which is where its liveness
    /// is rooted; a delegation the set does not hold is answered by the empty set.
    async fn evaluated_for<T>(
        &self,
        delegation: Digest<Delegation>,
        query: impl FnOnce(&State<W>) -> T,
    ) -> Result<Versioned<T>, Error> {
        let snapshot = self.snapshot().await?;
        let subject =
            queries::delegation_subject(&snapshot.tx, delegation.as_slice().to_vec()).await?;
        let state = match subject {
            Some(subject) => {
                closure::load(&snapshot.tx, rows::identifier_from_sql(&subject), &self.memo)
                    .await?
                    .into_state()
            }
            None => State::new(),
        };
        let version = snapshot.finish().await?;
        Ok(Versioned {
            version,
            value: query(&state),
        })
    }
}

#[future_form(Sendable where W: Send + Sync, Local)]
impl<F: FutureForm, W: Encode + Decode, D: SqliteDb> Keyline<F> for SqliteKeyline<W, D> {
    type RetentionWatermark = W;
    type Error = Error;

    fn version(&self) -> F::Future<'_, Result<Version, Error>> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            snapshot.finish().await
        })
    }

    fn insert(&self, batch: Vec<VerifiedCertificate<W>>) -> F::Future<'_, Result<Version, Error>> {
        F::from_future(async move {
            // An error before the commit drops `tx`, which rolls it back.
            let tx = self.db.begin_write().await?;
            let staged = self.insert_in(&tx, batch).await?;
            D::commit(tx).await?;
            let version = staged.version();
            self.committed(staged);
            Ok(version)
        })
    }

    fn contains(&self, cert: CertificateId) -> Query<'_, F, bool, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let (kind, digest) = rows::kind_and_digest(cert);
            let value = queries::certificate_exists(&snapshot.tx, kind, digest).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned { version, value })
        })
    }

    fn get(&self, cert: CertificateId) -> Query<'_, F, Option<Certificate<W>>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let (kind, digest) = rows::kind_and_digest(cert);
            let signed = queries::certificate_signed(&snapshot.tx, kind, digest).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: signed.map(|signed| rows::certificate_from_sql(&signed)),
            })
        })
    }

    fn certificates(&self) -> Query<'_, F, Vec<Certificate<W>>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let rows = queries::all_certificates(&snapshot.tx).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: rows
                    .iter()
                    .map(|row| rows::certificate_from_sql(&row.signed))
                    .collect(),
            })
        })
    }

    fn revocations_of(
        &self,
        delegation: Digest<Delegation>,
    ) -> Query<'_, F, BTreeSet<Digest<RevocationId>>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let found =
                queries::revocations_of(&snapshot.tx, delegation.as_slice().to_vec()).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: found
                    .iter()
                    .map(|digest| rows::digest_from_sql(digest))
                    .collect(),
            })
        })
    }

    fn delegations_to(
        &self,
        subject: Identifier,
        audience: Identifier,
    ) -> Query<'_, F, BTreeMap<Digest<Delegation>, (Identifier, Delegation)>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let found = queries::delegations_to(
                &snapshot.tx,
                subject.to_bytes().to_vec(),
                audience.to_bytes().to_vec(),
            )
            .await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: found
                    .into_iter()
                    .map(|row| {
                        (
                            rows::digest_from_sql(&row.digest),
                            (
                                rows::identifier_from_sql(&row.issuer),
                                Delegation {
                                    audience,
                                    subject,
                                    power: rows::power_from_sql(row.power),
                                    citation: row
                                        .citation
                                        .map(|citation| rows::digest_from_sql(&citation)),
                                },
                            ),
                        )
                    })
                    .collect(),
            })
        })
    }

    fn effective_power(
        &self,
        subject: Identifier,
        audience: Identifier,
    ) -> Query<'_, F, Option<Power>, Error> {
        F::from_future(
            self.evaluated(subject, move |state| state.effective_power(subject, audience)),
        )
    }

    fn members(&self, subject: Identifier) -> Query<'_, F, BTreeMap<Identifier, Power>, Error> {
        F::from_future(self.evaluated(subject, move |state| state.members(subject)))
    }

    fn members_for_subset(
        &self,
        subset: BTreeSet<CertificateId>,
        subject: Identifier,
    ) -> Query<'_, F, BTreeMap<Identifier, Power>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let evaluated = closure::load_ids(&snapshot.tx, subset, &self.memo).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: evaluated.state().members(subject),
            })
        })
    }

    fn subjects(&self) -> Query<'_, F, BTreeSet<Identifier>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let found = queries::subjects(&snapshot.tx).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: found
                    .iter()
                    .map(|subject| rows::identifier_from_sql(subject))
                    .collect(),
            })
        })
    }

    fn is_live(&self, delegation: Digest<Delegation>) -> Query<'_, F, bool, Error> {
        F::from_future(self.evaluated_for(delegation, move |state| state.is_live(&delegation)))
    }

    fn conveyed_power(&self, delegation: Digest<Delegation>) -> Query<'_, F, Option<Power>, Error> {
        F::from_future(
            self.evaluated_for(delegation, move |state| state.conveyed_power(&delegation)),
        )
    }

    fn positive_power(
        &self,
        subject: Identifier,
        audience: Identifier,
    ) -> Query<'_, F, Option<Power>, Error> {
        F::from_future(
            self.evaluated(subject, move |state| state.positive_power(subject, audience)),
        )
    }

    fn closure(&self, subject: Identifier) -> Query<'_, F, Vec<Certificate<W>>, Error> {
        F::from_future(self.evaluated(subject, move |state| state.closure(subject)))
    }

    fn digest(&self) -> Query<'_, F, Digest<CertificateSet>, Error> {
        F::from_future(async move {
            let snapshot = self.snapshot().await?;
            let ids = queries::all_certificate_ids(&snapshot.tx).await?;
            let version = snapshot.finish().await?;
            Ok(Versioned {
                version,
                value: set_digest(
                    ids.iter()
                        .map(|row| rows::id_from_sql(row.kind, &row.digest)),
                ),
            })
        })
    }
}
