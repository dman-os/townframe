//! The store handle, its write transaction, and the trait impls that delegate to `write.rs`,
//! `read.rs`, `serve.rs` and `log.rs`.

use big_automata::BuckLevel;
use big_automata::Clock;
use big_automata::long_poll::LongPollHub;
use big_automata::part_store::AccessLevel;
use big_automata::part_store::AuthorizedPage;
use big_automata::part_store::Generation;
use big_automata::part_store::LeafPage;
use big_automata::part_store::LogEntry;
use big_automata::part_store::LogInterest;
use big_automata::part_store::LogKey;
use big_automata::part_store::LogRoutes;
use big_automata::part_store::LogSelector;
use big_automata::part_store::ObjPayload;
use big_automata::part_store::ObjState;
use big_automata::part_store::PartStoreRead;
use big_automata::part_store::PartStoreServe;
use big_automata::part_store::PartStoreWrite;
use big_automata::part_store::PartSummary;
use big_automata::part_store::PayloadDigest;
use big_automata::part_store::bucket_summary::BucketSummary;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use big_automata::rev::RevWatch;
use tokio::sync::watch;

use crate::buckets;
use crate::db::PartDb;
use crate::interlude::*;
use crate::log;
use crate::okv::Staged;
use crate::read;
use crate::serve;
use crate::sql;
use crate::sqlite::queries;
use crate::write;

/// The SQLite part store. Clones share the database handle and the watch.
///
/// Open one store per database: the watch is the store's own, moved by the commits made
/// through it, so two stores over one file would not wake each other's readers.
pub struct SqlitePartStore<D: PartDb> {
    pub(crate) db: D,
    /// The log's watch, moved after each stamping commit is visible to readers.
    wakeups: Arc<watch::Sender<Rev>>,
    /// The long-poll hub, told after each stamping commit is visible, with what the commit
    /// stamped.
    polls: LongPollHub<LogInterest>,
}

impl<D: PartDb> Clone for SqlitePartStore<D> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            wakeups: Arc::clone(&self.wakeups),
            polls: self.polls.clone(),
        }
    }
}

/// A write transaction: the database's writer, held from [`SqlitePartStore::begin`] until
/// [`commit`](Self::commit) or [`rollback`](Self::rollback); dropping it rolls it back.
pub struct SqlitePartTx<D: PartDb> {
    store: SqlitePartStore<D>,
    pub(crate) tx: D::Write,
    /// The rev every row this transaction stamps gets: the log's rev at `begin`, plus one.
    rev: Rev,
    /// The log rows this transaction has stamped, which the hub is told of at commit. A
    /// transaction can stamp one key twice (an add then a remove): the log keeps one row.
    stamped: BTreeSet<LogKey>,
    /// The okv tables this transaction has written, by namespace: their watches move after the
    /// commit, and never for a rollback (the map is dropped with the transaction).
    pub(crate) okv: BTreeMap<i64, Staged>,
}

impl<D: PartDb> SqlitePartStore<D> {
    /// Attach to `db`, applying its migrations if it has not run them. `db` must have been
    /// registered with [`MIGRATIONS`](crate::MIGRATIONS) under
    /// [`MIGRATION_TABLE`](crate::MIGRATION_TABLE). The long-poll hub reads `clock`, the
    /// embedder's injected time.
    pub async fn open(db: D, clock: Clock) -> Res<Self> {
        let reader = db.begin_read().await?;
        let rev = queries::meta_rev(&reader).await?;
        D::end_read(reader).await?;
        let (wakeups, _) = watch::channel(sql::rev_from_sql(rev));
        Ok(Self {
            db,
            wakeups: Arc::new(wakeups),
            polls: LongPollHub::new(clock),
        })
    }

    /// Start a write transaction. It holds the database's writer until it ends.
    pub async fn begin(&self) -> Res<SqlitePartTx<D>> {
        let tx = self.db.begin_write().await?;
        let rev = sql::rev_from_sql(queries::meta_rev(&tx).await?) + 1;
        Ok(SqlitePartTx {
            store: self.clone(),
            tx,
            rev,
            stamped: BTreeSet::new(),
            okv: BTreeMap::new(),
        })
    }

    /// One read snapshot for the duration of `D::end_read`.
    async fn snapshot(&self) -> Res<D::Read> {
        Ok(self.db.begin_read().await?)
    }
}

impl<D: PartDb> SqlitePartTx<D> {
    /// Commit every staged batch at once. Returns the rev that stamped the changed log rows, or
    /// `None` when no row changed (then no rev is consumed and the watch stays put).
    pub async fn commit(self) -> Res<Option<Rev>> {
        let Self {
            store,
            tx,
            rev,
            stamped,
            okv,
        } = self;
        if !stamped.is_empty() {
            queries::set_meta_rev(&tx, sql::rev_to_sql(rev)).await?;
        }
        D::commit(tx).await?;
        if !stamped.is_empty() {
            // After the commit is visible: a woken reader must find it in place.
            store.wakeups.send_replace(rev);
            let mut changes = BTreeMap::<LogInterest, u64>::new();
            for key in &stamped {
                for interest in key.interests() {
                    *changes.entry(interest).or_default() += 1;
                }
            }
            store.polls.committed(rev, changes);
        }
        for staged in okv.into_values() {
            staged.publish();
        }
        Ok((!stamped.is_empty()).then_some(rev))
    }

    pub async fn rollback(self) -> Res<()> {
        D::rollback(self.tx).await?;
        Ok(())
    }
}

impl<D: PartDb> PartStoreWrite for SqlitePartStore<D> {
    type Tx = SqlitePartTx<D>;

    async fn ensure_parts(&self, tx: &mut SqlitePartTx<D>, parts: &[PartKey]) -> Res<()> {
        write::ensure_parts(&tx.tx, parts).await
    }

    async fn set_payloads(
        &self,
        tx: &mut SqlitePartTx<D>,
        payloads: Vec<(ObjKey, ObjPayload)>,
    ) -> Res<()> {
        // Digests first: a payload with no canonical form refuses the whole batch before a
        // row is written.
        let digested = payloads
            .into_iter()
            .map(|(obj, payload)| {
                let digest = PayloadDigest::of(&payload)?;
                Ok((obj, payload, digest))
            })
            .collect::<Res<Vec<_>>>()?;
        tx.stamped
            .extend(write::set_payloads(&tx.tx, tx.rev, digested).await?);
        Ok(())
    }

    async fn add_members(
        &self,
        tx: &mut SqlitePartTx<D>,
        members: &[(ObjKey, PartKey)],
    ) -> Res<()> {
        tx.stamped
            .extend(write::add_members(&tx.tx, tx.rev, members).await?);
        Ok(())
    }

    async fn remove_members(
        &self,
        tx: &mut SqlitePartTx<D>,
        members: &[(ObjKey, PartKey)],
    ) -> Res<()> {
        tx.stamped
            .extend(write::remove_members(&tx.tx, tx.rev, members).await?);
        Ok(())
    }

    async fn drop_payloads(&self, tx: &mut SqlitePartTx<D>, objs: &[ObjKey]) -> Res<()> {
        write::drop_payloads(&tx.tx, objs).await
    }

    /// Its own write transaction, taken and committed here: the store's one writer, so it waits
    /// for an open [`SqlitePartTx`] and must not be called while holding one. It sets the meta
    /// row's generation and nothing else, so no rev is consumed and the watch stays put.
    async fn mint_generation(&self) -> Res<Generation> {
        let minted = Generation::mint();
        let tx = self.db.begin_write().await?;
        queries::set_meta_generation(&tx, sql::generation_to_sql(minted)).await?;
        D::commit(tx).await?;
        Ok(minted)
    }

    async fn grant_access(
        &self,
        tx: &mut SqlitePartTx<D>,
        grants: &[(PartKey, PeerKey, AccessLevel)],
    ) -> Res<()> {
        tx.stamped
            .extend(write::grant_access(&tx.tx, tx.rev, grants).await?);
        Ok(())
    }

    async fn revoke_access(
        &self,
        tx: &mut SqlitePartTx<D>,
        revokes: &[(PartKey, PeerKey)],
    ) -> Res<()> {
        tx.stamped
            .extend(write::revoke_access(&tx.tx, tx.rev, revokes).await?);
        Ok(())
    }
}

/// Run `$body` against a fresh read snapshot named `$reader`, then end the snapshot. An error
/// drops it, which rolls it back.
macro_rules! snapshot {
    ($self:ident, $reader:ident => $body:expr) => {{
        let $reader = $self.snapshot().await?;
        let answer = $body;
        D::end_read($reader).await?;
        answer
    }};
}

impl<D: PartDb> PartStoreRead for SqlitePartStore<D> {
    async fn obj_states(&self, objs: &[ObjKey]) -> Res<Vec<Option<ObjState>>> {
        Ok(snapshot!(self, reader => read::obj_states(&reader, None, objs).await?))
    }

    async fn bucket_summaries(&self, part: &PartKey, ids: &[BuckId]) -> Res<Vec<BucketSummary>> {
        Ok(snapshot!(self, reader => buckets::summaries(&reader, part, ids).await?))
    }

    async fn member_count(&self, part: &PartKey) -> Res<u64> {
        Ok(snapshot!(self, reader => read::member_count(&reader, part).await?))
    }

    async fn partless_objects(&self, limit: usize, after: Option<&ObjKey>) -> Res<Vec<ObjKey>> {
        Ok(snapshot!(self, reader => read::partless_objects(&reader, limit, after).await?))
    }
}

impl<D: PartDb> RevSource for SqlitePartStore<D> {
    type Key = LogKey;
    type Value = LogEntry;
    type Selector = LogSelector;

    async fn latest_rev(&self) -> Res<Rev> {
        Ok(snapshot!(self, reader => read::latest_rev(&reader).await?))
    }

    /// Unauthorized: every selected row, whoever asks. For the store's local consumers; a
    /// responder serves a peer through [`PartStoreServe::read_authorized`].
    async fn read(
        &self,
        selector: &LogSelector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<LogKey, LogEntry>,
    ) -> Res<()> {
        assert!(after < until, "empty read window ({after}, {until}]");
        snapshot!(self, reader => {
            let latest = read::latest_rev(&reader).await?;
            assert!(until <= latest, "read until {until} past the committed rev {latest}");
            log::read_selected(&reader, selector, after, until, limits, page).await?;
        });
        Ok(())
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }
}

impl<D: PartDb> PartStoreServe for SqlitePartStore<D> {
    async fn latest_rev(&self) -> Res<Rev> {
        RevSource::latest_rev(self).await
    }

    /// The log's own watch: the one [`RevSource::watch`] hands out.
    fn watch(&self) -> RevWatch {
        RevSource::watch(self)
    }

    fn long_poll(&self) -> LongPollHub<LogInterest> {
        self.polls.clone()
    }

    async fn generation(&self) -> Res<Generation> {
        Ok(snapshot!(self, reader => read::generation(&reader).await?))
    }

    async fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut AuthorizedPage,
    ) -> Res<()> {
        snapshot!(self, reader => {
            serve::read_authorized(&reader, principal, routes, after, until, limits, page).await?;
        });
        Ok(())
    }

    async fn part_summaries(
        &self,
        principal: &PeerKey,
        parts: &[(PartKey, Rev)],
    ) -> Res<Vec<Option<PartSummary>>> {
        Ok(snapshot!(self, reader => serve::part_summaries(&reader, principal, parts).await?))
    }

    async fn readable_obj_states(
        &self,
        principal: &PeerKey,
        objs: &[ObjKey],
    ) -> Res<Vec<Option<ObjState>>> {
        Ok(snapshot!(self, reader => read::obj_states(&reader, Some(principal), objs).await?))
    }

    async fn bucket_list(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        since: Rev,
        limit: usize,
    ) -> Res<Option<Vec<BucketSummary>>> {
        Ok(snapshot!(self, reader => {
            serve::bucket_list(&reader, principal, part, from, to_level, since, limit).await?
        }))
    }

    async fn bucket_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        bucks: &[(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
        byte_budget: usize,
    ) -> Res<Option<Vec<LeafPage>>> {
        Ok(snapshot!(self, reader => {
            serve::bucket_leaves(&reader, principal, part, bucks, since, limit, byte_budget)
                .await?
        }))
    }
}
