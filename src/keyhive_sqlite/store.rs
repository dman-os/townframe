//! [`SqliteKeyhiveStore`] and its [`KeyhiveStore`] implementation.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use beekem::operation::CgkaOperation;
use future_form::FutureForm;
use future_form::Local;
use future_form::Sendable;
use future_form::future_form;
use keyhive_content::reference::ContentRef;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId;
use keyhive_core::principal::individual::id::IndividualId;
use keyhive_core::principal::individual::op::KeyOp;
use keyhive_core::store::CgkaOpIndexRow;
use keyhive_core::store::KeyhiveStore;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::share_key::ShareKey;
use keyhive_crypto::share_key::ShareSecretKey;
use keyhive_crypto::symmetric_key::SymmetricKey;
use keyhive_crypto::verified::Verified;
use sqlite_rs::SqliteDb;

use crate::KekSource;
use crate::error::Error;
use crate::session::Session;

/// The keyhive state of one replica in a SQLite database.
///
/// Clones share the database handle and the key source. Used as a [`KeyhiveStore`], each call is
/// its own transaction (a read snapshot, or a writer transaction that commits when the call
/// succeeds). To commit keyhive rows together with others, open the transaction yourself and use
/// [`session`](Self::session).
pub struct SqliteKeyhiveStore<T, D: SqliteDb> {
    db: D,
    keks: Arc<dyn KekSource>,
    _content: PhantomData<fn() -> T>,
}

impl<T, D: SqliteDb> Clone for SqliteKeyhiveStore<T, D> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            keks: Arc::clone(&self.keks),
            _content: PhantomData,
        }
    }
}

impl<T: ContentRef, D: SqliteDb> SqliteKeyhiveStore<T, D> {
    /// Attach to `db`, which must have been registered with [`MIGRATIONS`](crate::MIGRATIONS)
    /// under [`MIGRATION_TABLE`](crate::MIGRATION_TABLE). Secrets are sealed under DEKs that
    /// `keks` wraps.
    pub fn new(db: D, keks: impl KekSource) -> Self {
        Self {
            db,
            keks: Arc::new(keks),
            _content: PhantomData,
        }
    }

    /// The operations of this store through `tx`, a transaction or snapshot of this database that
    /// the caller owns. See [`Session`].
    pub fn session<'a, E>(&'a self, tx: &'a E) -> Session<'a, T, E> {
        Session::new(tx, &*self.keks)
    }
}

/// Run `$body` over a session of a fresh read snapshot, then end the snapshot. An error before the
/// end drops the snapshot, which releases it.
macro_rules! read {
    ($store:ident, $session:ident => $body:expr) => {{
        let tx = $store.db.begin_read().await?;
        let out = {
            let $session = $store.session(&tx);
            $body.await?
        };
        D::end_read(tx).await?;
        Ok(out)
    }};
}

/// Run `$body` over a session of a fresh writer transaction, and commit it. An error before the
/// commit drops the transaction, which rolls it back.
macro_rules! write {
    ($store:ident, $session:ident => $body:expr) => {{
        let tx = $store.db.begin_write().await?;
        {
            let $session = $store.session(&tx);
            $body.await?;
        }
        D::commit(tx).await?;
        Ok(())
    }};
}

#[future_form(Sendable where T: Send + Sync, Local)]
impl<F: FutureForm, T: ContentRef, D: SqliteDb> KeyhiveStore<F, T> for SqliteKeyhiveStore<T, D> {
    type Error = Error;

    fn load_key_pairs(
        &self,
    ) -> F::Future<'_, Result<BTreeMap<ShareKey, ShareSecretKey>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_key_pairs()) })
    }

    fn put_key_pairs<'a>(
        &'a self,
        pairs: &'a [(ShareKey, ShareSecretKey)],
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.put_key_pairs(pairs)) })
    }

    fn load_prekey_ops(
        &self,
        who: IndividualId,
    ) -> F::Future<'_, Result<Vec<Arc<KeyOp>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_prekey_ops(who)) })
    }

    fn append_prekey_ops<'a>(
        &'a self,
        who: IndividualId,
        ops: &'a [Arc<KeyOp>],
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.append_prekey_ops(who, ops)) })
    }

    fn load_prekey_ops_by_digest<'a>(
        &'a self,
        who: IndividualId,
        digests: &'a [Digest<KeyOp>],
    ) -> F::Future<'a, Result<Vec<Arc<KeyOp>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_prekey_ops_by_digest(who, digests)) })
    }

    fn list_prekey_op_digests(
        &self,
        who: IndividualId,
    ) -> F::Future<'_, Result<Vec<Digest<KeyOp>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.list_prekey_op_digests(who)) })
    }

    fn list_individuals(&self) -> F::Future<'_, Result<Vec<IndividualId>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.list_individuals()) })
    }

    fn load_cgka_ops(
        &self,
        doc: DocumentId,
    ) -> F::Future<'_, Result<Vec<Arc<Verified<CgkaOperation>>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_cgka_ops(doc)) })
    }

    fn load_cgka_ops_by_digest<'a>(
        &'a self,
        doc: DocumentId,
        digests: &'a [Digest<CgkaOperation>],
    ) -> F::Future<'a, Result<Vec<Arc<Verified<CgkaOperation>>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_cgka_ops_by_digest(doc, digests)) })
    }

    fn list_cgka_op_index(
        &self,
        doc: DocumentId,
    ) -> F::Future<'_, Result<Vec<CgkaOpIndexRow>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.list_cgka_op_index(doc)) })
    }

    fn append_cgka_ops<'a>(
        &'a self,
        doc: DocumentId,
        ops: &'a [Arc<Verified<CgkaOperation>>],
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.append_cgka_ops(doc, ops)) })
    }

    fn list_documents(&self) -> F::Future<'_, Result<Vec<DocumentId>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.list_documents()) })
    }

    fn load_content_heads(
        &self,
        doc: DocumentId,
    ) -> F::Future<'_, Result<Option<Vec<T>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_content_heads(doc)) })
    }

    fn put_content_heads<'a>(
        &'a self,
        doc: DocumentId,
        heads: &'a [T],
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.put_content_heads(doc, heads)) })
    }

    fn get_cached_content_key<'a>(
        &'a self,
        doc: DocumentId,
        content_ref: &'a T,
    ) -> F::Future<'a, Result<Option<SymmetricKey>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.get_cached_content_key(doc, content_ref)) })
    }

    fn cache_content_key<'a>(
        &'a self,
        doc: DocumentId,
        content_ref: &'a T,
        key: &'a SymmetricKey,
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.cache_content_key(doc, content_ref, key)) })
    }

    fn load_pending_events(&self) -> F::Future<'_, Result<Vec<StaticEvent<T>>, Self::Error>> {
        F::from_future(async move { read!(self, s => s.load_pending_events()) })
    }

    fn put_pending_events<'a>(
        &'a self,
        events: &'a [StaticEvent<T>],
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.put_pending_events(events)) })
    }

    fn drop_pending_events<'a>(
        &'a self,
        digests: &'a [Digest<StaticEvent<T>>],
    ) -> F::Future<'a, Result<(), Self::Error>> {
        F::from_future(async move { write!(self, s => s.drop_pending_events(digests)) })
    }
}
