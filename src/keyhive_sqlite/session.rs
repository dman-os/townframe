//! [`Session`]: the store's operations over a transaction or snapshot the caller owns.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use beekem::operation::CgkaOperation;
use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyhive_content::reference::ContentRef;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId;
use keyhive_core::principal::individual::id::IndividualId;
use keyhive_core::principal::individual::op::KeyOp;
use keyhive_core::principal::individual::op::SignedKeyOp;
use keyhive_core::store::CgkaOpIndexRow;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::identifier::Identifier;
use keyhive_crypto::share_key::ShareKey;
use keyhive_crypto::share_key::ShareSecretKey;
use keyhive_crypto::symmetric_key::SymmetricKey;
use keyhive_crypto::verified::Signed;
use keyhive_crypto::verified::Verified;
use sqlite_rs::Executor;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::KekSource;
use crate::error::Error;
use crate::error::SecretFault;
use crate::error::StoredFault;
use crate::secret::Kind;
use crate::secret::open;
use crate::secret::seal;
use crate::sqlite::queries;

/// Headroom under SQLite's default `SQLITE_MAX_VARIABLE_NUMBER` of 32766.
const MAX_PARAMS: usize = 30_000;

const KEY_PAIR: &str = "kh_key_pair";
const PREKEY_OP: &str = "kh_prekey_op";
const CGKA_OP: &str = "kh_cgka_op";
const CONTENT_HEADS: &str = "kh_content_heads";
const CONTENT_KEY: &str = "kh_content_key";
const PENDING_EVENT: &str = "kh_pending_event";

/// The store's operations through `tx`, a transaction or read snapshot the caller owns:
/// [`KeyhiveStore`](keyhive_core::store::KeyhiveStore)'s methods without its futures, and
/// without a transaction of their own.
///
/// Reads need a [`QueryExecutor`] (a snapshot, or a writer transaction, which reads its own
/// writes); writes need an [`Executor`]. A write is in `tx` and for whoever reads through it
/// until `tx` commits, and for nobody if it rolls back. That is how keyhive rows commit with the
/// sedimentree and Keyline rows of the same database: take one writer transaction, hand it to each
/// store's `_in` entry point, commit once. Keyline answers can be read in the same snapshot as
/// keyhive's by sharing a read transaction the same way.
pub struct Session<'a, T, E> {
    pub(crate) tx: &'a E,
    pub(crate) keks: &'a dyn KekSource,
    _content: PhantomData<fn() -> T>,
}

impl<'a, T, E> Session<'a, T, E> {
    pub(crate) fn new(tx: &'a E, keks: &'a dyn KekSource) -> Self {
        Self {
            tx,
            keks,
            _content: PhantomData,
        }
    }
}

fn stored(table: &'static str, key: &[u8], source: impl Into<StoredFault>) -> Error {
    Error::Stored {
        table,
        key: key.to_vec(),
        source: source.into(),
    }
}

/// A `count(*)` column, which is never negative.
fn count(ops: i64) -> u64 {
    u64::try_from(ops).expect("a count is not negative")
}

fn digest_of<X>(bytes: &[u8]) -> Digest<X> {
    let bytes: [u8; 32] = bytes.try_into().expect("a digest column is 32 bytes");
    Digest::from(bytes)
}

fn document_bytes(doc: DocumentId) -> Vec<u8> {
    Identifier::from(doc).to_bytes().to_vec()
}

fn individual_bytes(who: IndividualId) -> Vec<u8> {
    Identifier::from(who).to_bytes().to_vec()
}

fn encoded<X: Encode>(value: &X) -> Vec<u8> {
    value.encode().as_bytes().to_vec()
}

fn digests_bytes<X>(digests: &[Digest<X>]) -> Vec<Vec<u8>> {
    digests
        .iter()
        .map(|digest| digest.as_slice().to_vec())
        .collect()
}

/// What binds a sealed secret to its row.
fn row_aad(table: &str, key: &[&[u8]]) -> Vec<u8> {
    let mut aad = table.as_bytes().to_vec();
    aad.push(0);
    for part in key {
        aad.extend_from_slice(part);
    }
    aad
}

/// A prekey operation read from `(who, digest)`, verified, and checked to be that row's.
fn prekey_op(who: &[u8], digest: &[u8], signed: &[u8]) -> Result<Arc<KeyOp>, Error> {
    let fault = |source: StoredFault| stored(PREKEY_OP, digest, source);
    let op = SignedKeyOp::decode(signed)
        .map_err(|e| fault(e.into()))?
        .verify()
        .map_err(|e| fault(e.into()))?;
    if op.digest().as_slice() != digest || Identifier::to_bytes(&op.issuer()).as_slice() != who {
        return Err(fault(StoredFault::Identity));
    }
    Ok(Arc::new(op))
}

fn cgka_op(digest: &[u8], signed: &[u8]) -> Result<Arc<Verified<CgkaOperation>>, Error> {
    let fault = |source: StoredFault| stored(CGKA_OP, digest, source);
    let op = Signed::<CgkaOperation>::decode(signed)
        .map_err(|e| fault(e.into()))?
        .verify()
        .map_err(|e| fault(e.into()))?;
    if op.digest().as_slice() != digest {
        return Err(fault(StoredFault::Identity));
    }
    Ok(Arc::new(op))
}

/// Run `INSERT ... VALUES` for `rows` in statements that stay under SQLite's variable limit.
/// `head` is the statement up to and including `VALUES `.
async fn insert_rows<R: sqlite_rs::BindTuple>(
    tx: &impl Executor,
    head: &str,
    width: usize,
    rows: Vec<R>,
    tail: &str,
) -> Result<(), Error> {
    let mut rows = rows.into_iter().peekable();
    while rows.peek().is_some() {
        let mut sql = Sql::new(head);
        for (index, row) in rows.by_ref().take(MAX_PARAMS / width).enumerate() {
            if index != 0 {
                sql.push(", ");
            }
            row.bind_tuple(&mut sql);
        }
        sql.push(tail);
        sql.execute(tx).await?;
    }
    Ok(())
}

impl<T: ContentRef, E: QueryExecutor> Session<'_, T, E> {
    /// Rows of `table` where `column` is `key` and `digest` is among `digests`, as
    /// `(digest, signed)`, chunked under the variable limit.
    async fn by_digests(
        &self,
        select: &str,
        key: Vec<u8>,
        digests: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut found = Vec::new();
        for chunk in digests.chunks(MAX_PARAMS - 1) {
            let mut sql = Sql::new(select);
            sql.bind(key.clone()).push(" AND digest IN ");
            sql.bind_list(chunk.iter().cloned());
            found.extend(sql.query(self.tx).await?.all::<(Vec<u8>, Vec<u8>)>()?);
        }
        Ok(found)
    }

    pub async fn load_key_pairs(&self) -> Result<BTreeMap<ShareKey, ShareSecretKey>, Error> {
        let rows = queries::key_pairs_all(self.tx).await?;
        let mut deks = HashMap::new();
        let mut pairs = BTreeMap::new();
        for row in rows {
            let key = (row.dek_id.clone(), row.dek_version);
            if !deks.contains_key(&key) {
                let dek = self.dek(KEY_PAIR, &row.dek_id, row.dek_version).await?;
                deks.insert(key.clone(), dek);
            }
            let aad = row_aad(KEY_PAIR, &[&row.public_key]);
            let secret = open(&deks[&key], &aad, &row.nonce, &row.sealed).map_err(|source| {
                Error::Secret {
                    table: KEY_PAIR,
                    source,
                }
            })?;
            let secret = ShareSecretKey::try_from_slice(&secret).ok_or(Error::Secret {
                table: KEY_PAIR,
                source: SecretFault::Shape,
            })?;
            let public = ShareKey::decode(&row.public_key)
                .map_err(|e| stored(KEY_PAIR, &row.public_key, e))?;
            if secret.share_key() != public {
                return Err(stored(KEY_PAIR, &row.public_key, StoredFault::Identity));
            }
            pairs.insert(public, secret);
        }
        Ok(pairs)
    }

    pub async fn load_prekey_ops(&self, who: IndividualId) -> Result<Vec<Arc<KeyOp>>, Error> {
        let who = individual_bytes(who);
        queries::prekey_ops_of(self.tx, who.clone())
            .await?
            .iter()
            .map(|row| prekey_op(&who, &row.digest, &row.signed))
            .collect()
    }

    /// In the order of `digests`, a digest listed twice yielding its operation twice, as the
    /// in-memory store does.
    pub async fn load_prekey_ops_by_digest(
        &self,
        who: IndividualId,
        digests: &[Digest<KeyOp>],
    ) -> Result<Vec<Arc<KeyOp>>, Error> {
        let who = individual_bytes(who);
        let select = "SELECT digest\n     , signed\n  FROM kh_prekey_op\n WHERE individual_id = ";
        let found = self
            .by_digests(select, who.clone(), digests_bytes(digests))
            .await?;
        let mut ops = HashMap::new();
        for (digest, signed) in found {
            let op = prekey_op(&who, &digest, &signed)?;
            ops.insert(digest, op);
        }
        Ok(digests
            .iter()
            .filter_map(|digest| ops.get(digest.as_slice()).cloned())
            .collect())
    }

    pub async fn list_prekey_op_digests(
        &self,
        who: IndividualId,
    ) -> Result<Vec<Digest<KeyOp>>, Error> {
        let rows = queries::prekey_op_digests_of(self.tx, individual_bytes(who)).await?;
        Ok(rows.iter().map(|digest| digest_of(digest)).collect())
    }

    /// The store-wide count of prekey and CGKA operations: one row, which the insert of each
    /// operation that was not held bumps (a trigger, so a duplicate that is ignored does not).
    pub async fn total_op_count(&self) -> Result<u64, Error> {
        let ops = queries::total_op_count(self.tx)
            .await?
            .expect("the migration inserts the counter's row");
        Ok(count(ops))
    }

    pub async fn list_individuals(&self) -> Result<Vec<IndividualId>, Error> {
        queries::individuals(self.tx)
            .await?
            .iter()
            .map(|id| {
                Identifier::decode_field(id, "individual_id")
                    .map(IndividualId::from)
                    .map_err(|e| stored(PREKEY_OP, id, e))
            })
            .collect()
    }

    pub async fn load_cgka_ops(
        &self,
        doc: DocumentId,
    ) -> Result<Vec<Arc<Verified<CgkaOperation>>>, Error> {
        queries::cgka_ops_of(self.tx, document_bytes(doc))
            .await?
            .iter()
            .map(|row| cgka_op(&row.digest, &row.signed))
            .collect()
    }

    /// In the order of `digests`, as [`load_prekey_ops_by_digest`](Self::load_prekey_ops_by_digest).
    pub async fn load_cgka_ops_by_digest(
        &self,
        doc: DocumentId,
        digests: &[Digest<CgkaOperation>],
    ) -> Result<Vec<Arc<Verified<CgkaOperation>>>, Error> {
        let select = "SELECT digest\n     , signed\n  FROM kh_cgka_op\n WHERE doc_id = ";
        let found = self
            .by_digests(select, document_bytes(doc), digests_bytes(digests))
            .await?;
        let mut ops = HashMap::new();
        for (digest, signed) in found {
            let op = cgka_op(&digest, &signed)?;
            ops.insert(digest, op);
        }
        Ok(digests
            .iter()
            .filter_map(|digest| ops.get(digest.as_slice()).cloned())
            .collect())
    }

    /// The operations' digests with their predecessors' (no operation is read, or verified: the
    /// predecessors are the columns the append copied out of each).
    pub async fn list_cgka_op_index(&self, doc: DocumentId) -> Result<Vec<CgkaOpIndexRow>, Error> {
        let mut index: Vec<CgkaOpIndexRow> = Vec::new();
        let mut last: Option<Vec<u8>> = None;
        for row in queries::cgka_op_index_of(self.tx, document_bytes(doc)).await? {
            // The join yields one operation's rows together.
            if last.as_deref() != Some(&row.digest) {
                index.push(CgkaOpIndexRow {
                    digest: digest_of(&row.digest),
                    predecessors: Vec::new(),
                });
                last = Some(row.digest);
            }
            if let Some(predecessor) = row.predecessor {
                index
                    .last_mut()
                    .expect("a row was pushed above")
                    .predecessors
                    .push(digest_of(&predecessor));
            }
        }
        Ok(index)
    }

    pub async fn list_documents(&self) -> Result<Vec<DocumentId>, Error> {
        queries::documents(self.tx)
            .await?
            .iter()
            .map(|id| {
                Identifier::decode_field(id, "doc_id")
                    .map(DocumentId::from)
                    .map_err(|e| stored(CGKA_OP, id, e))
            })
            .collect()
    }

    pub async fn load_content_heads(&self, doc: DocumentId) -> Result<Option<Vec<T>>, Error> {
        let doc = document_bytes(doc);
        let Some(blob) = queries::content_heads_get(self.tx, doc.clone()).await? else {
            return Ok(None);
        };
        let mut rest = blob.as_slice();
        let mut heads = Vec::new();
        while !rest.is_empty() {
            let (head, after) = T::decode_next(rest).map_err(|e| stored(CONTENT_HEADS, &doc, e))?;
            heads.push(head);
            rest = after;
        }
        Ok(Some(heads))
    }

    pub async fn get_cached_content_key(
        &self,
        doc: DocumentId,
        content_ref: &T,
    ) -> Result<Option<SymmetricKey>, Error> {
        let doc = document_bytes(doc);
        let content_ref = encoded(content_ref);
        let Some(row) = queries::content_key_get(self.tx, doc.clone(), content_ref.clone()).await?
        else {
            return Ok(None);
        };
        let dek = self.dek(CONTENT_KEY, &row.dek_id, row.dek_version).await?;
        let aad = row_aad(CONTENT_KEY, &[&doc, &content_ref]);
        let key = open(&dek, &aad, &row.nonce, &row.sealed).map_err(|source| Error::Secret {
            table: CONTENT_KEY,
            source,
        })?;
        SymmetricKey::try_from_slice(&key)
            .map(Some)
            .ok_or(Error::Secret {
                table: CONTENT_KEY,
                source: SecretFault::Shape,
            })
    }

    pub async fn load_pending_events(&self) -> Result<Vec<StaticEvent<T>>, Error> {
        queries::pending_events_all(self.tx)
            .await?
            .iter()
            .map(|row| {
                let event = StaticEvent::<T>::decode(&row.event)
                    .map_err(|e| stored(PENDING_EVENT, &row.digest, e))?;
                if event.digest().as_slice() != row.digest {
                    return Err(stored(PENDING_EVENT, &row.digest, StoredFault::Identity));
                }
                Ok(event)
            })
            .collect()
    }
}

impl<T: ContentRef, E: Executor> Session<'_, T, E> {
    pub async fn put_key_pairs(&self, pairs: &[(ShareKey, ShareSecretKey)]) -> Result<(), Error> {
        if pairs.is_empty() {
            return Ok(());
        }
        let dek = self.dek_for_write(KEY_PAIR, Kind::KeyPair).await?;
        let rows = pairs
            .iter()
            .map(|(public, secret)| {
                let aad = row_aad(KEY_PAIR, &[public.as_bytes()]);
                let (sealed, nonce) = seal(&dek, &aad, secret.as_slice());
                (
                    public.as_bytes().to_vec(),
                    sealed,
                    nonce,
                    dek.id.clone(),
                    dek.version,
                )
            })
            .collect();
        let head = "INSERT OR IGNORE INTO kh_key_pair\n     ( public_key\n     , sealed\n     , nonce\n     , dek_id\n     , dek_version\n     )\nVALUES ";
        insert_rows(self.tx, head, 5, rows, "").await
    }

    pub async fn append_prekey_ops(
        &self,
        who: IndividualId,
        ops: &[Arc<KeyOp>],
    ) -> Result<(), Error> {
        let who = individual_bytes(who);
        let rows = ops
            .iter()
            .map(|op| {
                (
                    who.clone(),
                    op.digest().as_slice().to_vec(),
                    encoded(&op.signed()),
                )
            })
            .collect();
        let head = "INSERT OR IGNORE INTO kh_prekey_op\n     ( individual_id\n     , digest\n     , signed\n     )\nVALUES ";
        insert_rows(self.tx, head, 3, rows, "").await
    }

    pub async fn append_cgka_ops(
        &self,
        doc: DocumentId,
        ops: &[Arc<Verified<CgkaOperation>>],
    ) -> Result<(), Error> {
        let doc = document_bytes(doc);
        let rows = ops
            .iter()
            .map(|op| {
                (
                    doc.clone(),
                    op.digest().as_slice().to_vec(),
                    encoded(&**op.signed()),
                )
            })
            .collect();
        let head = "INSERT OR IGNORE INTO kh_cgka_op\n     ( doc_id\n     , digest\n     , signed\n     )\nVALUES ";
        insert_rows(self.tx, head, 3, rows, "").await?;
        let edges = ops
            .iter()
            .flat_map(|op| {
                let digest = op.digest().as_slice().to_vec();
                let doc = doc.clone();
                op.payload().predecessors().iter().map(move |predecessor| {
                    (doc.clone(), digest.clone(), predecessor.as_slice().to_vec())
                })
            })
            .collect();
        let head = "INSERT OR IGNORE INTO kh_cgka_op_pred\n     ( doc_id\n     , digest\n     , predecessor\n     )\nVALUES ";
        insert_rows(self.tx, head, 3, edges, "").await
    }

    pub async fn put_content_heads(&self, doc: DocumentId, heads: &[T]) -> Result<(), Error> {
        let mut blob = Vec::new();
        for head in heads {
            head.encode_into(&mut blob);
        }
        queries::content_heads_put(self.tx, document_bytes(doc), blob).await?;
        Ok(())
    }

    pub async fn cache_content_key(
        &self,
        doc: DocumentId,
        content_ref: &T,
        key: &SymmetricKey,
    ) -> Result<(), Error> {
        let dek = self.dek_for_write(CONTENT_KEY, Kind::ContentKey).await?;
        let doc = document_bytes(doc);
        let content_ref = encoded(content_ref);
        let aad = row_aad(CONTENT_KEY, &[&doc, &content_ref]);
        let (sealed, nonce) = seal(&dek, &aad, key.as_slice());
        queries::content_key_put(
            self.tx,
            doc,
            content_ref,
            sealed,
            nonce,
            dek.id,
            dek.version,
        )
        .await?;
        Ok(())
    }

    pub async fn put_pending_events(&self, events: &[StaticEvent<T>]) -> Result<(), Error> {
        let rows = events
            .iter()
            .map(|event| (event.digest().as_slice().to_vec(), encoded(event)))
            .collect();
        let head =
            "INSERT OR IGNORE INTO kh_pending_event\n     ( digest\n     , event\n     )\nVALUES ";
        insert_rows(self.tx, head, 2, rows, "").await
    }

    pub async fn drop_pending_events(
        &self,
        digests: &[Digest<StaticEvent<T>>],
    ) -> Result<(), Error> {
        for chunk in digests_bytes(digests).chunks(MAX_PARAMS) {
            let mut sql = Sql::new("DELETE FROM kh_pending_event\n WHERE digest IN ");
            sql.bind_list(chunk.iter().cloned());
            sql.execute(self.tx).await?;
        }
        Ok(())
    }
}
