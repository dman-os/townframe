-- The keyhive state a `KeyhiveStore` holds, one entity per table. Group membership is not here: it
-- is the Keyline certificate set (`keyline_sqlite`), which commits in the same transaction.
-- Ids and digests are 32-byte blobs; signed values are their wire frames, verified again when
-- read.

-- The wrapped data-encryption keys of the secret columns: one row per (kind, version), so rows
-- written under an old version stay readable while any exist. `wrapped_dek` is the DEK sealed under
-- a key-encryption key the embedder holds outside the database; `kek_version` names which.
CREATE TABLE kh_dek (
      dek_id TEXT NOT NULL
    , dek_version INTEGER NOT NULL CHECK (dek_version >= 1)
    , algorithm TEXT NOT NULL
    , nonce BLOB NOT NULL CHECK (length(nonce) = 12)
    , wrapped_dek BLOB NOT NULL
    , kek_version INTEGER NOT NULL
    , PRIMARY KEY (dek_id, dek_version)
) STRICT, WITHOUT ROWID;

-- Every key pair this identity holds, by public key. `sealed` is the secret key under the row's DEK
-- (AEAD, with the public key as associated data); `nonce` is that seal's nonce.
CREATE TABLE kh_key_pair (
      public_key BLOB PRIMARY KEY CHECK (length(public_key) = 32)
    , sealed BLOB NOT NULL
    , nonce BLOB NOT NULL CHECK (length(nonce) = 12)
    , dek_id TEXT NOT NULL
    , dek_version INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

-- The prekey operations of each individual. The key serves a load by individual, a load by digest
-- and the listing of digests. The operations are small, so they live in the key's own B-tree.
CREATE TABLE kh_prekey_op (
      individual_id BLOB NOT NULL CHECK (length(individual_id) = 32)
    , digest BLOB NOT NULL CHECK (length(digest) = 32)
    , signed BLOB NOT NULL
    , PRIMARY KEY (individual_id, digest)
) STRICT, WITHOUT ROWID;

-- The CGKA operations of each document. A rowid table, unlike the others: an operation carries a
-- path of keys and can be kilobytes, and `list_cgka_op_index` must not read them. The unique index
-- is the digest listing, and it holds no operation bytes.
CREATE TABLE kh_cgka_op (
      seq INTEGER PRIMARY KEY
    , doc_id BLOB NOT NULL CHECK (length(doc_id) = 32)
    , digest BLOB NOT NULL CHECK (length(digest) = 32)
    , signed BLOB NOT NULL
    , UNIQUE (doc_id, digest)
) STRICT;

-- The dependency graph of a document's operations: one row per (operation, predecessor), copied out
-- of the operation by the insert that adds it. A predecessor need not be held: an operation can
-- arrive before the ones it names.
CREATE TABLE kh_cgka_op_pred (
      doc_id BLOB NOT NULL
    , digest BLOB NOT NULL
    , predecessor BLOB NOT NULL CHECK (length(predecessor) = 32)
    , PRIMARY KEY (doc_id, digest, predecessor)
) STRICT, WITHOUT ROWID;

-- Who names an operation as a predecessor: the reverse edge, for walking a document's graph
-- forward (which operations follow a head). Covering: the walk reads no table row.
CREATE INDEX kh_cgka_op_pred_by_predecessor
    ON kh_cgka_op_pred (doc_id, predecessor, digest);

-- The content heads the application last set for a document, replaced on each set. `heads` is the
-- heads' encodings back to back (each is self-delimiting); an empty value is a set of no heads, and
-- no row is never having set any.
CREATE TABLE kh_content_heads (
      doc_id BLOB PRIMARY KEY CHECK (length(doc_id) = 32)
    , heads BLOB NOT NULL
) STRICT, WITHOUT ROWID;

-- The key of each piece of content this replica wrote or read. Durable, not a cache in practice:
-- for content of an epoch before this replica's access, this row is the only copy of its key.
-- `sealed` is the key under the row's DEK, with `(doc_id, content_ref)` as associated data.
CREATE TABLE kh_content_key (
      doc_id BLOB NOT NULL CHECK (length(doc_id) = 32)
    , content_ref BLOB NOT NULL
    , sealed BLOB NOT NULL
    , nonce BLOB NOT NULL CHECK (length(nonce) = 12)
    , dek_id TEXT NOT NULL
    , dek_version INTEGER NOT NULL
    , PRIMARY KEY (doc_id, content_ref)
) STRICT, WITHOUT ROWID;

-- Events that waited for a dependency when they were kept, as received (not verified), by digest.
-- Nothing in an event is secret: a CGKA operation's secrets are already encrypted to leaf keys.
CREATE TABLE kh_pending_event (
      digest BLOB PRIMARY KEY CHECK (length(digest) = 32)
    , event BLOB NOT NULL
) STRICT, WITHOUT ROWID;
