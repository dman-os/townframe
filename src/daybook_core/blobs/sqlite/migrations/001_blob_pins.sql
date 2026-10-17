-- Pin state derived from Blob facets, keyed by branch, and the plug pin
-- state; `blob_pin_*_state` is what the inventory reconciliation diffs.
CREATE TABLE IF NOT EXISTS blob_pin_doc_state (
    doc_id TEXT NOT NULL
  , branch_id TEXT NOT NULL
  , blob_hash TEXT NOT NULL
  , length_octets INTEGER NOT NULL
  , PRIMARY KEY (doc_id, branch_id, blob_hash)
);
CREATE INDEX IF NOT EXISTS idx_blob_pin_doc_state_hash ON blob_pin_doc_state(blob_hash);

CREATE TABLE IF NOT EXISTS blob_pin_plug_state (
    plug_id TEXT NOT NULL
  , blob_hash TEXT NOT NULL
  , length_octets INTEGER NOT NULL
  , PRIMARY KEY (plug_id, blob_hash)
);
CREATE INDEX IF NOT EXISTS idx_blob_pin_plug_state_hash ON blob_pin_plug_state(blob_hash);

CREATE TABLE IF NOT EXISTS blob_pin_cipher_doc_state (
    doc_id TEXT NOT NULL
  , branch_id TEXT NOT NULL
  , cipher_hash TEXT NOT NULL
  , length_octets INTEGER NOT NULL
  , PRIMARY KEY (doc_id, branch_id, cipher_hash)
);
CREATE INDEX IF NOT EXISTS idx_blob_pin_cipher_doc_state_hash ON blob_pin_cipher_doc_state(cipher_hash);

-- The pair-root ledger (ADR 003 §19): a row exists from just before a
-- ciphertext pair is rooted until a durable facet or a pin release records it.
CREATE TABLE IF NOT EXISTS blob_pair_root (
    cipher_hash TEXT NOT NULL PRIMARY KEY
  , doc_id TEXT
  , branch_path TEXT
) STRICT;
