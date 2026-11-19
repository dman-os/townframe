CREATE TABLE IF NOT EXISTS doc_blob_pins (
    doc_id TEXT NOT NULL
  , branch_id TEXT NOT NULL
  , blob_hash TEXT NOT NULL
  , length_octets INTEGER NOT NULL DEFAULT 0
  , origin_heads TEXT NOT NULL
  , PRIMARY KEY(doc_id, branch_id, blob_hash)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_doc_branch ON doc_blob_pins(doc_id, branch_id);
CREATE INDEX IF NOT EXISTS idx_doc_blob_pins_blob_hash ON doc_blob_pins(blob_hash);
