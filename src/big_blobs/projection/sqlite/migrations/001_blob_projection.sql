CREATE TABLE IF NOT EXISTS big_blobs_projection_routes (
       doc_id TEXT NOT NULL
     , datum_key TEXT NOT NULL
     , source_revision INTEGER NOT NULL CHECK (source_revision >= 0)
     , blob_id BLOB
     , length_octets TEXT
     , PRIMARY KEY (doc_id, datum_key)
     , CHECK ((blob_id IS NULL) = (length_octets IS NULL))
     , CHECK (blob_id IS NULL OR length(blob_id) = 32)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS big_blobs_projection_digest
    ON big_blobs_projection_routes (blob_id, doc_id, datum_key);
CREATE TABLE IF NOT EXISTS big_blobs_projection_parked (
       doc_id TEXT NOT NULL
     , datum_key TEXT NOT NULL
     , source_revision INTEGER NOT NULL CHECK (source_revision >= 0)
     , membership BLOB NOT NULL
     , PRIMARY KEY (doc_id, datum_key)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS big_blobs_projection_progress (
       singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
     , source_revision INTEGER NOT NULL CHECK (source_revision >= 0)
);
INSERT OR IGNORE INTO big_blobs_projection_progress (singleton, source_revision)
VALUES (1, 0);
