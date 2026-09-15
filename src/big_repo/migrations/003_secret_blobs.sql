-- Wrapped DEKs, one row per (dek_id, dek_version). Old versions stay present
-- while blobs referencing them still exist, so rotation is incremental.
CREATE TABLE big_repo_deks (
      scope_id INTEGER NOT NULL
      ,dek_id TEXT NOT NULL
      ,dek_version INTEGER NOT NULL
      ,wrapped_dek BLOB NOT NULL
      ,kek_version INTEGER NOT NULL
      ,algorithm TEXT NOT NULL
    , PRIMARY KEY(scope_id, dek_id, dek_version)
    , FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
 ) STRICT;

-- Encrypted secret material (CGKA secrets, prekey sidecar, reservations).
CREATE TABLE big_repo_secret_blobs (
      scope_id INTEGER NOT NULL
      ,kind INTEGER NOT NULL
      ,blob_id BLOB NOT NULL
      ,dek_id TEXT NOT NULL
      ,dek_version INTEGER NOT NULL
      ,ciphertext BLOB NOT NULL
      ,nonce BLOB NOT NULL
    , PRIMARY KEY(scope_id, kind, blob_id)
    , FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
    , FOREIGN KEY(scope_id, dek_id, dek_version)
        REFERENCES big_repo_deks(scope_id, dek_id, dek_version)
 ) STRICT;

-- GC/rotation scans: find blobs encrypted under one (dek_id, dek_version).
CREATE INDEX big_repo_secret_blobs_dek_idx
    ON big_repo_secret_blobs(scope_id, dek_id, dek_version);
