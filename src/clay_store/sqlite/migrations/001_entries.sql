-- The ordered key-value table behind `SqliteStore`. SQLite compares BLOBs with
-- memcmp (shorter wins a tie), which is the bytewise order the store contract
-- promises, so the primary key's b-tree serves point reads and key-range scans.
CREATE TABLE IF NOT EXISTS clay_store_entry (
    key BLOB NOT NULL PRIMARY KEY
  , value BLOB NOT NULL
) STRICT, WITHOUT ROWID;
