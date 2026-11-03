CREATE TABLE sync_receive_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT
  , scope_id INTEGER NOT NULL UNIQUE REFERENCES big_sync_scopes(scope_id)
  , replay BLOB NOT NULL
) STRICT;
CREATE TABLE sync_receive_frontiers (
    scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
  , peer BLOB NOT NULL
  , backend TEXT NOT NULL
  , target BLOB NOT NULL
  , received INTEGER NOT NULL
  , applied INTEGER NOT NULL
  , PRIMARY KEY(scope_id, peer, backend, target)
) STRICT;
