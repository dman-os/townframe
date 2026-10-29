CREATE TABLE IF NOT EXISTS big_cabinet_commands (
    command_id TEXT PRIMARY KEY
  , kind TEXT NOT NULL CHECK (kind IN ('create', 'write', 'branch', 'merge'))
  , request_digest BLOB NOT NULL
  , actor_uuid TEXT NOT NULL
  , doc_id TEXT
  , placement TEXT
  , detail_json TEXT
  , state TEXT NOT NULL CHECK (state IN ('pending', 'committed', 'done'))
  , heads_json TEXT NOT NULL
  , created_at INTEGER NOT NULL
  , done_at INTEGER
) STRICT;
