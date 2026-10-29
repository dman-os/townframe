CREATE TABLE IF NOT EXISTS big_cabinet_drawers (
    drawer_id TEXT PRIMARY KEY
  , descriptor_json TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS big_cabinet_drawer_docs (
    drawer_id TEXT NOT NULL
  , doc_id TEXT NOT NULL
  , access TEXT NOT NULL CHECK (access IN ('Relay', 'Read', 'Write', 'Admin'))
  , PRIMARY KEY (drawer_id, doc_id)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_drawer_docs_by_doc
    ON big_cabinet_drawer_docs (doc_id);

CREATE TABLE IF NOT EXISTS big_cabinet_drawer_children (
    parent_id TEXT NOT NULL
  , child_id TEXT NOT NULL
  , access TEXT NOT NULL CHECK (access IN ('Relay', 'Read', 'Write', 'Admin'))
  , PRIMARY KEY (parent_id, child_id)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_drawer_children_by_child
    ON big_cabinet_drawer_children (child_id);

CREATE TABLE IF NOT EXISTS big_cabinet_drawer_local (
    role TEXT PRIMARY KEY CHECK (role IN ('present', 'originated', 'registry'))
  , drawer_id TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS big_cabinet_drawer_ops (
    command_id TEXT PRIMARY KEY
  , request_digest BLOB NOT NULL
  , actor_uuid TEXT NOT NULL
  , op_json TEXT NOT NULL
  , step TEXT NOT NULL CHECK (step IN ('begun', 'done'))
  , receipt_json TEXT
  , created_at INTEGER NOT NULL
  , done_at INTEGER
) STRICT;
