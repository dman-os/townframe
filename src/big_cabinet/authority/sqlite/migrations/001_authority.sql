CREATE TABLE IF NOT EXISTS authority_actors (
     uuid TEXT PRIMARY KEY
   , path TEXT NOT NULL UNIQUE
   , parent_uuid TEXT REFERENCES authority_actors (uuid)
   , public_key BLOB NOT NULL UNIQUE
   , sealed_key BLOB NOT NULL
   , key_nonce BLOB NOT NULL
   , certificate BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS authority_node (
     node_id INTEGER PRIMARY KEY CHECK (node_id = 1)
   , public_key BLOB NOT NULL
   , sealed_key BLOB NOT NULL
   , key_nonce BLOB NOT NULL
   , operator_uuid TEXT NOT NULL REFERENCES authority_actors (uuid)
   , actors_issuer TEXT NOT NULL
);
