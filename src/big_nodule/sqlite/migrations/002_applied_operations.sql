CREATE TABLE IF NOT EXISTS nodule_applied_operations (
     plug_id TEXT PRIMARY KEY
   , scope TEXT NOT NULL
   , capability_id BLOB NOT NULL
);
