CREATE TABLE IF NOT EXISTS nodule_actors (
     plug_id TEXT PRIMARY KEY
   , nodule_id TEXT NOT NULL UNIQUE
   , actor_uuid TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS nodule_applied_grants (
     plug_id TEXT NOT NULL
   , drawer_id TEXT NOT NULL
   , access TEXT NOT NULL
   , capability_id BLOB NOT NULL
   , PRIMARY KEY (plug_id, drawer_id)
);
