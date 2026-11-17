CREATE TABLE IF NOT EXISTS plugs_registry_drawer (
     singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
   , drawer_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plugs_first_seen (
     plug_id TEXT NOT NULL
   , version TEXT NOT NULL
   , revision TEXT NOT NULL
   , envelope_json TEXT NOT NULL
   , PRIMARY KEY (plug_id, version)
);
