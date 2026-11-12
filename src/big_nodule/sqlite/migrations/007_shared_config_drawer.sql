CREATE TABLE IF NOT EXISTS nodule_shared_config_drawer (
     singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
   , drawer_id TEXT NOT NULL
);
