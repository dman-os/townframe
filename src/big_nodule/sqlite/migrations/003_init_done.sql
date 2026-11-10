CREATE TABLE IF NOT EXISTS nodule_init_done (
     plug_id TEXT NOT NULL
   , init_key TEXT NOT NULL
   , revision TEXT NOT NULL
   , PRIMARY KEY (plug_id, init_key)
);
