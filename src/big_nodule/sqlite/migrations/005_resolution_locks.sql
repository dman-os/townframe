CREATE TABLE IF NOT EXISTS nodule_resolution_lock (
     plug_id TEXT NOT NULL
   , binding TEXT NOT NULL
   , contract TEXT NOT NULL
   , provider TEXT NOT NULL
   , provider_version TEXT
   , lexicon_revision INTEGER
   , PRIMARY KEY (plug_id, binding)
);
