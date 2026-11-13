CREATE TABLE IF NOT EXISTS nodule_applied_settings (
       cartridge_id TEXT PRIMARY KEY
     , doc_id TEXT NOT NULL
     , kinds TEXT NOT NULL
     , capability_id BLOB NOT NULL
);
