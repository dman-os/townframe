CREATE TABLE IF NOT EXISTS blob_pair_root (
     cipher_hash TEXT NOT NULL PRIMARY KEY
   , doc_id TEXT
   , branch_path TEXT
) STRICT;
