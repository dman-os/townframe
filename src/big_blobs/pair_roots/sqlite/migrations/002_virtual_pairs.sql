CREATE TABLE big_blobs_virtual_pairs (
     cipher_hash TEXT PRIMARY KEY NOT NULL
   , digest TEXT UNIQUE NOT NULL
   , plaintext_hash TEXT NOT NULL
   , master_key BLOB NOT NULL CHECK (length(master_key) = 32)
   , record_size INTEGER NOT NULL
   , padding TEXT NOT NULL CHECK (padding = 'padme')
   , source TEXT NOT NULL
) STRICT;
