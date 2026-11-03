CREATE TABLE big_blobs_retention (
                       doc_id TEXT NOT NULL
                     , digest TEXT NOT NULL
                     , plaintext TEXT
                     , length_octets INTEGER NOT NULL
                     , PRIMARY KEY (doc_id, digest)
                  ) STRICT;
                  CREATE INDEX big_blobs_retention_digest ON big_blobs_retention (digest);
                  CREATE TABLE big_blobs_retention_effects (digest TEXT PRIMARY KEY NOT NULL) STRICT;