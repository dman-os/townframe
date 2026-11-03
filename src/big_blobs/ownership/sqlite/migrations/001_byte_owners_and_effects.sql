CREATE TABLE big_blobs_byte_owners (
                     owner TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , PRIMARY KEY (owner, digest)
                 ) STRICT;
                 CREATE INDEX big_blobs_byte_owners_digest ON big_blobs_byte_owners (digest);
                 CREATE TABLE big_blobs_byte_dependencies (
                     digest TEXT PRIMARY KEY NOT NULL
                   , plaintext TEXT NOT NULL
                 ) STRICT;
                 CREATE INDEX big_blobs_byte_dependencies_plaintext ON big_blobs_byte_dependencies (plaintext);
                 CREATE TABLE big_blobs_byte_pin_roots (digest TEXT PRIMARY KEY NOT NULL) STRICT;
                 CREATE TABLE big_blobs_byte_effects (digest TEXT PRIMARY KEY NOT NULL) STRICT;