ALTER TABLE big_blobs_retention ADD COLUMN root TEXT CHECK (root IN ('public', 'sensitive'));
                  UPDATE big_blobs_retention SET root = 'public' WHERE plaintext IS NOT NULL;
                  CREATE TABLE big_blobs_writer_targets (
                       doc_id TEXT NOT NULL
                     , drawer_id TEXT NOT NULL
                     , root TEXT NOT NULL CHECK (root IN ('public', 'sensitive'))
                     , name TEXT NOT NULL
                     , inventory_id TEXT NOT NULL
                     , PRIMARY KEY (doc_id, drawer_id, root, name)
                  ) STRICT;
                  CREATE INDEX big_blobs_writer_targets_inventory ON big_blobs_writer_targets (inventory_id);
                  CREATE TABLE big_blobs_writer_claims (
                       doc_id TEXT NOT NULL
                     , drawer_id TEXT NOT NULL
                     , root TEXT NOT NULL CHECK (root IN ('public', 'sensitive'))
                     , name TEXT NOT NULL
                     , inventory_id TEXT NOT NULL
                     , digest TEXT NOT NULL
                     , length_octets INTEGER NOT NULL
                     , PRIMARY KEY (doc_id, drawer_id, root, name, inventory_id, digest)
                  ) STRICT;
                  CREATE INDEX big_blobs_writer_claims_pair ON big_blobs_writer_claims (inventory_id, digest);
                  CREATE INDEX big_blobs_writer_claims_digest ON big_blobs_writer_claims (digest);
                  CREATE TABLE big_blobs_writer_effects (
                       inventory_id TEXT NOT NULL
                     , digest TEXT NOT NULL
                     , PRIMARY KEY (inventory_id, digest)
                  ) STRICT;