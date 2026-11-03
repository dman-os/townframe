CREATE TABLE big_blobs_inventory_registry (
                     inventory_id TEXT PRIMARY KEY NOT NULL
                   , part_id BLOB NOT NULL UNIQUE
                   , readable INTEGER NOT NULL DEFAULT 0 CHECK (readable IN (0, 1))
                   , authorization INTEGER NOT NULL DEFAULT 0
                   , pin_revision INTEGER NOT NULL DEFAULT 0
                   , pins_complete INTEGER NOT NULL DEFAULT 0 CHECK (pins_complete IN (0, 1))
                 ) STRICT;
                 CREATE TABLE big_blobs_mirror_claims (
                     inventory_id TEXT NOT NULL
                   , owner TEXT NOT NULL
                   , PRIMARY KEY (inventory_id, owner)
                 ) STRICT;
                 CREATE TABLE big_blobs_admission_pairs (
                     inventory_id TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , length_octets INTEGER
                   , bytes INTEGER NOT NULL DEFAULT 0 CHECK (bytes IN (0, 1))
                   , generation INTEGER NOT NULL DEFAULT 1
                   , attempt INTEGER
                   , dirty INTEGER NOT NULL DEFAULT 1 CHECK (dirty IN (0, 1))
                   , PRIMARY KEY (inventory_id, digest)
                 ) STRICT;
                 CREATE INDEX big_blobs_admission_dirty ON big_blobs_admission_pairs (dirty, inventory_id, digest);
                 CREATE TABLE big_blobs_producer_claims (
                     inventory_id TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , owner TEXT NOT NULL
                   , PRIMARY KEY (inventory_id, digest, owner)
                 ) STRICT;
                 CREATE TABLE big_blobs_source_observations (
                     inventory_id TEXT NOT NULL
                   , digest TEXT NOT NULL
                   , peer_id BLOB NOT NULL
                   , revision INTEGER NOT NULL
                   , present INTEGER NOT NULL CHECK (present IN (0, 1))
                   , payload TEXT
                   , PRIMARY KEY (inventory_id, digest, peer_id)
                 ) STRICT;
                 CREATE INDEX big_blobs_source_present ON big_blobs_source_observations (inventory_id, digest, present);