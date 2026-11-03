CREATE TABLE big_blobs_discovery_policies (
                      drawer_id TEXT NOT NULL
                    , policy TEXT NOT NULL CHECK (policy IN ('root', 'override'))
                    , meta_root TEXT NOT NULL
                    , name TEXT NOT NULL
                    , PRIMARY KEY (drawer_id, policy)
                  ) STRICT;
                  CREATE TABLE big_blobs_discovery_documents (
                      drawer_id TEXT PRIMARY KEY NOT NULL
                    , generation INTEGER NOT NULL DEFAULT 1
                    , dirty INTEGER NOT NULL DEFAULT 1 CHECK (dirty IN (0, 1))
                    , waiting INTEGER NOT NULL DEFAULT 1 CHECK (waiting IN (0, 1))
                    , attempt INTEGER
                    , observed INTEGER NOT NULL DEFAULT 0 CHECK (observed IN (0, 1))
                    , self_doc INTEGER NOT NULL DEFAULT 0 CHECK (self_doc IN (0, 1))
                  ) STRICT;
                  CREATE TABLE big_blobs_discovery_edges (
                      parent_id TEXT NOT NULL
                    , child_id TEXT NOT NULL
                    , PRIMARY KEY (parent_id, child_id)
                  ) STRICT;
                  CREATE TABLE big_blobs_discovery_roots (
                      drawer_id TEXT NOT NULL
                    , meta_root TEXT NOT NULL
                    , authoritative INTEGER NOT NULL CHECK (authoritative IN (0, 1))
                    , PRIMARY KEY (drawer_id, meta_root)
                  ) STRICT;
                  CREATE TABLE big_blobs_discovery_slots (
                      drawer_id TEXT NOT NULL
                    , meta_root TEXT NOT NULL
                    , name TEXT NOT NULL
                    , inventory_id TEXT NOT NULL
                    , PRIMARY KEY (drawer_id, meta_root, name)
                  ) STRICT;
                  CREATE TABLE big_blobs_discovery_reach (
                      root_id TEXT NOT NULL
                    , drawer_id TEXT NOT NULL
                    , meta_root TEXT NOT NULL
                    , name TEXT NOT NULL
                    , PRIMARY KEY (root_id, drawer_id, meta_root, name)
                  ) STRICT;
                  CREATE TABLE big_blobs_discovery_claims (
                      inventory_id TEXT NOT NULL
                    , owner TEXT NOT NULL
                    , root_id TEXT NOT NULL
                    , drawer_id TEXT NOT NULL
                    , meta_root TEXT NOT NULL
                    , name TEXT NOT NULL
                    , PRIMARY KEY (inventory_id, owner)
                  ) STRICT;
                  CREATE INDEX big_blobs_discovery_reach_drawer ON big_blobs_discovery_reach (drawer_id);
                  CREATE INDEX big_blobs_discovery_claims_root ON big_blobs_discovery_claims (root_id, inventory_id);
                  CREATE INDEX big_blobs_discovery_dirty ON big_blobs_discovery_documents (dirty, drawer_id);