CREATE TABLE big_blobs_discovery_next_reach (
       root_id TEXT NOT NULL
     , drawer_id TEXT NOT NULL
     , meta_root TEXT NOT NULL
     , name TEXT NOT NULL
     , PRIMARY KEY (root_id, drawer_id, meta_root, name)
) STRICT;
CREATE TABLE big_blobs_discovery_next_claims (
       inventory_id TEXT NOT NULL
     , owner TEXT NOT NULL
     , root_id TEXT NOT NULL
     , drawer_id TEXT NOT NULL
     , meta_root TEXT NOT NULL
     , name TEXT NOT NULL
     , PRIMARY KEY (inventory_id, owner)
) STRICT;
