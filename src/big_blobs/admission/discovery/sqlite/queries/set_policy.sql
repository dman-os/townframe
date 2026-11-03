-- name: set_policy :exec
-- param: drawer_id String
-- param: policy String
-- param: meta_root String
-- param: name String
INSERT INTO big_blobs_discovery_policies (drawer_id, policy, meta_root, name)
VALUES (:drawer_id, :policy, :meta_root, :name)
ON CONFLICT (drawer_id, policy) DO UPDATE
    SET meta_root = excluded.meta_root
      , name = excluded.name
  WHERE meta_root != excluded.meta_root OR name != excluded.name;
