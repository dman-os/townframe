-- name: upsert_slot :exec
-- param: drawer_id String
-- param: meta_root String
-- param: name String
-- param: inventory_id String
INSERT INTO big_blobs_discovery_slots (drawer_id, meta_root, name, inventory_id)
VALUES (:drawer_id, :meta_root, :name, :inventory_id)
ON CONFLICT (drawer_id, meta_root, name) DO UPDATE
    SET inventory_id = excluded.inventory_id
  WHERE inventory_id != excluded.inventory_id;
