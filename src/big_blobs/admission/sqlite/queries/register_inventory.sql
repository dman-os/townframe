-- name: register_inventory :exec
-- param: inventory_id String
-- param: part_id Vec<u8>
INSERT OR IGNORE INTO big_blobs_inventory_registry (inventory_id, part_id)
VALUES (:inventory_id, :part_id);
