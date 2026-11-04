-- name: effect_by_inventory_digest :exec
-- param: inventory_id String
-- param: digest String
INSERT OR IGNORE INTO big_blobs_writer_effects (inventory_id, digest)
VALUES (:inventory_id, :digest);
