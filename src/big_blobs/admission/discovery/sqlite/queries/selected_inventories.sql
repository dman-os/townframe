-- name: selected_inventories :many
-- returns: inventory_id String
SELECT DISTINCT inventory_id
  FROM big_blobs_mirror_claims
 ORDER BY inventory_id;
