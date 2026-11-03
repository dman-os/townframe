-- name: source_peers :many
-- param: inventory_id String
-- param: digest String
-- returns: peer_id Vec<u8>
SELECT peer_id
  FROM big_blobs_source_observations
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND present = 1
 ORDER BY peer_id
