-- name: upsert_observation :exec
-- param: inventory_id String
-- param: digest String
-- param: peer_id Vec<u8>
-- param: present bool
-- param: payload Option<String>
INSERT INTO big_blobs_source_observations (inventory_id, digest, peer_id, revision, present, payload)
VALUES (:inventory_id, :digest, :peer_id, 0, :present, :payload)
ON CONFLICT (inventory_id, digest, peer_id) DO UPDATE
   SET present = excluded.present
     , payload = COALESCE(excluded.payload, payload)
