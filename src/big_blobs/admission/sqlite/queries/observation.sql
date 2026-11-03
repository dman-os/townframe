-- name: observation :optional
-- param: inventory_id String
-- param: digest String
-- param: peer_id Vec<u8>
-- returns: present bool, payload Option<String>
SELECT present
     , payload
  FROM big_blobs_source_observations
 WHERE inventory_id = :inventory_id
   AND digest = :digest
   AND peer_id = :peer_id
