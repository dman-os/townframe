-- name: state :optional
-- param: inventory_id String
-- param: digest String
-- returns: selected bool, writer bool, pinned bool, observed bool, readable bool, bytes bool, generation i64, authorization i64, fetching bool
SELECT EXISTS (SELECT 1 FROM big_blobs_mirror_claims s WHERE s.inventory_id = p.inventory_id) AS selected
     , EXISTS (SELECT 1 FROM big_blobs_producer_claims w WHERE w.inventory_id = p.inventory_id AND w.digest = p.digest) AS writer
     , p.length_octets IS NOT NULL AS pinned
     , EXISTS (SELECT 1 FROM big_blobs_source_observations o WHERE o.inventory_id = p.inventory_id AND o.digest = p.digest AND o.present = 1) AS observed
     , r.readable
     , p.bytes
     , p.generation
     , r.authorization
     , p.attempt IS NOT NULL AS fetching
  FROM big_blobs_admission_pairs p
  JOIN big_blobs_inventory_registry r USING (inventory_id)
 WHERE p.inventory_id = :inventory_id
   AND p.digest = :digest;
