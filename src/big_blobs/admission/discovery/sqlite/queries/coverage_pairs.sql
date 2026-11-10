-- name: coverage_pairs :many
-- param: root_id String
-- returns: inventory_id String
-- returns: digest String
-- returns: bytes bool
-- returns: dirty bool
-- returns: pinned bool
-- returns: observed bool
SELECT p.inventory_id
     , p.digest
     , p.bytes
     , p.dirty
     , p.length_octets IS NOT NULL AS pinned
     , (EXISTS (SELECT 1 FROM big_blobs_source_observations o
                 WHERE o.inventory_id = p.inventory_id AND o.digest = p.digest
                   AND o.present = 1)
        OR EXISTS (SELECT 1 FROM big_blobs_producer_claims w
                    WHERE w.inventory_id = p.inventory_id AND w.digest = p.digest)) AS observed
  FROM big_blobs_admission_pairs p
 WHERE (p.length_octets IS NOT NULL OR p.dirty = 1)
   AND EXISTS (SELECT 1 FROM big_blobs_discovery_claims c
                WHERE c.root_id = :root_id AND c.inventory_id = p.inventory_id)
 ORDER BY p.inventory_id, p.digest;
