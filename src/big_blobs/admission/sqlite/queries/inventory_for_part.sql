-- name: inventory_for_part :optional
-- param: part_id Vec<u8>
-- returns: inventory_id String
SELECT inventory_id
  FROM big_blobs_inventory_registry
 WHERE part_id = :part_id;
