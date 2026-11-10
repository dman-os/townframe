-- name: known_inventories :many
-- param: inventory_kind String
-- returns: inventory_id String
SELECT doc_id AS inventory_id
  FROM big_cabinet_drawer_meta_docs
 WHERE kind = :inventory_kind
 UNION
SELECT inventory_id
  FROM big_blobs_writer_targets
 UNION
SELECT inventory_id
  FROM big_blobs_writer_effects
 UNION
SELECT inventory_id
  FROM big_blobs_inventory_registry;
