-- name: known_inventory :many
-- param: document_id String
-- param: inventory_kind String
-- returns: marker i64
SELECT 1 AS marker
  FROM big_cabinet_drawer_meta_docs
 WHERE doc_id = :document_id
   AND kind = :inventory_kind
 UNION ALL
SELECT 1 AS marker
  FROM big_blobs_writer_targets
 WHERE inventory_id = :document_id
 UNION ALL
SELECT 1 AS marker
  FROM big_blobs_writer_effects
 WHERE inventory_id = :document_id
 UNION ALL
SELECT 1 AS marker
  FROM big_blobs_inventory_registry
 WHERE inventory_id = :document_id
 LIMIT 1;
