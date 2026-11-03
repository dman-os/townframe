-- name: is_inventory :many
-- param: doc_id String
-- param: inventory_kind String
-- returns: marker i64
SELECT 1 AS marker
  FROM big_cabinet_drawer_meta_docs
 WHERE doc_id = :doc_id
   AND kind = :inventory_kind
UNION ALL
SELECT 1 AS marker
  FROM big_blobs_writer_targets
 WHERE inventory_id = :doc_id
LIMIT 1;
