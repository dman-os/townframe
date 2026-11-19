-- name: drawer_holds_blob :many
-- param: drawer_id String
-- param: inventory_kind String
-- param: digest String
-- returns: marker i64
SELECT 1 AS marker
  FROM big_cabinet_drawer_meta_docs AS meta
  JOIN big_blobs_admission_pairs AS pair ON pair.inventory_id = meta.doc_id
 WHERE meta.drawer_id = :drawer_id
   AND meta.kind = :inventory_kind
   AND pair.digest = :digest
   AND pair.length_octets IS NOT NULL
 LIMIT 1;
