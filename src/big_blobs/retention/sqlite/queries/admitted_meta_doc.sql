-- name: admitted_meta_doc :many
-- param: drawer_id String
-- param: root String
-- param: kind String
-- param: name String
-- param: doc_id String
-- returns: marker i64
SELECT 1 AS marker
  FROM big_cabinet_drawer_meta_docs
 WHERE drawer_id = :drawer_id
   AND root = :root
   AND kind = :kind
   AND name = :name
   AND doc_id = :doc_id;
