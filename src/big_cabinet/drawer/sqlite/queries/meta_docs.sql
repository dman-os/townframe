-- name: meta_docs :many
-- param: drawer_id String
-- returns: kind String, doc_id String
SELECT kind
     , doc_id
  FROM big_cabinet_drawer_meta_docs
 WHERE drawer_id = :drawer_id
 ORDER BY kind;
