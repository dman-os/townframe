-- name: meta_docs :many
-- param: drawer_id String
-- returns: root String, kind String, name String, doc_id String
SELECT root
     , kind
     , name
     , doc_id
  FROM big_cabinet_drawer_meta_docs
 WHERE drawer_id = :drawer_id
 ORDER BY root, kind, name;