-- name: docs_of :many
-- param: drawer_id String
-- returns: doc_id String, access String
SELECT doc_id
     , access
  FROM big_cabinet_drawer_docs
 WHERE drawer_id = :drawer_id
 ORDER BY doc_id;
