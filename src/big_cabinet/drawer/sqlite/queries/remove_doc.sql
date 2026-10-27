-- name: remove_doc :exec
-- param: drawer_id String
-- param: doc_id String
DELETE FROM big_cabinet_drawer_docs
 WHERE drawer_id = :drawer_id
   AND doc_id = :doc_id;
