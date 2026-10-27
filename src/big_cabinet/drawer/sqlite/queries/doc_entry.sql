-- name: doc_entry :optional
-- param: drawer_id String
-- param: doc_id String
-- returns: access String
SELECT access
  FROM big_cabinet_drawer_docs
 WHERE drawer_id = :drawer_id
   AND doc_id = :doc_id;
