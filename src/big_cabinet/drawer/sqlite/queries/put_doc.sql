-- name: put_doc :exec
-- param: drawer_id String
-- param: doc_id String
-- param: access String
INSERT INTO big_cabinet_drawer_docs (drawer_id, doc_id, access)
VALUES (:drawer_id, :doc_id, :access)
ON CONFLICT (drawer_id, doc_id) DO UPDATE SET access = excluded.access;
