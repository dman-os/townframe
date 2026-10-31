-- name: put_meta_doc :exec
-- param: drawer_id String
-- param: kind String
-- param: doc_id String
INSERT INTO big_cabinet_drawer_meta_docs (drawer_id, kind, doc_id)
VALUES (:drawer_id, :kind, :doc_id)
ON CONFLICT (drawer_id, kind) DO UPDATE SET doc_id = excluded.doc_id;
