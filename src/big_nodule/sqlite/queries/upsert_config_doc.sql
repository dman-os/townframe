-- name: upsert_config_doc :exec
-- param: cartridge_id String
-- param: doc_id String
INSERT INTO nodule_config_docs
     ( cartridge_id
     , doc_id
     )
VALUES ( :cartridge_id
       , :doc_id
       )
ON CONFLICT (cartridge_id) DO UPDATE
   SET doc_id = excluded.doc_id;
