-- name: upsert_applied_settings :exec
-- param: cartridge_id String
-- param: doc_id String
-- param: kinds String
-- param: capability_id Vec<u8>
INSERT INTO nodule_applied_settings (
       cartridge_id
     , doc_id
     , kinds
     , capability_id
)
VALUES (
       :cartridge_id
     , :doc_id
     , :kinds
     , :capability_id
)
ON CONFLICT (cartridge_id) DO UPDATE SET
       doc_id = excluded.doc_id
     , kinds = excluded.kinds
     , capability_id = excluded.capability_id;
