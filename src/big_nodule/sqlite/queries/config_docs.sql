-- name: config_docs :many
-- returns: cartridge_id String, doc_id String
SELECT cartridge_id
     , doc_id
  FROM nodule_config_docs
 ORDER BY cartridge_id;
