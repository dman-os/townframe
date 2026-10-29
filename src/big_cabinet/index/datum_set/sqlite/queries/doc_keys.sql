-- name: doc_keys :many
-- param: doc_id String
-- returns: datum_key String
SELECT datum_key
  FROM big_cabinet_datum_set_entries
 WHERE doc_id = :doc_id;
