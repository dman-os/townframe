-- name: delete_entry :exec
-- param: doc_id String
-- param: datum_key String
DELETE FROM big_cabinet_datum_set_entries
 WHERE doc_id = :doc_id
   AND datum_key = :datum_key;
