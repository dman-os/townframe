-- name: delete_entry :exec
-- param: doc_id String
-- param: facet_key String
DELETE FROM big_cabinet_facet_set_entries
 WHERE doc_id = :doc_id
   AND facet_key = :facet_key;
