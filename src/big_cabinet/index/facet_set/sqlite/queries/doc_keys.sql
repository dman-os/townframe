-- name: doc_keys :many
-- param: doc_id String
-- returns: facet_key String
SELECT facet_key
  FROM big_cabinet_facet_set_entries
 WHERE doc_id = :doc_id;
