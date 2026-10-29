-- name: delete_parked :exec
-- param: origin_doc String
-- param: origin_datum String
DELETE FROM big_cabinet_datum_ref_parked
 WHERE origin_doc = :origin_doc
   AND origin_datum = :origin_datum;
