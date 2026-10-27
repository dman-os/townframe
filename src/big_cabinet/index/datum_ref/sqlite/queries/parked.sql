-- name: parked :many
-- returns: origin_doc String, origin_datum String, revision i64, membership Vec<u8>
SELECT origin_doc
     , origin_datum
     , revision
     , membership
  FROM big_cabinet_datum_ref_parked;
