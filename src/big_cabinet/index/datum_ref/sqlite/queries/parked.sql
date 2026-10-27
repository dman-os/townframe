-- name: parked :many
-- returns: origin_doc String, origin_facet String, revision i64, membership Vec<u8>
SELECT origin_doc
     , origin_facet
     , revision
     , membership
  FROM big_cabinet_facet_ref_parked;
