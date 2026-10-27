-- name: park :exec
-- param: origin_doc String
-- param: origin_datum String
-- param: revision i64
-- param: membership Vec<u8>
INSERT INTO big_cabinet_datum_ref_parked (
    origin_doc
    , origin_datum
    , revision
    , membership
) VALUES (
    :origin_doc
    , :origin_datum
    , :revision
    , :membership
)
ON CONFLICT (origin_doc, origin_datum) DO UPDATE
   SET revision = excluded.revision
     , membership = excluded.membership;
