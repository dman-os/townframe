-- name: park :exec
-- param: origin_doc String
-- param: origin_facet String
-- param: revision i64
-- param: membership Vec<u8>
INSERT INTO big_cabinet_facet_ref_parked (
    origin_doc
    , origin_facet
    , revision
    , membership
) VALUES (
    :origin_doc
    , :origin_facet
    , :revision
    , :membership
)
ON CONFLICT (origin_doc, origin_facet) DO UPDATE
   SET revision = excluded.revision
     , membership = excluded.membership;
