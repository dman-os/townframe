-- name: advance_cursor :exec
-- param: facet_tag String
-- param: revision i64
INSERT INTO big_cabinet_facet_ref_progress (
    facet_tag
    , revision
) VALUES (
    :facet_tag
    , :revision
)
ON CONFLICT (facet_tag) DO UPDATE SET revision = MAX(revision, excluded.revision);
