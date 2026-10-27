-- name: advance_cursor :exec
-- param: datum_tag String
-- param: revision i64
INSERT INTO big_cabinet_datum_ref_progress (
    datum_tag
    , revision
) VALUES (
    :datum_tag
    , :revision
)
ON CONFLICT (datum_tag) DO UPDATE SET revision = MAX(revision, excluded.revision);
