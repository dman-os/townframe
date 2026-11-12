-- name: insert_key_edge :exec
-- param: origin_doc String
-- param: origin_datum String
-- param: origin_path String
-- param: target_key String
INSERT INTO big_cabinet_datum_ref_key_edges (
    origin_doc
    , origin_datum
    , origin_path
    , target_key
) VALUES (
    :origin_doc
    , :origin_datum
    , :origin_path
    , :target_key
)
ON CONFLICT (origin_doc, origin_datum, origin_path) DO UPDATE
   SET target_key = excluded.target_key;
