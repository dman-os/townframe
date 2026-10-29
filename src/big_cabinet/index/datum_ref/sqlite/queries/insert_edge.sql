-- name: insert_edge :exec
-- param: origin_doc String
-- param: origin_datum String
-- param: origin_path String
-- param: target_doc String
-- param: target_datum String
-- param: ref_heads Vec<u8>
INSERT INTO big_cabinet_datum_ref_edges (
    origin_doc
    , origin_datum
    , origin_path
    , target_doc
    , target_datum
    , ref_heads
) VALUES (
    :origin_doc
    , :origin_datum
    , :origin_path
    , :target_doc
    , :target_datum
    , :ref_heads
);
