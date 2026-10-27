-- name: insert_edge :exec
-- param: origin_doc String
-- param: origin_facet String
-- param: origin_path String
-- param: target_doc String
-- param: target_facet String
-- param: ref_heads Vec<u8>
INSERT INTO big_cabinet_facet_ref_edges (
    origin_doc
    , origin_facet
    , origin_path
    , target_doc
    , target_facet
    , ref_heads
) VALUES (
    :origin_doc
    , :origin_facet
    , :origin_path
    , :target_doc
    , :target_facet
    , :ref_heads
);
