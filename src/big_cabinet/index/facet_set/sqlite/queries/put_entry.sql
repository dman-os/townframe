-- name: put_entry :exec
-- param: doc_id String
-- param: facet_key String
-- param: facet_tag String
-- param: doc_heads Vec<u8>
-- param: facet_heads Vec<u8>
-- param: clock i64
INSERT INTO big_cabinet_facet_set_entries (
    doc_id
    , facet_key
    , facet_tag
    , doc_heads
    , facet_heads
    , clock
) VALUES (
    :doc_id
    , :facet_key
    , :facet_tag
    , :doc_heads
    , :facet_heads
    , :clock
)
ON CONFLICT (doc_id, facet_key) DO UPDATE
   SET doc_heads = excluded.doc_heads
     , facet_heads = excluded.facet_heads
     , clock = excluded.clock;
