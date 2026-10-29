-- name: put_entry :exec
-- param: doc_id String
-- param: datum_key String
-- param: datum_tag String
-- param: doc_heads Vec<u8>
-- param: datum_heads Vec<u8>
-- param: clock i64
INSERT INTO big_cabinet_datum_set_entries (
    doc_id
    , datum_key
    , datum_tag
    , doc_heads
    , datum_heads
    , clock
) VALUES (
    :doc_id
    , :datum_key
    , :datum_tag
    , :doc_heads
    , :datum_heads
    , :clock
)
ON CONFLICT (doc_id, datum_key) DO UPDATE
   SET doc_heads = excluded.doc_heads
     , datum_heads = excluded.datum_heads
     , clock = excluded.clock;
