-- name: put_doc :exec
-- param: doc_id String
-- param: state Vec<u8>
INSERT INTO big_cabinet_facet_set_docs (
    doc_id
    , state
) VALUES (
    :doc_id
    , :state
)
ON CONFLICT (doc_id) DO UPDATE SET state = excluded.state;
