-- name: content_heads_put :exec
-- param: doc_id Vec<u8>
-- param: heads Vec<u8>
INSERT INTO kh_content_heads (doc_id, heads)
VALUES (:doc_id, :heads)
    ON CONFLICT (doc_id) DO UPDATE SET heads = excluded.heads
