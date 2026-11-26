-- name: content_heads_get :optional
-- param: doc_id Vec<u8>
-- returns: heads Vec<u8>
SELECT heads
  FROM kh_content_heads
 WHERE doc_id = :doc_id
