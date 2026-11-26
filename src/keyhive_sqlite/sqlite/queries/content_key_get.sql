-- name: content_key_get :optional
-- param: doc_id Vec<u8>
-- param: content_ref Vec<u8>
-- returns: sealed Vec<u8>, nonce Vec<u8>, dek_id String, dek_version i64
SELECT sealed
     , nonce
     , dek_id
     , dek_version
  FROM kh_content_key
 WHERE doc_id = :doc_id
   AND content_ref = :content_ref
