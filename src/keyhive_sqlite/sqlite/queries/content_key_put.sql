-- name: content_key_put :exec
-- param: doc_id Vec<u8>
-- param: content_ref Vec<u8>
-- param: sealed Vec<u8>
-- param: nonce Vec<u8>
-- param: dek_id String
-- param: dek_version i64
INSERT OR IGNORE INTO kh_content_key
     ( doc_id
     , content_ref
     , sealed
     , nonce
     , dek_id
     , dek_version
     )
VALUES (:doc_id, :content_ref, :sealed, :nonce, :dek_id, :dek_version)
