-- name: virtual_pairs :many
-- returns: cipher_hash String, digest String, plaintext_hash String, master_key Vec<u8>, record_size i64, padding String, source String, doc_id Option<String>, branch_path Option<String>, preclaim bool
SELECT pair.cipher_hash
     , pair.digest
     , pair.plaintext_hash
     , pair.master_key
     , pair.record_size
     , pair.padding
     , pair.source
     , root.doc_id
     , root.branch_path
     , root.cipher_hash IS NOT NULL AS preclaim
  FROM big_blobs_virtual_pairs AS pair
  LEFT JOIN blob_pair_root AS root ON root.cipher_hash = pair.cipher_hash;
