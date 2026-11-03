-- name: record_virtual :exec
-- param: cipher_hash String
-- param: digest String
-- param: plaintext_hash String
-- param: master_key Vec<u8>
-- param: record_size i64
-- param: padding String
-- param: source String
INSERT INTO big_blobs_virtual_pairs
     ( cipher_hash
     , digest
     , plaintext_hash
     , master_key
     , record_size
     , padding
     , source )
VALUES (:cipher_hash, :digest, :plaintext_hash, :master_key, :record_size, :padding, :source)
ON CONFLICT (cipher_hash) DO UPDATE SET source = excluded.source
 WHERE plaintext_hash = excluded.plaintext_hash
   AND master_key = excluded.master_key
   AND record_size = excluded.record_size
   AND padding = excluded.padding;
