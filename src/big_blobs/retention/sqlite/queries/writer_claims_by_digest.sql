-- name: writer_claims_by_digest :many
-- param: digest String
-- returns: doc_id String, drawer_id String, root String, name String, inventory_id String
SELECT doc_id
     , drawer_id
     , root
     , name
     , inventory_id
  FROM big_blobs_writer_claims
 WHERE digest = :digest;
