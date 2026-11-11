-- name: delete_upload_owners_by_digest :exec
-- param: digest String
DELETE FROM big_blobs_byte_owners
 WHERE digest = :digest
   AND owner GLOB 'upload:*';
