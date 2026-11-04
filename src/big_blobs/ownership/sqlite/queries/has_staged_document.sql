-- name: has_staged_document :many
-- param: staging_owner String
-- param: document_owner String
-- returns: present i64
SELECT 1 AS present
  FROM big_blobs_upload_receipts AS staging
  JOIN big_blobs_byte_owners AS document ON document.digest = staging.digest
 WHERE staging.owner = :staging_owner
   AND document.owner = :document_owner;
