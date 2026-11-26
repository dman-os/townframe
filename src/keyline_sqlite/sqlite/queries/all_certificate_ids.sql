-- name: all_certificate_ids :many
-- returns: kind i64, digest Vec<u8>
SELECT kind
     , digest
  FROM keyline_certificates
 ORDER BY kind, digest
