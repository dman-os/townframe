-- name: all_certificates :many
-- returns: kind i64, digest Vec<u8>, signed Vec<u8>
SELECT kind
     , digest
     , signed
  FROM keyline_certificates
 ORDER BY kind, digest
