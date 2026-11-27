-- name: revocations_of :many
-- param: delegation Vec<u8>
-- returns: digest Vec<u8>
SELECT digest
  FROM keyline_certificates
 WHERE revoke = :delegation
