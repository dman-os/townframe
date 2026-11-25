-- name: certificate_signed :optional
-- param: kind i64
-- param: digest Vec<u8>
-- returns: signed Vec<u8>
SELECT signed
  FROM keyline_certificates
 WHERE kind = :kind
   AND digest = :digest
