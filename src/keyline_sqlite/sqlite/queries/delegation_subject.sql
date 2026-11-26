-- name: delegation_subject :optional
-- param: digest Vec<u8>
-- returns: subject Vec<u8>
SELECT subject
  FROM keyline_certificates
 WHERE kind = 0
   AND digest = :digest
