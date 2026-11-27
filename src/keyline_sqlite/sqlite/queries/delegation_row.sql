-- name: delegation_row :optional
-- param: digest Vec<u8>
-- returns: subject Vec<u8>, issuer Vec<u8>, power i64
SELECT subject
     , issuer
     , power
  FROM keyline_certificates
 WHERE kind = 0
   AND digest = :digest
