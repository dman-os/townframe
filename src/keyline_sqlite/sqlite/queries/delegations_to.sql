-- name: delegations_to :many
-- param: subject Vec<u8>
-- param: audience Vec<u8>
-- returns: digest Vec<u8>, issuer Vec<u8>, power i64, citation Option<Vec<u8>>
SELECT digest
     , issuer
     , power
     , citation
  FROM keyline_certificates
 WHERE subject = :subject
   AND audience = :audience
