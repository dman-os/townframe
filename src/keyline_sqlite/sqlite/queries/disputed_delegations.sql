-- name: disputed_delegations :many
-- returns: delegation Vec<u8>, issuer Vec<u8>, audience Vec<u8>, revoker Vec<u8>
-- Every revocation of a delegation about a root, with what is needed to decide its effect.
SELECT d.digest AS delegation
     , d.issuer AS issuer
     , d.audience AS audience
     , r.issuer AS revoker
  FROM keyline_certificates d
  JOIN keyline_certificates r ON r.revoke = d.digest
 WHERE d.subject IN (SELECT s FROM keyline_root)
