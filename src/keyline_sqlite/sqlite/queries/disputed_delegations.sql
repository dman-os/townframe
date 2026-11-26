-- name: disputed_delegations :many
-- returns: delegation Vec<u8>, issuer Vec<u8>, audience Vec<u8>, revoker Vec<u8>
-- Every revocation of a delegation about a root, with what is needed to decide its effect.
SELECT d.digest AS delegation
     , d.issuer AS issuer
     , d.audience AS audience
     , r.issuer AS revoker
  FROM keyline_root rt
  JOIN keyline_certificates d ON d.subject = rt.s
  JOIN keyline_certificates r ON r.revoke = d.digest
