-- name: dsubject_of_delegations :exec
-- param: epoch i64
-- The subjects of the batch's delegations that a revocation already in the set names.
INSERT INTO keyline_dsubject (subject, epoch)
SELECT nw.subject
     , :epoch
  FROM keyline_new nw
 WHERE nw.kind = 0
   AND EXISTS (SELECT 1
                 FROM keyline_certificates r
                WHERE r.revoke = nw.digest)
    ON CONFLICT DO NOTHING
