-- name: dsubject_of_delegations :exec
-- param: epoch i64
-- The subjects of the batch's delegations that a revocation already in the set names.
INSERT INTO keyline_dsubject (subject, epoch)
SELECT d.subject
     , :epoch
  FROM keyline_new nw
  JOIN keyline_certificates d ON d.kind = 0 AND d.digest = nw.digest
 WHERE nw.kind = 0
   AND EXISTS (SELECT 1
                 FROM keyline_certificates r
                WHERE r.kind = 1
                  AND r.revoke = d.digest)
    ON CONFLICT DO NOTHING
