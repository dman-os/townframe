-- name: dsubject_of_revocations :exec
-- param: epoch i64
-- The subjects of the delegations the batch's revocations name.
INSERT INTO keyline_dsubject (subject, epoch)
SELECT d.subject
     , :epoch
  FROM keyline_new nw
  JOIN keyline_certificates r ON r.kind = 1 AND r.digest = nw.digest
  JOIN keyline_certificates d ON d.kind = 0 AND d.digest = r.revoke
 WHERE nw.kind = 1
    ON CONFLICT DO NOTHING
