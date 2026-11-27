-- name: touched_named :exec
-- The subjects of the delegations the batch's revocations name.
INSERT INTO keyline_touched (subject)
SELECT d.subject
  FROM keyline_new nw
  JOIN keyline_certificates d ON d.kind = 0 AND d.digest = nw.revoke
 WHERE nw.kind = 1
    ON CONFLICT DO NOTHING
