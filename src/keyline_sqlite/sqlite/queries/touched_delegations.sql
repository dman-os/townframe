-- name: touched_delegations :exec
-- The subjects of the batch's delegations: their closures gained a certificate.
INSERT INTO keyline_touched (subject)
SELECT nw.subject
  FROM keyline_new nw
 WHERE nw.kind = 0
    ON CONFLICT DO NOTHING
