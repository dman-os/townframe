-- name: touched_admin :exec
-- param: first i64
-- A node that newly holds Admin over a subject extends the coverage of every revocation it issued:
-- the subjects of the delegations those revocations name.
INSERT INTO keyline_touched (subject)
SELECT d.subject
  FROM keyline_certificates d
 WHERE d.kind = 0
   AND d.digest IN (SELECT rv.revoke
                      FROM keyline_reach r
                      JOIN keyline_certificates rv ON rv.issuer = r.n AND rv.revoke IS NOT NULL
                     WHERE r.epoch >= :first
                       AND r.lvl = 3)
    ON CONFLICT DO NOTHING
