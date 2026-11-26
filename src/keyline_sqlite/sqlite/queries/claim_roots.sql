-- name: claim_roots :exec
-- param: s Vec<u8>
-- The roots of the computation for `s`: the nodes in its closure that are the subject of some
-- delegation (a node nothing is delegated about has no routes of its own to compute).
INSERT INTO keyline_root (s)
SELECT d.subject
  FROM keyline_certificates d
 WHERE d.kind = 0
   AND (d.subject = :s
        OR d.subject IN (SELECT n FROM keyline_reach WHERE s = :s AND lvl = 0))
    ON CONFLICT DO NOTHING
