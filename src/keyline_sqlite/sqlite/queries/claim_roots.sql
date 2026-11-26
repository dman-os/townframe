-- name: claim_roots :exec
-- param: s Vec<u8>
-- The roots of the computation for `s`: the nodes in its closure that are the subject of some
-- delegation (a node nothing is delegated about has no routes of its own to compute).
INSERT INTO keyline_root (s)
SELECT DISTINCT d.subject
  FROM keyline_certificates d
 WHERE d.subject IN (SELECT :s AS node
                      UNION
                     SELECT n FROM keyline_reach WHERE s = :s AND lvl = 0)
    ON CONFLICT DO NOTHING
