-- name: claim_roots :exec
-- The roots of the computation for the affected subjects: the nodes in their closures that are the
-- subject of some delegation (a node nothing is delegated about has no routes of its own).
INSERT INTO keyline_root (s)
SELECT DISTINCT d.subject
  FROM keyline_certificates d
 WHERE d.subject IN (SELECT subject
                       FROM keyline_affected
                      UNION
                     SELECT r.n
                       FROM keyline_reach r
                      WHERE r.s IN (SELECT subject FROM keyline_affected)
                        AND r.lvl = 0)
    ON CONFLICT DO NOTHING
