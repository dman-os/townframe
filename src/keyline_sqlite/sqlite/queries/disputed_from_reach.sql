-- name: disputed_from_reach :exec
-- param: first i64
-- A subject that newly reaches a marked subject is disputed.
INSERT INTO keyline_disputed (subject)
SELECT r.s
  FROM keyline_reach r
 WHERE r.epoch >= :first
   AND r.lvl = 0
   AND EXISTS (SELECT 1
                 FROM keyline_dsubject ds
                WHERE ds.subject = r.n)
    ON CONFLICT DO NOTHING
