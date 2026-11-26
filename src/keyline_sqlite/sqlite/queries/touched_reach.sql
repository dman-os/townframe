-- name: touched_reach :exec
-- param: first i64
-- Subjects over which the batch gave some node standing: their closures grew.
INSERT INTO keyline_touched (subject)
SELECT s
  FROM keyline_reach
 WHERE epoch >= :first
    ON CONFLICT DO NOTHING
