-- name: subjects :many
-- returns: subject Vec<u8>
-- The narrow index is walked in subject order, so the distinct subjects are one pass over it
-- and the delegations' signed bytes are never read; left to itself the planner scans the
-- table and sorts.
SELECT DISTINCT subject
  FROM keyline_certificates INDEXED BY keyline_delegations_by_subject_audience
 WHERE subject IS NOT NULL
