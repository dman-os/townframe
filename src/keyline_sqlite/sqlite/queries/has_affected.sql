-- name: has_affected :one
-- returns: found bool
SELECT EXISTS (
         SELECT 1
           FROM keyline_affected
       ) AS found
