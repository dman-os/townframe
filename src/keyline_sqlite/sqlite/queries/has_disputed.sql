-- name: has_disputed :one
-- returns: found bool
SELECT EXISTS (
         SELECT 1
           FROM keyline_disputed
       ) AS found
