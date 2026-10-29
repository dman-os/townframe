-- name: central_grants :one
-- returns: present bool
SELECT EXISTS (
    SELECT 1
      FROM sqlite_schema
     WHERE type = 'table'
       AND name = 'authority_grants'
) AS present;
