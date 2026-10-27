-- name: legacy_schema :one
-- returns: legacy bool
SELECT EXISTS (
    SELECT 1
      FROM sqlite_schema
     WHERE type = 'table'
       AND name IN ('authority_caps', 'authority_actor_issuances')
    UNION ALL
    SELECT 1
      FROM pragma_table_info('authority_node')
     WHERE name = 'root_cap'
    UNION ALL
    SELECT 1
      FROM pragma_table_info('authority_grants')
     WHERE name = 'granted_via'
) AS legacy;
