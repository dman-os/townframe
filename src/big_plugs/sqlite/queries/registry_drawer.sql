-- name: registry_drawer :optional
-- returns: drawer_id String
SELECT drawer_id
  FROM plugs_registry_drawer
 WHERE singleton = 1;
