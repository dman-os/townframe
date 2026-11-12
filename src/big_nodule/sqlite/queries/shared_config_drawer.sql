-- name: shared_config_drawer :optional
-- returns: drawer_id String
SELECT drawer_id
  FROM nodule_shared_config_drawer
 WHERE singleton = 1;
