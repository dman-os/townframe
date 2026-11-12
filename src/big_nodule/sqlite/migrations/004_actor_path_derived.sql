-- The actor path segment is derived from the plug id (`store::actor_segment`), so it is no
-- longer stored. SQLite cannot drop a UNIQUE column, so the table is rebuilt.
CREATE TABLE nodule_actors_next (
     plug_id TEXT PRIMARY KEY
   , actor_uuid TEXT NOT NULL UNIQUE
);
INSERT INTO nodule_actors_next (plug_id, actor_uuid)
SELECT plug_id
     , actor_uuid
  FROM nodule_actors;
DROP TABLE nodule_actors;
ALTER TABLE nodule_actors_next RENAME TO nodule_actors;
