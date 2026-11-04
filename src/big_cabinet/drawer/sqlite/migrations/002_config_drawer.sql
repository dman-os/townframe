CREATE TABLE big_cabinet_drawer_local_v2 (
    role TEXT PRIMARY KEY CHECK (role IN ('present', 'originated', 'registry', 'config'))
  , drawer_id TEXT NOT NULL
) STRICT;

INSERT INTO big_cabinet_drawer_local_v2 (role, drawer_id)
SELECT role
     , drawer_id
  FROM big_cabinet_drawer_local;

DROP TABLE big_cabinet_drawer_local;

ALTER TABLE big_cabinet_drawer_local_v2 RENAME TO big_cabinet_drawer_local;
