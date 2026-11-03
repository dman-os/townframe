CREATE TABLE big_blobs_work (
       doc_id TEXT PRIMARY KEY NOT NULL
     , generation INTEGER NOT NULL
     , dirty INTEGER NOT NULL
     , waiting INTEGER NOT NULL
  ) STRICT;
CREATE INDEX big_blobs_work_dirty ON big_blobs_work (dirty, doc_id);
CREATE TABLE big_blobs_progress (
       source TEXT PRIMARY KEY NOT NULL
     , revision INTEGER NOT NULL
  ) STRICT;
