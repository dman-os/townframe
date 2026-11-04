CREATE TABLE IF NOT EXISTS big_cabinet_drawer_meta_docs (
    drawer_id TEXT NOT NULL
  , kind TEXT NOT NULL
  , doc_id TEXT NOT NULL
  , PRIMARY KEY (drawer_id, kind)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_drawer_meta_docs_by_doc
    ON big_cabinet_drawer_meta_docs (doc_id);
