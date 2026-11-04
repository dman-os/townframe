-- A metadata reference is per root: the public metadata root (the drawer doc) and the
-- sensitive metadata root name the same `(kind, name)` slot on different docs. The old
-- two-column rows stated no root, so they are dropped — a boot rescan repopulates the
-- projection from the roster datums a peer's build wrote, and slots this build cannot
-- split back into `(kind, name)` are skipped there.
DROP TABLE IF EXISTS big_cabinet_drawer_meta_docs;

CREATE TABLE IF NOT EXISTS big_cabinet_drawer_meta_docs (
    drawer_id TEXT NOT NULL
  , root TEXT NOT NULL
  , kind TEXT NOT NULL
  , name TEXT NOT NULL
  , doc_id TEXT NOT NULL
  , PRIMARY KEY (drawer_id, root, kind, name)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_drawer_meta_docs_by_doc
    ON big_cabinet_drawer_meta_docs (doc_id);