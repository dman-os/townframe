CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_docs (
    doc_id TEXT PRIMARY KEY NOT NULL
    , state BLOB NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_entries (
    doc_id TEXT NOT NULL
    , facet_key TEXT NOT NULL
    , facet_tag TEXT NOT NULL
    , doc_heads BLOB NOT NULL
    , facet_heads BLOB NOT NULL
    , clock INTEGER NOT NULL
    , PRIMARY KEY (doc_id, facet_key)
) STRICT;

CREATE INDEX IF NOT EXISTS big_cabinet_facet_set_entries_tag_idx
    ON big_cabinet_facet_set_entries (facet_tag, doc_id);

CREATE INDEX IF NOT EXISTS big_cabinet_facet_set_entries_key_idx
    ON big_cabinet_facet_set_entries (facet_key, doc_id);
