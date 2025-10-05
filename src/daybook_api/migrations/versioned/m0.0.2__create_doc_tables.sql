CREATE SCHEMA IF NOT EXISTS 
  doc;

COMMENT ON
  SCHEMA doc IS 'Entries created by users and robots';

CALL util.apply_default_schema_config('doc');

CREATE TABLE doc.docs (
    -- always put created_at and updated_at at top
    created_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP
,   updated_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP


,   id            TEXT                        NOT NULL
-- ,   username      extensions.CITEXT           NOT NULL

    -- all constraints (besides not null) go after the columns
,   PRIMARY KEY(id)
-- ,   UNIQUE(username)
);

--- default config should be applied on all tables unless a good reason exists not to
CALL util.apply_default_table_config('doc', 'docs');

-- most tables need a secondary table to store the deleted items
CALL util.create_deleted_rows_table('doc', 'docs');

---
