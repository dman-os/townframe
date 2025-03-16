CREATE SCHEMA IF NOT EXISTS 
  auth;

COMMENT ON
  SCHEMA auth IS 'Iss self document, don''t you think?';

CALL util.apply_default_schema_config('auth');

CREATE TABLE auth.users (
    -- always put created_at and updated_at at top
    created_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP
,   updated_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP


,   id            UUID                        NOT NULL  DEFAULT uuid_generate_v7()
,   username      extensions.CITEXT           NOT NULL
,   email         extensions.CITEXT
,   pic_url       TEXT
-- ,   pub_key       BYTEA                       NOT NULL
-- ,   pri_key       BYTEA                       NOT NULL

    -- all constraints (besides not null) go after the columns
,   PRIMARY KEY(id)
,   UNIQUE(username)
-- ,   UNIQUE(email)
);

--- default config should be applied on all tables unless a good reason exists not to
CALL util.apply_default_table_config('auth', 'users');

-- most tables need a secondary table to store the deleted items
CALL util.create_deleted_rows_table('auth', 'users');

---

CREATE TABLE auth.credentials (
    created_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP
,   updated_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP

,   user_id        UUID           NOT NULL
,   pass_hash      TEXT           NOT NULL

,   PRIMARY KEY(user_id)
,   FOREIGN KEY(user_id) REFERENCES auth.users  -- no need to specify foreign col if primary key
);

CALL util.apply_default_table_config('auth', 'credentials');

CALL util.create_deleted_rows_table('auth', 'credentials');

---

CREATE TABLE auth.sessions (
    created_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP
,   updated_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP

,   id                UUID            NOT NULL      DEFAULT uuid_generate_v7()
,   token             TEXT            NOT NULL
,   user_id           UUID            NOT NULL
,   expires_at        TIMESTAMPTZ     NOT NULL

,   PRIMARY KEY(id)
,   FOREIGN KEY(user_id) REFERENCES auth.users
);

CALL util.apply_default_table_config('auth', 'sessions');

CALL util.create_deleted_rows_table('auth', 'sessions');
