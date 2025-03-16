CREATE SCHEMA IF NOT EXISTS 
  web;

COMMENT ON
  SCHEMA web IS 'Tables relating to the web app';

CALL util.apply_default_schema_config('web');

CREATE TABLE web.sessions (
    created_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP
,   updated_at      TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP

,   id                UUID            NOT NULL      DEFAULT uuid_generate_v7()
,   expires_at        TIMESTAMPTZ     NOT NULL
,   auth_session_id   UUID
,   ip_addr           INET            NOT NULL
,   user_agent        TEXT            NOT NULL

,   PRIMARY KEY(id)
,   FOREIGN KEY(auth_session_id) REFERENCES auth.sessions
);

CALL util.apply_default_table_config('web', 'sessions');

CALL util.create_deleted_rows_table('web', 'sessions');
