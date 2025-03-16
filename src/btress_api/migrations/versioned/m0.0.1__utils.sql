-- Add migration script here

CREATE SCHEMA IF NOT EXISTS 
  extensions;

CREATE EXTENSION IF NOT EXISTS 
    "uuid-ossp"
  WITH SCHEMA
    extensions;

CREATE EXTENSION IF NOT EXISTS 
    citext 
  WITH SCHEMA 
    extensions;

-- CREATE EXTENSION IF NOT EXISTS 
--     pgtap 
--   WITH SCHEMA 
--     extensions;

-- CREATE EXTENSION IF NOT EXISTS 
--     pg_jsonschema 
--   WITH SCHEMA 
--     extensions;

---

CREATE SCHEMA IF NOT EXISTS 
  util;

COMMENT ON
  SCHEMA util IS 'Helper utilities.';

---

CREATE OR REPLACE FUNCTION 
    util.maintain_updated_at()
  RETURNS TRIGGER AS 
  $body$
      BEGIN
          NEW.updated_at := CURRENT_TIMESTAMP;
          RETURN NEW;
      END;
  $body$ LANGUAGE PLpgSQL;

---

CREATE DOMAIN 
    util.stdtext AS TEXT
  CONSTRAINT 
    check_stdtext CHECK (
      LENGTH(VALUE) > 0 AND LENGTH(VALUE) <= 1024
    );
COMMENT ON 
  DOMAIN util.stdtext is 'variant of text less that''s not empty and under 1KiB but nullable';

---

CREATE DOMAIN 
    util.email AS extensions.citext
  CONSTRAINT 
    check_email CHECK (
      LENGTH(VALUE) <= 255 AND
      value ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
    );

COMMENT ON 
  DOMAIN util.email IS 'lightly validated email address';

---

CREATE DOMAIN 
    util.phone_no AS extensions.citext
  CONSTRAINT 
    check_phone_no CHECK (
      value ~ '^\+[0-9]{12}$'
    );
COMMENT ON 
  DOMAIN util.phone_no IS 'lightly validated phone no';

--- INSTALL dbdev https://database.dev/installer

/* create extension if not exists http with schema extensions;
create extension if not exists pg_tle;
select pgtle.uninstall_extension_if_exists('supabase-dbdev');
drop extension if exists "supabase-dbdev";
select
    pgtle.install_extension(
        'supabase-dbdev',
        resp.contents ->> 'version',
        'PostgreSQL package manager',
        resp.contents ->> 'sql'
    )
from http(
    (
        'GET',
        'https://api.database.dev/rest/v1/'
        || 'package_versions?select=sql,version'
        || '&package_name=eq.supabase-dbdev'
        || '&order=version.desc'
        || '&limit=1',
        array[
            ('apiKey', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhtdXB0cHBsZnZpaWZyYndtbXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODAxMDczNzIsImV4cCI6MTk5NTY4MzM3Mn0.z2CN0mvO2No8wSi46Gw59DFGCTJrzM0AQKsu_5k134s')::http_header
        ],
        null,
        null
    )
) x,
lateral (
    select
        ((row_to_json(x) -> 'content') #>> '{}')::json -> 0
) resp(contents);
create extension "supabase-dbdev";
select dbdev.install('supabase-dbdev');
drop extension if exists "supabase-dbdev";
create extension "supabase-dbdev";


select dbdev.install('basejump-supabase_test_helpers'); */

---

CREATE SCHEMA IF NOT EXISTS 
  util;

COMMENT ON
  SCHEMA util IS 'Helper utilities.';

---

-- Lifted from https://github.com/jetpack-io/typeid-sql/blob/d72825bc2a009771fe4c0cadc5a278a14676b251/sql/01_uuidv7.sql
-- Function to generate new v7 UUIDs.
-- In the future we might want use an extension: https://github.com/fboulnois/pg_uuidv7
-- Or, once the UUIDv7 spec is finalized, it will probably make it into the 'uuid-ossp' extension
-- and a custom function will no longer be necessary.
CREATE OR REPLACE FUNCTION uuid_generate_v7() RETURNS UUID
  AS $$
  DECLARE
    unix_ts_ms BYTEA;
    uuid_bytes BYTEA;
  BEGIN
    unix_ts_ms = SUBSTRING(INT8SEND(FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT) FROM 3);
    uuid_bytes = UUID_SEND(gen_random_uuid());
    uuid_bytes = OVERLAY(uuid_bytes placing unix_ts_ms from 1 for 6);
    uuid_bytes = SET_BYTE(uuid_bytes, 6, (b'0111' || GET_BYTE(uuid_bytes, 6)::BIT(4))::BIT(8)::INT);
    return ENCODE(uuid_bytes, 'hex')::UUID;
  END
  $$
  LANGUAGE PLPGSQL VOLATILE;

CREATE OR REPLACE FUNCTION 
    util.maintain_updated_at()
  RETURNS TRIGGER AS 
  $body$
      BEGIN
          NEW.updated_at := CURRENT_TIMESTAMP;
          RETURN NEW;
      END;
  $body$ LANGUAGE PLpgSQL;

---
CREATE OR REPLACE PROCEDURE 
    util.apply_default_schema_config(
      schema_name TEXT
    )
  AS $body$
    BEGIN
      -- EXECUTE FORMAT('CREATE SCHEMA IF NOT EXISTS %I', schema_name);
      --
      -- EXECUTE FORMAT('GRANT USAGE ON SCHEMA %I TO postgres', schema_name);
      --
      -- EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES 
      -- IN SCHEMA %I 
      -- GRANT ALL ON TABLES TO 
      -- postgres', schema_name);
      --
      -- EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES 
      -- IN SCHEMA %I 
      -- GRANT ALL ON FUNCTIONS 
      -- TO postgres', schema_name);
      --
      -- EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES 
      -- IN SCHEMA %I 
      -- GRANT ALL ON SEQUENCES 
      -- TO postgres', schema_name);
    END;
  $body$ LANGUAGE PLpgSQL;
COMMENT ON 
  PROCEDURE util.apply_default_schema_config 
  IS 'Default config to apply to schemas after creation';


CALL util.apply_default_schema_config('extensions');
CALL util.apply_default_schema_config('utils');

---

CREATE OR REPLACE PROCEDURE 
    util.apply_default_table_config(
      schema_name TEXT
      ,table_name TEXT
    )
  AS $body$
      BEGIN
        -- EXECUTE FORMAT('ALTER TABLE %I.%I OWNER to postgres', schema_name, table_name);
        EXECUTE FORMAT('
        CREATE OR REPLACE TRIGGER maintain_updated_at
        BEFORE UPDATE
        ON %I.%I
        FOR EACH ROW
        EXECUTE PROCEDURE util.maintain_updated_at()', schema_name, table_name);
        /* EXECUTE FORMAT(
            'ALTER TABLE IF EXISTS %I.%I ENABLE ROW LEVEL SECURITY;'
            ,schema_name
            ,table_name
        ); */
      END;
  $body$ LANGUAGE PLpgSQL;

COMMENT ON 
  PROCEDURE util.apply_default_table_config 
  IS 'Default configurations to apply to tables after creation
This assumes that the table has a `updated_at` column.';

---

CREATE OR REPLACE PROCEDURE 
    util.create_deleted_rows_table(
      schema_name TEXT
      ,table_name TEXT
    )
  AS $body$
      BEGIN
        EXECUTE FORMAT('
CREATE TABLE %I.%I_deleted
(
    deleted_at    TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
,   row           JSONB           NOT NULL
);
          ', schema_name, table_name);
        -- EXECUTE FORMAT('ALTER TABLE %I.%I_deleted OWNER to postgres', schema_name, table_name);
        -- EXECUTE FORMAT(
        --     'ALTER TABLE IF EXISTS %I.%I_deleted ENABLE ROW LEVEL SECURITY;'
        --     ,schema_name
        --     ,table_name
        -- );
      END;
  $body$ LANGUAGE PLpgSQL;

COMMENT ON 
  PROCEDURE util.create_deleted_rows_table 
  IS 'Create a deleted rows store table under the specified names.';
