CREATE FUNCTION auth.create_user(
  username TEXT
, email TEXT
-- , pub_key BYTEA
-- , pri_key BYTEA
)
RETURNS auth.users
AS $body$
    DECLARE
        le_user    auth.users;
    BEGIN
        INSERT INTO auth.users (
                username
                ,email
                -- ,pub_key
                -- ,pri_key
            ) VALUES (
                username::extensions.citext
                ,email::extensions.citext
                -- ,pub_key
                -- ,pri_key
            ) RETURNING * INTO le_user;
        return le_user;
    END;
$body$ LANGUAGE PLpgSQL;

CREATE FUNCTION auth.update_user(
  user_id UUID
, new_username extensions.CITEXT
, new_email extensions.CITEXT
, new_pic_url TEXT
)
RETURNS SETOF auth.users -- use SETOF to allow return of 0 rows
AS $body$
    DECLARE
        le_user    auth.users;
    BEGIN
        UPDATE auth.users 
            SET 
                username = COALESCE(new_username, username),
                email = COALESCE(new_email, email),
                pic_url = COALESCE(new_pic_url, pic_url)
            WHERE id = user_id 
            RETURNING * INTO le_user;

        IF NOT FOUND THEN
          RETURN;
        END IF;

        RETURN NEXT le_user;
    END;
$body$ LANGUAGE PLpgSQL;

CREATE FUNCTION auth.delete_user(target_id UUID) RETURNS BOOLEAN
AS $body$
    BEGIN
        IF NOT (EXISTS (SELECT id FROM auth.users WHERE id = target_id)) THEN
          RETURN FALSE;
        END IF;

        -- delete foreign keys that refer to users first to avoid referential
        -- integrity errors
        -- WITH deleted AS (
        --   DELETE FROM auth.credentials
        --   WHERE user_id = target_id
        --   RETURNING *
        -- )
        -- INSERT INTO auth.credentials_deleted (row) 
        -- SELECT row_to_json(d.*)::jsonb FROM deleted AS d;

        -- null out any references as well
	    UPDATE web.sessions
	        SET auth_session_id = NULL
	        WHERE auth_session_id (
	            SELECT id 
		        FROM auth.sessions 
		        WHERE 
		            auth.sessions.user_id = target_id 
	        );

        WITH deleted AS (
                DELETE FROM auth.sessions
                WHERE user_id = target_id
                RETURNING *
            )
            INSERT INTO auth.sessions_deleted (row) 
            SELECT row_to_json(d.*)::jsonb FROM deleted AS d;

        WITH deleted AS (
                DELETE FROM auth.users
                WHERE id = target_id
                RETURNING *
            )
            INSERT INTO auth.users_deleted (row) 
            SELECT row_to_json(d.*)::jsonb FROM deleted AS d;

        RETURN TRUE; 
    END;
$body$ LANGUAGE PLpgSQL;
