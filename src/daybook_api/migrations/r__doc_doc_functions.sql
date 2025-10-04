CREATE FUNCTION doc.create_doc(
  id TEXT
-- , pub_key BYTEA
-- , pri_key BYTEA
)
RETURNS doc.docs
AS $body$
    DECLARE
        le_doc    doc.docs;
    BEGIN
        INSERT INTO doc.docs (
                id
            ) VALUES (
                id
            ) RETURNING * INTO le_doc;
        return le_doc;
    END;
$body$ LANGUAGE PLpgSQL;

-- CREATE FUNCTION doc.update_doc(
--   doc_id TEXT
-- , new_docname extensions.CITEXT
-- , new_email extensions.CITEXT
-- , new_pic_url TEXT
-- )
-- RETURNS SETOF doc.docs -- use SETOF to allow return of 0 rows
-- AS $body$
--     DECLARE
--         le_doc    doc.docs;
--     BEGIN
--         UPDATE doc.docs 
--         SET 
--             docname = COALESCE(new_docname, docname),
--             email = COALESCE(new_email, email),
--             pic_url = COALESCE(new_pic_url, pic_url)
--         WHERE id = doc_id 
--         RETURNING * INTO le_doc;
--
--         IF NOT FOUND THEN
--           RETURN;
--         END IF;
--
--         RETURN NEXT le_doc;
--     END;
-- $body$ LANGUAGE PLpgSQL;

CREATE FUNCTION doc.delete_doc(target_id TEXT) RETURNS BOOLEAN
AS $body$
    BEGIN
        IF NOT (EXISTS (SELECT id FROM doc.docs WHERE id = target_id)) THEN
          RETURN FALSE;
        END IF;

        -- delete foreign keys that refer to docs first to avoid referential
        -- integrity errors
        -- WITH deleted AS (
        --         DELETE FROM doc.credentials
        --         WHERE doc_id = target_id
        --         RETURNING *
        --     )
        --     INSERT INTO doc.credentials_deleted (row) 
        --     SELECT row_to_json(d.*)::jsonb FROM deleted AS d;

        -- null out any references as well
	    -- UPDATE web.sessions
	    --     SET doc_session_id = NULL
	    --     WHERE EXISTS (
	    --         SELECT 1 
	    --      FROM doc.sessions 
	    --      WHERE 
	    --          doc.sessions.doc_id = target_id 
	    --          AND doc.sessions.id = doc_session_id
	    --     );

        WITH deleted AS (
                DELETE FROM doc.docs
                WHERE id = target_id
                RETURNING *
            )
            INSERT INTO doc.docs_deleted (row) 
            SELECT row_to_json(d.*)::jsonb FROM deleted AS d;

        RETURN TRUE; 
    END;
$body$ LANGUAGE PLpgSQL;
