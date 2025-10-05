
-- strings use single quotes
BEGIN;

DO $body$
    DECLARE
        -- use variables in order to be able to access properties using the dot operator
        le_doc         doc.docs;
    BEGIN
        INSERT INTO doc.docs (
            id
            -- ,pub_key
            -- ,pri_key
        ) VALUES (
            'doc_01'
            -- ,'\x7c5bade04be3bb0fb9bd33f5eec539863c0c82866e333e525311823ef44b8cf5'::bytea
            -- ,'\xeb28ec6fa7d60b719af82e4de57391dfda3fd354a344acd5f4ae143ca6554d3e'::bytea
        ) RETURNING * INTO le_doc;
    END;
$body$ LANGUAGE PLpgSQL;


-- you can bypass the DO section though
-- INSERT UPDATE STUFF
COMMIT;
-- ROLLBACK;
