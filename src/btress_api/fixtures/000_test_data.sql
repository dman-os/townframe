
-- strings use single quotes
BEGIN;

DO $body$
    DECLARE
        -- use variables in order to be able to access properties using the dot operator
        le_user         auth.users;
        le_auth_sesh    auth.sessions;
    BEGIN
        INSERT INTO auth.users (
            id
            ,username
            ,email
            ,pic_url
            -- ,pub_key
            -- ,pri_key
        ) VALUES (
            'add83cdf-2ab3-443f-84dd-476d7984cf75'::uuid
            ,'sabrina'
            ,'hex.queen@teen.dj'
            ,'https://obj.teen.dj/d78xas'
            -- ,'\x7c5bade04be3bb0fb9bd33f5eec539863c0c82866e333e525311823ef44b8cf5'::bytea
            -- ,'\xeb28ec6fa7d60b719af82e4de57391dfda3fd354a344acd5f4ae143ca6554d3e'::bytea
        ) RETURNING * INTO le_user;
        INSERT INTO auth.credentials (
            user_id, pass_hash
        ) VALUES ( 
            le_user.id, 
            '$argon2i$v=19$m=4096,t=3,p=1$c29tZXNhbHQ$iWh06vD8Fy27wf9npn6FXWiCX4K6pW6Ue1Bnzz07Z8A'
        );
        INSERT INTO auth.sessions (
            id, token, user_id, expires_at
        ) VALUES (
            '9d827d5c-15bd-413c-9431-39ff96155d7b',
	    -- FIXME: use some random string
            '9d827d5c-15bd-413c-9431-39ff96155d7b',
            le_user.id,
            CURRENT_TIMESTAMP + interval '7 days'
        ) RETURNING * INTO le_auth_sesh;
        INSERT INTO web.sessions (
            id, auth_session_id, expires_at, ip_addr, user_agent
        ) VALUES (
            '13e4cbdf-aa7c-43ca-990c-a8b468d44616'::uuid,
            le_auth_sesh.id,
            CURRENT_TIMESTAMP + interval '7 days',
            '127.0.0.1'::inet,
            'ViolaWWW'
        );
    END;
$body$ LANGUAGE PLpgSQL;

DO $body$
    DECLARE
        le_user    auth.users;
    BEGIN
        INSERT INTO auth.users (
            id
            ,username
            ,email
            ,pic_url
            -- ,pub_key
            -- ,pri_key
        ) VALUES (
            'ce4fe993-04d6-462e-af1d-d734fcc9639d'::uuid
            ,'archie'
            ,'archie1941@poetry.ybn'
            ,'https://pictu.res/01'
            -- ,'\x67bf08ee99120acf1a708e8d41f1ff7fc2de8a4361d780626f569e8f81de5146'::bytea
            -- ,'\x7ceffe6e9dd0cba3bd2cd362e472b0b94d0f4b1417c665f7249967ebdc7fd6a0'::bytea
        ) RETURNING * INTO le_user;
        INSERT INTO auth.credentials (
            user_id, pass_hash
        ) VALUES ( 
            le_user.id, 
            '$argon2i$v=19$m=4096,t=3,p=1$c29tZXNhbHQ$iWh06vD8Fy27wf9npn6FXWiCX4K6pW6Ue1Bnzz07Z8A'
        );
    END;
$body$ LANGUAGE PLpgSQL;

DO $body$
    DECLARE
        le_user    auth.users;
    BEGIN
        INSERT INTO auth.users (
            id
            ,username
            ,email
            ,pic_url
            -- ,pub_key
            -- ,pri_key
        ) VALUES (
            'd437e73f-4610-462c-ab22-f94b76bba83a'::uuid
            ,'betty'
            ,'pInXy@melt.shake'
            ,null
            -- ,'\x16179796da54225bcfd6937d6ed275807a2818e59c89276f7b4992adee613edc'::bytea
            -- ,'\x223c52751e99d3acfa7dc2a9185fe7b6ec8f3acbc5503ae9f3815033e1f04846'::bytea
        ) RETURNING * INTO le_user;
        INSERT INTO auth.credentials (
            user_id, pass_hash
        ) VALUES ( 
            le_user.id, 
            '$argon2i$v=19$m=4096,t=3,p=1$c29tZXNhbHQ$iWh06vD8Fy27wf9npn6FXWiCX4K6pW6Ue1Bnzz07Z8A'
        );
    END;
$body$ LANGUAGE PLpgSQL;

DO $body$
    DECLARE
        le_user    auth.users;
        le_auth_sesh    auth.sessions;
    BEGIN
        INSERT INTO auth.users (
            id
            ,username
            ,email
            ,pic_url
            -- ,pub_key
            -- ,pri_key
        ) VALUES (
            '68cf4d43-62d2-4202-8c50-c79a5f4dd1cc'::uuid
            ,'veronica'
            ,'trekkiegirl@ln.pi'
            ,'ipns://goatsie'
            -- ,'\x642c72a0d589ba75c22351db61c7beada6a5e12d65373b86ecd6f8e248c654af'::bytea
            -- ,'\x359b2f5d06e233765fc2afcc51e39b716b0d790d4233f8f07e1ebb08a3de8223'::bytea
        ) RETURNING * INTO le_user;
        INSERT INTO auth.credentials (
            user_id, pass_hash
        ) VALUES ( 
            le_user.id, 
            '$argon2i$v=19$m=4096,t=3,p=1$c29tZXNhbHQ$iWh06vD8Fy27wf9npn6FXWiCX4K6pW6Ue1Bnzz07Z8A'
        );
        INSERT INTO auth.sessions (
            id, token, user_id, expires_at
        ) VALUES (
            'ebd3b465-be17-4077-bc4a-add9f76b5028',
            'ebd3b465-be17-4077-bc4a-add9f76b5028',
            le_user.id,
            CURRENT_TIMESTAMP + interval '7 days'
        ) RETURNING * INTO le_auth_sesh;
        INSERT INTO web.sessions (
            id, auth_session_id, expires_at, ip_addr, user_agent
        ) VALUES (
            '0a7f6a02-43a4-4738-b70c-0d66eb24459f'::uuid,
            le_auth_sesh.id,
            CURRENT_TIMESTAMP + interval '7 days',
            '8.0.0.1'::inet,
            'ViolaWWW'
        );
    END;
$body$ LANGUAGE PLpgSQL;

-- you can bypass the DO section though
-- INSERT UPDATE STUFF
COMMIT;
-- ROLLBACK;
