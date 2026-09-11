-- Binds: ?1 scope_key
INSERT INTO big_willow_scopes (scope_key)
VALUES (?1)
ON CONFLICT (scope_key) DO NOTHING;
