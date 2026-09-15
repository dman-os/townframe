EXPLAIN QUERY PLAN
-- Bind: 1 name.
INSERT INTO pauperfuse_backend (name)
VALUES (?)
ON CONFLICT (name) DO UPDATE SET name = excluded.name
RETURNING backend_id;
