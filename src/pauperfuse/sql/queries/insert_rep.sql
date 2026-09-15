-- Binds: ?1 name, ?2 updated_at
--
-- Run before any entry of the same update, because entries reference the rep. An existing rep
-- keeps its generation: only `bump_rep` moves that, once per update, so a rep that is updated
-- but ends up with nothing recorded still has a new generation.
INSERT INTO pauperfuse_rep (name, generation, updated_at)
VALUES (?1, 0, ?2)
ON CONFLICT (name) DO NOTHING;
