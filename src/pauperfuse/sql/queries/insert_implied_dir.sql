-- Binds: ?1 rep, ?2 path
--
-- A recorded path implies its parent directories, which may themselves never have been
-- recorded: a producer can emit `notes/plan.md` with no entry for `notes`, and a delta can arrive
-- for a path whose parent row was dropped. `DO NOTHING` because a recorded parent — with its
-- own stat and claim — is the better answer and must not be overwritten.
--
-- `kind = 1` is a directory and `avail = 0` is present. Nothing else is claimed: no origin, no
-- target, and no stat, because a directory has none (see the migration).
INSERT INTO pauperfuse_entry (rep, path, kind, avail)
VALUES (?1, ?2, 1, 0)
ON CONFLICT (rep, path) DO NOTHING;
