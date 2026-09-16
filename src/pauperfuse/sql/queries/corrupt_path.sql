-- Binds: ?1 new_path, ?2 path
--
-- Test-only. Writes a path that cannot be decoded back into a `RelPath`, which is what a row
-- written by a different, buggy encoder would look like.
UPDATE pauperfuse_entry
   SET path = ?1
 WHERE path = ?2;
