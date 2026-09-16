-- Binds: ?1 path
--
-- Test-only. Turns a recorded file into a directory *without* clearing its origin, which is a
-- row shape no update produces and no reader may guess at: reading it must fail rather than
-- report a directory carrying a file's provenance.
UPDATE pauperfuse_entry
   SET kind = 1
 WHERE path = ?1;
