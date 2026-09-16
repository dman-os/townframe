EXPLAIN QUERY PLAN
-- Binds: ?1 rep, ?2 path, ?3 limit
--
-- A page strictly after a cursor, which is how every walk resumes: the cursor is the last path
-- of the previous page, so a resumed walk repeats nothing and skips nothing. `>` and not `>=`
-- because the cursor row itself has already been handed out (ADR 011 §6).
SELECT path
     , kind
     , origin
     , content
     , target
     , avail
     , stat
     , claim
  FROM pauperfuse_entry
 WHERE rep = ?1
   AND path > ?2
 ORDER BY path
 LIMIT ?3;
