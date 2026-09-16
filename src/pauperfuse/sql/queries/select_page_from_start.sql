-- Binds: ?1 rep, ?2 limit
--
-- The first page of a rep. `ORDER BY path` is the primary key's order, so the plan is an index
-- walk and never a sort (ADR 010 §3.3).
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
 ORDER BY path
 LIMIT ?2;
