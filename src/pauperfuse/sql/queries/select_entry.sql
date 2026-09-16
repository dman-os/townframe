-- Binds: ?1 rep, ?2 path
--
-- An exact match on the primary key, so one index seek and no sort.
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
   AND path = ?2;
