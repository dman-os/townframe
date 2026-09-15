-- Binds: ?1 rep, ?2 path
--
-- Exactly one recorded path. A removed directory is not cascaded: the backend reports each path
-- it lost, so the rows for a removed subtree are deleted one by one, by their own deltas.
DELETE FROM pauperfuse_entry
 WHERE rep = ?1
   AND path = ?2;
