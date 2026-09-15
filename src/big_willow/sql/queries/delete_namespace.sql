-- Binds: ?1 scope_id, ?2 namespace
DELETE FROM big_willow_entries
 WHERE scope_id = ?1
   AND namespace = ?2;
