-- Binds: ?1 scope_id, ?2 namespace, ?3 subspace, ?4 path
SELECT timestamp
     , payload_digest
     , payload_length
  FROM big_willow_entries
 WHERE scope_id = ?1
   AND namespace = ?2
   AND subspace = ?3
   AND path = ?4;
