-- Binds: ?1 scope_id, ?2 namespace, ?3 subspace, ?4 from_path, ?5 until_path, ?6 from_time, ?7 until_time
DELETE FROM big_willow_entries
 WHERE scope_id = ?1
   AND namespace = ?2
   AND subspace = ?3
   AND (?4 IS NULL OR path >= ?4)
   AND (?5 IS NULL OR path < ?5)
   AND timestamp >= ?6
   AND (?7 IS NULL OR timestamp < ?7);
