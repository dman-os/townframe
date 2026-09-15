-- Binds: ?1 scope_id, ?2 namespace, ?3 from_path, ?4 until_path, ?5 from_time, ?6 until_time
DELETE FROM big_willow_entries
 WHERE scope_id = ?1
   AND namespace = ?2
   AND (?3 IS NULL OR path >= ?3)
   AND (?4 IS NULL OR path < ?4)
   AND timestamp >= ?5
   AND (?6 IS NULL OR timestamp < ?6);
