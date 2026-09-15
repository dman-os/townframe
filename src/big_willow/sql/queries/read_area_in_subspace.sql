-- Binds: ?1 scope_id, ?2 namespace, ?3 subspace, ?4 after_path, ?5 from_path, ?6 until_path, ?7 from_time, ?8 until_time, ?9 limit
SELECT entry
  FROM big_willow_entries
 WHERE scope_id = ?1
   AND namespace = ?2
   AND subspace = ?3
   AND (?4 IS NULL OR path > ?4)
   AND (?5 IS NULL OR path >= ?5)
   AND (?6 IS NULL OR path < ?6)
   AND timestamp >= ?7
   AND (?8 IS NULL OR timestamp < ?8)
 ORDER BY path
 LIMIT ?9;
