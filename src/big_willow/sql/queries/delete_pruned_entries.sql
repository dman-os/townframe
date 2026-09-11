-- Binds: ?1 scope_id, ?2 namespace, ?3 subspace, ?4 path_lo, ?5 path_hi, ?6 timestamp, ?7 payload_digest, ?8 payload_length
--
-- Removes the entries the new entry prunes: the entries at or under `path_lo` that the new
-- entry is newer than or equal to, ties included. The only exception is a row at `path_lo`
-- itself that ties the new entry exactly. That row is the one the upsert replaces, so deleting
-- it here would cascade its retained payload away for an entry that did not change. The same
-- rule is stated in Rust in `big_willow::store::prunes`.
DELETE FROM big_willow_entries
 WHERE scope_id = ?1
   AND namespace = ?2
   AND subspace = ?3
   AND path >= ?4
   AND (?5 IS NULL OR path < ?5)
   AND (
           timestamp < ?6
        OR timestamp = ?6 AND payload_digest < ?7
        OR timestamp = ?6 AND payload_digest = ?7 AND payload_length < ?8
        OR timestamp = ?6 AND payload_digest = ?7 AND payload_length = ?8 AND path <> ?4
       );
