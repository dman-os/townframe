-- Binds: ?1 scope_id
SELECT COUNT(*) AS orphans
  FROM big_willow_payloads
 WHERE scope_id = ?1
   AND NOT EXISTS (
           SELECT 1
             FROM big_willow_entries
            WHERE big_willow_entries.scope_id = big_willow_payloads.scope_id
              AND big_willow_entries.namespace = big_willow_payloads.namespace
              AND big_willow_entries.subspace = big_willow_payloads.subspace
              AND big_willow_entries.path = big_willow_payloads.path
       );
