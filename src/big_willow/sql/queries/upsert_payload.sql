-- Binds: ?1 scope_id, ?2 namespace, ?3 subspace, ?4 path, ?5 payload
INSERT INTO big_willow_payloads (
      scope_id
    , namespace
    , subspace
    , path
    , payload
)
VALUES (
      ?1
    , ?2
    , ?3
    , ?4
    , ?5
)
ON CONFLICT (scope_id, namespace, subspace, path) DO UPDATE SET
    payload = excluded.payload;
