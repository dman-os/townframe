-- Binds: ?1 scope_id, ?2 namespace, ?3 subspace, ?4 path, ?5 timestamp, ?6 payload_digest, ?7 payload_length, ?8 entry
INSERT INTO big_willow_entries (
      scope_id
    , namespace
    , subspace
    , path
    , timestamp
    , payload_digest
    , payload_length
    , entry
)
VALUES (
      ?1
    , ?2
    , ?3
    , ?4
    , ?5
    , ?6
    , ?7
    , ?8
)
ON CONFLICT (scope_id, namespace, subspace, path) DO UPDATE SET
      timestamp = excluded.timestamp
    , payload_digest = excluded.payload_digest
    , payload_length = excluded.payload_length
    , entry = excluded.entry;
