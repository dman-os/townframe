-- name: resolution_locks :many
-- returns: cartridge_id String, binding String, contract String, provider String, provider_version Option<String>, lexicon_revision Option<i64>
SELECT cartridge_id
     , binding
     , contract
     , provider
     , provider_version
     , lexicon_revision
  FROM nodule_resolution_lock
 ORDER BY cartridge_id
        , binding;
