-- name: upsert_resolution_lock :exec
-- param: cartridge_id String
-- param: binding String
-- param: contract String
-- param: provider String
-- param: provider_version Option<String>
-- param: lexicon_revision Option<i64>
INSERT INTO nodule_resolution_lock
     ( cartridge_id
     , binding
     , contract
     , provider
     , provider_version
     , lexicon_revision
     )
VALUES ( :cartridge_id
       , :binding
       , :contract
       , :provider
       , :provider_version
       , :lexicon_revision
       )
ON CONFLICT (cartridge_id, binding) DO UPDATE
   SET contract = excluded.contract
     , provider = excluded.provider
     , provider_version = excluded.provider_version
     , lexicon_revision = excluded.lexicon_revision;
