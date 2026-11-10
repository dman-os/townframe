-- name: upsert_first_seen :exec
-- param: cartridge_id String
-- param: version String
-- param: revision String
-- param: envelope_json String
INSERT INTO cartridges_first_seen
     ( cartridge_id
     , version
     , revision
     , envelope_json
     )
VALUES ( :cartridge_id
       , :version
       , :revision
       , :envelope_json
       )
ON CONFLICT (cartridge_id, version) DO UPDATE
   SET revision = excluded.revision
     , envelope_json = excluded.envelope_json;
