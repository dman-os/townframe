-- name: first_seen :many
-- returns: cartridge_id String, version String, revision String, envelope_json String
SELECT cartridge_id
     , version
     , revision
     , envelope_json
  FROM cartridges_first_seen
 ORDER BY cartridge_id
        , version;
