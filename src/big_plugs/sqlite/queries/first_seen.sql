-- name: first_seen :many
-- returns: plug_id String, version String, revision String, envelope_json String
SELECT plug_id
     , version
     , revision
     , envelope_json
  FROM plugs_first_seen
 ORDER BY plug_id
        , version;
