-- name: upsert_first_seen :exec
-- param: plug_id String
-- param: version String
-- param: revision String
-- param: envelope_json String
INSERT INTO plugs_first_seen
     ( plug_id
     , version
     , revision
     , envelope_json
     )
VALUES ( :plug_id
       , :version
       , :revision
       , :envelope_json
       )
ON CONFLICT (plug_id, version) DO UPDATE
   SET revision = excluded.revision
     , envelope_json = excluded.envelope_json;
