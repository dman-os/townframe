-- name: observe :exec
-- param: waiting bool
-- param: drawer_id String
-- param: generation i64
-- param: attempt i64
UPDATE big_blobs_discovery_documents
   SET attempt = NULL
     , waiting = :waiting
 WHERE drawer_id = :drawer_id
   AND generation = :generation
   AND attempt = :attempt;
