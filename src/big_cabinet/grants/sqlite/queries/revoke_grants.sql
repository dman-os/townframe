-- name: revoke_grants :exec
-- param: namespace String
-- param: subject String
-- param: objects Json<Vec<String>>
DELETE FROM cabinet_domain_grants
 WHERE namespace = :namespace
   AND subject = :subject
   AND object IN (SELECT value FROM json_each(:objects));
