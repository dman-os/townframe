-- name: matching_grants :many
-- param: objects Json<Vec<String>>
-- param: namespace String
-- param: subject String
-- returns: object String, rights i64
SELECT requested.value AS object
     , COALESCE(grants.rights, 0) AS rights
  FROM json_each(:objects) AS requested
  LEFT JOIN cabinet_domain_grants AS grants
    ON grants.namespace = :namespace
   AND grants.subject = :subject
   AND grants.object = requested.value
 ORDER BY CAST(requested.key AS INTEGER);
