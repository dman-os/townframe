-- name: get_grant :optional
-- param: namespace String
-- param: subject String
-- param: object String
-- returns: rights i64
SELECT rights
  FROM cabinet_domain_grants
 WHERE namespace = :namespace
   AND subject = :subject
   AND object = :object;
