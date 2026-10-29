-- name: put_grant :exec
-- param: namespace String
-- param: subject String
-- param: object String
-- param: rights i64
INSERT INTO cabinet_domain_grants
     ( namespace
     , subject
     , object
     , rights
     )
VALUES (:namespace, :subject, :object, :rights)
ON CONFLICT (namespace, subject, object)
DO UPDATE SET rights = excluded.rights;
