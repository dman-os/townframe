-- name: kv_get :optional
-- param: key String
-- returns: value String
SELECT value FROM kvstore WHERE key = :key
