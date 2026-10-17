-- name: get_meta :optional
-- param: key String
-- returns: value String
SELECT value FROM pauperfuse_meta WHERE key = :key;
