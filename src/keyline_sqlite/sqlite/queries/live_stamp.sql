-- name: live_stamp :optional
-- param: subject Vec<u8>
-- returns: version i64
SELECT version
  FROM keyline_live_at
 WHERE subject = :subject
