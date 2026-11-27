-- name: pending_events_all :many
-- returns: digest Vec<u8>, event Vec<u8>
SELECT digest
     , event
  FROM kh_pending_event
