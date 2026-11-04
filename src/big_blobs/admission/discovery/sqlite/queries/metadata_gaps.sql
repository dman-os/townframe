-- name: metadata_gaps :many
-- param: root_id String
-- returns: drawer_id String
-- returns: meta_root String
-- returns: name String
-- returns: observed bool
-- returns: waiting bool
-- returns: self_doc bool
-- returns: authoritative bool
-- returns: inventory_id Option<String>
SELECT r.drawer_id
     , r.meta_root
     , r.name
     , d.observed
     , d.waiting
     , d.self_doc
     , COALESCE(m.authoritative, 0) AS authoritative
     , s.inventory_id
  FROM big_blobs_discovery_reach r
  JOIN big_blobs_discovery_documents d USING (drawer_id)
  LEFT JOIN big_blobs_discovery_roots m USING (drawer_id, meta_root)
  LEFT JOIN big_blobs_discovery_slots s USING (drawer_id, meta_root, name)
 WHERE r.root_id = :root_id
 ORDER BY r.drawer_id, r.meta_root, r.name;
