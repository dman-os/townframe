-- name: delete_edges :exec
-- param: origin_doc String
-- param: origin_facet String
DELETE FROM big_cabinet_facet_ref_edges
 WHERE origin_doc = :origin_doc
   AND origin_facet = :origin_facet;
