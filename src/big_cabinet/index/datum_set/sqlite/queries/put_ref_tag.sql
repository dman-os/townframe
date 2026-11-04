-- name: put_ref_tag :exec
-- param: tag String
INSERT INTO big_cabinet_datum_set_ref_tags (tag)
VALUES (:tag)
ON CONFLICT (tag) DO NOTHING;
