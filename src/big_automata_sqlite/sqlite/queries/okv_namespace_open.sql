-- name: okv_namespace_open :exec
-- param: name String
INSERT INTO big_automata_okv_namespaces (name, rev)
VALUES (:name, 0)
    ON CONFLICT (name) DO NOTHING
