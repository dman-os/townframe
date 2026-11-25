-- name: okv_register_consumer :exec
-- param: namespace_id i64
-- param: consumer String
INSERT INTO big_automata_okv_consumers (namespace_id, consumer, cursor)
VALUES (:namespace_id, :consumer, 0)
    ON CONFLICT (namespace_id, consumer) DO NOTHING
