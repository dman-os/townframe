-- name: list_wflow_part_frontiers :many
-- returns: wflow_partition_id String, frontier i64
SELECT wflow_partition_id, frontier FROM wflow_partition_frontier
