-- name: set_wflow_part_frontier :exec
-- param: wflow_partition_id String
-- param: frontier i64
INSERT INTO wflow_partition_frontier(wflow_partition_id, frontier, updated_at)
VALUES (:wflow_partition_id, :frontier, unixepoch())
ON CONFLICT(wflow_partition_id) DO UPDATE SET
    frontier = excluded.frontier
  , updated_at = excluded.updated_at
