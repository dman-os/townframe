-- Bind: 1 backend_id.
UPDATE pauperfuse_backend
   SET generation = generation + 1
 WHERE backend_id = ?
RETURNING generation;
