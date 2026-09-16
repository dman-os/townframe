-- Binds: ?1 name, ?2 updated_at
--
-- The one statement that moves a generation, and it runs exactly once per rep update.
UPDATE pauperfuse_rep
   SET generation = generation + 1
     , updated_at = ?2
 WHERE name = ?1;
