-- Binds: ?1 name
--
-- Entries go with it: the foreign key's `ON DELETE CASCADE` is the whole story, so no statement
-- removes entries for a dropped rep.
DELETE FROM pauperfuse_rep
 WHERE name = ?1;
