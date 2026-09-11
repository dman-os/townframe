-- Folds the write-ahead log back into the database file.
--
-- Every mutation is already committed and durable in the log when its transaction returns, so
-- this is about the file layout rather than about durability. Durability across a crash is
-- governed by the connection's `synchronous` setting, which `SqlCtx` chooses, not by this.
PRAGMA wal_checkpoint(PASSIVE);
