-- "Plug" became "cartridge" (ADR 027): the column that keys a nodule by the cartridge that holds
-- it is renamed in place. SQLite carries primary keys and unique constraints across a column
-- rename.
ALTER TABLE nodule_actors RENAME COLUMN plug_id TO cartridge_id;
ALTER TABLE nodule_applied_grants RENAME COLUMN plug_id TO cartridge_id;
ALTER TABLE nodule_applied_operations RENAME COLUMN plug_id TO cartridge_id;
ALTER TABLE nodule_init_done RENAME COLUMN plug_id TO cartridge_id;
ALTER TABLE nodule_resolution_lock RENAME COLUMN plug_id TO cartridge_id;
