-- "Plug" became "cartridge" (ADR 027): the registry's tables and the column that keys a
-- first-seen envelope are renamed in place.
ALTER TABLE plugs_registry_drawer RENAME TO cartridges_registry_drawer;
ALTER TABLE plugs_first_seen RENAME TO cartridges_first_seen;
ALTER TABLE cartridges_first_seen RENAME COLUMN plug_id TO cartridge_id;
