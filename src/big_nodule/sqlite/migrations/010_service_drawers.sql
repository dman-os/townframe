CREATE TABLE IF NOT EXISTS nodule_service_drawers (
       drawer_id TEXT NOT NULL
     , cartridge_id TEXT NOT NULL
     , origin TEXT NOT NULL CHECK (origin IN ('granted', 'created'))
     , PRIMARY KEY (drawer_id, cartridge_id)
);
