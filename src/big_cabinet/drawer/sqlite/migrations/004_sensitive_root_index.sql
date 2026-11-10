CREATE INDEX IF NOT EXISTS big_cabinet_drawers_by_sensitive_root
    ON big_cabinet_drawers (json_extract(descriptor_json, '$.layout.sensitiveRoot'));
