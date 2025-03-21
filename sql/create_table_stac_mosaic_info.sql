CREATE TABLE IF NOT EXISTS dem.stac_mosaic_info (
    collection      TEXT,
    item_id         TEXT,
    pairname_ids    JSONB,
    start_datetime  TIMESTAMPTZ,
    end_datetime    TIMESTAMPTZ,

    PRIMARY KEY (collection, item_id)
);

COMMENT ON TABLE dem.stac_mosaic_info IS 'Store extra mosaic tile metadata for STAC item creation';

ALTER TABLE dem.stac_mosaic_info OWNER TO pgc_gis_admin;

GRANT SELECT ON dem.stac_mosaic_info TO pgc_users;
