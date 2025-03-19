CREATE TABLE IF NOT EXISTS dem.stac_raster_asset_info (
    collection      TEXT,
    item_id         TEXT,
    asset_key       TEXT,
    gsd             FLOAT,
    proj_code       TEXT,
    proj_shape      JSONB,
    proj_transform  JSONB,
    proj_bbox       JSONB,
    proj_geometry   JSONB,
    proj_centroid   JSONB,

    PRIMARY KEY (collection, item_id, asset_key)
);

COMMENT ON TABLE dem.stac_raster_asset_info IS 'Raster projection properties for STAC assets';

ALTER TABLE dem.stac_raster_asset_info OWNER TO pgc_gis_admin;

GRANT SELECT ON dem.stac_raster_asset_info TO pgc_users;
