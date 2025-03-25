CREATE MATERIALIZED VIEW dem.stac_static_item AS (
    SELECT collection, item_id, content FROM dem.stac_strip_item
    UNION ALL
    SELECT collection, item_id, content FROM dem.stac_mosaic_item
);

COMMENT ON MATERIALIZED VIEW dem.stac_static_item IS 'Static STAC items for public DEMs. Materialized for retrieval and content testing efficiency.';

ALTER MATERIALIZED VIEW dem.stac_static_item OWNER TO pgc_gis_admin;

GRANT SELECT ON dem.stac_static_item TO pgc_users;
