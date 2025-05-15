CREATE OR REPLACE VIEW dem.stac_mosaic_item AS (
WITH canonical_mosaics AS (
    -- Contains one row for each mosaic item that should be created
    SELECT
        format('%s-mosaics-v%s-%sm', md_release.project, md_release.release_ver, md_release.gsd) AS collection,
        dem_id AS item_id,
        dem_id
    FROM dem.mosaic_dem_release as md_release
    WHERE license = 'public'
),

href_parts AS (
    -- Contains all the information necessary to construct link and asset hrefs
    SELECT
        collection,
        item_id,
        'https://pgc-opendata-dems.s3.us-west-2.amazonaws.com' AS base_url,
        's3://pgc-opendata-dems' AS base_s3_url,
        split_part(collection, '-', 1) AS domain,
        split_part(collection, '-', 2) AS kind,
        split_part(collection, '-', 3) AS release_version,
        split_part(collection, '-', 4) AS resolution_str,
        sm_release.supertile AS supertile
    FROM canonical_mosaics

         LEFT JOIN dem.mosaic_dem_release AS sm_release
               USING (dem_id)
),

collection_title_lookup AS (
    SELECT *
    FROM (
        VALUES
            ('arcticdem-mosaics-v3.0-2m', 'Resolution Collection ArcticDEM 2m DEM Mosaics, version 3.0'),
            ('arcticdem-mosaics-v3.0-10m', 'Resolution Collection ArcticDEM 10m DEM Mosaics, version 3.0'),
            ('arcticdem-mosaics-v3.0-32m', 'Resolution Collection ArcticDEM 32m DEM Mosaics, version 3.0'),
            ('arcticdem-mosaics-v4.1-2m', 'Resolution Collection ArcticDEM 2m DEM Mosaics, version 4.1'),
            ('arcticdem-mosaics-v4.1-10m', 'Resolution Collection ArcticDEM 10m DEM Mosaics, version 4.1'),
            ('arcticdem-mosaics-v4.1-32m', 'Resolution Collection ArcticDEM 32m DEM Mosaics, version 4.1'),
            ('rema-mosaics-v2.0-2m', 'Resolution Collection REMA 2m DEM Mosaics, version 2.0'),
            ('rema-mosaics-v2.0-10m', 'Resolution Collection REMA 10m DEM Mosaics, version 2.0'),
            ('rema-mosaics-v2.0-32m', 'Resolution Collection REMA 32m DEM Mosaics, version 2.0')
    ) AS collection_title(collection, title)
),

links AS (
    -- STAC links object
    SELECT
        collection,
        item_id,
        jsonb_build_array(
            jsonb_build_object(
                'rel', 'self',
                'href', concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile, item_id) || '.json',
                'type', 'application/geo+json'
            ),
            jsonb_build_object(
                'rel', 'parent',
                'title', format('Tile Catalog %s', supertile),
                'href', concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile) || '.json',
                'type', 'application/json'
            ),
            jsonb_build_object(
                'rel', 'collection',
                'title', collection_title_lookup.title,
                'href', concat_ws('/', base_url, domain, kind, release_version, resolution_str) || '.json',
                'type', 'application/json'
            ),
            jsonb_build_object(
                'rel', 'root',
                'title', 'PGC Data Catalog',
                'href', concat_ws('/', base_url, 'pgc-data-stac.json'),
                'type', 'application/json'
            )
        ) AS content
    FROM href_parts

        LEFT JOIN collection_title_lookup
            USING (collection)
),

mosaic_properties AS (
    -- STAC properties object for mosaics
    SELECT
        collection,
        item_id,
        jsonb_build_object(
            'title', item_id,
            'created', to_char(md_release.creationdate AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- Field is truncated to date, existing items include H:M:S
            'license', 'CC-BY-4.0',
            'published', to_char(md_release.release_date AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- Field is truncated to date, existing items include H:M:S
            'description', 'Digital surface model mosaic from photogrammetric elevation extraction using the SETSM algorithm.  The mosaic tiles are a composite product using DEM strips from varying collection times.',
            'constellation', 'maxar',
            'datetime', to_char(extras.start_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'start_datetime', to_char(extras.start_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'end_datetime', to_char(extras.end_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),

            -- Projection properties
            'gsd', primary_asset.gsd,
            'proj:code', primary_asset.proj_code,
            'proj:shape', primary_asset.proj_shape,
            'proj:transform', primary_asset.proj_transform,
            'proj:bbox', primary_asset.proj_bbox,
            'proj:geometry', primary_asset.proj_geometry,
            'proj:centroid', jsonb_build_object(
                'lat', round(primary_asset.proj_centroid[0]::NUMERIC, 6),
                'lon', round(primary_asset.proj_centroid[1]::NUMERIC, 6)
            ),

            -- PGC properties
            'pgc:pairname_ids', extras.pairname_ids,
            'pgc:supertile', md_release.supertile,
            'pgc:tile', md_release.tile,
            'pgc:release_version', md_release.release_ver,
            'pgc:data_perc', round(md_release.data_percent::NUMERIC, 6),
            -- md_release.num_components is incorrect for Arcticdem v3.0 10m and 32m collections, so the length of the
            -- pairname_ids array is used instead.
            'pgc:num_components', jsonb_array_length(extras.pairname_ids)
        ) AS content
    FROM canonical_mosaics

        LEFT JOIN dem.mosaic_dem_release AS md_release
            USING (dem_id)

        LEFT JOIN (
            SELECT * FROM dem.stac_raster_asset_info WHERE asset_key = 'dem'
        ) AS primary_asset
            USING (collection, item_id)

        LEFT JOIN dem.stac_mosaic_info AS extras
            USING (collection, item_id)
),

full_res_assets_for_latest_version AS (
    -- Constructs asset objects for REMA v2.0 and ArcticDEM v4.1 mosaic items that are at the same resolution as the
    -- item itself (therefore doesn't include proj properties).
    SELECT
        collection,
        item_id,

        jsonb_build_object(
            'title', 'Hillshade',
            'href', href.partial_https || '_browse.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["overview", "visual"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', json_build_object('href', href.partial_s3 || '_browse.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'uint8'
        ) AS hillshade,

        jsonb_build_object(
            'title', format('%s DEM', href.resolution_str),
            'href', href.partial_https || '_dem.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["data"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_dem.tif')
            ),
            'unit', 'meter',
            'nodata', -9999,
            'data_type', 'float32'
        ) AS dem,

        jsonb_build_object(
            'title', 'Count',
            'href', href.partial_https || '_count.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["metadata", "count"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_count.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'uint8'
        ) AS count,

        jsonb_build_object(
            'title', 'Median Absolute Deviation',
            'href', href.partial_https || '_mad.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["metadata", "mad"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_mad.tif')
            ),
            'unit', 'meter',
            'nodata', -9999,
            'data_type', 'float32'
        ) AS mad,

        jsonb_build_object(
            'title', 'Max date',
            'href', href.partial_https || '_maxdate.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["metadata", "date"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_maxdate.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'int16'
        ) AS maxdate,

        jsonb_build_object(
            'title', 'Min date',
            'href', href.partial_https || '_mindate.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["metadata", "date"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_mindate.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'int16'
        ) AS mindate,

        jsonb_build_object(
            'title', 'Valid data mask',
            'href', href.partial_https || '_datamask.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["metadata", "data-mask"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_datamask.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'uint8'
        ) AS datamask,

        jsonb_build_object(
            'title', 'Metadata',
            'href', href.partial_https || '_meta.txt',
            'type', 'text/plain',
            'roles', '["metadata"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_meta.txt')
            )
        ) AS metadata

    FROM canonical_mosaics

        LEFT JOIN (
            SELECT
                collection,
                item_id,
                resolution_str,
                concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_https,
                concat_ws('/', base_s3_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_s3
            FROM href_parts
        ) AS href
            USING (collection, item_id)
),

downsampled_assets_for_latest_version AS (
    SELECT
        collection,
        item_id,

        jsonb_build_object(
            'title', 'Hillshade',
            'href', href.partial_https || '_browse.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["overview", "visual"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', json_build_object('href', href.partial_s3 || '_browse.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'uint8',
            -- Projection properties
            'gsd', secondary_asset.gsd,
            'proj:code', secondary_asset.proj_code,
            'proj:shape', secondary_asset.proj_shape,
            'proj:transform', secondary_asset.proj_transform,
            'proj:bbox', secondary_asset.proj_bbox,
            'proj:geometry', secondary_asset.proj_geometry,
            'proj:centroid', jsonb_build_object(
                'lat', round(secondary_asset.proj_centroid[0]::NUMERIC, 6),
                'lon', round(secondary_asset.proj_centroid[1]::NUMERIC, 6)
            )
        ) AS hillshade_with_proj

    FROM canonical_mosaics

        LEFT JOIN (
            SELECT *
            FROM dem.stac_raster_asset_info
            WHERE asset_key = 'hillshade'
        ) AS secondary_asset
            USING (collection, item_id)

        LEFT JOIN (
            SELECT
                collection,
                item_id,
                resolution_str,
                concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_https,
                concat_ws('/', base_s3_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_s3
            FROM href_parts
        ) AS href
            USING (collection, item_id)
),

full_res_assets_for_arcticdem_v3_0 AS (
    SELECT
        collection,
        item_id,

        jsonb_build_object(
            'title', format('%s DEM', href.resolution_str),
            'href', href.partial_https || '_reg_dem.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["data"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.partial_s3 || '_reg_dem.tif')
            ),
            'unit', 'meter',
            'nodata', -9999,
            'data_type', 'float32'
        ) AS dem,

        jsonb_build_object(
            'title', 'Metadata',
            'href', href.metadata_https,
            'type', 'text/plain',
            'roles', '["metadata"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', jsonb_build_object('href', href.metadata_s3)
            )
        ) AS metadata

    FROM canonical_mosaics

        LEFT JOIN (
            SELECT
                collection,
                item_id,
                resolution_str,
                concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_https,
                concat_ws('/', base_s3_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_s3,
                -- The metadata file is named by the supertile rather than the tile. E.G. <supertile>_<resolution_str>_<release_version>_dem_meta.txt
                concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile, format('%s_%s_%s_dem_meta.txt', supertile, resolution_str, release_version)) AS metadata_https,
                concat_ws('/', base_s3_url, domain, kind, release_version, resolution_str, supertile, format('%s_%s_%s_dem_meta.txt', supertile, resolution_str, release_version)) AS metadata_s3
            FROM href_parts
        ) AS href
            USING (collection, item_id)

),

downsampled_assets_for_arcticdem_v3_0 AS (
    SELECT
        collection,
        item_id,

        jsonb_build_object(
            'title', 'Browse',
            'href', href.partial_https || '_reg_dem_browse.tif',
            'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
            'roles', '["overview", "visual"]'::jsonb,
            'alternate', jsonb_build_object(
                's3', json_build_object('href', href.partial_s3 || '_reg_dem_browse.tif')
            ),
            -- 'unit' property omitted
            'nodata', 0,
            'data_type', 'uint8',
            -- Projection properties
            'gsd', secondary_asset.gsd,
            'proj:code', secondary_asset.proj_code,
            'proj:shape', secondary_asset.proj_shape,
            'proj:transform', secondary_asset.proj_transform,
            'proj:bbox', secondary_asset.proj_bbox,
            'proj:geometry', secondary_asset.proj_geometry,
            'proj:centroid', jsonb_build_object(
                'lat', round(secondary_asset.proj_centroid[0]::NUMERIC, 6),
                'lon', round(secondary_asset.proj_centroid[1]::NUMERIC, 6)
            )
        ) AS browse_with_proj

    FROM canonical_mosaics

        LEFT JOIN (
            SELECT *
            FROM dem.stac_raster_asset_info
            WHERE asset_key = 'browse'
        ) AS secondary_asset
            USING (collection, item_id)

        LEFT JOIN (
            SELECT
                collection,
                item_id,
                resolution_str,
                concat_ws('/', base_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_https,
                concat_ws('/', base_s3_url, domain, kind, release_version, resolution_str, supertile, item_id) AS partial_s3
            FROM href_parts
        ) AS href
            USING (collection, item_id)
),

mosaic_assets AS (
    SELECT
        collection,
        item_id,
        CASE
            WHEN collection = 'arcticdem-mosaics-v4.1-2m' THEN
                jsonb_build_object(
                    'hillshade', downsampled_latest.hillshade_with_proj,
                    'dem', full_res_latest.dem,
                    'count', full_res_latest.count,
                    'mad', full_res_latest.mad,
                    'maxdate', full_res_latest.maxdate,
                    'mindate', full_res_latest.mindate,
                    'datamask', full_res_latest.datamask,
                    'metadata', full_res_latest.metadata
                )
            WHEN collection IN ('arcticdem-mosaics-v4.1-10m', 'arcticdem-mosaics-v4.1-32m') THEN
                jsonb_build_object(
                    'hillshade', full_res_latest.hillshade,
                    'dem', full_res_latest.dem,
                    'count', full_res_latest.count,
                    'mad', full_res_latest.mad,
                    'maxdate', full_res_latest.maxdate,
                    'mindate', full_res_latest.mindate,
                    'datamask', full_res_latest.datamask,
                    'metadata', full_res_latest.metadata
                )
            WHEN collection = 'rema-mosaics-v2.0-2m' THEN
                jsonb_build_object(
                    'hillshade', downsampled_latest.hillshade_with_proj,
                    'dem', full_res_latest.dem,
                    'count', full_res_latest.count,
                    'mad', full_res_latest.mad,
                    'maxdate', full_res_latest.maxdate,
                    'mindate', full_res_latest.mindate,
                    'metadata', full_res_latest.metadata
                )
            WHEN collection IN ('rema-mosaics-v2.0-10m', 'rema-mosaics-v2.0-32m') THEN
                jsonb_build_object(
                    'hillshade', full_res_latest.hillshade,
                    'dem', full_res_latest.dem,
                    'count', full_res_latest.count,
                    'mad', full_res_latest.mad,
                    'maxdate', full_res_latest.maxdate,
                    'mindate', full_res_latest.mindate,
                    'metadata', full_res_latest.metadata
                )
            WHEN collection = 'arcticdem-mosaics-v3.0-2m' THEN
                jsonb_build_object(
                    'browse', downsampled_v3_0.browse_with_proj,
                    'dem', full_res_v3_0.dem,
                    'metadata', full_res_v3_0.metadata
                )
            WHEN collection IN ('arcticdem-mosaics-v3.0-10m', 'arcticdem-mosaics-v3.0-32m') THEN
                jsonb_build_object(
                    'dem', full_res_v3_0.dem,
                    'metadata', full_res_v3_0.metadata
                )
        END AS content

    FROM canonical_mosaics

        LEFT JOIN full_res_assets_for_latest_version AS full_res_latest
            USING (collection, item_id)

        LEFT JOIN downsampled_assets_for_latest_version AS downsampled_latest
            USING (collection, item_id)

        LEFT JOIN full_res_assets_for_arcticdem_v3_0 AS full_res_v3_0
            USING (collection, item_id)

        LEFT JOIN downsampled_assets_for_arcticdem_v3_0 AS downsampled_v3_0
            USING (collection, item_id)
),

mosaic_items AS (
    SELECT
        collection,
        item_id,
        jsonb_build_object(
            'id', item_id,
            'bbox', st_asgeojson(md_release.wkb_geometry, options := 1)::jsonb->'bbox',
            'type', 'Feature',
            'links', links.content,
            'assets', mosaic_assets.content,
            'geometry', st_asgeojson(md_release.wkb_geometry, maxdecimaldigits := 6)::jsonb,
            'collection', collection,
            'properties', mosaic_properties.content,
            'stac_version', '1.1.0',
            'stac_extensions', json_build_array(
                'https://stac-extensions.github.io/projection/v2.0.0/schema.json',
                'https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json'
            )
        ) AS content
    FROM canonical_mosaics

        LEFT JOIN links
            USING (collection, item_id)

        LEFT JOIN mosaic_properties
            USING (collection, item_id)

        LEFT JOIN mosaic_assets
            USING (collection, item_id)

        LEFT JOIN dem.mosaic_dem_release AS md_release
            USING (dem_id)
)

SELECT * FROM mosaic_items
);

COMMENT ON VIEW dem.stac_mosaic_item IS 'Static STAC items for public mosaic DEMs';

ALTER VIEW dem.stac_mosaic_item OWNER TO pgc_gis_admin;

GRANT SELECT ON dem.stac_mosaic_item TO pgc_users;
