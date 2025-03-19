WITH canonical_strips AS (
    -- Contains one row for each strip item that should be created
    -- NOTE: dem_id is not unique in dem.strip_dem_all. Use a combination of dem_id & stripdemid to
    -- ensure the correct row from dem.strip_dem_all is joined (e.g. USING (dem_id, stripdemid) )
    SELECT
        format('%s-strips-s2s041-2m', sd_release.project) AS collection,
        dem_id AS item_id,
        dem_id,
        stripdemid
    FROM dem.strip_dem_release AS sd_release
),

canonical_mosaics AS (
    -- Contains one row for each mosaic item that should be created
    SELECT
        format('%s-mosaics-v%s-%sm', md_release.project, md_release.release_ver, md_release.gsd) AS collection,
        dem_id AS item_id,
        dem_id
    FROM dem.mosaic_dem_release as md_release
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
        sd_all.geocell AS geocell_or_supertile
    FROM canonical_strips

        LEFT JOIN dem.strip_dem_all AS sd_all
            USING (dem_id, stripdemid)

    UNION ALL

    SELECT
        collection,
        item_id,
        'https://pgc-opendata-dems.s3.us-west-2.amazonaws.com' AS base_url,
        's3://pgc-opendata-dems' AS base_s3_url,
        split_part(collection, '-', 1) AS domain,
        split_part(collection, '-', 2) AS kind,
        split_part(collection, '-', 3) AS release_version,
        split_part(collection, '-', 4) AS resolution_str,
        sm_release.supertile AS geocell_or_supertile
    FROM canonical_mosaics

         LEFT JOIN dem.mosaic_dem_release AS sm_release
               USING (dem_id)
),

links AS (
    -- STAC links object
    SELECT
        collection,
        item_id,
        jsonb_build_array(
            jsonb_build_object(
                'rel', 'self',
                'href', concat_ws('/', base_url, domain, kind, release_version, resolution_str, geocell_or_supertile, item_id) || '.json',
                'type', 'application/geo+json'
            ),
            jsonb_build_object(
                'rel', 'parent',
--                 'title', format('Geocell %s', sda.geocell), -- TODO: Figure out where to generate this
                'href', concat_ws('/', base_url, domain, kind, release_version, resolution_str, geocell_or_supertile) || '.json',
                'type', 'application/json'
            ),
            jsonb_build_object(
                'rel', 'collection',
--                 'title', 'ArcticDEM 2m DEM Strips, version s2s041', -- TODO: Figure out where to generate this
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
),

strip_properties AS (
    -- STAC properties object for strips
    SELECT
        collection,
        item_id,
        jsonb_build_object(
            -- Common properties
            'title', item_id,
            'created', to_char(sd_all.cr_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- Field is truncated to date, existing items include H:M:S
            'license', 'CC-BY-4.0',
            'published', to_char(sd_release.release_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- Field is truncated to date, existing items include H:M:S
            'description', 'Digital surface models from photogrammetric elevation extraction using the SETSM algorithm.  The DEM strips are a time-stamped product suited to time-series analysis.',
            'instruments', jsonb_build_array(sd_all.sensor1, sd_all.sensor2),
            'constellation', 'maxar',
            'datetime', to_char(sd_all.avgacqtm1, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- TODO: Verify avgacqtm1 is always less than avgacqtm2
            'start_datetime', to_char(sd_all.avgacqtm1, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'end_datetime', to_char(sd_all.avgacqtm2, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),

            -- Projection properties
            'gsd', primary_asset.gsd,
            'proj:code', primary_asset.proj_code,
            'proj:shape', primary_asset.proj_shape,
            'proj:transform', primary_asset.proj_transform,
            'proj:bbox', primary_asset.proj_bbox,
            'proj:geometry', primary_asset.proj_geometry,
            'proj:centroid', primary_asset.proj_centroid,

            -- PGC properties
            'pgc:rmse', sd_all.rmse,
            'pgc:is_lsf', sd_all.is_lsf,
            'pgc:geocell', sd_all.geocell,
            'pgc:pairname', sd_all.pairname,
            'pgc:image_ids', json_build_array(sd_all.catalogid2, sd_all.catalogid2),
            'pgc:is_xtrack', sd_all.is_xtrack,
            'pgc:stripdemid', sd_all.stripdemid,
            'pgc:s2s_version', sd_all.s2s_ver,
            'pgc:avg_sun_elevs', json_build_array(sd_all.avg_sunel1, sd_all.avg_sunel2),
            'pgc:setsm_version', sd_all.algm_ver,
            'pgc:cloud_area_sqkm', sd_all.cloud_area,
            'pgc:valid_area_sqkm', sd_all.valid_area,
            'pgc:water_area_sqkm', sd_all.water_area,
            'pgc:cloud_area_percent', sd_all.cloud_perc,
            'pgc:valid_area_percent', sd_all.valid_perc,
            'pgc:water_area_percent', sd_all.water_perc,
            'pgc:avg_convergence_angle', sd_all.avgconvang,
            'pgc:masked_matchtag_density', sd_all.mask_dens,
            'pgc:valid_area_matchtag_density', sd_all.valid_dens,
            'pgc:avg_expected_height_accuracy', sd_all.avg_ht_acc
        ) AS content
    FROM canonical_strips

        LEFT JOIN dem.strip_dem_all AS sd_all
            USING (dem_id, stripdemid)

        LEFT JOIN dem.strip_dem_release AS sd_release
            USING (dem_id)

        LEFT JOIN (
            SELECT * FROM dem.stac_raster_asset_info WHERE asset_key = 'dem'
        ) AS primary_asset
            USING (collection, item_id)
),

mosaic_properties AS (
    -- STAC properties object for mosaics
    SELECT
        collection,
        item_id,
        jsonb_build_object(
            'title', item_id,
            'created', to_char(md_release.creationdate, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- Field is truncated to date, existing items include H:M:S
            'license', 'CC-BY-4.0',
            'published', to_char(md_release.release_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), -- Field is truncated to date, existing items include H:M:S
            'description', 'Digital surface model mosaic from photogrammetric elevation extraction using the SETSM algorithm.  The mosaic tiles are a composite product using DEM strips from varying collection times.',
            'constellation', 'maxar',
            'datetime', null, -- TODO: Make equal to start_datetime
            'start_datetime', null, -- TODO: Bring this into the data model
            'end_datetime', null, -- TODO: Bring this into the data model

            -- Projection properties
            -- TODO: Not all of these are necessary
            'gsd', primary_asset.gsd,
            'proj:code', primary_asset.proj_code,
            'proj:shape', primary_asset.proj_shape,
            'proj:transform', primary_asset.proj_transform,
            'proj:bbox', primary_asset.proj_bbox,
            'proj:geometry', primary_asset.proj_geometry,
            'proj:centroid', primary_asset.proj_centroid,

            -- PGC properties
            'pgc:pairname_ids', extras.pairname_ids,
            'pgc:supertile', md_release.supertile,
            'pgc:tile', md_release.tile,
            'pgc:release_version', md_release.release_ver,
            'pgc:data_perc', md_release.data_percent,
            'pgc:num_components', md_release.num_components
        ) AS content
    FROM canonical_mosaics

        LEFT JOIN dem.mosaic_dem_release AS md_release
            USING (dem_id)

        LEFT JOIN (
            SELECT * FROM dem.stac_raster_asset_info WHERE asset_key = 'dem'
        ) AS primary_asset
            USING (collection, item_id)

        LEFT JOIN dem.stac_mosaic_pairname_ids AS extras
            USING (collection, item_id)
),

strip_assets AS (
    -- STAC properties object for strips
    -- Assumes all strip collections have the same assets
    SELECT
        collection,
        item_id,
        jsonb_build_object(
            'hillshade', jsonb_build_object(
                'title', '10m hillshade',
                'href', href.partial_https || '_dem_10m_shade.tif',
                'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
                'roles', '["overview", "visual"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', json_build_object(
                        'href', href.partial_s3 || '_dem_10m_shade.tif'
                    )
                ),
                -- 'unit' property omitted
                'nodata', 0,
                'data_type', 'unit8',
                -- Projection properties
                'gsd', secondary_asset.gsd,
                'proj:code', secondary_asset.proj_code,
                'proj:shape', secondary_asset.proj_shape,
                'proj:transform', secondary_asset.proj_transform,
                'proj:bbox', secondary_asset.proj_bbox,
                'proj:geometry', secondary_asset.proj_geometry,
                'proj:centroid', secondary_asset.proj_centroid
            ),
            'hillshade_masked', jsonb_build_object(
                'title', 'Masked 10m hillshade',
                'href', href.partial_https || '_dem_10m_shade_masked.tif',
                'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
                'roles', '["overview", "visual"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', json_build_object(
                        'href', href.partial_s3 || '_dem_10m_shade_masked.tif'
                    )
                ),
                -- 'unit' property omitted
                'nodata', 0,
                'data_type', 'unit8',
                -- Projection properties
                'gsd', secondary_asset.gsd, 'proj:code', secondary_asset.proj_code,
                'proj:shape', secondary_asset.proj_shape,
                'proj:transform', secondary_asset.proj_transform,
                'proj:bbox', secondary_asset.proj_bbox,
                'proj:geometry', secondary_asset.proj_geometry,
                'proj:centroid', secondary_asset.proj_centroid
            ),
            'dem', jsonb_build_object(
                'title', '2m DEM',
                'href', href.partial_https || '_dem.tif',
                'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
                'roles', '["data"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', jsonb_build_object(
                        'href', href.partial_s3 || '_dem.tif'
                    )
                ),
                'unit', 'meter',
                'nodata', -9999,
                'data_type', 'float32'
            ),
            'mask', jsonb_build_object(
                'title', 'Valid data mask',
                'href', href.partial_https || '_bitmask.tif',
                'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
                'roles', '["metadata", "data-mask", "land-water", "water-mask", "cloud"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', jsonb_build_object(
                        'href', href.partial_s3 || '_bitmask.tif'
                    )
                ),
                'nodata', 1,
                'data_type', 'uint8'
            ),
            'matchtag', jsonb_build_object(
                'title', 'Match point mask',
                'href', href.partial_https || '_matchtag.tif',
                'type', 'image/tiff; application=geotiff; profile=cloud-optimized',
                'roles', '["metadata", "matchtag"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', jsonb_build_object(
                        'href', href.partial_s3 || '_matchtag.tif'
                    )
                ),
                'nodata', 0,
                'data_type', 'uint8'
            ),
            'metadata', jsonb_build_object(
                'title', 'Metadata',
                'href', href.partial_https || '_mdf.txt',
                'type', 'text/plain',
                'roles', '["metadata"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', jsonb_build_object(
                        'href', href.partial_s3 || '_mdf.txt'
                    )
                )
            ),
            'readme', jsonb_build_object(
                'title', 'Readme',
                'href', href.partial_https || '_readme.txt',
                'type', 'text/plain',
                'roles', '["metadata"]'::jsonb,
                'alternate', jsonb_build_object(
                    's3', jsonb_build_object(
                        'href', href.partial_s3 || '_readme.txt'
                    )
                )
            )
        ) AS content
    FROM canonical_strips

        LEFT JOIN (
            SELECT * FROM dem.stac_raster_asset_info WHERE asset_key = 'dem'
        ) AS primary_asset
            USING (collection, item_id)

        LEFT JOIN (
            SELECT * FROM dem.stac_raster_asset_info WHERE asset_key = 'hillshade'
        ) AS secondary_asset
            USING (collection, item_id)

        LEFT JOIN (
            SELECT
                collection,
                item_id,
                concat_ws('/', base_url, domain, kind, release_version, resolution_str, geocell_or_supertile, item_id) AS partial_https,
                concat_ws('/', base_s3_url, domain, kind, release_version, resolution_str, geocell_or_supertile, item_id) AS partial_s3
            FROM href_parts
        ) AS href
            USING (collection, item_id)
),

strip_items AS (
    SELECT
        collection,
        item_id,
        jsonb_build_object(
            'id', item_id,
            'bbox', st_asgeojson(sd_all.wkb_geometry, options := 1)::jsonb->'bbox',
            'type', 'Feature',
            'links', links.content,
            'assets', strip_assets.content,
            'geometry', st_asgeojson(sd_all.wkb_geometry)::jsonb,
            'collection', collection,
            'properties', strip_properties.content,
            'stac_version', '1.1.0',
            'stac_extensions', json_build_array(
                'https://stac-extensions.github.io/projection/v2.0.0/schema.json',
                'https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json'
            )
        ) AS content
    FROM canonical_strips

        LEFT JOIN dem.strip_dem_all AS sd_all
            USING (dem_id, stripdemid)

        LEFT JOIN links
            USING (collection, item_id)

        LEFT JOIN strip_properties
            USING (collection, item_id)

        LEFT JOIN strip_assets
            USING (collection, item_id)
)

SELECT
    *
FROM strip_items
-- WHERE jsonb_path_exists(content, '$.** ? (@==null)') -- Checks for null values in jsonb
LIMIT 1
;