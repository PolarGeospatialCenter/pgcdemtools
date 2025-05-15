#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import os
import pathlib
import sys


from lib import utils, dem, VERSION, SHORT_VERSION, stac

DOMAIN_TITLES = {
    "arcticdem": "ArcticDEM",
    "earthdem": "EarthDEM",
    "rema": "REMA"
}
LATEST_MOSAIC_VERSION = {
    "arcticdem": "4.1",
    "earthdem": "1.1",
    "rema": "2.0",
}
STAC_VERSION = "1.1.0"
STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
    "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
]
S3_BUCKET = "pgc-opendata-dems"

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Build STAC Item GeoJSON files for each strip DEM segment."
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory, text file of file paths, or dem")

    #### Optional Arguments
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing stac item json")

    # Disable: this isn't really useful because pystac wants to validate all the links, many of which are absolute
    #          and won't exist until the whole catalog is generated and published.
    #parser.add_argument('--validate', action='store_true', default=False,
    #                    help="validate stac item json")
    parser.add_argument('--stac-base-dir', help="base directory to write stac JSON files, otherwise write next to images")
    parser.add_argument('--stac-base-url', help="STAC Catalog Base URL", default="https://pgc-opendata-dems.s3.us-west-2.amazonaws.com")
    parser.add_argument('--domain', help="PGC Domain (arcticdem,earthdem,rema)", required=True, choices=DOMAIN_TITLES.keys())
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')

    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)

    if not args.domain in DOMAIN_TITLES:
        parser.error("Domain must be one of: " + ", ".join(DOMAIN_TITLES.keys()))

    #if args.validate:
    #    import pystac

    ## Setup Logging options
    if args.v:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    lsh = logging.StreamHandler()
    lsh.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    logger.info("Current version: %s", VERSION)

    #### ID rasters
    logger.info('Identifying DEMs')
    scene_paths = []
    if os.path.isfile(src) and src.endswith('.tif'):
        logger.debug(src)
        scene_paths.append(src)

    elif os.path.isfile(src) and src.endswith('.txt'):
        fh = open(src,'r')
        for line in fh.readlines():
            sceneid = line.strip()
            scene_paths.append(sceneid)

    elif os.path.isdir(src):
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    scene_paths.append(srcfp)

    else:
        logger.error("src must be a directory")

    scene_paths.sort() # not strictly necessary, but makes logs easier to read
    logger.info('Reading rasters')
    j = 0
    total = len(scene_paths)
    for sp in scene_paths:
        try:
            if pathlib.Path(sp).name.startswith('SETSM_'):
                # assume sp is a SETSM strip
                raster = dem.SetsmDem(sp)
                raster.get_dem_info()

                stac_item = build_strip_stac_item(args.stac_base_url, args.domain, raster)
            else:
                # assume sp is a mosaic tile
                raster = dem.SetsmTile(sp)
                raster.get_dem_info()

                if raster.release_version == "3.0":
                    # special case for ArcticDEM mosaics v3.0
                    stac_item = build_mosaic_v3_stac_item(args.stac_base_url, args.domain, raster)
                else:
                    stac_item = build_mosaic_stac_item(args.stac_base_url, args.domain, raster)

        except RuntimeError as e:
            logger.error( f'{e} while processing {sp}' )
        else:
            j+=1
            utils.progress(j, total, "DEMs identified")

            stac_item_json = json.dumps(stac_item, indent=2, sort_keys=False)
            #logger.debug(stac_item_json)

            if args.stac_base_dir:
                # Assumes that the 'rel': 'self' is the first link in the list
                # Fragile since the order is defined in other functions
                stac_item_geojson_path = stac_item["links"][0]["href"].replace(args.stac_base_url, args.stac_base_dir)
                pathlib.Path(stac_item_geojson_path).parent.mkdir(parents=True, exist_ok=True)
            else:
                stac_item_geojson_path = raster.srcfp.replace("_dem.tif", ".json")

            if not os.path.exists(stac_item_geojson_path) or args.overwrite:
                with open(stac_item_geojson_path, "w") as f:
                    logger.debug('Writing '+stac_item_geojson_path)
                    f.write(stac_item_json)

            # validate stac item
            #if args.validate:
            #    pystac_item = pystac.Item.from_file(stac_item_geojson_path)
            #    for i in pystac_item.validate():
            #        print(i)


def build_strip_stac_item(base_url, domain, raster):
    collection_name = f'{domain}-strips-{raster.release_version}-{raster.res_str}'
    domain_title = DOMAIN_TITLES[domain]
    start_time = min(raster.avg_acqtime1, raster.avg_acqtime2)
    end_time   = max(raster.avg_acqtime1, raster.avg_acqtime2)

    # validate stripid matches metadata - stripid is built off the filename in dem.py
    id_parts = raster.stripid.split('_')
    if raster.res_str != id_parts[6]:
        raise RuntimeError(f"Strip ID resolution mismatch: {raster.res_str} != {id_parts[6]}")
    if raster.release_version != id_parts[1]:
        raise RuntimeError(f"Strip ID version mismatch: v{raster.release_version} != {id_parts[1]}")

    # Get info for each raster asset
    hillshade_info = stac.RasterAssetInfo.from_raster(raster.browse)
    hillshade_masked_info = stac.RasterAssetInfo.from_raster(raster.browse_masked)
    dem_info = stac.RasterAssetInfo.from_raster(raster.dem)
    mask_info = stac.RasterAssetInfo.from_raster(raster.bitmask)
    matchtag_info = stac.RasterAssetInfo.from_raster(raster.matchtag)

    href_builder = stac.StacHrefBuilder(
        base_url=base_url, s3_bucket=S3_BUCKET, domain=domain, raster=raster
    )

    stac_item = {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": STAC_EXTENSIONS,
        "id": raster.stripid,
        "bbox": get_geojson_bbox(raster.get_geom_wgs84()),
        "collection": collection_name,
        "properties": {
            # Common properties
            "title": raster.stripid,
            "description": "Digital surface models from photogrammetric elevation extraction using the SETSM algorithm.  The DEM strips are a time-stamped product suited to time-series analysis.",
            "created": iso8601(raster.creation_date.date(), f"{raster.stripid} creation_date"),  # Sandwich equivalent is stored as date, ensure created_date is actually a date.
            "published": iso8601(datetime.datetime.now(datetime.UTC)), # now
            "datetime": iso8601(start_time), # this is only required if start_datetime/end_datetime are not specified
            "start_datetime": iso8601(start_time, f"{raster.stripid} start_time"),
            "end_datetime": iso8601(end_time, f"{raster.stripid} end_time"),
            "instruments": [ raster.sensor1, raster.sensor2 ],
            "constellation": "maxar",
            "license": "CC-BY-4.0",
            # Proj properties
            "gsd": dem_info.gsd,
            "proj:code": dem_info.proj_code,
            "proj:shape": dem_info.proj_shape,
            "proj:transform": dem_info.proj_transform,
            "proj:bbox": dem_info.proj_bbox,
            "proj:geometry": dem_info.proj_geojson,
            "proj:centroid": dem_info.proj_centroid,
            # PGC Properties
            "pgc:image_ids": [ raster.catid1, raster.catid2 ],
            "pgc:geocell": raster.geocell,
            "pgc:is_xtrack": raster.is_xtrack == True,
            "pgc:is_lsf": raster.is_lsf == True,
            "pgc:setsm_version": raster.algm_version,
            "pgc:s2s_version": raster.s2s_version,
            "pgc:rmse": round(raster.rmse, 6),
            "pgc:stripdemid": raster.stripdemid,
            "pgc:pairname": raster.pairname,
            "pgc:masked_matchtag_density": round(raster.masked_density, 6),
            "pgc:valid_area_matchtag_density": round(raster.valid_density, 6),
            "pgc:cloud_area_percent": round(raster.cloud_perc, 6),
            "pgc:water_area_percent": round(raster.water_perc, 6),
            "pgc:valid_area_percent": round(raster.valid_perc, 6),
            "pgc:cloud_area_sqkm": round(raster.cloud_area, 6),
            "pgc:water_area_sqkm": round(raster.water_area, 6),
            "pgc:valid_area_sqkm": round(raster.valid_area, 6),
            "pgc:avg_convergence_angle": round(raster.avg_conv_angle, 6),
            "pgc:avg_expected_height_accuracy": round(raster.avg_exp_height_acc, 6),
            "pgc:avg_sun_elevs": [
                round(raster.avg_sun_el1, 6),
                round(raster.avg_sun_el2, 6)
            ],
        },
        "links": [
            {
                "rel": "self",
                "href": href_builder.item_href(),
                "type": "application/geo+json"
            },
            {
                "rel": "parent",
                "title": f"Geocell {raster.geocell}",
                "href": href_builder.catalog_href(),
                "type": "application/json"
            },
            {
                "rel": "collection",
                "title": f"{domain_title} {raster.res_str} DEM Strips, version {raster.release_version}",
                "href": href_builder.collection_href(),
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": href_builder.root_href(),
                "type": "application/json"
            }
        ],
        "assets": {
            "hillshade": {
                "title": "10m hillshade",
                "href": href_builder.asset_href(raster.browse),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "overview", "visual" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.browse, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": hillshade_info.nodata,
                "data_type": hillshade_info.data_type,
                # Since this asset is at a different resolution than the primary item,
                # include PROJ properties to supersede the item-level ones
                "gsd": hillshade_info.gsd,
                "proj:code": hillshade_info.proj_code,
                "proj:shape": hillshade_info.proj_shape,
                "proj:transform": hillshade_info.proj_transform,
                "proj:bbox": hillshade_info.proj_bbox,
                "proj:geometry": hillshade_info.proj_geojson,
                "proj:centroid": hillshade_info.proj_centroid,
            },
            "hillshade_masked": {
                "title": "Masked 10m hillshade",
                "href": href_builder.asset_href(raster.browse_masked),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "overview", "visual" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.browse_masked, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": hillshade_masked_info.nodata,
                "data_type": hillshade_masked_info.data_type,
                # Since this asset is at a different resolution than the primary item,
                # include PROJ properties to supersede the item-level ones
                "gsd": hillshade_masked_info.gsd,
                "proj:code": hillshade_masked_info.proj_code,
                "proj:shape": hillshade_masked_info.proj_shape,
                "proj:transform": hillshade_masked_info.proj_transform,
                "proj:bbox": hillshade_masked_info.proj_bbox,
                "proj:geometry": hillshade_masked_info.proj_geojson,
                "proj:centroid": hillshade_masked_info.proj_centroid,
            },
            "dem": {
                "title": f"{raster.res_str} DEM",
                "href": href_builder.asset_href(raster.dem),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "data" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.dem, as_s3=True),
                    }
                },
                "unit": "meter",
                "nodata": dem_info.nodata,
                "data_type": dem_info.data_type,
            },
            "mask": {
                "title": "Valid data mask",
                "href": href_builder.asset_href(raster.bitmask),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "data-mask", "land-water", "water-mask", "cloud" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.bitmask, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": mask_info.nodata,
                "data_type": mask_info.data_type,
            },
            "matchtag": {
                "title": "Match point mask",
                "href": href_builder.asset_href(raster.matchtag),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "matchtag" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.matchtag, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": matchtag_info.nodata,
                "data_type": matchtag_info.data_type,
            },
            "metadata": {
                "title": "Metadata",
                "href": href_builder.asset_href(raster.mdf),
                "type": "text/plain",
                "roles": [ "metadata" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.mdf, as_s3=True),
                    }
                },
            },
            "readme": {
                "title": "Readme",
                "href": href_builder.asset_href(raster.readme),
                "type": "text/plain",
                "roles": [ "metadata" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(raster.readme, as_s3=True),
                    }
                },
            }
        },
        # Geometries are WGS84 in Lon/Lat order (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#item-fields)
        # Note: may have to introduce points to make the WGS84 reprojection follow the actual locations well enough

        # Geometries should be split at the antimeridian (https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.9)
        "geometry": json.loads(
            utils.getWrappedGeometry(raster.get_geom_wgs84()).ExportToJson(),
            parse_float=round_coordinates,
        )
    }

    return stac_item


def build_mosaic_stac_item(base_url, domain, tile):
    tile.release_version = LATEST_MOSAIC_VERSION[domain] # TODO: HACK for broken metadata.
    collection_name = f'{domain}-mosaics-v{tile.release_version}-{tile.res}'
    domain_title = DOMAIN_TITLES[domain]

    # validate tileid matches metadata - tileid is built off the filename in dem.py
    id_parts = tile.tileid.split('_')
    if tile.res != id_parts[-2]:
        raise RuntimeError(f"Tile ID resolution mismatch: {tile.res} != {id_parts[-2]}")
    if "v"+tile.release_version != id_parts[-1]:
        raise RuntimeError(f"Tile ID version mismatch: v{tile.release_version} != {id_parts[-1]}")

    # Get info for each raster asset
    hillshade_info = stac.RasterAssetInfo.from_raster(tile.browse)
    dem_info = stac.RasterAssetInfo.from_raster(tile.srcfp)
    count_info = stac.RasterAssetInfo.from_raster(tile.count)
    mad_info = stac.RasterAssetInfo.from_raster(tile.mad)
    maxdate_info = stac.RasterAssetInfo.from_raster(tile.maxdate)
    mindate_info = stac.RasterAssetInfo.from_raster(tile.mindate)

    href_builder = stac.StacHrefBuilder(
        base_url=base_url, s3_bucket=S3_BUCKET, domain=domain, raster=tile
    )

    # Attempt to force density to always be calculated from the source
    tile.density = dem.get_raster_density(tile.count, tile.geom.Area())

    stac_item = {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": STAC_EXTENSIONS,
        "id": tile.tileid,
        "bbox": get_geojson_bbox(tile.get_geom_wgs84()),
        "collection": collection_name,
        "properties": {
            # Common properties
            "title": tile.tileid,
            "description": "Digital surface model mosaic from photogrammetric elevation extraction using the SETSM algorithm.  The mosaic tiles are a composite product using DEM strips from varying collection times.",
            "created": iso8601(tile.creation_date.date(), tile.tileid),
            "published": iso8601(datetime.datetime.now(datetime.UTC)),
            "datetime": iso8601(tile.acqdate_min), # this is only required if start_datetime/end_datetime are not specified
            "start_datetime": iso8601(tile.acqdate_min, tile.tileid),
            "end_datetime": iso8601(tile.acqdate_max, tile.tileid),
            "constellation": "maxar",
            "license": "CC-BY-4.0",
            # PROJ properties
            "gsd": dem_info.gsd,
            "proj:code": dem_info.proj_code,
            "proj:shape": dem_info.proj_shape,
            "proj:transform": dem_info.proj_transform,
            "proj:bbox": dem_info.proj_bbox,
            "proj:geometry": dem_info.proj_geojson,
            "proj:centroid": dem_info.proj_centroid,
            # PGC properties
            "pgc:pairname_ids": tile.pairname_ids,
            "pgc:supertile": tile.supertile_id_no_res,  # use for dir path
            "pgc:tile": tile.tile_id_no_res,
            "pgc:release_version": tile.release_version,
            "pgc:data_perc": round(tile.density, 6),
            "pgc:num_components": tile.num_components,
        },
        "links": [
            {
                "rel": "self", # the main function relies on this being the first item in the list
                "href": href_builder.item_href(),
                "type": "application/geo+json"
            },
            {
                "rel": "parent",
                "title": f"Tile Catalog {tile.supertile_id_no_res}",
                "href": href_builder.catalog_href(),
                "type": "application/json"
            },
            {
                "rel": "collection",
                "title": f"Resolution Collection {domain_title} {tile.res} DEM Mosaics, version {tile.release_version}",
                "href": href_builder.collection_href(),
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": href_builder.root_href(),
                "type": "application/json"
            }
        ],
        "assets": {
            "hillshade": {
                "title": "Hillshade",
                "href": href_builder.asset_href(tile.browse),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "overview", "visual" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.browse, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": hillshade_info.nodata,
                "data_type": hillshade_info.data_type,
                # This asset will be at a different resolution than the primary item for
                # 2m mosaics. For other mosaic resolutions, these properties are removed
                # prior to returning the stac item.
                "gsd": hillshade_info.gsd,
                "proj:code": hillshade_info.proj_code,
                "proj:shape": hillshade_info.proj_shape,
                "proj:transform": hillshade_info.proj_transform,
                "proj:bbox": hillshade_info.proj_bbox,
                "proj:geometry": hillshade_info.proj_geojson,
                "proj:centroid": hillshade_info.proj_centroid,
            },
            "dem": {
                "title": f"{tile.res} DEM",
                "href": href_builder.asset_href(tile.srcfp),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "data" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.srcfp, as_s3=True),
                    }
                },
                "unit": "meter",
                "nodata": dem_info.nodata,
                "data_type": dem_info.data_type,
            },
            "count": {
                "title": "Count",
                "href": href_builder.asset_href(tile.count),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "count" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.count, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": count_info.nodata,
                "data_type": count_info.data_type,
            },
            "mad": {
                "title": "Median Absolute Deviation",
                "href": href_builder.asset_href(tile.mad),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "mad" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.mad, as_s3=True),
                    }
                },
                "unit": "meter",
                "nodata": mad_info.nodata,
                "data_type": mad_info.data_type,
            },
            "maxdate": {
                "title": "Max date",
                "href": href_builder.asset_href(tile.maxdate),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "date" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.maxdate, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": maxdate_info.nodata,
                "data_type": maxdate_info.data_type,
            },
            "mindate": {
                "title": "Min date",
                "href": href_builder.asset_href(tile.mindate),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "date" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.mindate, as_s3=True),
                    }
                },
                # "unit" property is meaningless here, so it is omitted
                "nodata": mindate_info.nodata,
                "data_type": mindate_info.data_type,
            },
            "metadata": {
                "title": "Metadata",
                "href": href_builder.asset_href(tile.metapath),
                "type": "text/plain",
                "roles": [ "metadata" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.metapath, as_s3=True),
                    }
                },
            }
        },
        # Geometries are WGS84 in Lon/Lat order (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#item-fields)
        # Note: may have to introduce points to make the WGS84 reprojection follow the actual locations well enough

        # Geometries should be split at the antimeridian (https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.9)
        "geometry": json.loads(
            utils.getWrappedGeometry(tile.get_geom_wgs84()).ExportToJson(),
            parse_float=round_coordinates,
        )
    }

    if domain in ("arcticdem", "earthdem"):
        # REMA mosaics don't have a datamask asset
        datamask_info = stac.RasterAssetInfo.from_raster(tile.datamask)
        datamask_asset = {
            "title": "Valid data mask",
            "href": href_builder.asset_href(tile.datamask),
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": ["metadata", "data-mask"],
            "alternate": {
                "s3": {
                    "href": href_builder.asset_href(tile.datamask, as_s3=True),
                }
            },
            # "unit" property is meaningless here, so it is omitted
            "nodata": datamask_info.nodata,
            "data_type": datamask_info.data_type,
        }
        stac_item["assets"]["datamask"] = datamask_asset

    if dem_info.gsd != 2:
        # For resolutions other than 2m, the hillshade will be the same resolution as the
        # dem, so these keys are not needed on the asset
        proj_keys = {"gsd", "proj:code", "proj:shape", "proj:transform", "proj:bbox",
                     "proj:geometry", "proj:centroid"}
        for key in proj_keys:
            del stac_item["assets"]["hillshade"][key]

    return stac_item


# For ArcticDEM v3 mosaics
def build_mosaic_v3_stac_item(base_url, domain, tile):
    collection_name = f'{domain}-mosaics-v{tile.release_version}-{tile.res}'
    domain_title = DOMAIN_TITLES[domain]

    # validate tileid matches metadata - tileid is built off the filename in dem.py
    id_parts = tile.tileid.split('_')
    if tile.res != id_parts[-2]:
        raise RuntimeError(f"Tile ID resolution mismatch: {tile.res} != {id_parts[-2]}")
    if "v"+tile.release_version != id_parts[-1]:
        raise RuntimeError(f"Tile ID version mismatch: v{tile.release_version} != {id_parts[-1]}")

    # Get info for each raster asset, except "browse" which is done conditionally later
    dem_info = stac.RasterAssetInfo.from_raster(tile.srcfp)

    href_builder = stac.StacHrefBuilder(
        base_url=base_url, s3_bucket=S3_BUCKET, domain=domain, raster=tile
    )

    # Attempt to force density to always be calculated from the source
    tile.density = dem.get_raster_density(tile.srcfp, tile.geom.Area())

    stac_item = {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": STAC_EXTENSIONS,
        "id": tile.tileid,
        "bbox": get_geojson_bbox(tile.get_geom_wgs84()),
        "collection": collection_name,
        "properties": {
            # Common properties
            "title": tile.tileid,
            "description": "Digital surface model mosaic from photogrammetric elevation extraction using the SETSM algorithm.  The mosaic tiles are a composite product using DEM strips from varying collection times.",
            "created": iso8601(tile.creation_date.date(), f"{tile.tileid} creation_date"),
            "published": iso8601(datetime.datetime.now(datetime.UTC)),
            "datetime": iso8601(tile.acqdate_min), # this is only required if start_datetime/end_datetime are not specified
            "start_datetime": iso8601(tile.acqdate_min, f"{tile.tileid} acqdate_min"),
            "end_datetime": iso8601(tile.acqdate_max, f"{tile.tileid} acqdate_max"),
            "constellation": "maxar",
            "license": "CC-BY-4.0",
            # PROJ properties
            "gsd": dem_info.gsd,
            "proj:code": dem_info.proj_code,
            "proj:shape": dem_info.proj_shape,
            "proj:transform": dem_info.proj_transform,
            "proj:bbox": dem_info.proj_bbox,
            "proj:geometry": dem_info.proj_geojson,
            "proj:centroid": dem_info.proj_centroid,
            # PGC properties
            "pgc:pairname_ids": tile.pairname_ids,
            "pgc:supertile": tile.supertile_id_no_res, # use for dir path
            "pgc:tile": tile.tile_id_no_res,
            "pgc:release_version": tile.release_version,
            "pgc:data_perc": tile.density,
            "pgc:num_components": tile.num_components,
        },
        "links": [
            {
                "rel": "self", # the main function relies on this being the first item in the list
                "href": href_builder.item_href(),
                "type": "application/geo+json"
            },
            {
                "rel": "parent",
                "title": f"Tile Catalog {tile.supertile_id_no_res}",
                "href": href_builder.catalog_href(),
                "type": "application/json"
            },
            {
                "rel": "collection",
                "title": f"Resolution Collection {domain_title} {tile.res} DEM Mosaics, version {tile.release_version}",
                "href": href_builder.collection_href(),
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": href_builder.root_href(),
                "type": "application/json"
            }
        ],
        "assets": {
            "dem": {
                "title": f"{tile.res} DEM",
                "href": href_builder.asset_href(tile.srcfp),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "data" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.srcfp, as_s3=True),
                    }
                },
                "unit": "meter",
                "nodata": dem_info.nodata,
                "data_type": dem_info.data_type,
            },
            "metadata": {
                "title": "Metadata",
                "href": href_builder.asset_href(tile.metapath),
                "type": "text/plain",
                "roles": [ "metadata" ],
                "alternate": {
                    "s3": {
                        "href": href_builder.asset_href(tile.metapath, as_s3=True),
                    }
                },
            }
        },
        # Geometries are WGS84 in Lon/Lat order (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#item-fields)
        # Note: may have to introduce points to make the WGS84 reprojection follow the actual locations well enough

        # Geometries should be split at the antimeridian (https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.9)
        "geometry": json.loads(
            utils.getWrappedGeometry(tile.get_geom_wgs84()).ExportToJson(),
            parse_float=round_coordinates,
        )
    }

    # _reg_dem_browse.tif only exists for 2m, only construct browse asset for that res
    if tile.res == "2m":
        browse_info = stac.RasterAssetInfo.from_raster(tile.browse)
        browse_asset = {
            "title": "Browse",
            "href": href_builder.asset_href(tile.browse),
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": [ "overview", "visual" ],
            "alternate": {
                "s3": {
                    "href": href_builder.asset_href(tile.browse, as_s3=True),
                }
            },
            # "unit" property is meaningless here, so it is omitted
            "nodata": browse_info.nodata,
            "data_type": browse_info.data_type,
            "gsd": browse_info.gsd,
            "proj:code": browse_info.proj_code,
            "proj:shape": browse_info.proj_shape,
            "proj:transform": browse_info.proj_transform,
            "proj:bbox": browse_info.proj_bbox,
            "proj:geometry": browse_info.proj_geojson,
            "proj:centroid": browse_info.proj_centroid,
        }
        stac_item["assets"]["browse"] = browse_asset

    return stac_item


def get_geojson_bbox(src_geom):
    # Note: bbox of geometries that cross the antimeridian are represented by minx > maxx
    # https://datatracker.ietf.org/doc/html/rfc7946#section-5.2

    ring = src_geom.GetGeometryRef(0) ### assumes a 1 part polygon
    minx = miny = 9999
    maxx = maxy = -9999
    crosses_180 = False
    for i in range(0, ring.GetPointCount()-1):
        pt1 = ring.GetPoint(i)
        pt2 = ring.GetPoint(i+1)

        minx = min([ pt1[0], pt2[0], minx ])
        maxx = max([ pt1[0], pt2[0], maxx ])
        miny = min([ pt1[1], pt2[1], miny ])
        maxy = max([ pt1[1], pt2[1], maxy ])

        if (pt1[0] > 0) - (pt1[0] < 0) != (pt2[0] > 0) - (pt2[0] < 0):
            crosses_180 = True

    if crosses_180:
        return [ maxx, miny, minx, maxy ]

    return [ minx, miny, maxx, maxy ]


# Converts date to ISO-8601 format.  Returns None on None vs throwing.
def iso8601(date_time, msg=""):
    if date_time:
        return date_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.error(f"null date: {msg}")
    return None


def round_coordinates(s: str) -> float:
    return round(float(s), 6)


if __name__ == '__main__':
    main()
