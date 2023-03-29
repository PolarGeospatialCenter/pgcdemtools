#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import os
import pathlib
import sys

from lib import utils, dem
from lib import VERSION

DOMAIN_TITLES = {
    "arcticdem": "ArcticDEM",
    "earthdem": "EarthDEM",
    "rema": "REMA"
}

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
    parser.add_argument('-v', '--version', action='store_true', default=False, help='print version and exit')

    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)

    if args.version:
        print("Current version: %s", VERSION)
        sys.exit(0)

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
    scenes = []
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

                stac_item = build_mosaic_stac_item(args.stac_base_url, args.domain, raster)

        except RuntimeError as e:
            logger.error( f'{e} while processing {sp}' )
        else:
            j+=1
            utils.progress(j, total, "DEMs identified")

            stac_item_json = json.dumps(stac_item, indent=2, sort_keys=False)
            #logger.debug(stac_item_json)

            if args.stac_base_dir:
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

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [ "https://stac-extensions.github.io/projection/v1.0.0/schema.json" ],
        "id": raster.stripid,
        "bbox": get_geojson_bbox(raster.get_geom_wgs84()),
        "collection": collection_name,
        "properties": {
            "title": raster.stripid,
            "description": "Digital surface models from photogrammetric elevation extraction using the SETSM algorithm.  The DEM strips are a time-stamped product suited to time-series analysis.",
            "created": iso8601(raster.creation_date), # are these actually in UTC, does it matter?
            "published": iso8601(datetime.datetime.utcnow()), # now
            "datetime": iso8601(start_time), # this is only required if start_datetime/end_datetime are not specified
            "start_datetime": iso8601(start_time),
            "end_datetime": iso8601(end_time),
            "instruments": [ raster.sensor1, raster.sensor2 ],
            "constellation": "maxar",
            "gsd": (raster.xres + raster.yres)/2.0, ## dem.res is a string ('2m','50cm'), gsd be a float (2.0).
            "proj:epsg": raster.epsg,
            "pgc:image_ids": [ raster.catid1, raster.catid2 ],
            "pgc:geocell": raster.geocell,
            "pgc:is_xtrack": raster.is_xtrack == True,
            "pgc:is_lsf": raster.is_lsf == True,
            "pgc:setsm_version": raster.algm_version,
            "pgc:s2s_version": raster.s2s_version,
            "pgc:rmse": raster.rmse,
            "pgc:stripdemid": raster.stripdemid,
            "pgc:pairname": raster.pairname,
            "pgc:masked_matchtag_density": raster.masked_density,
            "pgc:valid_area_matchtag_density": raster.valid_density,
            "pgc:cloud_area_percent": raster.cloud_perc,
            "pgc:water_area_percent": raster.water_perc,
            "pgc:valid_area_percent": raster.valid_perc,
            "pgc:cloud_area_sqkm": raster.cloud_area,
            "pgc:water_area_sqkm": raster.water_area,
            "pgc:valid_area_sqkm": raster.valid_area,
            "pgc:avg_convergence_angle": raster.avg_conv_angle,
            "pgc:avg_expected_height_accuracy": raster.avg_exp_height_acc,
            "pgc:avg_sun_elevs": [ raster.avg_sun_el1, raster.avg_sun_el2 ],
            "license": "CC-BY-4.0"
            },
        "links": [
            {
                "rel": "self",
                "href": f"{base_url}/{domain}/strips/{raster.release_version}/{raster.res_str}/{raster.geocell}/{raster.stripid}.json",
                "type": "application/geo+json"
            },
            {
                "rel": "parent",
                "title": f"Geocell {raster.geocell}",
                "href": f"../{raster.geocell}.json",
                "type": "application/json"
            },
            {
                "rel": "collection",
                "title": f"{domain_title} {raster.res_str} DEM Strips, version {raster.release_version}",
                "href": f"{base_url}/{domain}/strips/{raster.release_version}/{raster.res_str}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            }
            ],
        "assets": {
            "hillshade": {
                "title": "10m hillshade",
                "href": "./"+raster.stripid+"_dem_10m_shade.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "overview", "visual" ]

            },
            "hillshade_masked": {
                "title": "Masked 10m hillshade",
                "href": "./"+raster.stripid+"_dem_10m_shade_masked.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "overview", "visual" ],
            },
            "dem": {
                "title": f"{raster.res_str} DEM",
                "href": "./"+raster.stripid+"_dem.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "data" ]
            },
            "mask": {
                "title": "Valid data mask",
                "href": "./"+raster.stripid+"_bitmask.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "data-mask", "land-water", "water-mask", "cloud" ]
            },
            "matchtag": {
                "title": "Match point mask",
                "href": "./"+raster.stripid+"_matchtag.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "matchtag" ]
            },
            "metadata": {
                "title": "Metadata",
                "href": "./"+raster.stripid+"_mdf.txt",
                "type": "text/plain",
                "roles": [ "metadata" ]
            },
            "readme": {
                "title": "Readme",
                "href": "./"+raster.stripid+"_readme.txt",
                "type": "text/plain",
                "roles": [ "metadata" ]
            }
        },
            # Geometries are WGS84 in Lon/Lat order (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#item-fields)
            # Note: may have to introduce points to make the WGS84 reprojection follow the actual locations well enough

            # Geometries should be split at the antimeridian (https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.9)
            "geometry": json.loads(utils.getWrappedGeometry(raster.get_geom_wgs84()).ExportToJson())
        }

    return stac_item


def build_mosaic_stac_item(base_url, domain, tile):
    collection_name = f'{domain}-mosaics-v{tile.release_version}-{tile.res}'
    domain_title = DOMAIN_TITLES[domain]
    gsd = int(tile.res[0:-1]) # strip off trailing 'm'. fails for cm!

    # validate tileid matches metadata - tileid is built off the filename in dem.py
    id_parts = tile.tileid.split('_')
    if tile.res != id_parts[-2]:
        raise RuntimeError(f"Tile ID resolution mismatch: {tile.res} != {id_parts[-2]}")
    if "v"+tile.release_version != id_parts[-1]:
        raise RuntimeError(f"Tile ID version mismatch: v{tile.release_version} != {id_parts[-1]}")

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [ "https://stac-extensions.github.io/projection/v1.0.0/schema.json" ],
        "id": tile.tileid,
        "bbox": get_geojson_bbox(tile.get_geom_wgs84()),
        "collection": collection_name,
        "properties": {
            "title": tile.tileid,
            "description": "Digital surface model mosaic from photogrammetric elevation extraction using the SETSM algorithm.  The mosaic tiles are a composite product using DEM strips from varying collection times.",
            "created": iso8601(tile.creation_date, tile.tileid),
            "published": iso8601(datetime.datetime.utcnow()),
            "datetime": iso8601(tile.acqdate_min), # this is only required if start_datetime/end_datetime are not specified
            "start_datetime": iso8601(tile.acqdate_min),
            "end_datetime": iso8601(tile.acqdate_max),
            "constellation": "maxar",
            "gsd": gsd,
            "proj:epsg": tile.epsg,
            "pgc:pairname_ids": tile.pairname_ids,
            "pgc:supertile": tile.supertile_id_no_res, # use for dir path
            "pgc:tile": tile.tile_id_no_res,
            "pgc:release_version": tile.release_version,
            "pgc:data_perc": tile.density,
            "pgc:num_components": tile.num_components,
            "license": "CC-BY-4.0"
            },
        "links": [
            {
                "rel": "self",
                "href": f"{base_url}/{domain}/mosaics/v{tile.release_version}/{tile.res}/{tile.supertile_id_no_res}/{tile.tileid}.json",
                "type": "application/geo+json"
            },
            {
                "rel": "parent",
                "title": f"Tile Catalog {tile.supertile_id_no_res}",
                "href": f"../{tile.supertile_id_no_res}.json",
                "type": "application/json"
            },
            {
                "rel": "collection",
                "title": f"Resolution Collection {domain_title} {tile.res} DEM Mosaics, version {tile.release_version}",
                "href": f"{base_url}/{domain}/mosaic/{tile.release_version}/{tile.res}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            }
            ],
        "assets": {
            "hillshade": {
                "title": "Hillshade",
                "href": "./"+tile.tileid+"_browse.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "overview", "visual" ]

            },
            "dem": {
                "title": f"{tile.res} DEM",
                "href": "./"+tile.tileid+"_dem.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "data" ]
            },
            "count": {
                "title": "Count",
                "href": "./"+tile.tileid+"_count.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "count" ]
            },
            "count_matchtag": {
                "title": "Count of Match points",
                "href": "./"+tile.tileid+"_countmt.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "matchtag" ]
            },
            "mad": {
                "title": "Median Absolute Deviation",
                "href": "./"+tile.tileid+"_mad.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "mad" ]
            },
            "maxdate": {
                "title": "Max date",
                "href": "./"+tile.tileid+"_maxdate.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "date" ]
            },
            "mindate": {
                "title": "Min date",
                "href": "./"+tile.tileid+"_mindate.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": [ "metadata", "date" ]
            },
            "metadata": {
                "title": "Metadata",
                "href": "./"+tile.tileid+"_meta.txt",
                "type": "text/plain",
                "roles": [ "metadata" ]
            }
        },
            # Geometries are WGS84 in Lon/Lat order (https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#item-fields)
            # Note: may have to introduce points to make the WGS84 reprojection follow the actual locations well enough

            # Geometries should be split at the antimeridian (https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.9)
            "geometry": json.loads(utils.getWrappedGeometry(tile.get_geom_wgs84()).ExportToJson())
        }

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

if __name__ == '__main__':
    main()
