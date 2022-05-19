#!/usr/bin/env python3

import argparse
import datetime
import json
import os, sys, string, shutil, glob, re, logging, tarfile, zipfile
import pathlib

from osgeo import gdal, osr, ogr, gdalconst

from lib import utils, dem, taskhandler

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
    parser.add_argument('--validate', action='store_true', default=False,
                        help="validate stac item json")
    parser.add_argument('--stac-base-dir', help="base directory to write stac JSON files, otherwise write next to images")
    parser.add_argument('--stac-base-url', help="STAC Catalog Base URL", default="https://pgc-opendata-dems.s3.us-west-2.amazonaws.com")
    parser.add_argument('--domain', help="PGC Domain (arcticdem,earthdem,rema)")
    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)

    if not args.domain in DOMAIN_TITLES:
        parser.error("Domain must be one of: " + ", ".join(DOMAIN_TITLES.keys()))
        
    if args.validate:
        import pystac
        
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
        logger.error("src must be a directory, a strip dem, or a text file")

    logger.info('Reading rasters')
    j = 0
    total = len(scene_paths)
    scenes = []
    for sp in scene_paths:
        try:
            raster = dem.SetsmDem(sp)
            raster.get_dem_info()
        except RuntimeError as e:
            logger.error( e )
        else:
            j+=1
            utils.progress(j, total, "DEMs identified")

            stac_item = build_stac_item(args.stac_base_url, args.domain, raster)
            stac_item_json = json.dumps(stac_item, indent=2, sort_keys=False)
            #logger.debug(stac_item_json)

            if args.stac_base_dir:
                stac_item_geojson_path = stac_item["links"][0]["href"].replace(args.stac_base_url, args.stac_base_dir)
                pathlib.Path(stac_item_geojson_path).parent.mkdir(parents=True, exist_ok=True)
            else:    
                stac_item_geojson_path = raster.srcfp.replace("_dem.tif", ".json")
                
            if not os.path.exists(stac_item_geojson_path) or args.overwrite:
                with open(stac_item_geojson_path, "w") as f:
                    logger.info('Writing '+stac_item_geojson_path)
                    f.write(stac_item_json)
            
            # validate stac item
            if args.validate:
                pystac_item = pystac.Item.from_file(stac_item_geojson_path)
                for i in pystac_item.validate():
                    print(i)


def build_stac_item(base_url, domain, raster):
    collection_name = f'{domain}-strips-{raster.release_version}-{raster.res_str}'
    domain_title = DOMAIN_TITLES[domain]

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [ "https://stac-extensions.github.io/projection/v1.0.0/schema.json" ],
        "id": raster.stripid,
        "bbox": get_geojson_bbox(raster.get_geom_wgs84()),
        "collection": collection_name,
        "properties": {
            "title": raster.stripid,
            "description": "Imagine you are a bowl of petunias falling from a great height.  This tells you how far you can fall before saying 'hello ground'.",
            "created": raster.creation_date.strftime("%Y-%m-%dT%H:%M:%SZ"), # are these actually in UTC, does it matter?
            "published": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), # now?
            "datetime": raster.avg_acqtime1.strftime("%Y-%m-%dT%H:%M:%SZ"), # this is only needed if start_datetime/end_datetime are not specified
            "start_datetime": raster.avg_acqtime1.strftime("%Y-%m-%dT%H:%M:%SZ"), # TODO may need to min/max these to know right order
            "end_datetime": raster.avg_acqtime2.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "instruments": [ raster.sensor1, raster.sensor2 ],
            "constellation": "maxar",
            "gsd": (raster.xres + raster.yres)/2.0,
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
                "title": f"{domain_title} 2m DEM Strips, version {raster.release_version}",
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
                "title": "2m DEM",
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
                "href": "./"+raster.stripid+".mdf.txt",
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
            # Note: need to import doesCrosses180 from setsm-submit-bw/pgc-standalone-overlaps-selector
            "geometry": json.loads(wrap_180(raster.get_geom_wgs84()).ExportToJson())
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


def wrap_180(src_geom):

    ## create 2 point lists for west component and east component
    west_points = []
    east_points = []

    ## for each point in geom except final point
    ring  = src_geom.GetGeometryRef(0)  #### assumes a 1 part polygon
    for i in range(0, ring.GetPointCount()-1):
        pt1 = ring.GetPoint(i)
        pt2 = ring.GetPoint(i+1)

        ## test if point is > or < 0 and add to correct bin
        if pt1[0] < 0:
            west_points.append(pt1)
        else:
            east_points.append(pt1)

        ## test if segment to next point crosses 180 (x is opposite sign)
        if (pt1[0] > 0) - (pt1[0] < 0) != (pt2[0] > 0) - (pt2[0] < 0):

            ## if segment crosses,calculate interesection point y value
            pt3_y = calc_y_intersection_180(pt1, pt2)

            ## add intersection point to both bins (make sureot change 180 to -180 for western list)
            pt3_west = ( -180, pt3_y )
            pt3_east = ( 180, pt3_y )

            west_points.append(pt3_west)
            east_points.append(pt3_east)


    #print "west", len(west_points)
    #for pt in west_points:
    #    print pt[0], pt[1]
    #
    #print "east", len(east_points)
    #for pt in east_points:
    #    print pt[0], pt[1]

    ## cat point lists to make multipolygon(remember to add 1st point to the end)
    geom_multipoly = ogr.Geometry(ogr.wkbMultiPolygon)

    for ring_points in west_points, east_points:
        if len(ring_points) > 0:
            poly = ogr.Geometry(ogr.wkbPolygon)
            ring = ogr.Geometry(ogr.wkbLinearRing)

            for pt in ring_points:
                ring.AddPoint(pt[0],pt[1])

            ring.AddPoint(ring_points[0][0],ring_points[0][1])

            poly.AddGeometry(ring)
            geom_multipoly.AddGeometry(poly)
            del poly
            del ring

    #print geom_multipoly
    return geom_multipoly


def calc_y_intersection_180(pt1, pt2):

    # add 360 to all x coords < 0
    if pt1[0] < 0:
        pt1_x = pt1[0] + 360
    else:
        pt1_x = pt1[0]

    if pt2[0] < 0:
        pt2_x = pt2[0] + 360
    else:
        pt2_x = pt2[0]

    rise = pt2[1] - pt1[1]
    run = pt2_x - pt1_x
    run_prime = 180.0 - pt1_x
    pt3_y = ((run_prime * rise) / run) + pt1[1]

    return pt3_y


if __name__ == '__main__':
    main()
