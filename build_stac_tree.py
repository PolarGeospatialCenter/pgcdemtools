#!/usr/bin/env python3

import argparse
import datetime
import json
import os, sys, string, shutil, glob, re, logging, tarfile, zipfile
import pathlib

from osgeo import gdal, osr, ogr, gdalconst

from lib import utils, dem, taskhandler

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Build STAC Catalog & Collection JSON tree."
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory")

    #### Optional Arguments
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing stac item json")
    parser.add_argument('--validate', action='store_true', default=False,
                        help="validate stac item json")
    parser.add_argument('--stac-base-url', help="STAC Catalog Base URL", default="https://pgc-opendata-dems.s3.us-west-2.amazonaws.com")
    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)

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
    logger.info('Identifying STAC Items')
    item_paths = []

    if os.path.isdir(src):
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith(".json") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    item_paths.append(pathlib.Path(srcfp))

    else:
        logger.error("src must be a directory")

    src_path = pathlib.Path(src)
    
    logger.info('Reading STAC Items')
    j = 0
    total = len(item_paths)

    # Collect ID Tree
    pgc_catalog = { }
  
    for ip in item_paths:
        j+=1
        utils.progress(j, total, "STAC Items identified")
        with open(ip) as f:
            stac_item = json.load(f)
            stac_item_id = stac_item["id"]
            ids = stac_item["collection"].split("-")

        stac_geocell_catalog_dir = ip.parent
        stac_geocell_catalog_id = stac_item["collection"] + "-" + stac_item["properties"]["pgc:geocell"]
        
        stac_resolution_collection_dir = stac_geocell_catalog_dir.parent
        stac_resolution_collection_id = "-".join(ids)
        
        stac_version_collection_dir = stac_resolution_collection_dir.parent
        stac_version_collection_id = "-".join(ids[0:-1])
        
        stac_kind_collection_dir = stac_version_collection_dir.parent
        stac_kind_collection_id = "-".join(ids[0:-2])
        
        stac_domain_collection_dir = stac_kind_collection_dir.parent
        stac_domain_collection_id = "-".join(ids[0:-3])
        
        stac_pgc_catalog_dir = stac_domain_collection_dir.parent

        if stac_pgc_catalog_dir != src_path:
            logger.error("Directory tree not as expected")
            sys.exit(1)


        if not stac_domain_collection_id in pgc_catalog:
            pgc_catalog[stac_domain_collection_id] = {}

        if not stac_kind_collection_id in pgc_catalog[stac_domain_collection_id]:
            pgc_catalog[stac_domain_collection_id][stac_kind_collection_id] = {}

        if not stac_version_collection_id in pgc_catalog[stac_domain_collection_id][stac_kind_collection_id]:
            pgc_catalog[stac_domain_collection_id][stac_kind_collection_id][stac_version_collection_id] = {}

        if not stac_resolution_collection_id in pgc_catalog[stac_domain_collection_id][stac_kind_collection_id][stac_version_collection_id]:
            pgc_catalog[stac_domain_collection_id][stac_kind_collection_id][stac_version_collection_id][stac_resolution_collection_id] = {}

        if not stac_geocell_catalog_id in pgc_catalog[stac_domain_collection_id][stac_kind_collection_id][stac_version_collection_id][stac_resolution_collection_id]:
            pgc_catalog[stac_domain_collection_id][stac_kind_collection_id][stac_version_collection_id][stac_resolution_collection_id][stac_geocell_catalog_id] = {}


        pgc_catalog[stac_domain_collection_id][stac_kind_collection_id][stac_version_collection_id][stac_resolution_collection_id][stac_geocell_catalog_id][stac_item_id] = stac_item
        
        logger.info(stac_domain_collection_dir)

    #print(json.dumps(pgc_catalog))

    # Collate/roll-up PGC catalog levels and create STAC JSON items
    stac_catalog = stac_pgc_catalog(args.stac_base_url)
    stac_catalog_path = src + "/pgc-data-stac.json"
    for domain_id, kinds in pgc_catalog.items():
        domain_collection = stac_domain_collection(args.stac_base_url, domain_id)
        domain_collection_path = src + f"/{domain_id}.json"

        domain_bbox = [ 180, 90, -180, -90 ]
        domain_mindate = "9999-01-01T00:00:00Z"

        for kind_id, versions in kinds.items():
            kind_collection = stac_kind_collection(args.stac_base_url, domain_collection, kind_id)
            kind_collection_path = src + f"/{kind_id.replace('-','/')}.json"

            kind_bbox = [ 180, 90, -180, -90 ]
            kind_mindate = "9999-01-01T00:00:00Z"

            for version_id, resolutions in versions.items():
                version_collection = stac_version_collection(args.stac_base_url, kind_collection, version_id)
                version_collection_path = src + f"/{version_id.replace('-','/')}.json"

                version_bbox = [ 180, 90, -180, -90 ]
                version_mindate = "9999-01-01T00:00:00Z"

                for resolution_id, geocells in resolutions.items():
                    resolution_collection = stac_resolution_collection(args.stac_base_url, version_collection, resolution_id)
                    resolution_collection_path = src + f"/{resolution_id.replace('-','/')}.json"

                    resolution_bbox = [ 180, 90, -180, -90 ]
                    resolution_mindate = "9999-01-01T00:00:00Z"

                    for geocell_id, items in geocells.items():
                        geocell_catalog = stac_geocell_catalog(args.stac_base_url, resolution_collection, geocell_id)
                        geocell_catalog_path = src + f"/{geocell_id.replace('-','/')}.json"

                        for item_id, item in items.items():
                            stac_add_child(args.stac_base_url, geocell_catalog, item)
                            resolution_bbox = merge_bbox(resolution_bbox, item["bbox"])
                            resolution_mindate = min(resolution_mindate, item["properties"]["datetime"])
                            
                        write_json(geocell_catalog, geocell_catalog_path, args.overwrite)
                        stac_add_child(args.stac_base_url, resolution_collection, geocell_catalog)
                    # end geocell    

                    version_bbox = merge_bbox(version_bbox, resolution_bbox)
                    version_mindate = min(version_mindate, resolution_mindate)

                    resolution_collection["extent"]["spatial"]["bbox"][0] = resolution_bbox
                    resolution_collection["extent"]["temporal"]["interval"][0][0] = resolution_mindate
                    
                    write_json(resolution_collection, resolution_collection_path, args.overwrite)
                    stac_add_child(args.stac_base_url, version_collection, resolution_collection)
                # end resolution
                
                kind_bbox = merge_bbox(kind_bbox, version_bbox)
                kind_mindate = min(kind_mindate, version_mindate)

                version_collection["extent"]["spatial"]["bbox"][0] = version_bbox
                version_collection["extent"]["temporal"]["interval"][0][0] = version_mindate

                write_json(version_collection, version_collection_path, args.overwrite)
                stac_add_child(args.stac_base_url, kind_collection, version_collection)
            # end version
            
            domain_bbox = merge_bbox(domain_bbox, kind_bbox)
            domain_mindate = min(domain_mindate, kind_mindate)

            kind_collection["extent"]["spatial"]["bbox"][0] = kind_bbox
            kind_collection["extent"]["temporal"]["interval"][0][0] = kind_mindate

            write_json(kind_collection, kind_collection_path, args.overwrite)
            stac_add_child(args.stac_base_url, domain_collection, kind_collection)
        # end kind

        domain_collection["extent"]["spatial"]["bbox"][0] = domain_bbox
        domain_collection["extent"]["temporal"]["interval"][0][0] = domain_mindate

        write_json(domain_collection, domain_collection_path, args.overwrite)
        stac_add_child(args.stac_base_url, stac_catalog, domain_collection)
    # end domain
    
    write_json(stac_catalog, stac_catalog_path, args.overwrite)


def merge_bbox(bbox1, bbox2):
    # note compare with both min and max values for bbox lon in case spanning 180
    return [
        min(bbox1[0], bbox1[2], bbox2[0], bbox2[2]),
        min(bbox1[1], bbox1[3], bbox2[1], bbox2[3]),
        max(bbox1[0], bbox1[2], bbox2[0], bbox2[2]),
        max(bbox1[1], bbox1[3], bbox2[1], bbox2[3])
        ]


    
def write_json(stac_obj, stac_json_path, overwrite):
    if not os.path.exists(stac_json_path) or overwrite:
        with open(stac_json_path, "w") as f:
            logger.info('Writing '+stac_json_path)
            stac_json = json.dumps(stac_obj, indent=2, sort_keys=False)
            f.write(stac_json)
    

def stac_get_self_link(stac):
    for link in stac["links"]:
        if link["rel"] == "self":
            return link
    return None
    

def stac_add_child(base_url, parent, child):
    
    # find child.link{rel=self}
    child_href = stac_get_self_link(child)["href"]

    child_title = None
    if "title" in child:
        child_title = child["title"]
    else:
        child_title = child["properties"]["title"]

    child_type = "application/json"
    if child["type"] == "Feature":
        child_type = "application/geo+json"

    parent["links"].append({
        "rel": "child",
        "href": child_href,
        "title": child_title,
        "type": child_type

        })
    return parent


        
def stac_pgc_catalog(base_url):
    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "pgc-data-stac",
        "title": "PGC Data Catalog",
        "description": "PGC Data Catalog of open digital elevation models",
        "links": [
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            },
            {
                "rel": "self",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            }
            ]
        }
    return catalog


# ArcticDEM, EarthDEM, REMA
def stac_domain_collection(base_url, domain):
    id = domain.lower()
    
    collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": id,
        "title": domain,
        "description": f"{domain} digital elevation models",
        "license": "CC-BY-4.0",
        "providers": [
            {
                "name": "maxar",
                "description": "Maxar/Digital Globe",
                "roles": ["producer"],
                "url": "https://www.maxar.com"
            },
            {
                "name": "pgc",
                "description": "Polar Geospatial Center",
                "roles": ["processor"],
                "url": "https://pgc.umn.edu"
            }
        ],
        "extent": {
            "spatial": {
                "bbox": [[ 180, 90, -180, -90 ]]
            },
            "temporal": {
                "interval": [[ "1970-01-01T00:00:00Z", None ]]
            }
        },                
        "links": [
            {
                "rel": "self",
                "title": domain,
                "href": f"{base_url}/{id}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            },
            {
                "rel": "parent",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            }
            ]
        }
    return collection


# strips, mosaic
def stac_kind_collection(base_url, domain, kind):
    id = f"{domain['id']}-{kind}"
    domain_self = stac_get_self_link(domain)
    
    collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": id,
        "title": f"{domain['title']} {kind}",
        "description": f"{domain['title']} {kind} digital elevation models",
        "providers": [
            {
                "name": "maxar",
                "description": "Maxar/Digital Globe",
                "roles": ["producer"],
                "url": "https://www.maxar.com"
            },
            {
                "name": "pgc",
                "description": "Polar Geospatial Center",
                "roles": ["processor"],
                "url": "https://pgc.umn.edu"
            }
        ],
        "extent": {
            "spatial": {
                "bbox": [[ 180, 90, -180, -90 ]]
            },
            "temporal": {
                "interval": [[ "1970-01-01T00:00:00Z", None ]]
            }
        },                
        "links": [
            {
                "rel": "self",
                "title": f"{domain['title']} {kind}",
                "href": f"{domain_self['href']}/{id}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            },
            {
                "rel": "parent",
                "title": domain_self["title"],
                "href": domain_self["href"],
                "type": "application/json"
            }
            ]
        }
    return collection


# s2s041
def stac_version_collection(base_url, kind, version):
    id = f"{kind['id']}-{version}"
    kind_self = stac_get_self_link(kind)
    
    collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": id,
        "title": f"{kind['title']} {version}",
        "description": f"{kind['title']} {version} digital elevation models",
        "providers": [
            {
                "name": "maxar",
                "description": "Maxar/Digital Globe",
                "roles": ["producer"],
                "url": "https://www.maxar.com"
            },
            {
                "name": "pgc",
                "description": "Polar Geospatial Center",
                "roles": ["processor"],
                "url": "https://pgc.umn.edu"
            }
        ],
        "extent": {
            "spatial": {
                "bbox": [[ 1, 2, 3, 4 ]]
            },
            "temporal": {
                "interval": [[ "2010-01-01T00:00:00Z", None ]]
            }
        },                
        "links": [
            {
                "rel": "self",
                "title": f"{kind['title']} {version}",
                "href": f"{kind_self['href']}/{id}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            },
            {
                "rel": "parent",
                "title": kind_self["title"],
                "href": kind_self["href"],
                "type": "application/json"
            }
            ]
        }
    return collection


# 2m
def stac_resolution_collection(base_url, version, resolution):
    id = f"{version['id']}-{resolution}"
    version_self = stac_get_self_link(version)
    
    collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": id,
        "title": f"{version['title']} {resolution}",
        "description": f"{version['title']} {resolution} digital elevation models",
        "providers": [
            {
                "name": "maxar",
                "description": "Maxar/Digital Globe",
                "roles": ["producer"],
                "url": "https://www.maxar.com"
            },
            {
                "name": "pgc",
                "description": "Polar Geospatial Center",
                "roles": ["processor"],
                "url": "https://pgc.umn.edu"
            }
        ],
        "extent": {
            "spatial": {
                "bbox": [[ 1, 2, 3, 4 ]]
            },
            "temporal": {
                "interval": [[ "2010-01-01T00:00:00Z", None ]]
            }
        },                
        "links": [
            {
                "rel": "self",
                "title": f"{version['title']} {resolution}",
                "href": f"{version_self['href']}/{id}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            },
            {
                "rel": "parent",
                "title": version_self["title"],
                "href": version_self["href"],
                "type": "application/json"
            }
            ]
        }
    return collection

# n67w132
def stac_geocell_catalog(base_url, resolution, geocell):
    id = f"{resolution['id']}-{geocell}"
    resolution_self = stac_get_self_link(resolution)
    
    collection = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": id,
        "title": f"{resolution['title']} {geocell}",
        "description": f"{resolution['title']} {geocell} digital elevation models",
        "links": [
            {
                "rel": "self",
                "title": f"{resolution['title']} {geocell}",
                "href": f"{resolution_self['href']}/{id}.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "title": "PGC Data Catalog",
                "href": f"{base_url}/pgc-data-stac.json",
                "type": "application/json"
            },
            {
                "rel": "parent",
                "title": resolution_self["title"],
                "href": resolution_self["href"],
                "type": "application/json"
            }
            ]
        }
    return collection


if __name__ == '__main__':
    main()
