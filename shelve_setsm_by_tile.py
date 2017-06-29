import os, sys, string, shutil, glob, re, logging, math
from datetime import *
import gdal, osr, ogr, gdalconst
import argparse
from collections import namedtuple
import numpy
from numpy import flatnonzero
from lib import utils, dem

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

grid_xmin = -4000000
grid_xmax = 4000000
grid_ymin = -4000000
grid_ymax = 4000000
grid_xinterval = 100000
grid_yinterval = 100000
grid_cols = (grid_xmax - grid_xmin) / grid_xinterval
grid_rows = (grid_ymax - grid_ymin) / grid_yinterval


def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="shelve setsm files by ArcticDEM tile number"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('dst', help="destination directory")
    
    #### Optionsl Arguments
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    
    #### Parse Arguments
    args = parser.parse_args()
    
    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    
    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)
        
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    
    if args.log:
        if os.path.isdir(args.log):
            tm = datetime.now()
            logfile = os.path.join(args.log,"shelve_setsm{}.log".format(tm.strftime("%Y%m%d%H%M%S")))
        else:
            parser.error('log folder does not exist: {}'.format(args.log))
        
        lfh = logging.FileHandler(logfile)
        lfh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
        lfh.setFormatter(formatter)
        logger.addHandler(lfh)
    
    srs_wgs84 = osr.SpatialReference()
    srs_wgs84.ImportFromEPSG(4326)
    
    rasters = []

    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src):
        logger.info(src)
        try:
            raster = dem.SetsmDem(src)
        except RuntimeError, e:
            logger.error( e )
        else:
            if raster.metapath is not None:
                rasters.append(raster)
            else:
                logger.warning("DEM does not include a valid meta.txt and cannot be shelved: {}".format(raster.srcfp))
    
    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif"):
                    logger.debug(os.path.join(root,f))
                    try:
                        raster = dem.SetsmDem(os.path.join(root,f))
                    except RuntimeError, e:
                        logger.error( e )
                    else:
                        if raster.metapath is not None:
                            rasters.append(raster)
                        else:
                            logger.warning("DEM does not include a valid meta.txt and cannot be shelved: {}".format(raster.srcfp))
    
    logger.info('Shelving DEMs')
    total = len(rasters)
    i = 0
    for raster in rasters:
        #### print count/total as progress meter
        i+=1
        #logger.info("[{} of {}] - {}".format(i,total,raster.stripid))
        
        ## run intersect with tile geom to get overlapping tiles
        raster.get_metafile_info()
        
        (minx, maxx, miny, maxy) = raster.exact_geom.GetEnvelope()
        min_col = int(math.ceil((minx - grid_xmin) / grid_xinterval))
        max_col = int(math.ceil((maxx - grid_xmin) / grid_xinterval))
        min_row = int(math.ceil((miny - grid_ymin) / grid_yinterval))
        max_row = int(math.ceil((maxy - grid_ymin) / grid_yinterval))
                
        tile_overlaps = []
        for col in range(min_col, max_col + 1):
            for row in range(min_row, max_row + 1):
                tile_wkt = 'POLYGON (( {0} {1}, {2} {3}, {4} {5}, {6} {7}, {0} {1} ))'.format(
                    ((col-1) * grid_xinterval) + grid_xmin,
                    ((row-1) * grid_yinterval) + grid_ymin,
                    ((col-1) * grid_xinterval) + grid_xmin,
                    ((row) * grid_yinterval) + grid_ymin,
                    ((col) * grid_xinterval) + grid_xmin,
                    ((row) * grid_yinterval) + grid_ymin,
                    ((col) * grid_xinterval) + grid_xmin,
                    ((row-1) * grid_yinterval) + grid_ymin,
                )
                tile_geom = ogr.CreateGeometryFromWkt(tile_wkt)
                if raster.exact_geom.Intersects(tile_geom):
                    #print row, col, raster.exact_geom.Intersection(tile_geom).Area()
                    if raster.exact_geom.Intersection(tile_geom).Area() > 4000:
                        tile_overlaps.append((row, col))
                
        for tile in tile_overlaps:
            tile_name = '{:02d}_{:02d}'.format(tile[0], tile[1])
            logger.info("{} shelved to tile {}".format(raster.stripid, tile_name))
            
            dst_dir = os.path.join(dst, tile_name)
    
            if not os.path.isdir(dst_dir):
                if not args.dryrun:
                    os.makedirs(dst_dir)
            
            for ifp in glob.glob(os.path.join(raster.srcdir,raster.stripid)+"*"):
                ofp = os.path.join(dst_dir,os.path.basename(ifp))
                if os.path.isfile(ofp) and args.overwrite:
                    logger.debug("Moving {} to {}".format(ifp,ofp))
                    if not args.dryrun:
                        os.remove(ofp)
                        os.link(ifp,ofp)
                        
                elif not os.path.isfile(ofp):
                    logger.debug("Moving {} to {}".format(ifp,ofp))
                    if not args.dryrun:
                        os.link(ifp,ofp)
                        
                else:
                    logger.warning("Cannot move {} to {}".format(ifp,ofp))
            
    logger.info('Done')

if __name__ == '__main__':
    main()