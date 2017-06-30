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


def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="shelve setsm files by geocell"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('dst', help="destination directory")
    
    #### Optionsl Arguments
    parser.add_argument('--res', choices=['2m','8m'], help="only shelve DEMs of resolution <res> (2 or 8)")
    parser.add_argument('--try-link', action='store_true', default=False,
                        help="try linking instead of moving files")
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
                if args.res:
                    if raster.res == args.res:
                        rasters.append(raster)
                else:
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
                            if args.res:
                                if raster.res == args.res:
                                    rasters.append(raster)
                            else:
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
        
        ## get centroid and round down to floor to make geocell folder
        raster.get_metafile_info()
        geocell = raster.get_geocell()
            
        #dst_dir = os.path.join(dst, platform, year, month, day, pair_folder)
        dst_dir = os.path.join(dst, geocell)

        if not os.path.isdir(dst_dir):
            if not args.dryrun:
                os.makedirs(dst_dir)
        
        for ifp in glob.glob(os.path.join(raster.srcdir,raster.stripid)+"*"):
            ofp = os.path.join(dst_dir,os.path.basename(ifp))
            if os.path.isfile(ofp) and args.overwrite:
                logger.debug("Linking {} to {}".format(ifp,ofp))
                if not args.dryrun:
                    os.remove(ofp)
                    if args.try_link:
                        os.link(ifp,ofp)
                    else:
                        os.rename(ifp,ofp)
                    
            elif not os.path.isfile(ofp):
                logger.debug("Moving {} to {}".format(ifp,ofp))
                if not args.dryrun:
                    if args.try_link:
                        os.link(ifp,ofp)
                    else:
                        os.rename(ifp,ofp)
                    
            else:
                logger.debug("File already exists, skipping {} to {}".format(ifp,ofp))
            
    logger.info('Done')

if __name__ == '__main__':
    main()