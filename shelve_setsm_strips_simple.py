import os, sys, string, shutil, glob, re, logging, math
from datetime import *
from osgeo import gdal, osr, ogr, gdalconst
import argparse
from collections import namedtuple
import numpy
from numpy import flatnonzero
from lib import utils, dem

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


MODES = (
    'geocell',  # ./n67w056/
    'shp',      # ./<according to shp polygon>
    'date'      # ./WV02/2021/06/31
)
DEFAULT_MODE = 'geocell'

def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="shelve setsm files by geocell"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('dst', help="destination directory")
    
    #### Optionsl Arguments
    parser.add_argument('--mode', choices=MODES, default=DEFAULT_MODE,
                        help='shelving folder structure')
    parser.add_argument('--shp', help='if mode = shp, provide a shapefile here of target tiling scheme')
    parser.add_argument('--field', help='if mode = shp, provide a field name for the folders in the --shp file')
    parser.add_argument('--res', choices=['2m','8m'], help="only shelve DEMs of resolution <res> (2 or 8)")
    parser.add_argument('--try-link', action='store_true', default=False,
                        help="try linking instead of copying files")
    parser.add_argument('--skip-ortho', action='store_true', default=False,
                        help='skip shelving ortho tifs')
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

    ## check args d o not conflict
    if args.mode == 'shp':
        if not args.shp or not args.field:
            parser.error("--mode shp requires a --shp <tile_shapefile> and a --field <field_name> argument")
        if not os.path.isfile(args.shp):
            parser.error("--shp file does not exist")
    
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

    ### Open shp, verify field, verify projetion, extract tile geoms
    proceed = True
    if args.mode == 'shp':

        tiles = {}
        shp = os.path.abspath(args.shp)
        ds = ogr.Open(shp)
        if ds is not None:

            lyr = ds.GetLayerByName(os.path.splitext(os.path.basename(shp))[0])
            lyr.ResetReading()

            i = lyr.FindFieldIndex(args.field, 1)
            if i == -1:
                logger.error("Cannot locate field {} in {}".format(args.field, shp))
                sys.exit(-1)

            shp_srs = lyr.GetSpatialRef()
            if shp_srs is None:
                logger.error("Shp must have a defined spatial reference")
                sys.exit(-1)

            for feat in lyr:
                tile_name = feat.GetFieldAsString(i)
                tile_geom = feat.GetGeometryRef().Clone()
                if not tile_name in tiles:
                    tiles[tile_name] = tile_geom
                else:
                    logger.error("Found features with duplicate name: {} - Ignoring 2nd feature".format(tile_name))

        else:
            logger.error("Cannot open {}".format(src))

        if len(tiles) == 0:
            logger.error("No features found in shp")
            proceed = False

    if proceed:
        #### ID rasters
        srcfps = []
        logger.info('Identifying DEMs')
        if os.path.isfile(src):
            logger.info(src)
            srcfps.append(src)
        else:
            for root,dirs,files in os.walk(src):
                for f in files:
                    if f.endswith("_dem.tif"):
                        srcfp = os.path.join(root,f)
                        logger.debug(srcfp)
                        srcfps.append(srcfp)

        for srcfp in srcfps:
            try:
                raster = dem.SetsmDem(srcfp)
            except RuntimeError as e:
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
            dst_dir = None
            if args.mode == 'geocell':
                ## get centroid and round down to floor to make geocell folder
                raster.get_metafile_info()
                geocell = raster.get_geocell()
                dst_dir = os.path.join(dst, geocell)

            elif args.mode == 'date':
                platform = raster.sensor1
                year = raster.acqdate1.strftime("%Y")
                month = raster.acqdate1.strftime("%m")
                day = raster.acqdate1.strftime("%d")
                dst_dir = os.path.join(dst, platform, year, month, day)

            elif args.mode == 'shp':
                ## Convert geom to match shp srs and get centroid
                raster.get_metafile_info()
                geom_copy = raster.geom.Clone()
                srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
                srs.ImportFromProj4(raster.proj4_meta)
                if not shp_srs.IsSame(srs):
                    ctf = osr.CoordinateTransformation(srs, shp_srs)
                    geom_copy.Transform(ctf)
                centroid = geom_copy.Centroid()

                ## Run intersection with each tile
                tile_overlaps = []
                for tile_name, tile_geom in tiles.items():
                    if centroid.Intersects(tile_geom):
                        tile_overlaps.append(tile_name)

                ## Raise an error on multiple intersections or zero intersections
                if len(tile_overlaps) == 0:
                    logger.warning("raster {} does not intersect the index shp, skipping".format(raster.srcfn))

                elif len(tile_overlaps) > 1:
                    logger.warning("raster {} intersects more than one tile ({}), skipping".format(raster.srcfn,
                                                                                                 ','.join(tile_overlaps)))
                else:
                    #logger.info("{} shelved to tile {}".format(raster.stripid, tile_overlaps[0]))
                    dst_dir = os.path.join(dst, tile_overlaps[0])

            if dst_dir:
                if not os.path.isdir(dst_dir):
                    if not args.dryrun:
                        os.makedirs(dst_dir)

                glob1 = glob.glob(os.path.join(raster.srcdir, raster.stripid)+"_*")
                tar_path = os.path.join(raster.srcdir, raster.stripid)+".tar.gz"
                if os.path.isfile(tar_path):
                    glob1.append(tar_path)

                if args.skip_ortho:
                    glob2 = [f for f in glob1 if 'ortho' not in f]
                    glob1 = glob2

                ## Check if existing and remove all matching files if overwrite
                glob3 = glob.glob(os.path.join(dst_dir, raster.stripid) + "_*")
                tar_path = os.path.join(dst_dir, raster.stripid) + ".tar.gz"
                if os.path.isfile(tar_path):
                    glob3.append(tar_path)

                proceed = True
                if len(glob3) > 0:
                    if args.overwrite:
                        logger.info("Destination files already exist for {} - overwriting all dest files".format(
                                raster.stripid))
                        for ofp in glob3:
                            logger.debug("Removing {} due to --overwrite flag".format(ofp))
                            if not args.dryrun:
                                os.remove(ofp)
                    else:
                        logger.info("Destination files already exist for {} - skipping DEM. Use --overwrite to overwrite".format(
                            raster.stripid))
                        proceed = False

                ## Link or copy files
                if proceed:
                    for ifp in glob1:
                        ofp = os.path.join(dst_dir, os.path.basename(ifp))
                        logger.debug("Linking {} to {}".format(ifp, ofp))
                        if not args.dryrun:
                            if args.try_link:
                                try:
                                    os.link(ifp, ofp)
                                except OSError:
                                    logger.error("os.link failed on {}".format(ifp))
                            else:
                                logger.debug("Copying {} to {}".format(ifp, ofp))
                                shutil.copy2(ifp, ofp)

        logger.info('Done')


if __name__ == '__main__':
    main()
