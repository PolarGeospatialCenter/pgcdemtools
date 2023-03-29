import argparse
import glob
import logging
import os
import sys
from datetime import *

from lib import utils, dem
from lib import VERSION

#### Create Logger
logger = utils.get_logger()


MODES = (
    'geocell',  # ./n67w056/
    'shp',      # ./<according to shp polygon>
    'date'      # ./WV02/2021/06/31
)
DEFAULT_MODE = 'geocell'

class RawTextArgumentDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter): pass

def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        formatter_class=RawTextArgumentDefaultsHelpFormatter,
        description="shelve setsm files by geocell"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory, text file (of file paths or dir paths), or dem")
    parser.add_argument('dst', help="destination directory")
    
    #### Optionsl Arguments
    parser.add_argument('--mode', choices=MODES, default=DEFAULT_MODE,
                        help='shelving folder structure')
    parser.add_argument('--shp', help='if mode = shp, provide a shapefile here of target tiling scheme')
    parser.add_argument('--field', help='if mode = shp, provide a field name for the folders in the --shp file')
    parser.add_argument('--res', choices=['2m', '8m'], help="only shelve DEMs of resolution <res> (2 or 8)")
    parser.add_argument('--try-link', action='store_true', default=False,
                        help="try linking instead of copying files")
    parser.add_argument('--skip-ortho', action='store_true', default=False,
                        help='skip shelving ortho tifs')
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('-v', '--version', action='store_true', default=False, help='print version and exit')
    
    #### Parse Arguments
    args = parser.parse_args()

    if args.version:
        print("Current version: %s", VERSION)
        sys.exit(0)
    
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

    logger.info("Current version: %s", VERSION)

    rasters = []

    ### Open shp, verify field, verify projection, extract tile geoms
    proceed = True
    if args.mode == 'shp':
        tiles, shp_srs = utils.get_tiles_from_shp(os.path.abspath(args.shp), args.field)

        if len(tiles) == 0:
            logger.error("No features found in shp")
            proceed = False

    if proceed:
        #### ID rasters
        srcfps = []
        logger.info('Identifying DEMs')
        if os.path.isfile(src) and src.endswith('.tif'):
            logger.info(src)
            srcfps.append(src)

        elif os.path.isfile(src) and src.endswith(('.txt', '.csv')):
            fh = open(src, 'r')
            for line in fh.readlines():
                l = line.strip()
                if os.path.isfile(l):
                    srcfps.append(l)
                elif os.path.isdir(l):
                    srcfp_list = glob.glob(l + '/*dem.tif')
                    srcfps.extend(srcfp_list)
                else:
                    logger.warning('Text file input line is not a file or folder: {}'.format(l))

        else:
            for root, dirs, files in os.walk(src):
                for f in files:
                    if f.endswith("_dem.tif"):
                        srcfp = os.path.join(root, f)
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

        logger.info('{} DEMs found'.format(len(rasters)))

        logger.info('Shelving DEMs')
        total = len(rasters)
        i = 0
        for raster in rasters:
            #### print count/total as progress meter
            i+=1
            logger.debug("[{} of {}] - {}".format(i,total,raster.stripid))
            if args.mode == 'shp':
                utils.shelve_item(raster, dst, args, tiles, shp_srs)
            else:
                utils.shelve_item(raster, dst, args)

        logger.info('Done')


if __name__ == '__main__':
    main()
