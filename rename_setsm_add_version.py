import argparse
import glob
import logging
import os
import sys

from lib import dem, VERSION, SHORT_VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


def main():

    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="rename setsm strips"
        )
    
    parser.add_argument("srcdir", help="source directory")
    parser.add_argument("version", help="strip DEM version string (ex: s2s041)")
    parser.add_argument("--dryrun", action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')

    #### Parse Arguments
    args = parser.parse_args()
    src = args.srcdir

    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    logger.info("Current version: %s", VERSION)

    rasters = []
    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src) and src.endswith("_dem.tif") and not os.path.basename(src).startswith("SETSM_"):
        logger.debug(src)
        try:
            raster = dem.SetsmDem(os.path.join(src))
        except RuntimeError as e:
            logger.error( e )
        else:
            rasters.append(raster)

    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and not f.startswith("SETSM_"):
                    #logger.info(os.path.join(root, f))
                    try:
                        raster = dem.SetsmDem(os.path.join(root, f))
                    except RuntimeError as e:
                        logger.error( e )
                    else:
                        rasters.append(raster)

    i=0
    total = len(rasters)
    logger.info('Renaming DEMs')
    if total > 0:
        for raster in rasters:
            i+=1
            logger.info("[{}/{}] {}".format(i,total,raster.srcfp))
            rename(raster, args)
    else:
        print("No DEMs found requiring processing")
                            
                            
def rename(raster, args):

    # get creation date string:
    raster.get_metafile_info()
    creation_date_str = raster.creation_date.strftime("%Y%m%d")

    # glob and rename files
    glob1 = glob.glob(os.path.join(raster.srcdir, raster.stripid) + "_*")
    tar_path = os.path.join(raster.srcdir, raster.stripid) + ".tar.gz"
    if os.path.isfile(tar_path):
        glob1.append(tar_path)

    for ifp in glob1:
        dirp, fn = os.path.split(ifp)
        #print(ifp)

        ofn = "SETSM_{}_{}".format(args.version, fn)
        ofp = os.path.join(dirp, ofn)
        if os.path.isfile(ofp):
            print("Output file already exists: {}".format(ofp))
        else:
            logger.debug("{} --> {}".format(ifp, ofp))
            if not args.dryrun:
                os.rename(ifp, ofp)
            

if __name__ == '__main__':
    main()