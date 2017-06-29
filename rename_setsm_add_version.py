import os, string, sys, logging, argparse, glob
from datetime import datetime
from lib import utils, dem

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


def main():

    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="rename setsm strips"
        )
    
    parser.add_argument("srcdir", help="source directory")
    parser.add_argument("version", help="version string (ex: v1.2)")
    parser.add_argument("--dryrun", action='store_true', default=False,
                        help="print actions without executing")
    
    #### Parse Arguments
    args = parser.parse_args()
    src = args.srcdir

    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    rasters = []
    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src) and f.endswith("_dem.tif") and not f.startswith("SETSM_"):
        logger.debug(src)
        try:
            raster = dem.SetsmDem(os.path.join(src))
        except RuntimeError, e:
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
                    except RuntimeError, e:
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
    for ifp in glob.glob(os.path.join(raster.srcdir, raster.stripid+"*")):
        dirp, fn = os.path.split(ifp)

        ofn = "SETSM_{}_{}{}".format(fn[:len(raster.stripid)], args.version, fn[len(raster.stripid):])
        ofp = os.path.join(dirp, ofn)
        if os.path.isfile(ofp):
            print "Output file already exists: {}".format(ofp)
        else:
            logger.info("{} --> {}".format(ifp, ofp))
            if not args.dryrun:
                os.rename(ifp, ofp)
            

if __name__ == '__main__':
    main()