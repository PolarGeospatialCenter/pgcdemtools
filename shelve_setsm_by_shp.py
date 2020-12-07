import os, sys, string, shutil, glob, re, logging, math
from datetime import *
import gdal, osr, ogr, gdalconst
import argparse
from lib import utils, dem

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="shelve setsm files by centroid location relative to a shp index"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('dst', help="destination directory")
    parser.add_argument('shp', help='shp index defining grid scheme')
    parser.add_argument('field', help='shp index field with grid name')

    
    #### Optionsl Arguments
    parser.add_argument('--try-link', action='store_true', default=False,
                        help="try linking instead of copying files")
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
    shp = os.path.abspath(args.shp)
        
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
        
    ### Open shp, verify field, verify projetion, extract tile geoms
    tiles = {}
    ds = ogr.Open(shp)
    if ds is not None:

        lyr = ds.GetLayerByName(os.path.splitext(os.path.basename(shp))[0])
        lyr.ResetReading()

        src_srs = lyr.GetSpatialRef()
        
        i = lyr.FindFieldIndex(args.field,1)
        if i == -1:
            logger.error("Cannot locate field {} in {}".format(args.field, args.shp))
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
        
    else:
        rasters = []
    
        #### ID rasters
        logger.info('Identifying DEMs')
        if os.path.isfile(src):
            logger.info(src)
            try:
                raster = dem.SetsmDem(src)
            except RuntimeError as e:
                logger.error( e )
            else:
                if raster.metapath or os.path.isfile(raster.mdf):
                    rasters.append(raster)
                else:
                    logger.warning("DEM does not include a valid meta.txt or mdf.txt, skipping: {}".format(raster.srcfp))
        
        else:
            for root,dirs,files in os.walk(src):
                for f in files:
                    if f.endswith("_dem.tif"):
                        logger.debug(os.path.join(root,f))
                        try:
                            raster = dem.SetsmDem(os.path.join(root,f))
                        except RuntimeError as e:
                            logger.error( e )
                        else:
                            if raster.metapath or os.path.isfile(raster.mdf):
                                rasters.append(raster)
                            else:
                                logger.warning("DEM does not include a valid meta.txt or mdf.txt, skipping: {}".format(raster.srcfp))
        
        logger.info('Shelving DEMs')
        total = len(rasters)
        i = 0
        for raster in rasters:
            #### print count/total as progress meter
            i+=1
            #logger.info("[{} of {}] - {}".format(i,total,raster.stripid))
            
            ## Convert geom to match shp srs and get centroid
            raster.get_metafile_info()
            geom_copy = raster.exact_geom.Clone()
            srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
            srs.ImportFromProj4(raster.proj4_meta)
            if not shp_srs.IsSame(srs):
                ctf = osr.CoordinateTransformation(srs, shp_srs)
                geom_copy.Transform(ctf)
            centroid = geom_copy.Centroid()
            
            ## Run intersection with each tile 
            tile_overlaps = []
            for tile_name, tile_geom in tiles.iteritems():
                if centroid.Intersects(tile_geom):
                    tile_overlaps.append(tile_name)
            
            ## Raise an error on multiple intersections or zero intersections
            if len (tile_overlaps) == 0:
                logger.error("raster {} does not intersect the index shp, skipping".format(raster.srcfn))
            
            elif len(tile_overlaps) > 1:
                logger.error("raster {} intersects more than one tile ({}), skipping".format(raster.srcfn, ','.join(tile_overlaps)))
            
            else:
                logger.info("{} shelved to tile {}".format(raster.stripid, tile_overlaps[0]))
                dst_dir = os.path.join(dst, tile_overlaps[0])
        
                if not os.path.isdir(dst_dir):
                    if not args.dryrun:
                        os.makedirs(dst_dir)
                
                for ifp in glob.glob(os.path.join(raster.srcdir,raster.stripid)+"*"):
                    ofp = os.path.join(dst_dir,os.path.basename(ifp))
                    if os.path.isfile(ofp) and args.overwrite:
                        logger.debug("Copying {} to {}".format(ifp,ofp))
                        if not args.dryrun:
                            os.remove(ofp)
                            if args.try_link:
                                os.link(ifp,ofp)
                            else:
                                shutil.copy2(ifp,ofp)
                            
                    elif not os.path.isfile(ofp):
                        logger.debug("Copying {} to {}".format(ifp,ofp))
                        if not args.dryrun:
                            if args.try_link:
                                os.link(ifp,ofp)
                            else:
                                shutil.copy2(ifp,ofp)
                            
                    else:
                        logger.debug("File already exists, skipping {} to {}".format(ifp,ofp))
                
    logger.info('Done')

if __name__ == '__main__':
    main()
