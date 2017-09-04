import os, sys, string, shutil, glob, re, logging, argparse
from datetime import *
import gdal, osr, ogr, gdalconst
from lib import utils, dem, taskhandler

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="build setsm DEM tile index"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or tile")
    parser.add_argument('dst', help="destination index feature class")
    
    #### Optionsl Arguments
    parser.add_argument('--epsg', type=int, default=3413,
                        help="egsg code for output index projection (default epsg:3413)")
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    
    #### Parse Arguments
    args = parser.parse_args()
    
    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    
    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)
    
    try:
        dst_dir, dst_lyr = utils.get_source_names(dst)
    except RuntimeError, e:
        parser.error(e)
        
    print (dst_dir,dst_lyr)
    
    ogr_driver_str = "ESRI Shapefile" if dst.lower().endswith(".shp") else "FileGDB"
    ogrDriver = ogr.GetDriverByName(ogr_driver_str)
    if ogrDriver is None:
        parser.error("GDAL FileGDB driver is not available")
    
    #### Test epsg
    try:
        spatial_ref = utils.SpatialRef(args.epsg)
    except RuntimeError, e:
        parser.error(e)
        
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    
    if args.log:
        if os.path.isdir(args.log):
            tm = datetime.now()
            logfile = os.path.join(args.log,"index_setsm_{}.log".format(tm.strftime("%Y%m%d%H%M%S")))
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
            raster = dem.SetsmTile(os.path.join(src))
        except RuntimeError, e:
            logger.error( e )
        else:
            rasters.append(raster)
    
    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif"):
                    logger.debug(os.path.join(root,f))
                    try:
                        raster = dem.SetsmTile(os.path.join(root,f))
                    except RuntimeError, e:
                        logger.error( e )
                    else:
                        rasters.append(raster)
    
    total = len(rasters)
    
    if total > 0:
        #### Write index
        #### delete old index if shp,  delete layer if GDB?
        if dst.endswith('.shp') and os.path.isfile(dst):
            if args.overwrite:
                logger.info("Removing old index... %s" %os.path.basename(dst))
                ogrDriver.DeleteDataSource(dst)
            else:
                logger.error("Dst shapefile exists.  Use the --overwrite flag to overwrite.")
                sys.exit()
        
        if dst_dir.endswith('.gdb') and os.path.isdir(dst_dir):
            ds = ogrDriver.Open(dst_dir,1)
            if ds is not None:
                #logger.info(ds.TestCapability(ogr.ODsCDeleteLayer))
                for i in range(ds.GetLayerCount()):   
                    lyr = ds.GetLayer(i)
                    if lyr.GetName() == dst_lyr:
                        if args.overwrite:
                            logger.info("Removing old index layer: {}".format(dst_lyr))
                            del lyr
                            ds.DeleteLayer(i)
                            break
                        else:
                            logger.error("Dst GDB layer exists.  Use the --overwrite flag to overwrite.")
                            sys.exit()
                ds = None      
            
        if dst.endswith('.shp'):
            if not os.path.isfile(dst):
                ds = ogrDriver.CreateDataSource(dst)
            else:
                ds = None
            
        else:
            if os.path.isdir(dst_dir):
                ds = ogrDriver.Open(dst_dir,1)
            else:
                ds = ogrDriver.CreateDataSource(dst_dir)
        
        if ds is not None:
            
            logger.info("Building index...")
            #### build new index
            tgt_srs = osr.SpatialReference()
            tgt_srs.ImportFromEPSG(args.epsg)
            
            layer = ds.CreateLayer(dst_lyr, tgt_srs, ogr.wkbPolygon)
            
            if layer is not None:
        
                for field_def in utils.TILE_DEM_ATTRIBUTE_DEFINITIONS:
                    
                    field = ogr.FieldDefn(field_def.fname, field_def.ftype)
                    field.SetWidth(field_def.fwidth)
                    field.SetPrecision(field_def.fprecision)
                    layer.CreateField(field)
                
                #### loop through rasters and add features
                i=0
                for raster in rasters:
                    i+=1
                    progress(i,total,"features written")
                    logger.debug("Adding {}".format(raster.tileid))
                    try:
                        raster.get_dem_info()
                    except RuntimeError, e:
                        logger.error( e )
                    else:
                        feat = ogr.Feature(layer.GetLayerDefn())
                            
                        ## Set fields
                        feat.SetField("DEM_ID",raster.tileid)
                        feat.SetField("TILE",raster.tilename)
                        feat.SetField("ND_VALUE",raster.ndv)
                        feat.SetField("DEM_NAME",raster.srcfn)
                        feat.SetField("FILEPATH",raster.srcdir)
                        if raster.version:
                            feat.SetField("REL_VER",raster.version)
                        if raster.srcdir.startswith(r'/mnt/pgc'):
                            winpath = raster.srcdir.replace(r'/mnt/pgc',r'V:/pgc')
                            feat.SetField("WIN_PATH",winpath)
                        res = (raster.xres + raster.yres) / 2.0
                        feat.SetField("DEM_RES",res)
                        feat.SetField("DENSITY",raster.density)
                        feat.SetField("NUM_COMP",raster.num_components)
                        
                        if raster.reg_src:
                            feat.SetField("REG_SRC",raster.reg_src)
                            feat.SetField("NUM_GCPS",raster.num_gcps)
                        if raster.mean_resid_z is not None:
                            feat.SetField("MEANRESZ",raster.mean_resid_z)
                        
                        #### Set fields if populated (will not be populated if metadata file is not found)
                        if raster.creation_date:
                            feat.SetField("CR_DATE",raster.creation_date.strftime("%Y-%m-%d"))
                
                        ## transfrom and write geom
                        src_srs = osr.SpatialReference()
                        src_srs.ImportFromWkt(raster.proj)
                        
                        if raster.geom:
                            geom = raster.geom.Clone()
                            if not src_srs.IsSame(tgt_srs):
                                transform = osr.CoordinateTransformation(src_srs,tgt_srs)
                                geom.Transform(transform) #### Verify this works over 180
            
                            feat.SetGeometry(geom)
                            #### add new feature to layer
                            layer.CreateFeature(feat)
                            
                        else:
                            logger.error('No valid geom found, feature skipped: {}'.format(raster.srcfp))
                    
                    
            else:
                logger.error('Cannot create layer: {}'.format(dst_lyr))
                
            ds = None
        
        else:
            logger.info("Cannot open/create dataset: %s" %dst)
    
    else:
        logger.info("No valid rasters found")
    
    logger.info("Done")
    
        


def progress(count, total, suffix=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()  # As suggested by Rom Ruben

    

if __name__ == '__main__':
    main()
        
            
            
        
        
        
        