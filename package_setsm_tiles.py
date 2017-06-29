import os, sys, string, shutil, glob, re, logging, tarfile, zipfile
from datetime import *
import gdal, osr, ogr, gdalconst
import argparse
from lib import utils, dem

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ogrDriver = ogr.GetDriverByName("ESRI Shapefile")
tgt_srs = osr.SpatialReference()
tgt_srs.ImportFromEPSG(3413)



def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="package setsm dems (build mdf and readme files and create archive) in place in the filesystem"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('scratch', help="scratch space to build index shps")
    
    #### Optionsl Arguments
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    
    #### Parse Arguments
    args = parser.parse_args()
    
    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    if not os.path.isdir(args.scratch) and not os.path.isfile(args.scratch):
        parser.error("Source directory or file does not exist: %s" %args.scratch)
    
    src = os.path.abspath(args.src)
    scratch = os.path.abspath(args.scratch)
    
    if args.v:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    lsh = logging.StreamHandler()
    lsh.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    
    if args.log:
        if os.path.isdir(args.log):
            tm = datetime.now()
            logfile = os.path.join(args.log,"package_setsm_tiles_{}.log".format(tm.strftime("%Y%m%d%H%M%S")))
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
        logger.debug(src)
        try:
            raster = dem.SetsmTile(os.path.join(src))
        except RuntimeError, e:
            logger.error( e )
        else:
            if not os.path.isfile(raster.archive) or args.overwrite:
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
                        if not os.path.isfile(raster.archive) or args.overwrite:
                            rasters.append(raster)
                                
    i=0
    total = len(rasters)
    logger.info('Packaging DEMs')
    if total > 0:
        for raster in rasters:
            i+=1
            logger.info("[{}/{}] {}".format(i,total,raster.srcfp))
            build_archive(raster,scratch,args)
    else:
        logger.info("No DEMs found requiring processing")
        
def build_archive(raster,scratch,args):

    #### create archive
    
    dstfp = raster.archive
    dstdir, dstfn = os.path.split(raster.archive)
    #print dstfn
    #print dstfp
    
    try:
        raster.get_dem_info()
    except RuntimeError, e:
        logger.error(e)
    else:
        
        #### Build Archive
        if os.path.isfile(dstfp) and args.overwrite is True:
            if not args.dryrun:
                try:
                    os.remove(dstfp)
                except:
                    print "Cannot replace archive: %s" %srcfp
    
        if not os.path.isfile(dstfp):    
        
            components = (
                os.path.basename(raster.srcfp), # dem
                os.path.basename(raster.matchtag), # matchtags
                os.path.basename(raster.metapath), # mdf
                # index shp files
            )

            optional_components = [os.path.basename(raster.regmetapath)] #reg
            
            os.chdir(dstdir)
            #logger.info(os.getcwd())
            
            k = 0
            existing_components = sum([int(os.path.isfile(component)) for component in components])
            ### check if exists, print
            #logger.info(existing_components)
            if existing_components == len(components):
                
                ## Build index
                index = os.path.join(scratch,raster.tileid+"_index.shp")
                
                ## create dem index shp: <strip_id>_index.shp
                try:
                    index_dir, index_lyr = utils.get_source_names(index)
                except RuntimeError, e:
                    logger.error("{}: {}".format(index,e))            
                
                if os.path.isfile(index):
                    ogrDriver.DeleteDataSource(index)
                
                if not os.path.isfile(index):
                    ds = ogrDriver.CreateDataSource(index)
                    if ds is not None:
                    
                        lyr = ds.CreateLayer(index_lyr, tgt_srs, ogr.wkbPolygon)
            
                        if lyr is not None:
                    
                            for field_def in utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC:
                                
                                field = ogr.FieldDefn(field_def.fname, field_def.ftype)
                                field.SetWidth(field_def.fwidth)
                                field.SetPrecision(field_def.fprecision)
                                lyr.CreateField(field)
                                
                            #print raster.stripid
                            feat = ogr.Feature(lyr.GetLayerDefn())
                                
                            ## Set fields
                            feat.SetField("DEM_ID",raster.tileid)
                            feat.SetField("TILE",raster.tilename)
                            feat.SetField("ND_VALUE",raster.ndv)
                            feat.SetField("DEM_NAME",raster.srcfn)
                            res = (raster.xres + raster.yres) / 2.0
                            feat.SetField("DEM_RES",res)
                            feat.SetField("DENSITY",raster.density)
                            feat.SetField("NUM_COMP",raster.num_components)
                            
                            if raster.version:
                                feat.SetField("REL_VER",raster.version)
                            
                            if raster.reg_src:
                                feat.SetField("REG_SRC",raster.reg_src)
                                feat.SetField("NUM_GCPS",raster.num_gcps)
                            if raster.mean_resid_z:
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
                            
                            else:
                                logger.error('No valid geom found: {}'.format(raster.srcfp))
                        
                            #### add new feature to layer
                            lyr.CreateFeature(feat)
                            
                            ## Close layer and dataset
                            lyr = None
                            ds = None
                            
                            if os.path.isfile(index):
                                ## Create archive
                                if not args.dryrun:
                                    #archive = tarfile.open(dstfp,"w:")
                                    archive = tarfile.open(dstfp,"w:gz")
                                    if not os.path.isfile(dstfp):
                                        logger.error("Cannot create archive: {}".format(dstfn))
                            
                                ## Add components
                                for component in components:
                                    logger.debug("Adding {} to {}".format(component,dstfn))
                                    k+=1
                                    if not args.dryrun:
                                        archive.add(component)
                                        #archive.write(component)

                                ## Add optional components
                                for component in optional_components:
                                    if os.path.isfile(component):
                                        logger.debug("Adding {} to {}".format(component,dstfn))
                                        k+=1
                                        if not args.dryrun:
                                            archive.add(component)
                                        
                                ## Add index in subfolder
                                os.chdir(scratch)
                                for f in glob.glob(index_lyr+".*"):
                                    arcname = os.path.join("index",f)
                                    logger.debug("Adding {} to {}".format(f,dstfn))
                                    k+=1
                                    if not args.dryrun:
                                        archive.add(f,arcname=arcname)
                                    os.remove(f)
                                
                                logger.info("Added {} items to archive: {}".format(k,dstfn))
                                
                                ## Close archive
                                if not args.dryrun:
                                    try:
                                        archive.close()
                                    except Exception,e:
                                        print e
                        
                        else:
                            logger.error('Cannot create layer: {}'.format(dst_lyr))    
                    else:
                        logger.error("Cannot create index: {}".format(index))    
                else:
                    logger.error("Cannot remove existing index: {}".format(index))       
            else:
                logger.error("Not enough existing components to make a valid archive: {} ({} found, {} required)".format(raster.srcfp,existing_components,len(components)))
    

if __name__ == '__main__':
    main()
        
            
            
        
        
        
        