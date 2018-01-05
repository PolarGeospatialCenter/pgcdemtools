import os, sys, string, shutil, glob, re, logging, tarfile, zipfile
from datetime import *
import gdal, osr, ogr, gdalconst
import argparse
from lib import utils, dem, taskhandler

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ogrDriver = ogr.GetDriverByName("ESRI Shapefile")
tgt_srs = osr.SpatialReference()
tgt_srs.ImportFromEPSG(4326)



def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="package setsm dems (build mdf and readme files and create archive) in place in the filesystem"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('scratch', help="scratch space to build index shps")
    
    #### Optionsl Arguments
    parser.add_argument('--mdf-only', action='store_true', default=False,
                        help="build mdf and readme files only, do not archive")
    parser.add_argument('--filter-dems', action='store_true', default=False,
                        help="filter dems with area < 5.6 sqkm and density < 0.1")
    parser.add_argument('--force-filter-dems', action='store_true', default=False,
                        help="filter dems where tar has already been built with area < 5.6 sqkm and density < 0.1")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="qsub script to use in PBS submission (default is qsub_package.sh in script root folder)")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    pos_arg_keys = ['src','scratch']
    
    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)
    scratch = os.path.abspath(args.scratch)
    
    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    if not os.path.isdir(args.scratch) and not args.pbs:  #scratch dir may not exist on head node when running jobs via pbs
        parser.error("Scratch directory does not exist: %s" %args.scratch)
    
    ## Verify qsubscript
    if args.qsubscript is None:
        qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_package.sh')
    else:
        qsubpath = os.path.abspath(args.qsubscript)
    if not os.path.isfile(qsubpath):
        parser.error("qsub script path is not valid: %s" %qsubpath)
    
    ## Verify processing options do not conflict
    if args.pbs and args.parallel_processes > 1:
        parser.error("Options --pbs and --parallel-processes > 1 are mutually exclusive")
    
    if args.v:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    lsh = logging.StreamHandler()
    lsh.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    
    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    i=0
    j=0
    task_queue = []
    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src):
        logger.debug(src)
        try:
            raster = dem.SetsmDem(src)
        except RuntimeError, e:
            logger.error( e )
        else:
            j+=1
            if not os.path.isfile(raster.archive) or not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme) or args.overwrite or args.force_filter_dems:
                i+=1
                task = taskhandler.Task(
                    os.path.basename(src),
                    'Pkg{:04g}'.format(i),
                    'python',
                    '{} {} {} {}'.format(scriptpath, arg_str_base, src, scratch),
                    build_archive,
                    [src, scratch, args]
                )
                task_queue.append(task)
    
    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    try:
                        raster = dem.SetsmDem(srcfp)
                    except RuntimeError, e:
                        logger.error( e )
                    else:
                        j+=1
                        if not os.path.isfile(raster.archive) or not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme) or args.overwrite or args.force_filter_dems:
                            i+=1
                            task = taskhandler.Task(
                                f,
                                'Pkg{:04g}'.format(i),
                                'python',
                                '{} {} {} {}'.format(scriptpath, arg_str_base, srcfp, scratch),
                                build_archive,
                                [srcfp, scratch, args]
                            )
                            task_queue.append(task)
                                
    
    logger.info('Number of src rasters: {}'.format(j))
    logger.info('Number of incomplete tasks: {}'.format(i))
    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.pbs:
            task_handler = taskhandler.PBSTaskHandler(qsubpath)
            if not args.dryrun:
                task_handler.run_tasks(task_queue)
            
        elif args.parallel_processes > 1:
            task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            logger.info("Number of child processes to spawn: {0}".format(task_handler.num_processes))
            if not args.dryrun:
                task_handler.run_tasks(task_queue)
    
        else:         
            for task in task_queue:
                src, scratch, task_arg_obj = task.method_arg_list
                
                if not args.dryrun:
                    task.method(src, scratch, task_arg_obj)
    
    else:
        logger.info("No tasks found to process")
        
def build_archive(src,scratch,args):

    logger.info("Packaging Raster: {}".format(src))
    raster = dem.SetsmDem(src)
    dstfp = raster.archive
    dstdir, dstfn = os.path.split(raster.archive)
    #print dstfn
    #print dstfp
    
    try:
        raster.get_dem_info()
    except RuntimeError, e:
        logger.error(e)
    else:
        process = True
        
        ## get raster density if not precomputed
        if raster.density is None:
            try:
                raster.compute_density_and_statistics()
            except RuntimeError, e:
                logger.warning(e)
        
        if args.filter_dems:
            # filter dems with area < 5.5 sqkm and density < .1
            
            area = raster.exact_geom.Area()
            if area < 5500000:
                logger.info("Raster area {} falls below threshold: {}".format(area,raster.srcfp))
                process = False
            elif raster.density < 0.1:
                logger.info("Raster density {} falls below threshold: {}".format(raster.density,raster.srcfp))
                process = False
                
            if not process:
                logger.info('Removing {}'.format(raster.srcfp))
                to_remove = glob.glob(raster.srcfp[:-8]+'*')
                for f in to_remove:
                    #logger.info('Removing {}'.format(f))
                    os.remove(f)
                
        if process:
            #### Build mdf
            if not os.path.isfile(raster.mdf) or args.overwrite:
                if os.path.isfile(raster.mdf):
                    if not args.dryrun:
                        os.remove(raster.mdf)
                try:
                    if not args.dryrun:
                        raster.write_mdf_file()
                except RuntimeError, e:
                    logger.error(e)
            
            #### Build Readme
            if not os.path.isfile(raster.readme) or args.overwrite:
                if os.path.isfile(raster.readme):
                    if not args.dryrun:
                        os.remove(raster.readme)
                if not args.dryrun:
                    raster.write_readme_file()
            
            #### Build Archive
            if not args.mdf_only:
                
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
                        os.path.basename(raster.mdf), # mdf
                        os.path.basename(raster.readme), # readme
                        # index shp files
                    )
    
                    optional_components = [os.path.basename(r) for r in raster.reg_files] #reg
                    
                    os.chdir(dstdir)
                    #logger.info(os.getcwd())
                    
                    k = 0
                    existing_components = sum([int(os.path.isfile(component)) for component in components])
                    ### check if exists, print
                    #logger.info(existing_components)
                    if existing_components == len(components):
                        
                        ## Build index
                        index = os.path.join(scratch,raster.stripid+"_index.shp")
                        
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
                            
                                    for field_def in utils.DEM_ATTRIBUTE_DEFINITIONS_BASIC:
                                        
                                        field = ogr.FieldDefn(field_def.fname, field_def.ftype)
                                        field.SetWidth(field_def.fwidth)
                                        field.SetPrecision(field_def.fprecision)
                                        lyr.CreateField(field)
                                        
                                    #print raster.stripid
                                    feat = ogr.Feature(lyr.GetLayerDefn())
                                        
                                    ## Set fields
                                    feat.SetField("DEM_ID",raster.stripid)
                                    feat.SetField("PAIRNAME",raster.pairname)
                                    feat.SetField("SENSOR",raster.sensor)
                                    feat.SetField("ACQDATE",raster.acqdate.strftime("%Y-%m-%d"))
                                    feat.SetField("CATALOGID1",raster.catid1)
                                    feat.SetField("CATALOGID2",raster.catid2)
                                    feat.SetField("ND_VALUE",raster.ndv)
                                    feat.SetField("DEM_NAME",raster.srcfn)
                                    feat.SetField("ALGM_VER",raster.algm_version)
                                    res = (raster.xres + raster.yres) / 2.0
                                    feat.SetField("DEM_RES",res)
                                    feat.SetField("DENSITY",raster.density)
                                    
                                    if raster.version:
                                        feat.SetField("REL_VER",raster.version)
                                    
                                    #### Set fields if populated (will not be populated if metadata file is not found)
                                    if raster.creation_date:
                                        feat.SetField("CR_DATE",raster.creation_date.strftime("%Y-%m-%d"))
                            
                                    ## transfrom and write geom
                                    feat.SetField("PROJ4",raster.proj4)
                                    feat.SetField("EPSG",raster.epsg)
                                    
                                    src_srs = osr.SpatialReference()
                                    src_srs.ImportFromWkt(raster.proj)
                                    
                                    if raster.exact_geom:
                                        geom = raster.exact_geom.Clone()
                                        transform = osr.CoordinateTransformation(src_srs,tgt_srs)
                                        geom.Transform(transform)
                                        
                                        centroid = geom.Centroid()
                                        feat.SetField("CENT_LAT",centroid.GetY())
                                        feat.SetField("CENT_LON",centroid.GetX())
                        
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
                                            archive = tarfile.open(dstfp,"w:gz")
                                            #archive = tarfile.open(dstfp,"w:")
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
                                        
                                        ## Close archive and compress with gz
                                        if not args.dryrun:
                                            try:
                                                archive.close()
                                            except Exception,e:
                                                print e
                                                        
                                                        
                                else:
                                    logger.error('Cannot create layer: {}'.format(index_lyr))    
                            else:
                                logger.error("Cannot create index: {}".format(index))    
                        else:
                            logger.error("Cannot remove existing index: {}".format(index))       
                    else:
                        logger.error("Not enough existing components to make a valid archive: {} ({} found, {} required)".format(raster.srcfp,existing_components,len(components)))
            

if __name__ == '__main__':
    main()
        
            
            
        
        
        
        