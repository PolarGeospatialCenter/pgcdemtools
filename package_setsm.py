import os, sys, string, shutil, glob, re, logging, tarfile, zipfile
from datetime import *
from osgeo import gdal, osr, ogr, gdalconst
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
    # parser.add_argument('--lsf', action='store_true', default=False,
    #                     help="package LSF DEM instead of original DEM. Includes metadata flag.")
    parser.add_argument('--filter-dems', action='store_true', default=False,
                        help="filter dems with area < 5.6 sqkm and density < 0.1")
    parser.add_argument('--force-filter-dems', action='store_true', default=False,
                        help="filter dems where tar has already been built with area < 5.6 sqkm and density < 0.1")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument("--tasks-per-job", type=int, help="number of tasks to bundle into a single job (requires pbs option)")
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
    
    if args.tasks_per_job and not args.pbs:
        parser.error("jobs-per-task argument requires the pbs option")
    
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
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes', 'tasks_per_job')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    # if args.lsf:
    #     logger.info('Packaging LSF DEMs')
    # else:
    #     logger.info('Packaging non-LSF DEMs')
    
    j=0
    scenes = []
    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src) and src.endswith('.tif'):
        logger.debug(src)
        try:
            raster = dem.SetsmDem(src)
        except RuntimeError as e:
            logger.error( e )
        else:
            j+=1
            if args.overwrite or args.force_filter_dems:
                scenes.append(src)
            elif args.mdf_only:
                if (not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme)):
                    scenes.append(src)
            elif not os.path.isfile(raster.archive) or not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme):
                scenes.append(src)
                
    elif os.path.isfile(src) and src.endswith('.txt'):
        fh = open(src,'r')
        for line in fh.readlines():
            sceneid = line.strip()
            
            try:
                raster = dem.SetsmDem(sceneid)
            except RuntimeError as e:
                logger.error( e )
            else:
                j+=1
                if args.overwrite or args.force_filter_dems:
                    scenes.append(sceneid)
                elif args.mdf_only:
                    if (not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme)):
                        scenes.append(sceneid)
                elif not os.path.isfile(raster.archive) or not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme):
                    scenes.append(sceneid)
    
    elif os.path.isdir(src):
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    try:
                        raster = dem.SetsmDem(srcfp)
                    except RuntimeError as e:
                        logger.error( e )
                    else:
                        j+=1
                        if args.overwrite or args.force_filter_dems:
                            scenes.append(srcfp)
                        elif args.mdf_only:
                            if (not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme)):
                                scenes.append(srcfp)
                        elif not os.path.isfile(raster.archive) or not os.path.isfile(raster.mdf) or not os.path.isfile(raster.readme):
                            scenes.append(srcfp)
                            
    else:
        logger.error( "src must be a directory, a strip dem, or a text file")
    
    scenes = list(set(scenes))
    logger.info('Number of src rasters: {}'.format(j))
    logger.info('Number of incomplete tasks: {}'.format(len(scenes)))
    
    tm = datetime.now()
    job_count=0
    scene_count=0
    scenes_in_job_count=0
    task_queue = []
    
    for srcfp in scenes:
        scene_count+=1
        srcdir, srcfn = os.path.split(srcfp)            
        if args.tasks_per_job:
            # bundle tasks into text files in the dst dir and pass the text file in as src
            scenes_in_job_count+=1
            src_txt = os.path.join(scratch,'src_dems_{}_{}.txt'.format(tm.strftime("%Y%m%d%H%M%S"),job_count))
            
            if scenes_in_job_count == 1:
                # remove text file if dst already exists
                try:
                    os.remove(src_txt)
                except OSError:
                    pass
                
            if scenes_in_job_count <= args.tasks_per_job:
                # add to txt file
                fh = open(src_txt,'a')
                fh.write("{}\n".format(srcfp))
                fh.close()
            
            if scenes_in_job_count == args.tasks_per_job or scene_count == len(scenes):
                scenes_in_job_count=0
                job_count+=1
                
                task = taskhandler.Task(
                    'Pkg{:04g}'.format(job_count),
                    'Pkg{:04g}'.format(job_count),
                    'python',
                    '{} {} {} {}'.format(scriptpath, arg_str_base, src_txt, scratch),
                    build_archive,
                    [srcfp, scratch, args]
                )
                task_queue.append(task)
            
        else:
            job_count += 1
            task = taskhandler.Task(
                srcfn,
                'Pkg{:04g}'.format(job_count),
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str_base, srcfp, scratch),
                build_archive,
                [srcfp, scratch, args]
            )
            task_queue.append(task)
       
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
    except RuntimeError as e:
        logger.error(e)
    else:
        process = True
        
        ## get raster density if not precomputed
        if raster.density is None:
            try:
                raster.compute_density_and_statistics()
            except RuntimeError as e:
                logger.warning(e)
        
        if args.filter_dems or args.force_filter_dems:
            # filter dems with area < 5.5 sqkm and density < .1
            
            area = raster.geom.Area()
            # logger.info(raster.density)
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
                except RuntimeError as e:
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
                            print("Cannot replace archive: %s" %dstfp)
            
                if not os.path.isfile(dstfp):    

                    # if args.lsf:
                    #     components = (
                    #         os.path.basename(raster.srcfp).replace("dem.tif","dem_smooth.tif"), # dem
                    #         os.path.basename(raster.matchtag), # matchtag
                    #         os.path.basename(raster.mdf), # mdf
                    #         os.path.basename(raster.readme), # readme
                    #         os.path.basename(raster.browse), # browse
                    #         # index shp files
                    #     )
                    # else:
                    components = (
                        os.path.basename(raster.srcfp), # dem
                        os.path.basename(raster.matchtag), # matchtag
                        os.path.basename(raster.mdf), # mdf
                        os.path.basename(raster.readme), # readme
                        os.path.basename(raster.browse), # browse
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
                        except RuntimeError as e:
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
                                    valid_record = True

                                    ## Set fields
                                    attrib_map = {
                                        'DEM_ID': raster.stripid,
                                        'STRIPDEMID': raster.stripdemid,
                                        'PAIRNAME': raster.pairname,
                                        'SENSOR1': raster.sensor1,
                                        'SENSOR2': raster.sensor2,
                                        'ACQDATE1': raster.acqdate1.strftime('%Y-%m-%d'),
                                        'ACQDATE2': raster.acqdate2.strftime('%Y-%m-%d'),
                                        'CATALOGID1': raster.catid1,
                                        'CATALOGID2': raster.catid2,
                                        'GEOCELL': raster.geocell,

                                        'PROJ4': raster.proj4,
                                        'EPSG': raster.epsg,
                                        'ND_VALUE': raster.ndv,
                                        'DEM_RES': (raster.xres + raster.yres) / 2.0,
                                        'ALGM_VER': raster.algm_version,
                                        'IS_LSF': int(raster.is_lsf),
                                        'IS_XTRACK': int(raster.is_xtrack),
                                        'EDGEMASK': int(raster.mask_tuple[0]),
                                        'WATERMASK': int(raster.mask_tuple[1]),
                                        'CLOUDMASK': int(raster.mask_tuple[2]),
                                        'DENSITY': raster.density

                                    }
                                    
                                    #### Set fields if populated (will not be populated if metadata file is not found)
                                    if raster.creation_date:
                                        attrib_map["CR_DATE"] = raster.creation_date.strftime("%Y-%m-%d")
                            
                                    ## transform and write geom
                                    src_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
                                    src_srs.ImportFromWkt(raster.proj)

                                    if not raster.geom:
                                        logger.error('No valid geom found, feature skipped: {}'.format(raster.sceneid))
                                        valid_record = False
                                    else:
                                        temp_geom = raster.geom.Clone()
                                        transform = osr.CoordinateTransformation(src_srs, tgt_srs)
                                        try:
                                            temp_geom.Transform(transform)
                                        except TypeError as e:
                                            logger.error('Geom transformation failed, feature skipped: {} {}'.format(e, raster.sceneid))
                                            valid_record = False
                                        else:

                                            ## Get centroid coordinates
                                            centroid = temp_geom.Centroid()
                                            attrib_map['CENT_LAT'] = centroid.GetY()
                                            attrib_map['CENT_LON'] = centroid.GetX()

                                            ## If srs is geographic and geom crosses 180, split geom into 2 parts
                                            if tgt_srs.IsGeographic:

                                                ## Get Lat and Lon coords in arrays
                                                lons = []
                                                lats = []
                                                ring = temp_geom.GetGeometryRef(0)  # assumes a 1 part polygon
                                                for j in range(0, ring.GetPointCount()):
                                                    pt = ring.GetPoint(j)
                                                    lons.append(pt[0])
                                                    lats.append(pt[1])

                                                ## Test if image crosses 180
                                                if max(lons) - min(lons) > 180:
                                                    split_geom = wrap_180(temp_geom)
                                                    feat_geom = split_geom
                                                else:
                                                    mp_geom = ogr.ForceToMultiPolygon(temp_geom)
                                                    feat_geom = mp_geom

                                            else:
                                                mp_geom = ogr.ForceToMultiPolygon(temp_geom)
                                                feat_geom = mp_geom

                                    ## Add new feature to layer
                                    if valid_record:
                                        for fld, val in attrib_map.items():
                                            feat.SetField(fld, val)
                                        feat.SetGeometry(feat_geom)
                                        lyr.CreateFeature(feat)

                                    # Close layer and dataset
                                    ds = None
                                    lyr = None

                                    if os.path.isfile(index):
                                        ## Create archive
                                        if not args.dryrun:
                                            archive = tarfile.open(dstfp,"w:gz")
                                            if not os.path.isfile(dstfp):
                                                logger.error("Cannot create archive: {}".format(dstfn))
                                    
                                        ## Add components
                                        for component in components:
                                            logger.debug("Adding {} to {}".format(component,dstfn))
                                            k+=1
                                            if "dem_smooth.tif" in component:
                                                arcn = component.replace("dem_smooth.tif","dem.tif")
                                            else:
                                                arcn = component
                                            if not args.dryrun:
                                                archive.add(component,arcname=arcn)
    
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
                                            arcn = os.path.join("index",f)
                                            logger.debug("Adding {} to {}".format(f,dstfn))
                                            k+=1
                                            if not args.dryrun:
                                                archive.add(f,arcname=arcn)
                                            os.remove(f)
                                        
                                        logger.info("Added {} items to archive: {}".format(k,dstfn))
                                        
                                        ## Close archive and compress with gz
                                        if not args.dryrun:
                                            try:
                                                archive.close()
                                            except Exception as e:
                                                print(e)
                                                        
                                                        
                                else:
                                    logger.error('Cannot create layer: {}'.format(index_lyr))    
                            else:
                                logger.error("Cannot create index: {}".format(index))    
                        else:
                            logger.error("Cannot remove existing index: {}".format(index))       
                    else:
                        logger.error("Not enough existing components to make a valid archive: {} ({} found, {} required)".format(raster.srcfp,existing_components,len(components)))


def wrap_180(src_geom):

    ## create 2 point lists for west component and east component
    west_points = []
    east_points = []

    ## for each point in geom except final point
    ring  = src_geom.GetGeometryRef(0)  #### assumes a 1 part polygon
    for i in range(0, ring.GetPointCount()-1):
        pt1 = ring.GetPoint(i)
        pt2 = ring.GetPoint(i+1)

        ## test if point is > or < 0 and add to correct bin
        if pt1[0] < 0:
            west_points.append(pt1)
        else:
            east_points.append(pt1)

        ## test if segment to next point crosses 180 (x is opposite sign)
        if (pt1[0] > 0) - (pt1[0] < 0) != (pt2[0] > 0) - (pt2[0] < 0):

            ## if segment crosses,calculate interesection point y value
            pt3_y = calc_y_intersection_180(pt1, pt2)

            ## add intersection point to both bins (make sureot change 180 to -180 for western list)
            pt3_west = ( -180, pt3_y )
            pt3_east = ( 180, pt3_y )

            west_points.append(pt3_west)
            east_points.append(pt3_east)


    #print "west", len(west_points)
    #for pt in west_points:
    #    print pt[0], pt[1]
    #
    #print "east", len(east_points)
    #for pt in east_points:
    #    print pt[0], pt[1]

    ## cat point lists to make multipolygon(remember to add 1st point to the end)
    geom_multipoly = ogr.Geometry(ogr.wkbMultiPolygon)

    for ring_points in west_points, east_points:
        if len(ring_points) > 0:
            poly = ogr.Geometry(ogr.wkbPolygon)
            ring = ogr.Geometry(ogr.wkbLinearRing)

            for pt in ring_points:
                ring.AddPoint(pt[0],pt[1])

            ring.AddPoint(ring_points[0][0],ring_points[0][1])

            poly.AddGeometry(ring)
            geom_multipoly.AddGeometry(poly)
            del poly
            del ring

    #print geom_multipoly
    return geom_multipoly


if __name__ == '__main__':
    main()
