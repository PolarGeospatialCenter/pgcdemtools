import argparse
import glob
import logging
import os
import subprocess
import sys
import tarfile
from datetime import *

from osgeo import gdal, osr, ogr, gdalconst

from lib import utils, dem, taskhandler
from lib import VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ogrDriver = ogr.GetDriverByName("ESRI Shapefile")
tgt_srs = osr.SpatialReference()
tgt_srs.ImportFromEPSG(4326)

AREA_THRESHOLD = 5500000  # Filter threshold in sq meters
DENSITY_THRESHOLD = 0.05   # Masked matchtag density threshold
VALID_AREA_THRESHOLD = 16  # Valid area threshold in sqkm


def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="package setsm dems (build mdf and readme files, convert rasters to COG, and create archive)"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory, text file of file paths, or dem")
    parser.add_argument('scratch', help="scratch space to build index shps")
    
    #### Optional Arguments
    parser.add_argument('--skip-cog', action='store_true', default=False,
                        help="skip COG conversion and build archive with existing tiffs")
    parser.add_argument('--skip-archive', action='store_true', default=False,
                        help="build mdf and readme files and convert rasters to COG, do not archive")
    parser.add_argument('--rasterproxy-prefix',
                        help="build rasterProxy .mrf files using this s3 bucket and path prefix\
                         for the source data path with geocell folder and dem tif appended (must start with s3://)")
    parser.add_argument('--filter-dems', action='store_true', default=False,
                        help="remove dems with valid (masked) area < {} sqkm or masked density < {}".format(
                            VALID_AREA_THRESHOLD, DENSITY_THRESHOLD))
    parser.add_argument('--force-filter-dems', action='store_true', default=False,
                        help="remove already-packaged DEMs with valid (masked) area < {} sqkm or masked density < {}".format(
                            VALID_AREA_THRESHOLD, DENSITY_THRESHOLD))
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument("--tasks-per-job", type=int,
                        help="number of tasks to bundle into a single job (requires pbs option)")
    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="qsub script to use in PBS submission (default is qsub_package.sh in script root folder)")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('-v', '--version', action='store_true', default=False, help='print version and exit')

    pos_arg_keys = ['src','scratch']
    
    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)
    scratch = os.path.abspath(args.scratch)

    if args.version:
        print("Current version: %s", VERSION)
        sys.exit(0)
    
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

    # Check raster proxy prefix is well-formed
    if args.rasterproxy_prefix and not args.rasterproxy_prefix.startswith('s3://'):
        parser.error('--rasterproxy-prefix must start with s3://')
    
    lsh = logging.StreamHandler()
    lsh.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    logger.info("Current version: %s", VERSION)

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes', 'tasks_per_job')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)

    #### ID rasters
    logger.info('Identifying DEMs')
    scene_paths = []
    if os.path.isfile(src) and src.endswith('.tif'):
        logger.debug(src)
        scene_paths.append(src)

    elif os.path.isfile(src) and src.endswith('.txt'):
        fh = open(src,'r')
        for line in fh.readlines():
            sceneid = line.strip()
            scene_paths.append(sceneid)

    elif os.path.isdir(src):
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    scene_paths.append(srcfp)

    else:
        logger.error("src must be a directory, a strip dem, or a text file")

    logger.info('Reading rasters')
    scene_paths = list(set(scene_paths))
    j = 0
    total = len(scene_paths)
    scenes = []
    for sp in scene_paths:
        try:
            raster = dem.SetsmDem(sp)
        except RuntimeError as e:
            logger.error( e )
        else:
            j+=1
            utils.progress(j, total, "DEMs identified")

            cog_sem = os.path.join(raster.srcdir, raster.stripid + '.cogfin')
            rp = os.path.join(raster.srcdir, raster.stripid + '_dem.mrf')
            if args.overwrite or args.force_filter_dems:
                scenes.append(raster)

            else:
                expected_outputs = [
                    raster.mdf,
                    raster.readme
                ]
                if not args.skip_cog:
                    expected_outputs.append(cog_sem)
                if not args.skip_archive:
                    expected_outputs.append(raster.archive)
                if args.rasterproxy_prefix:
                    # this checks for only 1 of the several rasterproxies that are expected
                    expected_outputs.append(rp)

                if not all([os.path.isfile(f) for f in expected_outputs]):
                    scenes.append(raster)

    logger.info('Number of src rasters: {}'.format(j))
    logger.info('Number of incomplete tasks: {}'.format(len(scenes)))
    
    tm = datetime.now()
    job_count=0
    scene_count=0
    scenes_in_job_count=0
    task_queue = []
    
    for raster in scenes:
        scene_count+=1
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
                fh.write("{}\n".format(raster.srcfp))
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
                    [raster, scratch, args]
                )
                task_queue.append(task)
            
        else:
            job_count += 1
            task = taskhandler.Task(
                raster.srcfn,
                'Pkg{:04g}'.format(job_count),
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str_base, raster.srcfp, scratch),
                build_archive,
                [raster, scratch, args]
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
                raster, scratch, task_arg_obj = task.method_arg_list
                
                if not args.dryrun:
                    task.method(raster, scratch, task_arg_obj)
    
    else:
        logger.info("No tasks found to process")


def build_archive(raster, scratch, args):

    logger.info("Packaging Raster: {}".format(raster.srcfp))
    dstfp = raster.archive
    dstdir, dstfn = os.path.split(raster.archive)

    try:
        raster.get_dem_info()
    except RuntimeError as e:
        logger.error(e)
    else:
        process = True
        
        ## get raster density if not precomputed
        needed_attribs = (raster.density, raster.masked_density, raster.max_elev_value, raster.min_elev_value)
        if any([a is None for a in needed_attribs]):
            try:
                raster.compute_density_and_statistics()
            except RuntimeError as e:
                logger.warning(e)
        
        if args.filter_dems or args.force_filter_dems:
            # filter dems with small area or low density
            if raster.valid_area is not None:  # use valid area if that metadata exists, else skip this check
                if raster.valid_area < VALID_AREA_THRESHOLD:
                    logger.info("Raster valid area {} falls below threshold: {}".format(raster.valid_area, raster.srcfp))
                    process = False

            if process:
                if raster.masked_density < DENSITY_THRESHOLD:
                    logger.info("Raster masked density {} falls below threshold: {}".format(raster.masked_density, raster.srcfp))
                    process = False
                
            if not process:
                logger.info('Removing {}'.format(raster.srcfp))
                to_remove = glob.glob(os.path.join(raster.srcdir, raster.stripid + '_*'))
                to_remove2 = glob.glob(os.path.join(raster.srcdir, raster.stripid + '.*'))
                for f in to_remove + to_remove2:
                    os.remove(f)
                
        if process:
            os.chdir(dstdir)

            components = [  # plus index shp files
                #( path, lzw predictor, resample strategy)
                (os.path.basename(raster.srcfp), 'YES', 'BILINEAR'),  # dem
                (os.path.basename(raster.matchtag), 'NO', 'NEAREST'),  # matchtag
                (os.path.basename(raster.mdf), None, None),  # mdf
                (os.path.basename(raster.readme), None, None),  # readme
                (os.path.basename(raster.browse), 'YES', 'CUBIC'),  # browse
                (os.path.basename(raster.browse_masked), 'YES', 'CUBIC'),  # browse with mask
                (os.path.basename(raster.bitmask), 'NO', 'NEAREST'), # bitmask
                # For testing only
                # os.path.basename(raster.srcfp)[:-8] + '_ortho.tif',  # ortho1
                # os.path.basename(raster.srcfp)[:-8] + '_ortho2.tif',  # ortho2
                # os.path.basename(raster.srcfp)[:-8] + '_dem_10m.tif',  # 10m dem
            ]

            optional_components = [os.path.basename(r) for r in raster.reg_files]  # reg

            tifs = [c for c in components if c[0].endswith('.tif')]

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

            ## create rasterproxy MRF file
            if args.rasterproxy_prefix:
                logger.info("Creating raster proxy files")
                rasterproxy_prefix_parts = args.rasterproxy_prefix.split('/')
                bucket = rasterproxy_prefix_parts[2]
                bpath = '/'.join(rasterproxy_prefix_parts[3:]).strip(r'/')
                sourceprefix = '/vsicurl/http://{}.s3.us-west-2.amazonaws.com/{}'.format(bucket, bpath)
                dataprefix = 'z:/mrfcache/{}/{}'.format(bucket, bpath)
                for tif, _, _ in tifs:
                    suffix = tif[len(raster.stripid):-4]  # eg "_dem"
                    mrf = '{}{}.mrf'.format(raster.stripid, suffix)
                    if not os.path.isfile(mrf):
                        sourcepath = '{}/{}/{}{}.tif'.format(
                            sourceprefix,
                            raster.geocell,
                            raster.stripid,
                            suffix
                        )
                        datapath = '{}/{}/{}{}.mrfcache'.format(
                            dataprefix,
                            raster.geocell,
                            raster.stripid,
                            suffix
                        )
                        static_args = '-q -of MRF -co BLOCKSIZE=512 -co "UNIFORM_SCALE=2" -co COMPRESS=LERC -co NOCOPY=TRUE'
                        cmd = 'gdal_translate {0} -co INDEXNAME={1} -co DATANAME={1} -co CACHEDSOURCE={2} {3} {4}'.format(
                            static_args,
                            datapath,
                            sourcepath,
                            tif,
                            mrf
                        )
                        subprocess.call(cmd, shell=True)

            ## Convert all rasters to COG in place
            if not args.skip_cog:
                cog_sem = raster.stripid + '.cogfin'
                if os.path.isfile(cog_sem) and not args.overwrite:
                    logger.info('COG conversion already complete')

                else:
                    logger.info("Converting Rasters to COG")
                    cog_cnt = 0
                    for tif, predictor, resample in tifs:
                        if os.path.isfile(tif):

                            # if tif is already COG, increment cnt and move on
                            if not args.overwrite:
                                ds = gdal.Open(tif, gdalconst.GA_ReadOnly)
                                if 'LAYOUT=COG' in ds.GetMetadata_List('IMAGE_STRUCTURE'):
                                    cog_cnt+=1
                                    logger.info('\tAlready converted: {}'.format(tif))
                                    continue

                            tifbn = os.path.splitext(tif)[0]
                            cog = tifbn + '_cog.tif'
                            logger.info('\tConverting {} with PREDICTOR={}, RESAMPLING={}'.format(
                                tif, predictor, resample))

                            # Remove temp COG file if it exists, it must be a partial file
                            if os.path.isfile(cog):
                                os.remove(cog)

                            cos = '-co overviews=IGNORE_EXISTING -co compress=lzw -co predictor={} -co resampling={} -co bigtiff=yes'.format(
                                predictor, resample)
                            cmd = 'gdal_translate -q -a_srs EPSG:{} -of COG {} {} {}'.format(
                                raster.epsg, cos, tif, cog)
                            #logger.info(cmd)
                            subprocess.call(cmd, shell=True)

                            # delete original tif and increment cog count if successful
                            if os.path.isfile(cog):
                                os.remove(tif)
                                os.rename(cog, tif)
                            if os.path.isfile(tif):
                                cog_cnt+=1

                    # if all tifs are now cog, add semophore file
                    if cog_cnt == len(tifs):
                        open(cog_sem, 'w').close()

            ## Build Archive
            if not args.skip_archive:
                
                if os.path.isfile(dstfp) and args.overwrite is True:
                    if not args.dryrun:
                        try:
                            os.remove(dstfp)
                        except:
                            logger.error("Cannot replace archive: %s" %dstfp)
            
                if not os.path.isfile(dstfp):
                    logger.info("Building archive")

                    k = 0
                    existing_components = sum([int(os.path.isfile(component)) for component, _, _ in components])
                    if existing_components == len(components):
                        
                        ## Build index
                        index = os.path.join(scratch,raster.stripid+"_index.shp")
                        
                        ## create dem index shp: <strip_id>_index.shp
                        try:
                            index_dir, index_lyr = utils.get_source_names(index)
                        except RuntimeError as e:
                            logger.error("{}: {}".format(index, e))
                        
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
                                        'AVGACQTM1': raster.avg_acqtime1.strftime("%Y-%m-%d %H:%M:%S"),
                                        'AVGACQTM2': raster.avg_acqtime2.strftime("%Y-%m-%d %H:%M:%S"),
                                        'CATALOGID1': raster.catid1,
                                        'CATALOGID2': raster.catid2,
                                        'GEOCELL': raster.geocell,

                                        'PROJ4': raster.proj4,
                                        'EPSG': raster.epsg,
                                        'ND_VALUE': raster.ndv,
                                        'DEM_RES': (raster.xres + raster.yres) / 2.0,
                                        'ALGM_VER': raster.algm_version,
                                        'S2S_VER': raster.s2s_version,
                                        'IS_LSF': int(raster.is_lsf),
                                        'IS_XTRACK': int(raster.is_xtrack),
                                        'EDGEMASK': int(raster.mask_tuple[0]),
                                        'WATERMASK': int(raster.mask_tuple[1]),
                                        'CLOUDMASK': int(raster.mask_tuple[2]),
                                        'RMSE': raster.rmse
                                    }
                                    
                                    #### Set fields if populated (will not be populated if metadata file is not found)
                                    if raster.creation_date:
                                        attrib_map["CR_DATE"] = raster.creation_date.strftime("%Y-%m-%d")

                                    for f, a in utils.field_attrib_map.items():
                                        val = getattr(raster, a)
                                        attrib_map[f] = round(val, 6) if val is not None else -9999
                            
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
                                                    split_geom = utils.getWrappedGeometry(temp_geom)
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
                                        for component, _, _ in components:
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


if __name__ == '__main__':
    main()
