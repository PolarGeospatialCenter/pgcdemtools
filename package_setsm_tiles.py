import argparse
import glob
import logging
import os
import subprocess
import sys
import tarfile
import traceback
from datetime import *

from osgeo import osr, ogr, gdal, gdalconst

from lib import utils, dem, taskhandler, VERSION, SHORT_VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ogrDriver = ogr.GetDriverByName("ESRI Shapefile")
default_epsg = 4326

submission_script_map = {
    'pbs': 'pbs_package.sh',
    'slurm': 'slurm_package.sh'
}


def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="package setsm dems (build mdf and readme files and create archive) in place in the filesystem"
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('scratch', help="scratch space to build index shps")
    parser.add_argument('--project', required=True, choices=utils.PROJECTS.keys(),
                        help='project name')

    #### Optionsl Arguments
    parser.add_argument('--epsg', type=int, default=default_epsg,
                        help="egsg code for output index projection (default epsg:{})".format(default_epsg))
    parser.add_argument('--skip-cog', action='store_true', default=False,
                        help="skip converting dem files to COG before building the archive")
    parser.add_argument('--skip-archive', action='store_true', default=False,
                        help="build mdf and readme files and convert rasters to COG, do not build archive")
    parser.add_argument('--build-rasterproxies', action='store_true', default=False,
                        help='build rasterproxy .mrf files')
    parser.add_argument('--rasterproxy-prefix',
                        default="s3://pgc-opendata-dems/<project>/<type>/<version>/<resolution>/<group>/<dem_id>",
                        help="template for rasterproxy .mrf file s3 path")
    parser.add_argument('--release-fileurl', type=str,
                        default="https://data.pgc.umn.edu/elev/dem/setsm/<project>/<type>/<version>/<resolution>/<group>/<dem_id>.tar.gz",
                        help="template for release field 'fileurl'")
    parser.add_argument('--release-s3url', type=str,
                        default="https://polargeospatialcenter.github.io/stac-browser/#/external/pgc-opendata-dems.s3.us-west-2.amazonaws.com/<project>/<type>/<version>/<resolution>/<group>/<dem_id>.json",
                        help="template for release field 's3url'")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')
    parser.add_argument("--slurm-job-name", default=None,
                        help="assign a name to the slurm job for easier job tracking")

    pos_arg_keys = ['src', 'scratch']
    arg_keys_to_remove = utils.SCHEDULER_ARGS + ['dryrun']
    utils.add_scheduler_options(parser, submission_script_map)

    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    scratch = os.path.abspath(args.scratch)

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    if not os.path.isdir(args.scratch) and not os.path.isfile(args.scratch):
        parser.error("Source directory or file does not exist: %s" %args.scratch)

    ## Verify qsubscript
    qsubpath = utils.verify_scheduler_args(parser, args, scriptpath, submission_script_map)

    # Check raster proxy prefix is well-formed
    if args.rasterproxy_prefix and not args.rasterproxy_prefix.startswith('s3://'):
        parser.error('--rasterproxy-prefix (e.g. s3://pgc-opendata-dems/arcticdem/mosaics/v4.1/2m)')

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
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)

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

    logger.info("Current version: %s", VERSION)

    #### ID rasters
    logger.info('Identifying DEMs')
    raster_paths = []
    if os.path.isfile(src) and src.endswith('.tif'):
        logger.debug(src)
        raster_paths.append(src)

    elif os.path.isfile(src) and src.endswith('.txt'):
        fh = open(src,'r')
        for line in fh.readlines():
            sceneid = line.strip()
            raster_paths.append(sceneid)

    elif os.path.isdir(src):
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    raster_paths.append(srcfp)

    else:
        logger.error("src must be a directory, a dem mosaic tile, or a text file")

    logger.info('Reading rasters')
    raster_paths = list(set(raster_paths))
    rasters = []
    j = 0
    total = len(raster_paths)

    for rp in raster_paths:
        try:
            raster = dem.SetsmTile(rp)
        except RuntimeError as e:
            logger.error(e)
        else:
            j += 1
            utils.progress(j, total, "DEMs identified")

            cog_sem = os.path.join(raster.srcdir, raster.tileid + '.cogfin')
            suffix = raster.srcfn[len(raster.tileid):-4]  # eg "_dem"
            proxy = '{}/{}{}.mrf'.format(raster.srcdir, raster.tileid, suffix)
            if args.overwrite:
                rasters.append(raster)

            else:
                expected_outputs = [
                    #raster.readme
                ]
                if not args.skip_cog:
                    expected_outputs.append(cog_sem)
                if not args.skip_archive:
                    expected_outputs.append(raster.archive)
                if args.rasterproxy_prefix:
                    # this checks for only 1 of the several rasterproxies that are expected
                    expected_outputs.append(proxy)

                if not all([os.path.isfile(f) for f in expected_outputs]):
                    rasters.append(raster)

    logger.info('Number of src rasters: {}'.format(j))
    logger.info('Number of incomplete tasks: {}'.format(len(rasters)))

    task_queue = []
    logger.info('Packaging DEMs')
    for raster in rasters:

        # add a custom name to the job
        if not args.slurm_job_name:
            job_name = 'Pkg_{}'.format(raster.tileid)
        else:
            job_name = str(args.slurm_job_name)

        task = taskhandler.Task(
            raster.srcfn,
            job_name,
            'python',
            '{} {} {} {}'.format(scriptpath, arg_str_base, raster.srcfp, scratch),
            build_archive,
            [raster, scratch, args]
        )
        task_queue.append(task)

    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.scheduler:
            try:
                task_handler = taskhandler.get_scheduler_taskhandler(args.scheduler, qsubpath)
            except RuntimeError as e:
                logger.error(e)
            else:
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

    logger.info("Packaging tile {}".format(raster.srcfn))
    dstfp = raster.archive
    dstdir, dstfn = os.path.split(raster.archive)

    try:
        raster.get_dem_info()
    except RuntimeError as e:
        logger.error(e)
    else:

        ## get raster density if not precomputed
        if raster.density is None:
            try:
                raster.compute_density_and_statistics()
            except RuntimeError as e:
                logger.warning(e)

        os.chdir(dstdir)

        components = [
            os.path.basename(raster.srcfp), # dem
            os.path.basename(raster.metapath), # meta
            # index shp files
        ]

        optional_components = [
            os.path.basename(raster.regmetapath), #reg
            os.path.basename(raster.err), # err
            os.path.basename(raster.day), # day
            os.path.basename(raster.browse), # browse
            os.path.basename(raster.count),
            #os.path.basename(raster.countmt), # except countmt due to a bug in it's construction
            os.path.basename(raster.mad),
            os.path.basename(raster.mindate),
            os.path.basename(raster.maxdate),
            os.path.basename(raster.datamask),
            ]

        cog_params = {
            # ( path, lzw predictor, resample strategy)
            os.path.basename(raster.srcfp): ('YES', 'BILINEAR'),  # dem
            os.path.basename(raster.browse): ('YES', 'CUBIC'),  # browse
            os.path.basename(raster.count): ('NO', 'NEAREST'),
            os.path.basename(raster.mad): ('YES', 'BILINEAR'),
            os.path.basename(raster.mindate): ('NO', 'NEAREST'),
            os.path.basename(raster.maxdate): ('NO', 'NEAREST'),
            os.path.basename(raster.datamask): ('NO', 'NEAREST'),
        }

        ## create rasterproxy MRF file
        mrf_tifs = [c for c in components + optional_components if c.endswith('dem.tif') and os.path.isfile(c)]
        if args.rasterproxy_prefix:
            logger.info("Creating raster proxy files")
            rp = args.rasterproxy_prefix
            rp = rp.replace('<project>', args.project)
            rp = rp.replace('<type>', 'mosaics')
            rp = rp.replace('<version>', f'v{raster.release_version}')
            rp = rp.replace('<resolution>', raster.res_str)
            rp = rp.replace('<group>', raster.supertile_id_no_res)
            rp = rp.replace('<dem_id>', raster.id)
            rasterproxy_prefix_parts = rp.split('/')
            bucket = rasterproxy_prefix_parts[2]
            bpath = '/'.join(rasterproxy_prefix_parts[3:]).strip(r'/')
            sourceprefix = '/vsicurl/http://{}.s3.us-west-2.amazonaws.com/{}'.format(bucket, bpath)
            dataprefix = 'z:/mrfcache/{}/{}'.format(bucket, bpath)
            for tif in mrf_tifs:
                suffix = tif[len(raster.tileid):-4]  # eg "_dem"
                mrf = '{}{}.mrf'.format(raster.tileid, suffix)
                if not os.path.isfile(mrf):
                    sourcepath = f'{sourceprefix}{suffix}.tif'
                    datapath = f'{dataprefix}{suffix}.mrfcache'
                    static_args = '-q -of MRF -co BLOCKSIZE=512 -co "UNIFORM_SCALE=2" -co COMPRESS=LERC -co NOCOPY=TRUE'
                    cmd = 'gdal_translate {0} -co INDEXNAME={1} -co DATANAME={1} -co CACHEDSOURCE={2} {3} {4}'.format(
                        static_args,
                        datapath,
                        sourcepath,
                        tif,
                        mrf
                    )
                    rc = subprocess.call(cmd, shell=True)
                    if rc != 0:
                        logger.error("Received non-zero return code ({}) from gdal_translate call".format(rc))
                    if not os.path.isfile(mrf):
                        logger.error("Raster proxy file was not created")
                    else:
                        remove_output = False
                        if rc != 0:
                            logger.error("Removing output raster proxy file because non-zero return code was hit: {}".format(mrf))
                            remove_output = True
                        elif os.path.getsize(mrf) == 0:
                            logger.error("Created raster proxy file size is zero, removing: {}".format(mrf))
                            remove_output = True
                        if remove_output:
                            os.remove(mrf)

        ## Convert all rasters to COG in place (should no longer be needed)
        tifs = [c for c in components + optional_components if c.endswith('.tif') and os.path.isfile(c)]
        if not args.skip_cog:
            cog_sem = raster.tileid + '.cogfin'
            if os.path.isfile(cog_sem) and not args.overwrite:
                logger.info('COG conversion already complete')

            else:
                logger.info("Converting Rasters to COG")
                cog_cnt = 0
                for tif in tifs:
                    if tif in cog_params:
                        predictor, resample = cog_params[tif]
                        if os.path.isfile(tif):

                            # if tif is already COG, increment cnt and move on
                            if not args.overwrite:
                                ds = gdal.Open(tif, gdalconst.GA_ReadOnly)
                                if 'LAYOUT=COG' in ds.GetMetadata_List('IMAGE_STRUCTURE'):
                                    cog_cnt += 1
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
                            # logger.info(cmd)
                            subprocess.call(cmd, shell=True)

                            # delete original tif and increment cog count if successful
                            if os.path.isfile(cog):
                                os.remove(tif)
                                os.rename(cog, tif)
                            if os.path.isfile(tif):
                                cog_cnt += 1

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
                        logger.error("Cannot replace archive: %s" % dstfp)

            if not os.path.isfile(dstfp):
                logger.info("Building archive")

                k = 0
                existing_components = sum([int(os.path.isfile(component)) for component in components])
                if existing_components == len(components):

                    ## Build index
                    index = os.path.join(scratch,raster.tileid+"_index.shp")

                    ## create dem index shp: <tile_id>_index.shp
                    try:
                        index_dir, index_lyr = utils.get_source_names(index)
                    except RuntimeError as e:
                        logger.error("{}: {}".format(index, e))

                    if os.path.isfile(index):
                        ogrDriver.DeleteDataSource(index)

                    if not os.path.isfile(index):
                        ds = ogrDriver.CreateDataSource(index)
                        if ds is not None:
                            tgt_srs = osr.SpatialReference()
                            tgt_srs.ImportFromEPSG(args.epsg)

                            lyr = ds.CreateLayer(index_lyr, tgt_srs, ogr.wkbMultiPolygon)

                            if lyr is not None:

                                for field_def in utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_RELEASE:
                                    fname = field_def.fname.lower()
                                    fstype = None
                                    if field_def.ftype == ogr.OFTDateTime:
                                        ftype = ogr.OFTString
                                        fwidth = 28
                                    elif field_def.ftype == ogr.OFSTBoolean:
                                        ftype = ogr.OFTInteger
                                        fstype = field_def.ftype
                                        fwidth = field_def.fwidth
                                    else:
                                        ftype = field_def.ftype
                                        fwidth = field_def.fwidth
                                    field = ogr.FieldDefn(fname, ftype)
                                    if fstype:
                                        field.SetSubType(fstype)
                                    field.SetWidth(fwidth)
                                    field.SetPrecision(field_def.fprecision)
                                    lyr.CreateField(field)

                                feat = ogr.Feature(lyr.GetLayerDefn())
                                valid_record = True

                                ## Set fields
                                attrib_map = {
                                    "DEM_ID": raster.tileid,
                                    "TILE": raster.tile_id_no_res,
                                    "SUPERTILE": raster.supertile_id_no_res,
                                    "GSD": (raster.xres + raster.yres) / 2.0,
                                    'EPSG': raster.epsg,
                                    "RELEASEVER": raster.release_version,
                                    "DATA_PERC": raster.density,
                                    "NUM_COMP": raster.num_components,
                                }

                                #### Set fields if populated (will not be populated if metadata file is not found)
                                if raster.creation_date:
                                    attrib_map["CR_DATE"] = raster.creation_date.strftime("%Y-%m-%d")

                                filurl = args.release_fileurl
                                pretty_project = utils.PROJECTS[args.project]
                                filurl = filurl.replace('<project>', pretty_project)
                                filurl = filurl.replace('<type>', 'strips')
                                filurl = filurl.replace('<version>', f'v{raster.release_version}')
                                filurl = filurl.replace('<resolution>', raster.res_str)
                                filurl = filurl.replace('<group>', raster.supertile_id_no_res)
                                filurl = filurl.replace('<dem_id>', raster.id)
                                attrib_map['FILEURL'] = filurl

                                s3url = args.release_s3url
                                s3url = s3url.replace('<project>', args.project)
                                s3url = s3url.replace('<type>', 'strips')
                                s3url = s3url.replace('<version>', raster.release_version)
                                s3url = s3url.replace('<resolution>', raster.res_str)
                                s3url = s3url.replace('<group>', raster.supertile_id_no_res)
                                s3url = s3url.replace('<dem_id>', raster.id)
                                attrib_map['S3URL'] = s3url

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

                                ## Close layer and dataset
                                lyr = None
                                ds = None

                                if os.path.isfile(index):
                                    archive = None
                                    remove_output = False
                                    try:
                                        ## Create archive
                                        if not args.dryrun:
                                            archive = tarfile.open(dstfp,"w:gz")
                                            if not os.path.isfile(dstfp):
                                                raise RuntimeError("Cannot create archive: {}".format(dstfn))

                                        ## Add components
                                        for component in components:
                                            logger.debug("Adding {} to {}".format(component, dstfn))
                                            k+=1
                                            if not args.dryrun:
                                                archive.add(component)

                                        ## Add optional components
                                        for component in optional_components:
                                            if os.path.isfile(component):
                                                logger.debug("Adding {} to {}".format(component, dstfn))
                                                k+=1
                                                if not args.dryrun:
                                                    archive.add(component)

                                        ## Add index in subfolder
                                        os.chdir(scratch)
                                        for f in glob.glob(index_lyr+".*"):
                                            arcname = os.path.join("index", f)
                                            logger.debug("Adding {} to {}".format(f, dstfn))
                                            k+=1
                                            if not args.dryrun:
                                                archive.add(f, arcname=arcname)
                                            os.remove(f)

                                        logger.info("Added {} items to archive: {}".format(k, dstfn))

                                    except Exception as e:
                                        traceback.print_exc()
                                        logger.error("Caught exception during creation of output archive file {}; error message: {}".format(dstfp, e))
                                        if not args.dryrun:
                                            remove_output = True

                                    finally:
                                        if archive is not None:
                                            ## Close archive
                                            try:
                                                archive.close()
                                            except Exception as e:
                                                traceback.print_exc()
                                                logger.error("Caught exception while trying to close archive file {}; error message: {}".format(dstfp, e))
                                        if os.path.isfile(dstfp):
                                            if os.path.getsize(dstfp) == 0:
                                                logger.error("Output archive file size is zero: {}".format(dstfp))
                                                remove_output = True
                                            if remove_output:
                                                logger.error("Removing output archive file due to error or zero-size: {}".format(dstfp))
                                                os.remove(dstfp)

                            else:
                                logger.error('Cannot create layer: {}'.format(index_lyr))
                        else:
                            logger.error("Cannot create index: {}".format(index))
                    else:
                        logger.error("Cannot remove existing index: {}".format(index))
                else:
                    logger.error("Not enough existing components to make a valid archive: {} ({} found, {} required)".format(
                        raster.srcfp, existing_components, len(components)))


if __name__ == '__main__':
    main()
