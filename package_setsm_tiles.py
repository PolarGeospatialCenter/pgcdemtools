import argparse
import glob
import logging
import os
import subprocess
import sys
import tarfile
from datetime import *

from osgeo import osr, ogr, gdal, gdalconst

from lib import utils, dem, taskhandler, VERSION, SHORT_VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ogrDriver = ogr.GetDriverByName("ESRI Shapefile")

default_epsg = 4326


def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="package setsm dems (build mdf and readme files and create archive) in place in the filesystem"
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('scratch', help="scratch space to build index shps")

    #### Optionsl Arguments
    parser.add_argument('--epsg', type=int, default=default_epsg,
                        help="egsg code for output index projection (default epsg:{})".format(default_epsg))
    parser.add_argument('--skip-archive', action='store_true', default=False,
                        help="build mdf and readme files and convert rasters to COG, do not archive")
    parser.add_argument('--rasterproxy-prefix',
                        help="build rasterProxy .mrf files using this s3 bucket and path prefix\
                             for the source data path with geocell folder and dem tif appended (must start with s3://)")
    parser.add_argument('--convert-to-cog', action='store_true', default=False,
                        help="convert dem files to COG before building the archive")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="qsub script to use in PBS submission (default is qsub_package.sh in script root folder)")
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')
    #### Parse Arguments
    args = parser.parse_args()

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    if not os.path.isdir(args.scratch) and not os.path.isfile(args.scratch):
        parser.error("Source directory or file does not exist: %s" %args.scratch)

    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    scratch = os.path.abspath(args.scratch)

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

    # Check raster proxy prefix is well-formed
    if args.rasterproxy_prefix and not args.rasterproxy_prefix.startswith('s3://'):
        parser.error('--rasterproxy-prefix must start with s3://')

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
    pos_arg_keys = ['src','scratch']
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes')
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
                if args.convert_to_cog:
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
        # logger.info("[{}/{}] {}".format(i,total,raster.srcfp))
        task = taskhandler.Task(
            raster.srcfn,
            'Pkg_{}'.format(raster.tileid),
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
            ]

        cog_params = {
            os.path.basename(raster.srcfp): ('YES', 'BILINEAR'),  # dem
            os.path.basename(raster.browse): ('YES', 'CUBIC'),  # browse
        }

        tifs = [c for c in components + optional_components if c.endswith('.tif') and os.path.isfile(c)]

        ## create rasterproxy MRF file
        if args.rasterproxy_prefix:
            logger.info("Creating raster proxy files")
            rasterproxy_prefix_parts = args.rasterproxy_prefix.split('/')
            bucket = rasterproxy_prefix_parts[2]
            bpath = '/'.join(rasterproxy_prefix_parts[3:]).strip(r'/')
            sourceprefix = '/vsicurl/http://{}.s3.us-west-2.amazonaws.com/{}'.format(bucket, bpath)
            dataprefix = 'z:/mrfcache/{}/{}'.format(bucket, bpath)
            for tif in tifs:
                suffix = tif[len(raster.tileid):-4]  # eg "_dem"
                mrf = '{}{}.mrf'.format(raster.tileid, suffix)
                if not os.path.isfile(mrf):
                    sourcepath = '{}/{}/{}{}.tif'.format(
                        sourceprefix,
                        raster.supertile_id_no_res,
                        raster.tileid,
                        suffix
                    )
                    datapath = '{}/{}/{}{}.mrfcache'.format(
                        dataprefix,
                        raster.supertile_id_no_res,
                        raster.tileid,
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

        ## Convert all rasters to COG in place (should no longer be needed)
        if args.convert_to_cog:
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

                            lyr = ds.CreateLayer(index_lyr, tgt_srs, ogr.wkbPolygon)

                            if lyr is not None:

                                for field_def in utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC + utils.DEM_ATTRIBUTE_DEFINITION_RELVER:
                                    if field_def.ftype == ogr.OFTDateTime:
                                        ftype = ogr.OFTDate
                                    else:
                                        ftype = field_def.ftype
                                    field = ogr.FieldDefn(field_def.fname, ftype)
                                    field.SetWidth(field_def.fwidth)
                                    field.SetPrecision(field_def.fprecision)
                                    lyr.CreateField(field)

                                feat = ogr.Feature(lyr.GetLayerDefn())
                                valid_record = True

                                ## Set fields
                                attrib_map = {
                                    "DEM_ID": raster.tileid,
                                    "TILE": raster.supertile_id_no_res,
                                    "ND_VALUE": raster.ndv,
                                    "DEM_RES" : (raster.xres + raster.yres) / 2.0,
                                    "DENSITY": raster.density,
                                    "NUM_COMP": raster.num_components
                                }

                                if raster.release_version:
                                    attrib_map["REL_VER"] = raster.release_version

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
