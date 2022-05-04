import os, sys, string, shutil, glob, re, logging, tarfile, zipfile
from datetime import *
from osgeo import gdal, osr, ogr, gdalconst
import argparse
from lib import utils, dem, taskhandler

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
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--epsg', type=int, default=default_epsg,
                        help="egsg code for output index projection (default epsg:{})".format(default_epsg))
    parser.add_argument('--rasterproxy-prefix',
                        help="build rasterProxy .mrf files using this s3 bucket and path prefix\
                             for the source data path with geocell folder and dem tif appended (must start with s3://)")
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

            rp = os.path.join(raster.srcdir, raster.stripid + '_dem.mrf')
            if args.overwrite or args.force_filter_dems:
                rasters.append(rp)

            else:
                expected_outputs = [
                    #raster.readme
                ]
                if not args.skip_archive:
                    expected_outputs.append(raster.archive)
                if args.rasterproxy_prefix:
                    # this checks for only 1 of the several rasterproxies that are expected
                    expected_outputs.append(rp)

                if not all([os.path.isfile(f) for f in expected_outputs]):
                    rasters.append(rp)

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
    #### create archive
    dstfp = raster.archive
    dstdir, dstfn = os.path.split(raster.archive)
    #print dstfn
    #print dstfp

    try:
        raster.get_dem_info()
    except RuntimeError as e:
        logger.error(e)
        # print(raster.ndv)
    else:

        ## get raster density if not precomputed
        if raster.density is None:
            try:
                raster.compute_density_and_statistics()
            except RuntimeError as e:
                logger.warning(e)

        #### Build Archive
        if os.path.isfile(dstfp) and args.overwrite is True:
            if not args.dryrun:
                try:
                    os.remove(dstfp)
                except:
                    print("Cannot replace archive: %s" %dstfp)

        if not os.path.isfile(dstfp):
            os.chdir(dstdir)
            #logger.info(os.getcwd())

            components = (
                os.path.basename(raster.srcfp), # dem
                os.path.basename(raster.metapath), # meta
                # index shp files
            )

            optional_components = [
                os.path.basename(raster.regmetapath), #reg
                os.path.basename(raster.err), # err
                os.path.basename(raster.day), # day
                os.path.basename(raster.browse), # browse
                os.path.basename(raster.count),
                os.path.basename(raster.countmt),
                os.path.basename(raster.mad),
                os.path.basename(raster.mindate),
                os.path.basename(raster.maxdate),
                ]

            tifs = [c for c in components if c.endswith('.tif')]

            k = 0
            existing_components = sum([int(os.path.isfile(component)) for component in components])
            ### check if exists, print
            #logger.info(existing_components)
            if existing_components == len(components):

                ## create rasterproxy MRF file
                if args.rasterproxy_prefix:
                    logger.info("Creating RasterProxy files")
                    rasterproxy_prefix_parts = args.rasterproxy_prefix.split('/')
                    bucket = rasterproxy_prefix_parts[2]
                    bpath = '/'.join(rasterproxy_prefix_parts[3:])
                    sourceprefix = '/vsicurl/http://{}.s3.us-west-2.amazonaws.com/{}'.format(bucket, bpath)
                    dataprefix = 'z:/mrfcache/{}/{}'.format(bucket, bpath)
                    for tif in tifs:
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

                ## Build index
                index = os.path.join(scratch,raster.tileid+"_index.shp")

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
                        tgt_srs = osr.SpatialReference()
                        tgt_srs.ImportFromEPSG(args.epsg)

                        lyr = ds.CreateLayer(index_lyr, tgt_srs, ogr.wkbPolygon)

                        if lyr is not None:

                            for field_def in utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC + utils.DEM_ATTRIBUTE_DEFINITION_RELVER:

                                field = ogr.FieldDefn(field_def.fname, field_def.ftype)
                                field.SetWidth(field_def.fwidth)
                                field.SetPrecision(field_def.fprecision)
                                lyr.CreateField(field)

                            #print raster.stripid
                            feat = ogr.Feature(lyr.GetLayerDefn())

                            ## Set fields
                            feat.SetField("DEM_ID",raster.tileid)
                            feat.SetField("TILE",raster.supertile_id)
                            feat.SetField("ND_VALUE",raster.ndv)
                            res = (raster.xres + raster.yres) / 2.0
                            feat.SetField("DEM_RES",res)
                            feat.SetField("DENSITY",raster.density)
                            feat.SetField("NUM_COMP",raster.num_components)

                            if raster.release_version:
                                feat.SetField("REL_VER",raster.release_version)

                            if raster.reg_src:
                                feat.SetField("REG_SRC",raster.reg_src)
                                feat.SetField("NUM_GCPS",raster.num_gcps)
                            if raster.mean_resid_z:
                                feat.SetField("MEANRESZ",raster.mean_resid_z)

                            #### Set fields if populated (will not be populated if metadata file is not found)
                            if raster.creation_date:
                                feat.SetField("CR_DATE",raster.creation_date.strftime("%Y-%m-%d"))

                            ## transfrom and write geom
                            src_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
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
