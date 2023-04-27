import argparse
import glob
import logging
import math
import os
import shutil
import subprocess
import sys

from osgeo import gdal

from lib import utils, dem, taskhandler, VERSION, SHORT_VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

components = (
    'dem',
    #'matchtag',
    #'ortho',
)

submission_script_map = {
    'pbs': 'pbs_divide.sh',
    'slurm': 'slurm_divide.sh'
}


def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="divide setsm mosaics into subtiles"
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")


    #### Optionsl Arguments
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--num-rows', type=int, default=1,
                        help="number of subtile rows")
    parser.add_argument('--num-cols', type=int, default=1,
                        help="number of subtile columns")
    parser.add_argument('--res', type=int, default=2,
                        help="resolution in meters")
    parser.add_argument('--tiles', help="list of tiles to process, comma delimited")
    parser.add_argument("--dem-version", help="version string (ex: v1.2)")
    parser.add_argument("--cutline-loc", help="directory containing cutline shps indicating areas of bad data")
    parser.add_argument('--build-ovr', action='store_true', default=False,
                        help="build overviews")
    parser.add_argument('--resample', default="bilinear", help="dem_resampling strategy (default=bilinear). matchtag resampling is always nearest neighbor")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')

    pos_arg_keys = ['src']
    arg_keys_to_remove = utils.SCHEDULER_ARGS + ['dryrun', 'tiles']
    utils.add_scheduler_options(parser, submission_script_map)

    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()

    #### Verify Arguments
    src = os.path.abspath(args.src)
    if not os.path.isdir(src) and not os.path.isfile(src):
        parser.error("Source directory or file does not exist: %s" %src)

    if args.cutline_loc:
        if not os.path.isdir(args.cutline_loc):
            parser.error("Cutline directory does not exist: {}".format(args.cutline_loc))

    ## Verify qsubscript
    qsubpath = utils.verify_scheduler_args(parser, args, scriptpath, submission_script_map)

    if args.dem_version:
        dem_version_str = '_{}'.format(args.dem_version)
    else:
        dem_version_str = ''

    if args.tiles:
        tiles = args.tiles.split(',')

    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    logger.info("Current version: %s", VERSION)

    #### Get args ready to pass to task handler
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)

    task_queue = []
    i=0
    #### ID rasters
    logger.info('Identifying DEM mosaics')
    if os.path.isfile(src):
        logger.info(src)
        try:
            raster = dem.SetsmTile(src)
        except RuntimeError as e:
            logger.error( e )
        else:
            if src.endswith('reg_dem.tif'):
                dstfp_list = glob.glob('{}*{}m{}_reg_dem.tif'.format(src[:-15], args.res, dem_version_str))
            else:
                dstfp_list = glob.glob('{}*{}m{}_dem.tif'.format(src[:-12], args.res, dem_version_str))

            #### verify that cutlines can be found if requested
            if args.cutline_loc:
                tile = raster.tile_name
                cutline_shp = os.path.join(args.cutline_loc, tile + '_cut.shp')
                if not os.path.isfile(cutline_shp):
                    logger.warning("Cutline shp not found for tile {}".format(raster.tileid))

            if len(dstfp_list) == 0:
                i+=1
                task = taskhandler.Task(
                    raster.tileid,
                    'div_{}'.format(raster.tileid),
                    'python',
                    '{} {} {}'.format(scriptpath, arg_str_base, raster.srcfp),
                    divide_tile,
                    [raster.srcfp, args]
                )
                task_queue.append(task)
            else:
                logger.info("output tile(s) already exist: {}".format(src))

    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith(("2m_dem.tif","2m_reg_dem.tif")):
                    if args.tiles:
                        tile = f[:5]
                        process = True if tile in tiles else False
                    else:
                        process = True

                    if process:
                        srcfp = os.path.join(root,f)
                        try:
                            raster = dem.SetsmTile(srcfp)
                        except RuntimeError as e:
                            logger.error( e )
                        else:
                            if srcfp.endswith('reg_dem.tif'):
                                dstfp_list = glob.glob('{}*{}m{}_reg_dem.tif'.format(srcfp[:-15], args.res, dem_version_str))
                            else:
                                dstfp_list = glob.glob('{}*{}m{}_dem.tif'.format(srcfp[:-12], args.res, dem_version_str))
                            if len(dstfp_list) == 0:
                                logger.info("computing tile: {}".format(srcfp))

                                #### verify that cutlines can be found if requested
                                if args.cutline_loc:
                                    tile = raster.tilename
                                    cutline_shp = os.path.join(args.cutline_loc, tile + '_cut.shp')
                                    if not os.path.isfile(cutline_shp):
                                        logger.warning("Cutline shp not found for tile {}".format(raster.tileid))

                                i+=1
                                task = taskhandler.Task(
                                    raster.tileid,
                                    'div_{}'.format(raster.tileid),
                                    'python',
                                    '{} {} {}'.format(scriptpath, arg_str_base, raster.srcfp),
                                    divide_tile,
                                    [raster.srcfp, args]
                                )
                                #print '{} {} {}'.format(scriptpath, arg_str_base, raster.srcfp)
                                task_queue.append(task)
                            else:
                                logger.info("output tile(s) already exist: {}".format(','.join(dstfp_list)))

    logger.info('Number of incomplete tasks: {}'.format(i))

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
                src, task_arg_obj = task.method_arg_list

                #### Set up processing log handler
                logfile = os.path.splitext(src)[0]+".log"
                lfh = logging.FileHandler(logfile)
                lfh.setLevel(logging.DEBUG)
                formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
                lfh.setFormatter(formatter)
                logger.addHandler(lfh)

                if not args.dryrun:
                    task.method(src, task_arg_obj)

                #### remove existing file handler
                logger.removeHandler(lfh)

    else:
        logger.info("No tasks found to process")


def divide_tile(src, args):

    if args.dem_version:
        dem_version_str = '_{}'.format(args.dem_version)
    else:
        dem_version_str = ''

    ## get tile geom and make subtiles
    ds = gdal.Open(src)
    if ds:
        proj = ds.GetProjectionRef()
        gtf = ds.GetGeoTransform()
        minx = gtf[0]
        maxy = gtf[3]
        maxx = minx + 100000
        miny = maxy - 100000
        tilesizex = (maxx - minx) / float(args.num_cols)
        tilesizey = (maxy - miny) / float(args.num_rows)
        logger.info('Tile extent (minx, maxx, miny, maxy): {} {} {} {}'.format(minx, maxx, miny, maxy))
        logger.info('Tile size (x, y): {} {}'.format(tilesizex, tilesizey))

        if src.endswith('reg_dem.tif'):
            reg_str = 'reg_'
            tile_base = src[:-12]
        else:
            reg_str = ''
            tile_base = src[:-8]

        src_metapath = '{}_dem_meta.txt'.format(tile_base)
        src_regmetapath = '{}_reg.txt'.format(tile_base)
        dst_metapath = '{}_{}m{}_dem_meta.txt'.format(tile_base[:-3], args.res, dem_version_str)
        dst_regmetapath = '{}_{}m{}_reg.txt'.format(tile_base[:-3], args.res, dem_version_str)

        shutil.copy2(src_metapath, dst_metapath)
        if os.path.isfile(src_regmetapath):
            shutil.copy2(src_regmetapath, dst_regmetapath)

        ## apply cutline file if present
        if args.cutline_loc:
            tile = '_'.join(os.path.basename(src).split('_')[:2])
            cutline_shp = os.path.join(args.cutline_loc, tile + '_cut.shp')
            mask = '{}_{}m{}_mask.tif'.format(tile_base[:-3], args.res, dem_version_str)
            if not os.path.isfile(cutline_shp):
                logger.info('No cutline file found for src tile: {}'.format(cutline_shp))
            else:
                if not os.path.isfile(mask):
                    cmd = 'gdal_rasterize -burn -9999 -init 0 -ot Int16 -a_nodata 0 -tr {0} {0} {1} {2}'.format(args.res, cutline_shp, mask)
                    logger.info(cmd)
                    subprocess.call(cmd, shell=True)
        else:
            mask = None

        # for each component type, call gdal_translate with projwin
        if args.num_rows == 1 and args.num_cols == 1:
            for component in components:
                if component == 'matchtag':
                    resample = 'near'
                else:
                    resample = args.resample
                srcfp = '{}_{}{}.tif'.format(tile_base, reg_str, component)
                dstfp = '{}_{}m{}_{}{}.tif'.format(tile_base[:-3], args.res, dem_version_str, reg_str, component)
                logger.info("Building {}".format(dstfp))
                if not os.path.isfile(dstfp):
                    cmd = 'gdalwarp -ovr NONE -co tiled=yes -co bigtiff=yes -co compress=lzw -tr {2} {2} -r {7} -te {3} {4} {5} {6} {0} {1}'.format(
                        srcfp, dstfp, args.res, minx, miny, maxx, maxy, resample
                    )
                    logger.info(cmd)
                    subprocess.call(cmd, shell=True)

                    if mask:
                        if os.path.isfile(dstfp) and os.path.isfile(mask):
                            cmd = 'gdalwarp -ovr NONE {} {}'.format(mask, dstfp)
                            logger.info(cmd)
                            subprocess.call(cmd, shell=True)
                            os.remove(mask)

                if os.path.isfile(dstfp) and not os.path.isfile(dstfp+'.ovr'):
                    if args.build_ovr:
                        cmd = 'gdaladdo -ro {} 2 4 8 16'.format(dstfp)
                        logger.info(cmd)
                        subprocess.call(cmd, shell=True)

        else:
            # for each subtile and each component type, call gdal_translate with projwin
            # [-projwin ulx uly lrx lry]
            for xorigin in utils.drange(minx, maxx, tilesizex):
                tilenumx = int(math.ceil(xorigin - minx) / tilesizex + 1)
                for yorigin in utils.drange(miny, maxy, tilesizey):
                    tilenumy = int(math.ceil(yorigin - miny) / tilesizey + 1)
                    subtile_name = '{}_{}'.format(tilenumy,tilenumx)
                    logger.info('Subtile {} x-origin: {}, y-origin: {}'.format(subtile_name, xorigin, yorigin))

                    for component in components:
                        if component == 'matchtag':
                            resample = 'near'
                        else:
                            resample = args.resample
                        srcfp = '{}_{}{}.tif'.format(tile_base, reg_str, component)
                        dstfp = '{}_{}_{}m{}_{}{}.tif'.format(tile_base[:-3], subtile_name, args.res, dem_version_str, reg_str, component)
                        logger.info("Building {}".format(dstfp))
                        if not os.path.isfile(dstfp):
                            cmd = 'gdalwarp -ovr NONE -co tiled=yes -co bigtiff=yes -co compress=lzw -tr {2} {2} -r {7} -te {3} {4} {5} {6} {0} {1}'.format(
                                srcfp, dstfp, args.res, xorigin, yorigin, xorigin+tilesizex,  yorigin+tilesizey, resample
                            )
                            logger.info(cmd)
                            subprocess.call(cmd, shell=True)

                        if mask:
                            if os.path.isfile(dstfp) and os.path.isfile(mask):
                                cmd = 'gdalwarp -ovr NONE {} {}'.format(mask, dstfp)
                                logger.info(cmd)
                                subprocess.call(cmd, shell=True)

                        # check if file has any data, delete if not
                        ds = gdal.Open(dstfp)
                        try:
                            stats = ds.GetRasterBand(1).GetStatistics(True,True)
                        except RuntimeError as e:
                            logger.info("subtile has no data pixels, removing: {}".format(dstfp))
                            os.remove(dstfp)
                        else:
                            logger.info("Tile statistics: {}".format(str(stats)))

                        if os.path.isfile(dstfp) and not os.path.isfile(dstfp+'.ovr'):
                            if args.build_ovr:
                                cmd = 'gdaladdo -ro {} 2 4 8 16'.format(dstfp)
                                logger.info(cmd)
                                subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    main()
