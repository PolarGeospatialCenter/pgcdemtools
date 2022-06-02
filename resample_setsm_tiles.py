import argparse
import datetime
import logging
import math
import os
import sys

from osgeo import gdal

from lib import taskhandler, dem, walk as wk

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

default_res = 32
default_src_res = 2
default_depth = float('inf')
res_min = 0.5
res_max = 5000
output_settings = {
    ## component: (resampling strategy, overview resampling, predictor)
    'dem':      ('bilinear', 'bilinear', 'yes'),
    'browse':   ('cubic', 'cubic', 'yes'),
    'count':    ('near', 'nearest', 'no'),
    'countmt':  ('near', 'nearest', 'no'),
    'mad':      ('bilinear', 'bilinear', 'yes'),
    'maxdate':  ('near', 'nearest', 'no'),
    'mindate':  ('near', 'nearest', 'no')
}
suffixes = list(output_settings.keys())


def main():
    parser = argparse.ArgumentParser()
    
    #### Set Up Options
    parser.add_argument("src", help="source directory or image")
    parser.add_argument("-c", "--component",  choices=suffixes+['all'], default='all',
                help="SETSM DEM component to resample (default = all")
    parser.add_argument("-tr", "--tgt-resolution",  default=default_res, type=float,
                help="output resolution in meters between {} and {} (default={})".format(res_min, res_max, default_res))
    parser.add_argument("-sr", "--src-resolution", default=default_src_res, type=float,
                help="source resolution in meters between {} and {} (default={})".format(res_min, res_max, default_src_res))
    parser.add_argument("--output-cogs", action='store_true', default=False,
                help="create cloud-optimized geotiff output")
    parser.add_argument("--depth", type=int,
                help="search depth (default={})".format(default_depth))
    parser.add_argument("--merge-by-tile", action="store_true", default=False,
                help="merge resampled rasters by tile directory (assumes one supertile set per directory)")
    parser.add_argument("-o", "--overwrite", action="store_true", default=False,
                help="overwrite existing files if present")
    parser.add_argument("--pbs", action='store_true', default=False,
                help="submit tasks to PBS")
    parser.add_argument("--parallel-processes", type=int, default=1,
                help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                help="qsub script to use in PBS submission (default is qsub_resample.sh in script root folder)")
    parser.add_argument("--dryrun", action="store_true", default=False,
                help="print actions without executing")
    pos_arg_keys = ['src']

    ## Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    
    ## Validate Required Arguments
    if not os.path.isdir(src) and not os.path.isfile(src):
        parser.error('src must be a valid directory or file')

    if args.src_resolution >= args.tgt_resolution:
        parser.error("source resolution values must be greater than output resolution")
        
    ## Verify qsubscript
    if args.qsubscript is None:
        qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_resample.sh')
    else:
        qsubpath = os.path.abspath(args.qsubscript)
    if not os.path.isfile(qsubpath):
        parser.error("qsub script path is not valid: %s" %qsubpath)
    
    ## Verify processing options do not conflict
    if args.pbs and args.parallel_processes > 1:
        parser.error("Options --pbs and --parallel-processes > 1 are mutually exclusive")
    
    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    #lso.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)
    
    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)

    rasters = []
    task_queue = []
    i=0
    logger.info("Searching for SETSM rasters")
    components = suffixes if args.component == 'all' else [args.component]
    components = ['{}.tif'.format(c) for c in components] + ['meta.txt']

    if os.path.isfile(src):
        srcfn = os.path.basename(src)
        match = dem.setsm_tile_pattern.match(srcfn)
        if match:
            groups = match.groupdict()
            if groups['res'] == res_float_to_str(args.src_resolution):
                rasters.append((src, match))
    else:
        for root, dirs, files in wk.walk(src, maxdepth=args.depth):
            for f in files:
                match = dem.setsm_tile_pattern.match(f)
                if match:
                    groups = match.groupdict()
                    if groups['res'] == res_float_to_str(args.src_resolution):
                        rasters.append((os.path.join(root, f), match))

    rasters.sort()
    if len(rasters) > 0:
        dirs_to_run = []
        for r, m in rasters:
            ddir, dbase, release_version, sptbase = get_dem_path_parts(r, args, match=m)
            tgt_res = res_float_to_str(args.tgt_resolution)
            if args.overwrite:
                dirs_to_run.append(ddir)
            if ddir not in dirs_to_run:
                ## Check the merge raster
                if args.merge_by_tile:
                    expected_outputs = [os.path.join(ddir, '{}{}_{}{}'.format(sptbase, tgt_res, release_version, c))
                                        for c in components]
                ## Check the individual raster output
                else:
                    expected_outputs = [os.path.join(ddir, '{}{}_{}{}'.format(dbase, tgt_res, release_version, c))
                                        for c in components]

                # for f in expected_outputs:
                #     if not os.path.isfile(f):
                #         print(f)

                if not all([os.path.isfile(f) for f in expected_outputs]):
                    dirs_to_run.append(ddir)

        # submit tasks by directory
        for ddir in dirs_to_run:
            task_src = ddir
            i+=1
            logger.debug("Adding task: {}".format(task_src))
            task = taskhandler.Task(
                os.path.basename(task_src),
                'Resample{:04g}'.format(i),
                'python',
                '{} {} {}'.format(scriptpath, arg_str_base, task_src),
                resample_setsm,
                [task_src, args]
            )
            task_queue.append(task)

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


def get_dem_path_parts(raster, args, match=None):
    ddir, dbaset = os.path.split(raster)
    if not match:
        match = dem.setsm_tile_pattern.match(dbaset)
    if match:
        groups = match.groupdict()
        src_res = res_float_to_str(args.src_resolution)
        release_version = '{}_'.format(groups['relversion']) if groups['relversion'] else ''
        search_suffix = '{}_{}dem.tif'.format(src_res, release_version)
        dbase = dbaset[:-1*len(search_suffix)]
        len_subtile = len(groups['subtile'])+1 if groups['subtile'] else 0
        sptbase = dbase[:-1*len_subtile]
        return ddir, dbase, release_version, sptbase
    else:
        raise RuntimeError('Raster name does not match expected pattern: {}'.format(raster))


def res_float_to_str(res):

    if 0.5 < res < 1:
        scale_factor = 100
        units = 'cm'
    elif 1 <= res < 1000:
        scale_factor = 1
        units = 'm'
    elif 1000 <= res < 5000:
        scale_factor = 1000
        units = 'km'
    else:
        raise RuntimeError('Resolution falls outside allowed values: {}'.format(res))

    return '{}{}'.format(int(res * scale_factor), units)


def resample_setsm(task_src, args):

    rasters = []
    supertiles = {}
    components = suffixes if args.component == 'all' else [args.component]
    components = ['{}.tif'.format(c) for c in components] + ['meta.txt']
    src_res = res_float_to_str(args.src_resolution)
    tgt_res = res_float_to_str(args.tgt_resolution)
    for root, dirs, files in wk.walk(task_src, maxdepth=args.depth):
        for f in files:
            match = dem.setsm_tile_pattern.match(f)
            if match:
                groups = match.groupdict()
                if groups['res'] == res_float_to_str(args.src_resolution):
                    rasters.append((os.path.join(root, f), match))

    if len(rasters) > 0:
        for raster, m in rasters:
            ddir, dbase, release_version, sptbase = get_dem_path_parts(raster, args, match=m)

            # Add raster to supertile list
            sptpath = os.path.join(ddir, sptbase)
            dpath = os.path.join(ddir, dbase)
            if sptpath not in supertiles:
                supertiles[sptpath] = []
            ## Check if source and dst are the same
            if sptpath != dpath:
                supertiles[sptpath].append((dpath, release_version))
            else:
                logger.error("Cannot merge by tile: No subtiles found")

            for component in components:
                inputp = os.path.join(ddir, '{}{}_{}{}'.format(dbase, src_res, release_version, component))
                sptoutput = '{}{}_{}{}'.format(sptpath, tgt_res, release_version, component)
                output = os.path.join(ddir, '{}{}_{}{}'.format(dbase, tgt_res, release_version, component))
                if (not os.path.isfile(output) and not os.path.isfile(sptoutput)) or args.overwrite:
                    if component == 'meta.txt':
                        build_meta([inputp], output, tgt_res, dbase.rstrip('_'), release_version, args)
                    else:
                        process_raster(inputp, output, component, args)

        if args.merge_by_tile:
            for sptpath in supertiles:
                spt = os.path.basename(sptpath).rstrip('_')
                for component in components:
                    inputps = []
                    for dpath, release_version in supertiles[sptpath]:
                        inputp = '{}{}_{}{}'.format(dpath, tgt_res, release_version, component)
                        output = '{}{}_{}{}'.format(sptpath, tgt_res, release_version, component)
                        if os.path.isfile(output):
                            break
                        if os.path.isfile(inputp):
                            inputps.append(inputp)
                        else:
                            raise RuntimeError('Expected source file not found: {}'.format(inputp))
                    if component == 'meta.txt':
                        output = '{}{}_{}meta.txt'.format(sptpath, tgt_res, release_version)
                        if not os.path.isfile(output) or args.overwrite:
                            build_meta(inputps, output, tgt_res, spt, release_version, args, merge=True)
                    elif not os.path.isfile(output) or args.overwrite:
                        merge_rasters(inputps, output, component, args)

                    ## Clean up
                    if not args.dryrun:
                        if os.path.isfile(output):
                            for inputp in inputps:
                                os.remove(inputp)
                        else:
                            logger.error("Output file not found, leaving temp file in place: {}".format(output))

    logger.info("Done")


def build_meta(metas, output_meta, tgt_res, tile_base, release_version, args, merge=False):

    if len(metas) > 1 and not merge:
        raise RuntimeError("Metadata builder was handed more than one source file without merge=True")

    logger.info("Building metadata file: {}".format(tile_base))
    tm = datetime.datetime.today()
    dems = []
    title = None
    tile_blend_lines = []
    for meta in metas:
        with open(meta, 'r') as input_fh:
            lines = input_fh.read().splitlines()
            lines = [line.strip() for line in lines]
            title = lines[0]

            try:
                i = lines.index('Adjacent Tile Blend Status')
            except ValueError:
                pass
            else:
                tile_blend_lines = lines[i:i+6]
            i = lines.index('List of DEMs used in mosaic:')
            dems.extend(lines[i+1:])

    output_lines = [
        title,
        'Tile: {}_{}'.format(tile_base, tgt_res),
        'Creation Date: {}'.format(tm.strftime('%d-%b-%Y %H:%M:%S')),
        'Version: {}'.format(release_version.strip('v_')),
        ''
    ]
    if not merge:
        output_lines.extend(tile_blend_lines)

    output_lines.append('List of DEMs used in mosaic:',)
    dems = list(set(dems))
    dems.sort()
    output_lines.extend(dems)

    if not args.dryrun:
        with open(output_meta, 'w') as output_fh:
            output_fh.write('\n'.join(output_lines))


def merge_rasters(inputps, output, component, args):
    if os.path.isfile(output) and args.overwrite:
        os.remove(output)
    if os.path.isfile(output):
        logger.info("Merging files into {}".format(output))

    resampling_method, ovr_resample, predictor = output_settings[component[:-4]] # remove .tif from component

    vrt = output[:-4] + 'temp.vrt'
    cmd = 'gdalbuildvrt {} {}'.format(vrt, ' '.join([i for i in inputps]))
    if not args.dryrun:
        taskhandler.exec_cmd(cmd)

    if args.output_cogs:
        cos = '-of COG -co compress=lzw -co bigtiff=yes -co overviews=IGNORE_EXISTING ' \
              '-co compress=lzw -co predictor={} -co resampling={}'.format(predictor, ovr_resample)
    else:
        cos = '-of GTiff -co tiled=yes -co compress=lzw -co bigtiff=yes'

    cmd = 'gdal_translate -q {} "{}" "{}"'.format(
        cos, vrt, output
    )

    if not args.dryrun:
        taskhandler.exec_cmd(cmd)
        os.remove(vrt)


def process_raster(inputp, output, component, args):

    if os.path.isfile(output) and args.overwrite:
        os.remove(output)
    if not os.path.isfile(output):
        logger.info("Resampling {}".format(inputp))

        # Open src raster to determine extent.  Set -te so that -tap extent does not extend beyond the original
        ds = gdal.Open(inputp)
        if ds:
            ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()
            lrx = ulx + (ds.RasterXSize * xres)
            lry = uly + (ds.RasterYSize * yres)
            new_xmax = args.tgt_resolution * math.floor(lrx / args.tgt_resolution)
            new_xmin = args.tgt_resolution * math.ceil(ulx / args.tgt_resolution)
            new_ymax = args.tgt_resolution * math.floor(uly / args.tgt_resolution)
            new_ymin = args.tgt_resolution * math.ceil(lry / args.tgt_resolution)
            co_extent = '{} {} {} {}'.format(
                new_xmin, new_ymin, new_xmax, new_ymax
            )

            resampling_method, ovr_resample, predictor = output_settings[component[:-4]]
            if args.output_cogs:
                cos = '-of COG -co tiled=yes -co compress=lzw -co bigtiff=yes -co overviews=IGNORE_EXISTING' \
                      ' -co compress=lzw -co predictor={} -co resampling={}'.format(predictor, ovr_resample)
            else:
                cos = '-of GTiff -co tiled=yes -co compress=lzw -co bigtiff=yes'

            cmd = 'gdalwarp -q {5} -tap -r {3} -te {4} -tr {0} {0}  "{1}" "{2}"'.format(
                args.tgt_resolution, inputp, output, resampling_method, co_extent, cos
            )

            if not args.dryrun:
                taskhandler.exec_cmd(cmd)
        else:
            logger.error("Cannot open {}".format(inputp))


if __name__ == '__main__':
    main()

