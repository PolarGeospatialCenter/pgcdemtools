import argparse
import logging
import math
import os
import sys

from osgeo import gdal

from lib import taskhandler, walk as wk

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

#setsm_tile_pattern = re.compile("(?P<tile>\d+_\d+)(_(?P<subtile>\d+_\d+))?_(?P<res>2m)(_(?P<version>v[\d/.]+))?(_reg)?_dem.tif\Z", re.I)
default_res = 32
default_src_res = 2
suffixes = ('matchtag', 'dem', 'count', 'countmt')
default_depth = float('inf')

def main():
    parser = argparse.ArgumentParser()
    
    #### Set Up Options
    parser.add_argument("srcdir", help="source directory or image")
    parser.add_argument("-c", "--component",  choices=suffixes, default='dem',
                help="SETSM DEM component to resample")
    parser.add_argument("-tr", "--tgt-resolution",  default=default_res, type=int,
                help="output resolution (default={})".format(default_res))
    parser.add_argument("-sr", "--src-resolution", default=default_src_res, type=int,
                help="source resolution (default={})".format(default_src_res))
    parser.add_argument("--depth", type=int,
                help="search depth (default={})".format(default_depth))
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
    pos_arg_keys = ['srcdir']
    
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    path = os.path.abspath(args.srcdir)
    
    #### Validate Required Arguments
    if not os.path.isdir(path) and not os.path.isfile(path):
        parser.error('src must be a valid directory or file')
        
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
    lso.setLevel(logging.DEBUG)
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
    if os.path.isfile(path):
        if path.endswith('{}m_{}.tif'.format(args.src_resolution, args.component)):
            rasters.append(path)
    else:
        for root, dirs, files in wk.walk(path, maxdepth=args.depth):
            for f in files:
                if f.endswith('{}m_{}.tif'.format(args.src_resolution, args.component)):
                    rasters.append(os.path.join(root, f))

    for dem in rasters:
        low_res_dem = "{}.tif".format(os.path.splitext(dem)[0])
        low_res_dem = os.path.join(
            os.path.dirname(low_res_dem),
            os.path.basename(low_res_dem.replace("_{}m".format(args.src_resolution), "_{}m".format(args.tgt_resolution)))
        )

        if dem != low_res_dem and not os.path.isfile(low_res_dem):
            i+=1
            logger.debug("Adding task: {}".format(dem))
            task = taskhandler.Task(
                os.path.basename(dem),
                'Resample{:04g}'.format(i),
                'python',
                '{} {} {}'.format(scriptpath, arg_str_base, dem),
                resample_setsm,
                [dem, args]
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
    
    
def resample_setsm(dem, args):
    low_res_dem = "{}.tif".format(os.path.splitext(dem)[0])
    low_res_dem = os.path.join(
        os.path.dirname(low_res_dem),
        os.path.basename(low_res_dem.replace("_{}m".format(args.src_resolution), "_{}m".format(args.tgt_resolution)))
    )
                    
    if not os.path.isfile(low_res_dem) or args.overwrite is True:
        # Open src raster to determine extent.  Set -te so that -tap extent does not extend beyond the original
        ds = gdal.Open(dem)
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

            logger.info("Resampling {}".format(dem))
            #print low_res_dem
            resampling_method = 'bilinear' if args.component == 'dem' else 'near'
            cmd = 'gdalwarp -q -co tiled=yes -co compress=lzw -tap -r {3} -te {4} -tr {0} {0}  "{1}" "{2}"'.format(
                args.tgt_resolution, dem, low_res_dem, resampling_method, co_extent
            )
            #print cmd
            if not args.dryrun:
                taskhandler.exec_cmd(cmd)
        else:
            logger.error("Cannot open {}".format(dem))
    logger.info("Done")
            
        
if __name__ == '__main__':
    main()

