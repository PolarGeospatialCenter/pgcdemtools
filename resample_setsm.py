import argparse
import logging
import os
import re
import sys

from lib import taskhandler
from lib import VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)

strip_pattern = re.compile("SETSM_(?P<pairname>(?P<sensor>[A-Z]{2}\d{2})_(?P<timestamp>\d{8})_(?P<catid1>[A-Z0-9]{16})_(?P<catid2>[A-Z0-9]{16}))_(?P<partnum>\d+)_(?P<res>\d+m)_matchtag.tif", re.I)
default_res = 16
suffixes = ('matchtag', 'dem')

def main():
    parser = argparse.ArgumentParser()
    
    #### Set Up Options
    parser.add_argument("srcdir", help="source directory or image")
    parser.add_argument("-c", "--component",  choices=suffixes, default='dem',
                      help="SETSM DEM component to resample")
    parser.add_argument("-r", "--resolution",  default=default_res, type=int,
                      help="output resolution (default={})".format(default_res))
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
    parser.add_argument('-v', '--version', action='store_true', default=False, help='print version and exit')

    pos_arg_keys = ['srcdir']

    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    path = os.path.abspath(args.srcdir)

    if args.version:
        print("Current version: %s", VERSION)
        sys.exit(0)

    #### Validate Required Arguments
    if not os.path.isdir(path) and not os.path.isfile(path):
        parser.error('src must be avalid directory or file')
        
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
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    logger.info("Current version: %s", VERSION)

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    task_queue = []
    i=0
    logger.info("Searching for SETSM rasters")
    if os.path.isfile(path):
        if path.endswith('{}.tif'.format(args.component)):
            dem = path
            low_res_dem = "{}_{}m.tif".format(os.path.splitext(dem)[0],args.resolution)
            if not os.path.isfile(low_res_dem):
                i+=1
                task = taskhandler.Task(
                    os.path.basename(dem),
                    'Resample{:04g}'.format(i),
                    'python',
                    '{} {} {}'.format(scriptpath, arg_str_base, dem),
                    resample_setsm,
                    [dem, args]
                )
                task_queue.append(task)
    
    else:                
        for root,dirs,files in os.walk(path):
            for f in files:
                if f.endswith('{}.tif'.format(args.component)):
                    dem = os.path.join(root,f)
                    low_res_dem = "{}_{}m.tif".format(os.path.splitext(dem)[0],args.resolution)
                    if not os.path.isfile(low_res_dem):
                        i+=1
                        task = taskhandler.Task(
                            f,
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
    low_res_dem = "{}_{}m.tif".format(os.path.splitext(dem)[0],args.resolution)
    
    if not os.path.isfile(low_res_dem) or args.overwrite is True:
        logger.info("Resampling {}".format(dem))
        #print low_res_dem
        resampling_method = 'bilinear' if args.component == 'dem' else 'near'
        cmd = 'gdalwarp -q -ovr NONE -co tiled=yes -co compress=lzw -r {3} -tr {0} {0} "{1}" "{2}"'.format(args.resolution, dem, low_res_dem, resampling_method)
        #print cmd
        if not args.dryrun:
            taskhandler.exec_cmd(cmd)
            
        if os.path.isfile(low_res_dem):
            cmd = 'gdaladdo -q "{}" 2 4 8 16'.format(low_res_dem)
            #print cmd
            if not args.dryrun:
                taskhandler.exec_cmd(cmd)
        
if __name__ == '__main__':
    main()

