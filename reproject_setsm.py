import os, string, sys, re, glob, argparse, subprocess, logging, math
from osgeo import gdal, gdalconst
from lib import dem, utils, taskhandler

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)

strip_pattern = re.compile("SETSM_(?P<pairname>(?P<sensor>[A-Z]{2}\d{2})_(?P<timestamp>\d{8})_(?P<catid1>[A-Z0-9]{16})_(?P<catid2>[A-Z0-9]{16}))_(?P<partnum>\d+)_(?P<res>\d+m)_matchtag.tif", re.I)
default_res = 2
suffixes = ('ortho', 'matchtag', 'dem')

def main():
    parser = argparse.ArgumentParser()
    
    #### Set Up Options
    parser.add_argument("srcdir", help="source directory or image")
    parser.add_argument("--dstdir", required=True, help="dstination directory")
    parser.add_argument("--epsg", required=True, type=int, help="target epsg code")
    parser.add_argument("-r", "--resolution",  default=default_res, type=int,
                help="output resolution (default={})".format(default_res))
    parser.add_argument("-c", "--component",  choices=suffixes, default='dem',
                help="SETSM DEM component to operate on")
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
    
    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
        
    task_queue = []
    i=0
    logger.info("Searching for SETSM rasters")
    if os.path.isfile(path):
        if path.endswith('{}.tif'.format(args.component)):
            dem = path
            new_raster = os.path.join(args.dstdir, os.path.basename(dem))
            if not os.path.isfile(new_raster):
                i+=1
                task = taskhandler.Task(
                    os.path.basename(dem),
                    'Reproject{:04g}'.format(i),
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
                    new_raster = os.path.join(args.dstdir, os.path.basename(dem))
                    if not os.path.isfile(new_raster):
                        i+=1
                        task = taskhandler.Task(
                            f,
                            'Reproject{:04g}'.format(i),
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
                
                if not args.dryrun:
                    #### Set up processing log handler
                    logfile = os.path.splitext(src)[0]+".log"
                    lfh = logging.FileHandler(logfile)
                    lfh.setLevel(logging.DEBUG)
                    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
                    lfh.setFormatter(formatter)
                    logger.addHandler(lfh)
                    task.method(src, task_arg_obj)
    
    else:
        logger.info("No tasks found to process")
    
    
def resample_setsm(dem, args):
    new_raster = os.path.join(args.dstdir, os.path.basename(dem))
    
    if not os.path.isfile(new_raster):
        logger.info("Gathering origin info for {}".format(dem))
        # ds = gdal.Open(dem, gdalconst.GA_ReadOnly)
        # if ds:
            # #Get projection and built coordinate transformation
            # proj = GetProjectionRef()
            # s_srs = osr.SpatialReference(proj)
            # t_srs = osr.SpatialReference()
            # t_srs.ImportFromEPSG(args.epsg)
            # ct = osr.CoordinateTransformation(s_srs,t_srs)
            # 
            # # Get origin and res
            # gtf = ds.GetGeoTransform()
            # originx, resx, rotx, originy, roty, resy = gtf
            # 
            # # Transform origin to new srs
            # pt = ogr.Geometry(ogr.wkbPoint)
            # pt.AddPoint(originx,originy)
            # logger.info(pt)
            # pt.Transform(ct)
            # logger.info(pt)
            # 
            # # get new origin on setsm grid
            # new_originx = math.floor(pt.GetX()/float(resx)) * resx
            # new_originy = math.floor(pt.GetY()/float(resy)) * resy
            # logger.info("{},{}".format(new_originx, new_originy))
            # 
        logger.info("Reprojecting {}".format(dem))
        #print new_raster
        if args.component == 'dem':
            cmd = 'gdalwarp -q -tap -t_srs "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs" -tr {3} {3} -r bilinear "{1}" "{2}"'.format(args.epsg, dem, new_raster, args.resolution)
            
        else:
            cmd = 'gdalwarp -q -tap -t_srs "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs" -tr {3} {3} -r near "{1}" "{2}"'.format(args.epsg, dem, new_raster, args.resolution)
            
        #print cmd
        if not args.dryrun:
            taskhandler.exec_cmd(cmd)
    
        # if not args.dryrun:
        #     for f in deletables:
        #         if os.path.isfile(f):
        #             try:
        #                 os.remove(f)
        #             except:
        #                 print "Cannot remove %s" %f
    
        # else:
        #     logger.error("Cannot open source image: {}".format(dem))
    
if __name__ == '__main__':
    main()

