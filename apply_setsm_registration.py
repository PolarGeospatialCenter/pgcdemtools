import argparse
import logging
import os
import re
import subprocess
import sys

from osgeo import gdal, gdalconst, ogr
from lib import VERSION

from lib import taskhandler

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)

strip_pattern = re.compile("SETSM_(?P<pairname>(?P<sensor>[A-Z]{2}\d{2})_(?P<timestamp>\d{8})_(?P<catid1>[A-Z0-9]{16})_(?P<catid2>[A-Z0-9]{16}))_(?P<partnum>\d+)_(?P<res>\d+m)_matchtag.tif", re.I)
default_res = 16
default_format = 'JPEG'
suffixes = ('ortho', 'matchtag', 'dem')
formats = ('JPEG', 'GTiff')


def main():
    parser = argparse.ArgumentParser()
    
    #### Set Up Options
    parser.add_argument("src", help="source directory or image")
    parser.add_argument("--dstdir", help="dstination directory")
    parser.add_argument("-o", "--overwrite", action="store_true", default=False,
                help="overwrite existing files if present")
    parser.add_argument("--pbs", action='store_true', default=False,
                help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                help="submit tasks to SLURM")
    parser.add_argument("--parallel-processes", type=int, default=1,
                help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                help="qsub script to use in scheduler submission (PBS default is qsub_resample.sh, SLURM default is slurm_resample.sh)")
    parser.add_argument("--dryrun", action="store_true", default=False,
                help="print actions without executing")
    parser.add_argument('-v', '--version', action='store_true', default=False, help='print version and exit')

    pos_arg_keys = ['src']
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)

    if args.version:
        print("Current version: %s", VERSION)
        sys.exit(0)
    
    #### Validate Required Arguments
    if not os.path.isdir(src) and not os.path.isfile(src):
        parser.error('src must be avalid directory or file')
        
    ## Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_resample.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath),'slurm_resample.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: %s" %qsubpath)
    
    ## Verify processing options do not conflict
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")
    if (args.pbs or args.slurm) and args.parallel_processes > 1:
        parser.error("HPC Options (--pbs or --slurm) and --parallel-processes > 1 are mutually exclusive")
    
    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    logger.info("Current version: %s", VERSION)
    
    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('qsubscript', 'dryrun', 'pbs', 'slurm', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
        
    task_queue = []
    i=0
    logger.info("Searching for SETSM rasters")
    if os.path.isfile(src):
        if src.endswith(('dem.tif','matchtag.tif','ortho.tif')):
            srcfp = src
            regfp = srcfp.replace('dem.tif','reg.txt').replace('matchtag.tif','reg.txt').replace('ortho.tif','reg.txt')
            
            if not os.path.isfile(regfp):
                logger.info("No regfile found for {}".format(src))
            else:
                if args.dstdir:
                    dstfp = "{}_reg.tif".format(os.path.join(args.dstdir, os.path.basename(os.path.splitext(srcfp)[0])))
                else:
                    dstfp = "{}_reg.tif".format(os.path.splitext(srcfp)[0])
                if not os.path.isfile(dstfp):
                    i+=1
                    task = taskhandler.Task(
                        os.path.basename(srcfp),
                        'Reg{:04g}'.format(i),
                        'python',
                        '{} {} {}'.format(scriptpath, arg_str_base, srcfp),
                        apply_reg,
                        [srcfp, args]
                    )
                    task_queue.append(task)
    
    else:                
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith(('dem.tif','matchtag.tif','ortho.tif')):
                    srcfp = os.path.join(root,f)
                    regfp = srcfp.replace('dem.tif','reg.txt').replace('matchtag.tif','reg.txt').replace('ortho.tif','reg.txt')
                    if not os.path.isfile(regfp):
                        logger.info("No regfile found for {}".format(srcfp))
                    else:
                        if args.dstdir:
                            dstfp = "{}_reg.tif".format(os.path.join(args.dstdir, os.path.basename(os.path.splitext(srcfp)[0])))
                        else:
                            dstfp = "{}_reg.tif".format(os.path.splitext(srcfp)[0])
                        if not os.path.isfile(dstfp):
                            i+=1
                            task = taskhandler.Task(
                                f,
                                'Reg{:04g}'.format(i),
                                'python',
                                '{} {} {}'.format(scriptpath, arg_str_base, srcfp),
                                apply_reg,
                                [srcfp, args]
                            )
                            task_queue.append(task)
    
    logger.info('Number of incomplete tasks: {}'.format(i))
    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.pbs:
            try:
                task_handler = taskhandler.PBSTaskHandler(qsubpath)
            except RuntimeError as e:
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
        
        elif args.slurm:
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath)
            except RuntimeError as e:
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
            
        elif args.parallel_processes > 1:
            try:
                task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            except RuntimeError as e:
                logger.error(e)
            else:
                logger.info("Number of child processes to spawn: {0}".format(task_handler.num_processes))
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
    
        else:         
            for task in task_queue:
                src, task_arg_obj = task.method_arg_list
                
                #### Set up processing log handler
                if args.dstdir:
                    logfile = "{}.log".format(os.path.join(args.dstdir, os.path.basename(os.path.splitext(src)[0])))
                else:
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
    
    
def apply_reg(srcfp, args):
    
    VRTdrv = gdal.GetDriverByName("VRT")
    files_to_remove = []
    
    logger.info("Source raster: {}".format(srcfp))
    if args.dstdir:
        dstfp = "{}_reg.tif".format(os.path.join(args.dstdir, os.path.basename(os.path.splitext(srcfp)[0])))
    else:
        dstfp = "{}_reg.tif".format(os.path.splitext(srcfp)[0])
    regfp = srcfp.replace('dem.tif','reg.txt').replace('matchtag.tif','reg.txt').replace('ortho.tif','reg.txt')
    reg_vrt = dstfp[:-4]+".vrt"
    temp_fp = dstfp[:-4]+"_temp.tif"
    
    ## read regfile and get offsets
    regfh = open(regfp,'r')
    for line in regfh.readlines():
        if line.startswith('Translation Vector (dz,dx,dy)(m)='):
            trans_vector = line.strip().split('=')[1]
            trans_vector = trans_vector.split(', ')
            dz, dx, dy = [float(parm.strip()) for parm in trans_vector]
            logger.info("Translation Vector (dz, dx, dy) (m) = {}, {}, {}".format(dz, dx, dy))
            break
    regfh.close()
    
    ## open image and built vrt with modified geotransform for x and y offset
    
    sds = gdal.Open(srcfp,gdalconst.GA_ReadOnly)
    if sds is not None:
        gtf = sds.GetGeoTransform()
        xsize = sds.RasterXSize
        ysize = sds.RasterYSize
        #print gtf
    
        origin_x = gtf[0]
        origin_y = gtf[3]
        trans_origin_x = origin_x + dx
        trans_origin_y = origin_y + dy
        #print ex, ey

        ##  get new image geom, not nodata trimmed
        minx = trans_origin_x
        maxx = minx + xsize * gtf[1]
        maxy = trans_origin_y
        miny = maxy + ysize * gtf[5]
        
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(minx, miny)
        ring.AddPoint(minx, maxy)
        ring.AddPoint(maxx, maxy)
        ring.AddPoint(maxx, miny)
        ring.AddPoint(minx, miny)
        
        geom = ogr.Geometry(ogr.wkbPolygon)
        geom.AddGeometry(ring)
        
        # -projwin ulx uly lrx lry
        # -te xmin ymin xmax ymax
        target_extent = "{} {} {} {}".format(minx, miny, maxx, maxy)
                
        vds = VRTdrv.CreateCopy(reg_vrt,sds,0)
        dgtf = (trans_origin_x,gtf[1],gtf[2],trans_origin_y,gtf[4],gtf[5])
        vds.SetGeoTransform(dgtf)
                
    vds = None
    sds = None
    files_to_remove.append(reg_vrt)
    
    if not srcfp.endswith("dem.tif"):
        if os.path.isfile(reg_vrt) and not os.path.isfile(temp_fp):
            cmd = 'gdalwarp -ovr NONE -te {} -co COMPRESS=LZW -co TILED=YES "{}" "{}"'.format(target_extent,reg_vrt,dstfp)
            if not args.dryrun:
                logger.info(cmd)
                subprocess.call(cmd,shell=True)
    
    else:
        if os.path.isfile(reg_vrt) and not os.path.isfile(temp_fp):
            cmd = 'gdalwarp -ovr NONE -te {} -co COMPRESS=LZW -co TILED=YES "{}" "{}"'.format(target_extent,reg_vrt,temp_fp)
            if not args.dryrun:
                logger.info(cmd)
                subprocess.call(cmd,shell=True)
                
        ## use gdal_calc to modify for z offset if raster is a DEM only
        if os.path.isfile(temp_fp) and not os.path.isfile(dstfp):
            cmd = 'gdal_calc.py --co COMPRESS=LZW --co TILED=YES --NoDataValue=-9999 -A {} --calc "A+{}" --outfile {}'.format(temp_fp,dz,dstfp)
            if not args.dryrun:
                logger.info(cmd)
                subprocess.call(cmd,shell=True)
        
        files_to_remove.append(temp_fp)
        
    for f in files_to_remove:
        try:
            os.remove(f)
        except OSError as e:
            logger.info('Cannot remove {}'.format(f))
    
if __name__ == '__main__':
    main()

