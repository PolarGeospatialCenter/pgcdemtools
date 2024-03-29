import argparse
import logging
import os
import sys
from datetime import *

from osgeo import osr, ogr

from lib import dem, taskhandler, VERSION, SHORT_VERSION, utils

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ogrDriver = ogr.GetDriverByName("ESRI Shapefile")
tgt_srs = osr.SpatialReference()
tgt_srs.ImportFromEPSG(4326)

submission_script_map = {
    'pbs': 'pbs_package.sh',
    'slurm': 'slurm_package.sh'
}


def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="Calculate density for setsm strip DEMS and save to a density.txt file"
        )
    
    #### Positional Arguments
    parser.add_argument('src', help="source directory or dem")
    parser.add_argument('scratch', help="scratch space")
    
    #### Optionsl Arguments
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help="print actions without executing")
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')

    pos_arg_keys = ['src', 'scratch']
    arg_keys_to_remove = utils.SCHEDULER_ARGS + ['dryrun']
    utils.add_scheduler_options(parser, submission_script_map, include_tasks_per_job=True)

    #### Parse Arguments
    scriptpath = os.path.abspath(sys.argv[0])
    args = parser.parse_args()
    src = os.path.abspath(args.src)
    scratch = os.path.abspath(args.scratch)
    
    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)
    if not os.path.isdir(args.scratch) and not args.scheduler:  #scratch dir may not exist on head node when running jobs via pbs
        parser.error("Scratch directory does not exist: %s" %args.scratch)
    
    ## Verify qsubscript
    qsubpath = utils.verify_scheduler_args(parser, args, scriptpath, submission_script_map)

    if args.v:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    lsh = logging.StreamHandler()
    lsh.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    logger.info("Current version: %s", VERSION)

    #### Get args ready to pass to task handler
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
      
    srcfps = []
    scenes = []
    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src) and src.endswith('.tif'):
        logger.debug(src)
        srcfps.append(src)

    elif os.path.isfile(src) and src.endswith('.txt'):
        fh = open(src,'r')
        for line in fh.readlines():
            sceneid = line.strip()
            logger.debug(sceneid)
            srcfps.append(sceneid)
    
    elif os.path.isdir(src):
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("_dem.tif") and "m_" in f:
                    srcfp = os.path.join(root,f)
                    logger.debug(srcfp)
                    srcfps.append(srcfp)

    else:
        logger.error("src must be a directory, a strip dem raster, or a text file")

    srcfps = list(set(srcfps))
    logger.info('Number of src rasters: {}'.format(len(srcfps)))

    for srcfp in srcfps:
        try:
            raster = dem.SetsmDem(srcfp)
            raster.get_metafile_info()
        except RuntimeError as e:
            logger.error(e)
        else:
            needed_attribs = (raster.density, raster.masked_density, raster.max_elev_value, raster.min_elev_value)
            if any([a is None for a in needed_attribs]):
                scenes.append(srcfp)

    logger.info('Number of incomplete tasks: {}'.format(len(scenes)))
    
    tm = datetime.now()
    job_count=0
    scene_count=0
    scenes_in_job_count=0
    task_queue = []
    
    for srcfp in scenes:
        scene_count+=1
        srcdir, srcfn = os.path.split(srcfp)            
        if args.tasks_per_job:
            # bundle tasks into text files in the dst dir and pass the text file in as src
            scenes_in_job_count+=1
            src_txt = os.path.join(scratch, 'src_dems_{}_{}.txt'.format(tm.strftime("%Y%m%d%H%M%S"),job_count))
            
            if scenes_in_job_count == 1:
                # remove text file if dst already exists
                try:
                    os.remove(src_txt)
                except OSError:
                    pass
                
            if scenes_in_job_count <= args.tasks_per_job:
                # add to txt file
                fh = open(src_txt,'a')
                fh.write("{}\n".format(srcfp))
                fh.close()
            
            if scenes_in_job_count == args.tasks_per_job or scene_count == len(scenes):
                scenes_in_job_count=0
                job_count+=1
                
                task = taskhandler.Task(
                    'Dens{:04g}'.format(job_count),
                    'Dens{:04g}'.format(job_count),
                    'python',
                    '{} {} {} {}'.format(scriptpath, arg_str_base, src_txt, scratch),
                    build_archive,
                    [srcfp, scratch, args]
                )
                task_queue.append(task)
            
        else:
            job_count += 1
            task = taskhandler.Task(
                srcfn,
                'Dens{:04g}'.format(job_count),
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str_base, srcfp, scratch),
                build_archive,
                [srcfp, scratch, args]
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
                src, scratch, task_arg_obj = task.method_arg_list
                
                if not args.dryrun:
                    task.method(src, scratch, task_arg_obj)
    
    else:
        logger.info("No tasks found to process")


def build_archive(src, scratch, args):

    logger.info("Calculating density of raster: {}".format(src))
    raster = dem.SetsmDem(src)
    raster.get_metafile_info()

    needed_attribs = (raster.density, raster.masked_density, raster.max_elev_value, raster.min_elev_value)
    if any([a is None for a in needed_attribs]):
        try:
            raster.compute_density_and_statistics()
        except RuntimeError as e:
            logger.warning(e)

    needed_attribs = (raster.density, raster.masked_density, raster.max_elev_value, raster.min_elev_value)
    if any([a is None for a in needed_attribs]):
        logger.warning("Density or stats calculation failed")


if __name__ == '__main__':
    main()

