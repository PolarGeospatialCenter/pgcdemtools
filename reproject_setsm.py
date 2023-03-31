import argparse
import logging
import numpy
import os
import re
import sys

from osgeo import ogr, osr

from lib import taskhandler, SHORT_VERSION
from lib import VERSION

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)

strip_pattern = re.compile("SETSM_(?P<pairname>(?P<sensor>[A-Z]{2}\d{2})_(?P<timestamp>\d{8})_(?P<catid1>[A-Z0-9]{16})_(?P<catid2>[A-Z0-9]{16}))_(?P<partnum>\d+)_(?P<res>\d+m)_matchtag.tif", re.I)
default_res = 2
suffixes = ['ortho.tif', 'matchtag.tif', 'dem.tif', 'meta.txt']
component_choices = suffixes + ['all']

def main():
    parser = argparse.ArgumentParser()

    #### Set Up Options
    parser.add_argument("srcdir", help="source directory or image")
    parser.add_argument("dstdir", help="destination directory")
    parser.add_argument("epsg", type=int, help="target epsg code")
    
    parser.add_argument("-r", "--resolution",  default=default_res, type=int,
                help="output resolution (default={})".format(default_res))
    parser.add_argument("--pbs", action='store_true', default=False,
                help="submit tasks to PBS")
    parser.add_argument("--parallel-processes", type=int, default=1,
                help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                help="qsub script to use in PBS submission (default is qsub_resample.sh in script root folder)")
    parser.add_argument("--dryrun", action="store_true", default=False,
                help="print actions without executing")
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')

    pos_arg_keys = ['srcdir','dstdir','epsg']

    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    srcpath = os.path.abspath(args.srcdir)

    #### Validate Required Arguments
    if not os.path.isdir(srcpath) and not os.path.isfile(srcpath):
        parser.error('src must be a valid directory or file')
    dstdir = os.path.abspath(args.dstdir)
    if not os.path.isdir(dstdir):
        os.makedirs(dstdir)

    ## Verify EPSG
    test_sr = osr.SpatialReference()
    status = test_sr.ImportFromEPSG(args.epsg)
    if status != 0:
        parser.error('EPSG test osr.SpatialReference.ImportFromEPSG returns error code %d' %status)

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
    if os.path.isfile(srcpath):
        if srcpath.endswith('dem.tif'):
            raster = srcpath
            new_raster = os.path.join(args.dstdir, os.path.basename(raster))
            if not os.path.isfile(new_raster):
                i+=1
                task = taskhandler.Task(
                    os.path.basename(raster),
                    'Reproject{:04g}'.format(i),
                    'python',
                    '{} {} {} {} {}'.format(scriptpath, arg_str_base, raster, dstdir, args.epsg),
                    resample_setsm,
                    [raster, dstdir, args]
                )
                task_queue.append(task)
        

    else:
        for root,dirs,files in os.walk(srcpath):
            for f in files:
                if f.endswith('dem.tif'):
                    raster = os.path.join(root,f)
                    new_raster = os.path.join(args.dstdir, os.path.basename(raster))
                    if not os.path.isfile(new_raster):
                        i+=1
                        task = taskhandler.Task(
                            f,
                            'Reproject{:04g}'.format(i),
                            'python',
                            '{} {} {} {} {}'.format(scriptpath, arg_str_base, raster, dstdir, args.epsg),
                            resample_setsm,
                            [raster, dstdir, args]
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
                src, dst, task_arg_obj = task.method_arg_list

                if not args.dryrun:
                    #### Set up processing log handler
                    logfile = os.path.join(dst,os.path.splitext(os.path.basename(src))[0]+".log")
                    lfh = logging.FileHandler(logfile)
                    lfh.setLevel(logging.DEBUG)
                    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
                    lfh.setFormatter(formatter)
                    logger.addHandler(lfh)
                    task.method(src, dst, task_arg_obj)

    else:
        logger.info("No tasks found to process")


def resample_setsm(raster, dstdir, args):
    
    for suffix in suffixes:
        
        srcdir, srcfn = os.path.split(raster)
        component = os.path.join(srcdir, srcfn.replace('dem.tif',suffix))
        new_raster = os.path.join(dstdir, srcfn.replace('dem.tif',suffix))
    
        if not os.path.isfile(new_raster):
           
            logger.info("Reprojecting {}".format(component))
            #print new_raster
            if suffix == 'dem.tif':
                cmd = ('gdalwarp -q -ovr NONE -tap -t_srs EPSG:{0} -tr {3} {3} -r bilinear -co tiled=yes -co compress=lzw '
                      '-co bigtiff=yes "{1}" "{2}"'.format(args.epsg, component, new_raster, args.resolution))
                if not args.dryrun:
                    taskhandler.exec_cmd(cmd)
            elif suffix == 'meta.txt':
                resample_stripmeta(component, new_raster, args.epsg)
            else:
                cmd = ('gdalwarp -q -ovr NONE -tap -t_srs EPSG:{0} -tr {3} {3} -r near -co tiled=yes -co compress=lzw '
                      '-co bigtiff=yes "{1}" "{2}"'.format(args.epsg, component, new_raster, args.resolution))
                if not args.dryrun:
                    taskhandler.exec_cmd(cmd)
    logger.info('Done')


def resample_stripmeta(metaFile, new_metaFile, new_epsg):
    # #### Set up processing log handler
    # logfile = os.path.splitext(metaFile)[0]+".log"
    # lfh = logging.FileHandler(logfile)
    # lfh.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    # lfh.setFormatter(formatter)
    # logger.addHandler(lfh)

    logger.info("Resampling strip metadata {}".format(metaFile))

    outmeta_txt = ""
    inmeta_fp = open(metaFile, 'r')

    # Create output spatial reference and get
    # output projection in proj4 format.
    spatref_out = osr.SpatialReference()
    spatref_out.ImportFromEPSG(new_epsg)
    proj4_out = spatref_out.ExportToProj4()

    # Get strip projection.
    line = inmeta_fp.readline()
    while not line.startswith('Strip projection (proj4):') and line != "":
        outmeta_txt += line
        line = inmeta_fp.readline()
    if line == "":
        logger.error("Projection string cannot be parsed from meta file: {}".format(metaFile))
        inmeta_fp.close()
        return
    proj4_in = line.split("'")[1]
    outmeta_txt += "Strip projection (proj4): '{}'\n".format(proj4_out)

    # Get strip footprint geometry.
    line = inmeta_fp.readline()
    while not line.startswith('Strip Footprint Vertices') and line != "":
        outmeta_txt += line
        line = inmeta_fp.readline()
    if line == "":
        logger.error("Footprint vertices cannot be parsed from meta file: {}".format(metaFile))
        inmeta_fp.close()
        return
    outmeta_txt += line
    line = inmeta_fp.readline()
    x_in = numpy.fromstring(line.replace('X:', '').strip(), dtype=numpy.float32, sep=' ')
    line = inmeta_fp.readline()
    y_in = numpy.fromstring(line.replace('Y:', '').strip(), dtype=numpy.float32, sep=' ')
    wkt_in = coordsToWkt(numpy.array([x_in, y_in]).T)

    # Create input footprint geometry with spatial reference.
    geom = ogr.Geometry(wkt=wkt_in)
    spatref_in = osr.SpatialReference()
    spatref_in.ImportFromProj4(proj4_in)
    geom.AssignSpatialReference(spatref_in)

    # Transform geometry to new spatial reference
    # and extract transformed coordinates.
    geom.TransformTo(spatref_out)
    wkt_out = geom.ExportToWkt()
    x_out, y_out = wktToCoords(wkt_out).T
    outmeta_txt += "X: {} \n".format(' '.join(numpy.array_str(x_out, max_line_width=numpy.inf).strip()[1:-1].split()))
    outmeta_txt += "Y: {} \n".format(' '.join(numpy.array_str(y_out, max_line_width=numpy.inf).strip()[1:-1].split()))

    outmeta_txt += inmeta_fp.read()
    inmeta_fp.close()

    # Write output metadata file.
    outmeta_fp = open(new_metaFile, 'w')
    outmeta_fp.write(outmeta_txt)
    outmeta_fp.close()


def coordsToWkt(point_coords):
    """
    Retrieve a WKT polygon representation of an ordered list of
    point coordinates.

    Parameters
    ----------
    point_coords : 2D sequence of floats/ints like ndarray
                   of shape (npoints, ndim)
        Ordered list of points, each represented by a list of
        coordinates that define its position in space.

    Returns
    -------
    coordsToWkt : str
        WKT polygon representation of `point_coords`.

    """
    return 'POLYGON (({}))'.format(
        ','.join([" ".join([str(c) for c in xy]) for xy in point_coords])
    )


def wktToCoords(wkt):
    """
    Create an array of point coordinates from a WKT polygon string.

    Parameters
    ----------
    wkt : str
        WKT polygon representation of points with coordinate data
        to be extracted.

    Returns
    -------
    wktToCoords : ndarray of shape (npoints, ndim)
        Ordered list of point coordinates extracted from `wkt`.

    """
    coords_list = eval(
        wkt.replace('POLYGON ','').replace('(','[').replace(')',']').replace(',','],[').replace(' ',',')
    )
    return numpy.array(coords_list)



if __name__ == '__main__':
    main()
