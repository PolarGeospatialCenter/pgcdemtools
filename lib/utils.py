#!/usr/bin/env python

"""
demtools utilties and constants
"""
import glob
import logging
import os
import re
import shutil
import sys

import numpy as np
from collections import namedtuple

from osgeo import osr, ogr, gdalconst, gdal

SCHEDULERS = ['pbs', 'slurm']
SCHEDULER_ARGS = ['qsubscript', 'scheduler', 'parallel_processes', 'slurm', 'pbs', 'tasks_per_job']

gdal.UseExceptions()


## Shared logger setup
class LoggerInfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno in (logging.DEBUG, logging.INFO)

LOGGER = None
LOGGER_STREAM_HANDLER = None
def get_logger():
    global LOGGER, LOGGER_STREAM_HANDLER
    if LOGGER is not None:
        return LOGGER
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    h1 = logging.StreamHandler(sys.stdout)
    h1.setLevel(logging.INFO)
    h1.setFormatter(formatter)
    h1.addFilter(LoggerInfoFilter())
    h2 = logging.StreamHandler(sys.stderr)
    h2.setLevel(logging.WARNING)
    h2.setFormatter(formatter)
    logger.addHandler(h1)
    logger.addHandler(h2)
    LOGGER = logger
    LOGGER_STREAM_HANDLER = h1
    return LOGGER

def set_logger_streamhandler_level(level):
    global LOGGER_STREAM_HANDLER
    get_logger()
    LOGGER_STREAM_HANDLER.setLevel(level)

def logger_streamhandler_debug():
    set_logger_streamhandler_level(logging.DEBUG)
def logger_streamhandler_info():
    set_logger_streamhandler_level(logging.INFO)

logger = get_logger()


## GDAL error handler setup
class GdalErrorHandler(object):
    def __init__(self, catch_warnings=None, print_uncaught_warnings=None):
        self.catch_warnings = catch_warnings if catch_warnings is not None else True
        self.print_warnings = print_uncaught_warnings if print_uncaught_warnings is not None else True
        self.errored = False
        self.err_level = None
        self.err_no = None
        self.err_msg = None
        self.reset_error_state()

    def reset_error_state(self):
        self.errored = False
        self.err_level = gdal.CE_None
        self.err_no = 0
        self.err_msg = ''

    def handler(self, err_level, err_no, err_msg):
        self.errored = True
        self.err_level = err_level
        self.err_no = err_no
        self.err_msg = err_msg
        error_message = (
            "Caught GDAL error (err_level={}, err_no={}) "
            "where level >= gdal.CE_Warning({}); error message below:\n{}".format(
                self.err_level, self.err_no, gdal.CE_Warning, self.err_msg
            )
        )
        if self.err_level == gdal.CE_Warning:
            if self.catch_warnings:
                raise RuntimeError(error_message)
            elif self.print_warnings:
                logger.warning(error_message)
        elif self.err_level > gdal.CE_Warning:
            raise RuntimeError(error_message)

GDAL_ERROR_HANDLER = None
def setup_gdal_error_handler(catch_warnings=None, print_uncaught_warnings=None):
    global GDAL_ERROR_HANDLER
    if GDAL_ERROR_HANDLER is None:
        err = GdalErrorHandler(catch_warnings, print_uncaught_warnings)
        handler = err.handler  # Note: Don't pass class method directly or python segfaults
        gdal.PushErrorHandler(handler)
        gdal.UseExceptions()  # Exceptions will get raised on anything >= gdal.CE_Failure
        GDAL_ERROR_HANDLER = err
    else:
        if catch_warnings is not None:
            GDAL_ERROR_HANDLER.catch_warnings = catch_warnings
        if print_uncaught_warnings is not None:
            GDAL_ERROR_HANDLER.print_warnings = print_uncaught_warnings

def get_gdal_error_handler():
    setup_gdal_error_handler()
    return GDAL_ERROR_HANDLER

class GdalHandleWarnings(object):
    def __init__(self, catch_warnings, print_warnings=None):
        setup_gdal_error_handler()
        self.catch_warnings = catch_warnings
        self.print_warnings = print_warnings
        self.catch_warnings_backup = None
        self.print_warnings_backup = None
    def __enter__(self):
        global GDAL_ERROR_HANDLER
        if self.print_warnings is None:
            self.print_warnings = GDAL_ERROR_HANDLER.print_warnings
        self.catch_warnings_backup = GDAL_ERROR_HANDLER.catch_warnings
        self.print_warnings_backup = GDAL_ERROR_HANDLER.print_warnings
        GDAL_ERROR_HANDLER.catch_warnings = self.catch_warnings
        GDAL_ERROR_HANDLER.print_warnings = self.print_warnings
    def __exit__(self, exc_type, exc_val, exc_tb):
        global GDAL_ERROR_HANDLER
        GDAL_ERROR_HANDLER.catch_warnings = self.catch_warnings_backup
        GDAL_ERROR_HANDLER.print_warnings = self.print_warnings_backup

# Use the following classes in a 'with' statement to wrap a code block
# -- Example --
# with GdalCatchWarnings():
#     ...
class GdalCatchWarnings(GdalHandleWarnings):
    def __init__(self, print_warnings=None):
        super(GdalCatchWarnings, self).__init__(catch_warnings=True, print_warnings=print_warnings)
class GdalAllowWarnings(GdalHandleWarnings):
    def __init__(self, print_warnings=None):
        super(GdalAllowWarnings, self).__init__(catch_warnings=False, print_warnings=print_warnings)

# Setup GDAL error handler for all scripts that import this module.
# If this behavior is not desired, you can instead call this setup
# at the top of only the scripts in which you want to handle warnings.
setup_gdal_error_handler(catch_warnings=True, print_uncaught_warnings=True)


# Copy DEM global vars
deliv_suffixes = (
    ### ASP
    '-DEM.prj',
    '-DEM.tif',
    '-DRG.tif',
    '-IntersectionErr.tif',
    '-GoodPixelMap.tif',
    '-stereo.default',
    '-PC.laz',
    '-PC.las',
    '.geojson',

    ### SETSM
    '_dem.tif',
    '_ortho.tif',
    '_matchtag.tif',
    '_meta.txt'
)

archive_suffix = ".tar"

shp_suffixes = (
    '.shp',
    '.shx',
    '.prj',
    '.dbf'
)

pc_suffixes = (
    '-PC.tif',
    '-PC-center.txt'
)

fltr_suffixes = (
    '_fltr-DEM.tif',
    '_fltr-DEM.prj'
)

log_suffixes = (
    '-log-point2dem',
    '-log-stereo_corr',
    '-log-stereo_pprc',
    '-log-stereo_fltr',
    '-log-stereo_rfne',
    '-log-stereo_tri',
)

field_attrib_map = {
    # 'DENSITY': 'density',
    'MASK_DENS': 'masked_density',
    'VALID_DENS': 'valid_density',
    'VALID_AREA': 'valid_area',
    'VALID_PERC': 'valid_perc',
    'WATER_AREA': 'water_area',
    'WATER_PERC': 'water_perc',
    'CLOUD_AREA': 'cloud_area',
    'CLOUD_PERC': 'cloud_perc',
    'AVGCONVANG': 'avg_conv_angle',
    'AVG_HT_ACC': 'avg_exp_height_acc',
    'AVG_SUNEL1': 'avg_sun_el1',
    'AVG_SUNEL2': 'avg_sun_el2',
}

# common name id, attribute field name, storage type, field width, field precision
StandardAttribute = namedtuple("StandardAttribute", ("fname", "fname_long", "ftype", "fwidth", "fprecision"))

# Attributes
# TODO change field type for strips
DEM_ATTRIBUTE_DEFINITIONS_BASIC = [

    ## Overlap attributes
    StandardAttribute("DEM_ID", "", ogr.OFTString, 254, 0),
    StandardAttribute("STRIPDEMID", "", ogr.OFTString, 254, 0),
    StandardAttribute("PAIRNAME", "", ogr.OFTString, 64, 0),
    StandardAttribute("SENSOR1", "", ogr.OFTString, 8, 0),
    StandardAttribute("SENSOR2", "", ogr.OFTString, 8, 0),
    StandardAttribute("ACQDATE1", "", ogr.OFTString, 32, 0),
    StandardAttribute("ACQDATE2", "", ogr.OFTString, 32, 0),
    StandardAttribute("AVGACQTM1", "", ogr.OFTString, 32, 0),
    StandardAttribute("AVGACQTM2", "", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID1", "", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID2", "", ogr.OFTString, 32, 0),
    StandardAttribute("CENT_LAT", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CENT_LON", "", ogr.OFTReal, 0, 0),
    StandardAttribute("GEOCELL", "", ogr.OFTString, 10, 0),
    StandardAttribute("REGION", "", ogr.OFTString, 64, 0),

    ## Result DEM attributes
    StandardAttribute("EPSG", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("PROJ4", "", ogr.OFTString, 100, 0),
    StandardAttribute("ND_VALUE", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CR_DATE", "", ogr.OFTString, 32, 0),
    # StandardAttribute("CR_DATE", "", ogr.OFTDateTime, 32, 0),
    StandardAttribute("ALGM_VER", "", ogr.OFTString, 32, 0),
    StandardAttribute("S2S_VER", "", ogr.OFTString, 32, 0),
    StandardAttribute("IS_LSF", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("IS_XTRACK", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("EDGEMASK", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("WATERMASK", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("CLOUDMASK", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("MASK_DENS", "", ogr.OFTReal, 0, 0),
    StandardAttribute("VALID_DENS", "", ogr.OFTReal, 0, 0),
    StandardAttribute("VALID_AREA", "", ogr.OFTReal, 0, 0),
    StandardAttribute("VALID_PERC", "", ogr.OFTReal, 0, 0),
    StandardAttribute("WATER_AREA", "", ogr.OFTReal, 0, 0),
    StandardAttribute("WATER_PERC", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CLOUD_AREA", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CLOUD_PERC", "", ogr.OFTReal, 0, 0),
    StandardAttribute("AVGCONVANG", "", ogr.OFTReal, 0, 0),
    StandardAttribute("AVG_HT_ACC", "", ogr.OFTReal, 0, 0),
    StandardAttribute("AVG_SUNEL1", "", ogr.OFTReal, 0, 0),
    StandardAttribute("AVG_SUNEL2", "", ogr.OFTReal, 0, 0),
    StandardAttribute("RMSE", "", ogr.OFTReal, 0, 0),
]

DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION = [
    StandardAttribute("REG_SRC", "", ogr.OFTString, 20, 0),
    StandardAttribute("DX", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DY", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DZ", "", ogr.OFTReal, 0, 0),
    StandardAttribute("NUM_GCPS", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("MEANRESZ", "", ogr.OFTReal, 0, 0),
]

DEM_ATTRIBUTE_DEFINITIONS = DEM_ATTRIBUTE_DEFINITIONS_BASIC + [
    StandardAttribute("LOCATION", "", ogr.OFTString, 512, 0),
    StandardAttribute("FILESZ_DEM", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_MT", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR2", "", ogr.OFTReal, 0, 0),
    StandardAttribute("INDEX_DATE", "", ogr.OFTString, 32, 0),
    # StandardAttribute("INDEX_DATE", "", ogr.OFTDateTime, 32, 0),
]

DEM_ATTRIBUTE_DEFINITION_RELVER = [
    StandardAttribute("REL_VER", "", ogr.OFTString, 20, 0)
]

SCENE_ATTRIBUTE_DEFINITIONS_BASIC = [

    ## Overlap attributes
    StandardAttribute("SCENEDEMID", "", ogr.OFTString, 254, 0),
    StandardAttribute("STRIPDEMID", "", ogr.OFTString, 254, 0),
    StandardAttribute("STATUS", "", ogr.OFTString, 8, 0),
    StandardAttribute("PAIRNAME", "", ogr.OFTString, 64, 0),
    StandardAttribute("SENSOR1", "", ogr.OFTString, 8, 0),
    StandardAttribute("SENSOR2", "", ogr.OFTString, 8, 0),
    StandardAttribute("ACQDATE1", "", ogr.OFTDateTime, 0, 0),
    StandardAttribute("ACQDATE2", "", ogr.OFTDateTime, 0, 0),
    StandardAttribute("CATALOGID1", "", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID2", "", ogr.OFTString, 32, 0),
    StandardAttribute("SCENE1", "", ogr.OFTString, 100, 0),
    StandardAttribute("SCENE2", "", ogr.OFTString, 100, 0),
    StandardAttribute("GEN_TIME1", "", ogr.OFTDateTime, 0, 0),
    StandardAttribute("GEN_TIME2", "", ogr.OFTDateTime, 0, 0),
    StandardAttribute("CENT_LAT", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CENT_LON", "", ogr.OFTReal, 0, 0),
    StandardAttribute("REGION", "", ogr.OFTString, 64, 0),

    ## Result DEM attributes
    StandardAttribute("EPSG", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("PROJ4", "", ogr.OFTString, 100, 0),
    StandardAttribute("ND_VALUE", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CR_DATE", "", ogr.OFTDateTime, 0, 0),
    StandardAttribute("ALGM_VER", "", ogr.OFTString, 32, 0),
    StandardAttribute("PROD_VER", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("HAS_LSF", "", ogr.OFSTBoolean, 0, 0),
    StandardAttribute("HAS_NONLSF", "", ogr.OFSTBoolean, 0, 0),
    StandardAttribute("IS_XTRACK", "", ogr.OFSTBoolean, 0, 0),
    StandardAttribute("IS_DSP", "", ogr.OFSTBoolean, 0, 0),
]

SCENE_ATTRIBUTE_DEFINITIONS_REGISTRATION = []

SCENE_ATTRIBUTE_DEFINITIONS = SCENE_ATTRIBUTE_DEFINITIONS_BASIC + [
    StandardAttribute("LOCATION", "", ogr.OFTString, 512, 0),
    StandardAttribute("FILESZ_DEM", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_LSF", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_MT", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR", "", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR2", "", ogr.OFTReal, 0, 0),
    StandardAttribute("INDEX_DATE", "", ogr.OFTDateTime, 0, 0),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC = [

    ## Overlap attributes
    StandardAttribute("DEM_ID", "", ogr.OFTString, 80, 0),
    StandardAttribute("TILE", "", ogr.OFTString, 20, 0),
    StandardAttribute("SUPERTILE", "", ogr.OFTString, 50, 0),
    StandardAttribute("EPSG", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("ND_VALUE", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CR_DATE", "", ogr.OFTDateTime, 0, 0),
    StandardAttribute("DENSITY", "", ogr.OFTReal, 0, 0),
    StandardAttribute("NUM_COMP", "", ogr.OFTInteger, 8, 8),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION = [
    StandardAttribute("REG_SRC", "", ogr.OFTString, 20, 0),
    StandardAttribute("NUM_GCPS", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("MEANRESZ", "", ogr.OFTReal, 0, 0),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS = TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC + [
    StandardAttribute("LOCATION", "", ogr.OFTString, 512, 0),
    StandardAttribute("FILESZ_DEM", "", ogr.OFTReal, 0, 0),
    StandardAttribute("INDEX_DATE", "", ogr.OFTDateTime, 0, 0),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS_RELEASE = [
    StandardAttribute("DEM_ID", "", ogr.OFTString, 100, 0),
    StandardAttribute("TILE", "", ogr.OFTString, 50, 0),
    StandardAttribute("SUPERTILE", "", ogr.OFTString, 50, 0),
    StandardAttribute("GSD", "", ogr.OFTReal, 0, 0),
    StandardAttribute("EPSG", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("RELEASEVER", "RELEASE_VER", ogr.OFTString, 20, 0),
    StandardAttribute("CR_DATE", "CREATIONDATE", ogr.OFTDate, 0, 0),
    StandardAttribute("DATA_PERC", "DATA_PERCENT", ogr.OFTReal, 0, 0),
    StandardAttribute("NUM_COMP", "NUM_COMPONENTS", ogr.OFTInteger, 8, 8),
    StandardAttribute("FILEURL", "", ogr.OFTString, 254, 0),
    StandardAttribute("S3URL", "", ogr.OFTString, 254, 0),
]

OVERLAP_FILE_BASIC_ATTRIBUTE_DEFINITIONS = [

    ## Overlap attributes, written on overlap submission
    StandardAttribute("OVERLAP", "", ogr.OFTString, 254, 0),
    StandardAttribute("PAIRNAME", "", ogr.OFTString, 64, 0),
    StandardAttribute("STATUS", "", ogr.OFTInteger, 2, 0)
]

OVERLAP_FILE_ADDITIONAL_ATTRIBUTE_DEFINITIONS = [

    StandardAttribute("MODE", "", ogr.OFTString, 16, 0),
    StandardAttribute("CATALOGID1", "", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID2", "", ogr.OFTString, 32, 0),
    StandardAttribute("CENT_LAT", "", ogr.OFTReal, 0, 0),
    StandardAttribute("CENT_LON", "", ogr.OFTReal, 0, 0),
    StandardAttribute("EPSG", "", ogr.OFTInteger, 8, 8),
    StandardAttribute("EXT_AREA", "", ogr.OFTReal, 0, 0),

    ## Result DEM attributes, written after process finishes
    StandardAttribute("ND_AREA", "", ogr.OFTReal, 0, 0),
    StandardAttribute("ND_PERC", "", ogr.OFTReal, 0, 0),
    StandardAttribute("ND_VALUE", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", "", ogr.OFTReal, 0, 0),
    StandardAttribute("PC_RES", "", ogr.OFTReal, 0, 0),

    ## Process atributes, written after process finishes
    StandardAttribute("ASPVERSION", "", ogr.OFTString, 64, 0),
    StandardAttribute("ASPBUILDID", "", ogr.OFTString, 16, 0),
    StandardAttribute("C_SEEDMODE", "", ogr.OFTInteger, 8, 0),
    StandardAttribute("C_TIMEOUT", "", ogr.OFTInteger, 8, 0),
    StandardAttribute("REFN_MTHD", "", ogr.OFTInteger, 8, 0),
    StandardAttribute("ALIGN_MTHD", "", ogr.OFTString, 64, 0),
    StandardAttribute("HOST", "", ogr.OFTString, 32, 0),
    StandardAttribute("SEED_DEM", "", ogr.OFTString, 512, 0),
    StandardAttribute("CR_DATE", "", ogr.OFTString, 32, 0),
    StandardAttribute("RUNTIME", "", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_NAME", "", ogr.OFTString, 254, 0)
]

OVERLAP_FILE_ATTRIBUTE_DEFINITIONS = OVERLAP_FILE_BASIC_ATTRIBUTE_DEFINITIONS + OVERLAP_FILE_ADDITIONAL_ATTRIBUTE_DEFINITIONS


class RasterIOError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)


class SpatialRef(object):

    def __init__(self, epsg):
        srs = osr_srs_preserve_axis_order(osr.SpatialReference())
        try:
            epsgcode = int(epsg)

        except ValueError:
            raise RuntimeError("EPSG value must be an integer: %s" % epsg)
        else:

            err = srs.ImportFromEPSG(epsgcode)
            if err == 7:
                raise RuntimeError("Invalid EPSG code: %d" % epsgcode)
            else:
                proj4_string = srs.ExportToProj4()

                proj4_patterns = {
                    "+ellps=GRS80 +towgs84=0,0,0,0,0,0,0": "+datum=NAD83",
                    "+ellps=WGS84 +towgs84=0,0,0,0,0,0,0": "+datum=WGS84",
                }

                for pattern, replacement in proj4_patterns.items():
                    if proj4_string.find(pattern) != -1:
                        proj4_string = proj4_string.replace(pattern, replacement)

                self.srs = srs
                self.proj4 = proj4_string
                self.epsg = epsgcode


def osr_srs_preserve_axis_order(osr_srs):
    try:
        osr_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    except AttributeError:
        pass
    return osr_srs


def check_file_inclusion(f, pairname, overlap_prefix, args):
    move_file = False

    #### determine if file is part of overlap
    if overlap_prefix in f:

        if f.endswith(deliv_suffixes):
            move_file = True
        if f.endswith(fltr_suffixes):
            move_file = False

        if args.include_pc is True:
            if f.endswith(pc_suffixes):
                move_file = True

        if args.include_logs is True:
            if f.endswith(log_suffixes):
                move_file = True

        if args.include_fltr is True:
            if f.endswith(fltr_suffixes):
                move_file = True

        if args.exclude_drg is True:
            if f.endswith(('-DRG.tif', '_ortho.tif')):
                move_file = False

        if args.exclude_err is True:
            if f.endswith('-IntersectionErr.tif'):
                move_file = False

        if args.dems_only is True:
            move_file = False
            if f.endswith(("-DEM.tif", '-DEM.prj', '.geojson', '_dem.tif', '_meta.txt')):
                move_file = True
            if f.endswith(("_fltr-DEM.tif", '_fltr-DEM.prj')):
                if args.include_fltr:
                    move_file = True
                else:
                    move_file = False

        if args.tar_only is True:
            move_file = False
            if f.endswith(".tar"):
                move_file = True

    #### determine if file is in pair shp
    if f.endswith(shp_suffixes) and pairname in f and '-DEM' not in f:
        if not args.dems_only:
            move_file = True

    return move_file


def get_source_names(src_fp):
    """Get the source footprint name and layer name, if provided"""

    if src_fp.lower().endswith((".shp", ".gdb", ".gpkg")):
        _src_fp = src_fp
        src_lyr = os.path.splitext(os.path.basename(src_fp))[0]
    elif ".gdb" in src_fp.lower() and not src_fp.lower().endswith(".gdb"):
        _src_fp, src_lyr = re.split(r"(?<=\.gdb)/", src_fp, re.I)
    else:
        msg = "The source {} does not appear to be a Shapefile, File GDB, or GeoPackage -- quitting".format(src_fp)
        raise RuntimeError(msg)

    return _src_fp, src_lyr


def get_source_names2(src_str):
    """Get the source data format type, dataset connection str, and layer name"""

    src_str_abs = os.path.abspath(src_str)

    if src_str.lower().endswith(".shp"):
        driver = ["ESRI Shapefile"]
        src_ds = src_str_abs
        src_lyr = os.path.splitext(os.path.basename(src_str_abs))[0]

    elif ".gdb" in src_str.lower():
        driver = ["FileGDB", "OpenFileGDB"]
        if not src_str_abs.lower().endswith(".gdb"):
            src_ds, src_lyr = re.split(r"(?<=\.gdb)/", src_str_abs, re.I)
        else:
            src_ds = src_str
            src_lyr = os.path.splitext(os.path.basename(src_str))[0]

    elif ".gpkg" in src_str.lower():
        driver = ["GPKG"]
        if not src_str_abs.lower().endswith(".gpkg"):
            src_ds, src_lyr = re.split(r"(?<=\.gpkg)/", src_str_abs, re.I)
        else:
            src_ds = src_str
            src_lyr = os.path.splitext(os.path.basename(src_str))[0]

    elif src_str.lower().startswith("pg:"):
        driver = ["PostgreSQL"]
        pfx, src_ds, src_lyr = src_str.split(":")

    else:
        msg = "The source {} does not appear to be a Shapefile, File GDB, GeoPackage, or PostgreSQL connection -- quitting".format(
            src_str)
        raise RuntimeError(msg)

    return driver, src_ds, src_lyr


def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step


def progress(count, total, suffix=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()  # As suggested by Rom Ruben


def get_tiles_from_shp(shp, field):
    tiles = {}
    shp_srs = None

    ds = ogr.Open(shp)
    if ds is not None:

        lyr = ds.GetLayerByName(os.path.splitext(os.path.basename(shp))[0])
        lyr.ResetReading()

        i = lyr.FindFieldIndex(field, 1)
        if i == -1:
            logger.error("Cannot locate field {} in {}".format(field, shp))
            sys.exit(-1)

        shp_srs = lyr.GetSpatialRef()
        if shp_srs is None:
            logger.error("Shp must have a defined spatial reference")
            sys.exit(-1)

        for feat in lyr:
            tile_name = feat.GetFieldAsString(i)
            tile_geom = feat.GetGeometryRef().Clone()
            if not tile_name in tiles:
                tiles[tile_name] = tile_geom
            else:
                logger.error("Found features with duplicate name: {} - Ignoring 2nd feature".format(tile_name))

    else:
        logger.error("Cannot open {}".format(shp))

    return tiles, shp_srs


def shelve_item(raster, dst, args, tiles=None, shp_srs=None):
    dst_dir = None
    if args.mode == 'geocell':
        ## get centroid and round down to floor to make geocell folder
        raster.get_metafile_info()
        geocell = raster.get_geocell()
        dst_dir = os.path.join(dst, geocell)

    elif args.mode == 'date':
        platform = raster.sensor1
        year = raster.acqdate1.strftime("%Y")
        month = raster.acqdate1.strftime("%m")
        day = raster.acqdate1.strftime("%d")
        dst_dir = os.path.join(dst, platform, year, month, day)

    elif args.mode == 'shp':
        ## Convert geom to match shp srs and get centroid
        raster.get_metafile_info()
        geom_copy = raster.geom.Clone()
        srs = osr_srs_preserve_axis_order(osr.SpatialReference())
        srs.ImportFromProj4(raster.proj4_meta)
        if not shp_srs.IsSame(srs):
            ctf = osr.CoordinateTransformation(srs, shp_srs)
            geom_copy.Transform(ctf)
        centroid = geom_copy.Centroid()

        ## Run intersection with each tile
        tile_overlaps = []
        for tile_name, tile_geom in tiles.items():
            if centroid.Intersects(tile_geom):
                tile_overlaps.append(tile_name)

        ## Raise an error on multiple intersections or zero intersections
        if len(tile_overlaps) == 0:
            logger.warning("raster {} does not intersect the index shp, skipping".format(raster.srcfn))

        elif len(tile_overlaps) > 1:
            logger.warning("raster {} intersects more than one tile ({}), skipping".format(raster.srcfn,
                                                                                           ','.join(tile_overlaps)))
        else:
            # logger.info("{} shelved to tile {}".format(raster.stripid, tile_overlaps[0]))
            dst_dir = os.path.join(dst, tile_overlaps[0])

    if dst_dir:
        if not os.path.isdir(dst_dir):
            if not args.dryrun:
                os.makedirs(dst_dir)

        glob1 = glob.glob(os.path.join(raster.srcdir, raster.stripid) + "_*")
        tar_path = os.path.join(raster.srcdir, raster.stripid) + ".tar.gz"
        if os.path.isfile(tar_path):
            glob1.append(tar_path)

        if args.skip_ortho:
            glob2 = [f for f in glob1 if 'ortho' not in f]
            glob1 = glob2

        ## Check if existing and remove all matching files if overwrite
        glob3 = glob.glob(os.path.join(dst_dir, raster.stripid) + "_*")
        tar_path = os.path.join(dst_dir, raster.stripid) + ".tar.gz"
        if os.path.isfile(tar_path):
            glob3.append(tar_path)

        proceed = True
        if len(glob3) > 0:
            if args.overwrite:
                logger.info("Destination files already exist for {} - overwriting all dest files".format(
                    raster.stripid))
                for ofp in glob3:
                    logger.debug("Removing {} due to --overwrite flag".format(ofp))
                    if not args.dryrun:
                        os.remove(ofp)
            else:
                logger.info(
                    "Destination files already exist for {} - skipping DEM. Use --overwrite to overwrite".format(
                        raster.stripid))
                proceed = False

        ## Link or copy files
        if proceed:
            for ifp in glob1:
                ofp = os.path.join(dst_dir, os.path.basename(ifp))
                logger.debug("Linking {} to {}".format(ifp, ofp))
                if not args.dryrun:
                    if args.try_link:
                        try:
                            os.link(ifp, ofp)
                        except OSError:
                            logger.error("os.link failed on {}".format(ifp))
                    else:
                        logger.debug("Copying {} to {}".format(ifp, ofp))
                        shutil.copy2(ifp, ofp)


def getWrappedGeometry(src_geom):
    """
    Change a single-polygon extent to multipart if it crosses 180 latitude
    Author: Claire Porter

    :param src_geom: <osgeo.ogr.Geometry>
    :return: <osgeo.ogr.Geometry> type wkbMultiPolygon
    """

    def calc_y_intersection_with_180(pt1, pt2):
        """
        Find y where x is 180 longitude

        :param pt1: <list> coordinate pair, as int or float
        :param pt2: <list> coordinate pair, int or float
        :return: <float>
        """
        # Add 360 to negative x coordinates
        pt1_x = pt1[0] + 360.0 if pt1[0] < 0.0 else pt1[0]
        pt2_x = pt2[0] + 360.0 if pt2[0] < 0.0 else pt2[0]

        rise = pt2[1] - pt1[1]      # Difference in y
        run = pt2_x - pt1_x         # Difference in x
        run_prime = 180.0 - pt1_x   # Difference in x to 180

        try:
            pt3_y = ((run_prime * rise) / run) + pt1[1]
        except ZeroDivisionError as err:
            raise RuntimeError(err)

        return pt3_y

    # Points lists for west and east components
    west_points = []
    east_points = []

    # Assume a single polygon, deconstruct to points, skipping last one
    ring_geom = src_geom.GetGeometryRef(0)
    for i in range(0, ring_geom.GetPointCount() - 1):
        pt1 = ring_geom.GetPoint(i)
        pt2 = ring_geom.GetPoint(i + 1)

        # Add point to appropriate bin (points on 0.0 go to east)
        if pt1[0] < 0.0:
            west_points.append(pt1)
        else:
            east_points.append(pt1)

        # Test if segment to next point crosses 180 (x is opposite sign)
        if (pt1[0] > 0) - (pt1[0] < 0) != (pt2[0] > 0) - (pt2[0] < 0):

            # If segment crosses, calculate y for the intersection point
            pt3_y = calc_y_intersection_with_180(pt1, pt2)

            # Add the intersection point to both bins (change 180 to -180 for west)
            west_points.append((-180.0, pt3_y))
            east_points.append((180.0, pt3_y))

    # Build a multipart polygon from the new point sets (repeat first point to close polygon)
    mp_geometry = ogr.Geometry(ogr.wkbMultiPolygon)

    for ring_points in (west_points, east_points):

        if len(ring_points) > 0:

            # Create the basic objects
            poly = ogr.Geometry(ogr.wkbPolygon)
            ring = ogr.Geometry(ogr.wkbLinearRing)

            # Add the points to the ring
            for pt in ring_points:
                ring.AddPoint(pt[0], pt[1])

            # Repeat the first point to close the ring
            ring.AddPoint(ring_points[0][0], ring_points[0][1])

            # Add the ring to the polygon and the polygon to the geometry
            poly.AddGeometry(ring)
            mp_geometry.AddGeometry(poly)

            # Clean up memory
            del poly
            del ring

    return mp_geometry


def verify_scheduler_args(parser, args, scriptpath, submission_script_map):

    # For back-compatibility take --pbs and --slurm args and translate them to the --scheduler arg
    if [args.pbs, args.slurm, args.scheduler is not None].count(True) > 1:
        parser.error("Command can only include one of the following options: --pbs, --slurm, --scheduler")

    for s in SCHEDULERS:
        if getattr(args, s):
            setattr(args, 'scheduler', s)
            break

    # Warn that back-compatibility will be deprecated
    if args.pbs or args.slurm:
        print("WARNING: --pbs and --slurm options will be deprecated.  Use --scheduler [pbs|slurm] syntax instead.")

    qsubpath = None
    if args.scheduler:
        if not args.qsubscript:
            qsubpath = os.path.join(os.path.dirname(scriptpath), submission_script_map[args.scheduler])
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: %s" % qsubpath)

    ## Verify processing options do not conflict
    if args.scheduler and args.parallel_processes > 1:
        parser.error("HPC Options --scheduler and --parallel-processes > 1 are mutually exclusive")

    if hasattr(args, 'tasks_per_job'):
        if args.tasks_per_job and not args.scheduler:
            parser.error("jobs-per-task argument requires the scheduler option")

    return qsubpath


def add_scheduler_options(parser, submission_script_map, include_tasks_per_job=False):
    parser.add_argument("--scheduler", choices=SCHEDULERS,
                        help="submit tasks to the specified scheduler")
    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to the pbs scheduler (same as `--scheduler pbs`)")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to the slurm scheduler (same as `--scheduler slurm`)")
    if include_tasks_per_job:
        parser.add_argument("--tasks-per-job", type=int,
                            help="number of tasks to bundle into a single job (requires scheduler option)")
    parser.add_argument("--qsubscript",
                        help="script to use in scheduler submission "
                             "({})".format(', '.join([f'{k}: {v}' for k, v in submission_script_map.items()])))
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")

