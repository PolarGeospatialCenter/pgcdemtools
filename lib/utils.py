#!/usr/bin/env python

"""
demtools utilties and constants
"""

import logging
import os
import re
import sys

import numpy as np
from collections import namedtuple

from osgeo import osr, ogr, gdalconst, gdal

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
        self.err_level = gdal.CE_None
        self.err_no = 0
        self.err_msg = ''
        self.catch_warnings = catch_warnings if catch_warnings is not None else True
        self.print_warnings = print_uncaught_warnings if print_uncaught_warnings is not None else True

    def handler(self, err_level, err_no, err_msg):
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

# common name id, attribute field name, storage type, field width, field precision
StandardAttribute = namedtuple("StandardAttribute", ("fname", "ftype", "fwidth", "fprecision"))

# Attributes
DEM_ATTRIBUTE_DEFINITIONS_BASIC = [

    ## Overlap attributes
    StandardAttribute("DEM_ID", ogr.OFTString, 254, 0),
    StandardAttribute("STRIPDEMID", ogr.OFTString, 254, 0),
    StandardAttribute("PAIRNAME", ogr.OFTString, 64, 0),
    StandardAttribute("SENSOR1", ogr.OFTString, 8, 0),
    StandardAttribute("SENSOR2", ogr.OFTString, 8, 0),
    StandardAttribute("ACQDATE1", ogr.OFTString, 32, 0),
    StandardAttribute("ACQDATE2", ogr.OFTString, 32, 0),
    StandardAttribute("AVGACQTM1", ogr.OFTString, 32, 0),
    StandardAttribute("AVGACQTM2", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID1", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID2", ogr.OFTString, 32, 0),
    StandardAttribute("CENT_LAT", ogr.OFTReal, 0, 0),
    StandardAttribute("CENT_LON", ogr.OFTReal, 0, 0),
    StandardAttribute("GEOCELL", ogr.OFTString, 10, 0),
    StandardAttribute("REGION", ogr.OFTString, 64, 0),

    ## Result DEM attributes
    StandardAttribute("EPSG", ogr.OFTInteger, 8, 8),
    StandardAttribute("PROJ4", ogr.OFTString, 100, 0),
    StandardAttribute("ND_VALUE", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", ogr.OFTReal, 0, 0),
    StandardAttribute("CR_DATE", ogr.OFTString, 32, 0),
    StandardAttribute("ALGM_VER", ogr.OFTString, 32, 0),
    StandardAttribute("S2S_VER", ogr.OFTString, 32, 0),
    StandardAttribute("IS_LSF", ogr.OFTInteger, 8, 8),
    StandardAttribute("IS_XTRACK", ogr.OFTInteger, 8, 8),
    StandardAttribute("EDGEMASK", ogr.OFTInteger, 8, 8),
    StandardAttribute("WATERMASK", ogr.OFTInteger, 8, 8),
    StandardAttribute("CLOUDMASK", ogr.OFTInteger, 8, 8),
    StandardAttribute("DENSITY", ogr.OFTReal, 0, 0),
    StandardAttribute("MASK_DENS", ogr.OFTReal, 0, 0),
    StandardAttribute("RMSE", ogr.OFTReal, 0, 0),
]

DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION = [
    StandardAttribute("REG_SRC", ogr.OFTString, 20, 0),
    StandardAttribute("DX", ogr.OFTReal, 0, 0),
    StandardAttribute("DY", ogr.OFTReal, 0, 0),
    StandardAttribute("DZ", ogr.OFTReal, 0, 0),
    StandardAttribute("NUM_GCPS", ogr.OFTInteger, 8, 8),
    StandardAttribute("MEANRESZ", ogr.OFTReal, 0, 0),
]

# FIXME: This collection of attributes seems to be misplaced/miscopied?
DEM_ATTRIBUTE_DEFINITIONS_MASKING = [
    StandardAttribute("REG_SRC", ogr.OFTString, 20, 0),
    StandardAttribute("DX", ogr.OFTReal, 0, 0),
    StandardAttribute("DY", ogr.OFTReal, 0, 0),
    StandardAttribute("DZ", ogr.OFTReal, 0, 0),
    StandardAttribute("NUM_GCPS", ogr.OFTInteger, 8, 8),
    StandardAttribute("MEANRESZ", ogr.OFTReal, 0, 0),
]

DEM_ATTRIBUTE_DEFINITIONS = DEM_ATTRIBUTE_DEFINITIONS_BASIC + [
    StandardAttribute("LOCATION", ogr.OFTString, 512, 0),
    StandardAttribute("FILESZ_DEM", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_MT", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR2", ogr.OFTReal, 0, 0),
    StandardAttribute("INDEX_DATE", ogr.OFTString, 32, 0),
]

DEM_ATTRIBUTE_DEFINITION_RELVER = [
    StandardAttribute("REL_VER", ogr.OFTString, 32, 0)
]

SCENE_ATTRIBUTE_DEFINITIONS_BASIC = [

    ## Overlap attributes
    StandardAttribute("SCENEDEMID", ogr.OFTString, 254, 0),
    StandardAttribute("STRIPDEMID", ogr.OFTString, 254, 0),
    StandardAttribute("STATUS", ogr.OFTString, 8, 0),
    StandardAttribute("PAIRNAME", ogr.OFTString, 64, 0),
    StandardAttribute("SENSOR1", ogr.OFTString, 8, 0),
    StandardAttribute("SENSOR2", ogr.OFTString, 8, 0),
    StandardAttribute("ACQDATE1", ogr.OFTString, 32, 0),
    StandardAttribute("ACQDATE2", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID1", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID2", ogr.OFTString, 32, 0),
    StandardAttribute("CENT_LAT", ogr.OFTReal, 0, 0),
    StandardAttribute("CENT_LON", ogr.OFTReal, 0, 0),
    StandardAttribute("REGION", ogr.OFTString, 64, 0),

    ## Result DEM attributes
    StandardAttribute("EPSG", ogr.OFTInteger, 8, 8),
    StandardAttribute("PROJ4", ogr.OFTString, 100, 0),
    StandardAttribute("ND_VALUE", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", ogr.OFTReal, 0, 0),
    StandardAttribute("CR_DATE", ogr.OFTString, 32, 0),
    StandardAttribute("ALGM_VER", ogr.OFTString, 32, 0),
    StandardAttribute("HAS_LSF", ogr.OFTInteger, 8, 8),
    StandardAttribute("HAS_NONLSF", ogr.OFTInteger, 8, 8),
    StandardAttribute("IS_XTRACK", ogr.OFTInteger, 8, 8),
    StandardAttribute("IS_DSP", ogr.OFTInteger, 8, 8),
]

SCENE_ATTRIBUTE_DEFINITIONS_REGISTRATION = []

SCENE_ATTRIBUTE_DEFINITIONS = SCENE_ATTRIBUTE_DEFINITIONS_BASIC + [
    StandardAttribute("LOCATION", ogr.OFTString, 512, 0),
    StandardAttribute("FILESZ_DEM", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_LSF", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_MT", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR", ogr.OFTReal, 0, 0),
    StandardAttribute("FILESZ_OR2", ogr.OFTReal, 0, 0),
    StandardAttribute("INDEX_DATE", ogr.OFTString, 32, 0),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC = [

    ## Overlap attributes
    StandardAttribute("DEM_ID", ogr.OFTString, 80, 0),
    StandardAttribute("TILE", ogr.OFTString, 20, 0),
    StandardAttribute("ND_VALUE", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", ogr.OFTReal, 0, 0),
    StandardAttribute("CR_DATE", ogr.OFTString, 32, 0),
    StandardAttribute("DENSITY", ogr.OFTReal, 0, 0),
    StandardAttribute("NUM_COMP", ogr.OFTInteger, 8, 8),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION = [
    StandardAttribute("REG_SRC", ogr.OFTString, 20, 0),
    StandardAttribute("NUM_GCPS", ogr.OFTInteger, 8, 8),
    StandardAttribute("MEANRESZ", ogr.OFTReal, 0, 0),
]

TILE_DEM_ATTRIBUTE_DEFINITIONS = TILE_DEM_ATTRIBUTE_DEFINITIONS_BASIC + [
    StandardAttribute("LOCATION", ogr.OFTString, 512, 0),
    StandardAttribute("FILESZ_DEM", ogr.OFTReal, 0, 0),
    StandardAttribute("INDEX_DATE", ogr.OFTString, 32, 0),
]

OVERLAP_FILE_BASIC_ATTRIBUTE_DEFINITIONS = [

    ## Overlap attributes, written on overlap submission
    StandardAttribute("OVERLAP", ogr.OFTString, 254, 0),
    StandardAttribute("PAIRNAME", ogr.OFTString, 64, 0),
    StandardAttribute("STATUS", ogr.OFTInteger, 2, 0)
]

OVERLAP_FILE_ADDITIONAL_ATTRIBUTE_DEFINITIONS = [

    StandardAttribute("MODE", ogr.OFTString, 16, 0),
    StandardAttribute("CATALOGID1", ogr.OFTString, 32, 0),
    StandardAttribute("CATALOGID2", ogr.OFTString, 32, 0),
    StandardAttribute("CENT_LAT", ogr.OFTReal, 0, 0),
    StandardAttribute("CENT_LON", ogr.OFTReal, 0, 0),
    StandardAttribute("EPSG", ogr.OFTInteger, 8, 8),
    StandardAttribute("EXT_AREA", ogr.OFTReal, 0, 0),

    ## Result DEM attributes, written after process finishes
    StandardAttribute("ND_AREA", ogr.OFTReal, 0, 0),
    StandardAttribute("ND_PERC", ogr.OFTReal, 0, 0),
    StandardAttribute("ND_VALUE", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_RES", ogr.OFTReal, 0, 0),
    StandardAttribute("PC_RES", ogr.OFTReal, 0, 0),

    ## Process atributes, written after process finishes
    StandardAttribute("ASPVERSION", ogr.OFTString, 64, 0),
    StandardAttribute("ASPBUILDID", ogr.OFTString, 16, 0),
    StandardAttribute("C_SEEDMODE", ogr.OFTInteger, 8, 0),
    StandardAttribute("C_TIMEOUT", ogr.OFTInteger, 8, 0),
    StandardAttribute("REFN_MTHD", ogr.OFTInteger, 8, 0),
    StandardAttribute("ALIGN_MTHD", ogr.OFTString, 64, 0),
    StandardAttribute("HOST", ogr.OFTString, 32, 0),
    StandardAttribute("SEED_DEM", ogr.OFTString, 512, 0),
    StandardAttribute("CR_DATE", ogr.OFTString, 32, 0),
    StandardAttribute("RUNTIME", ogr.OFTReal, 0, 0),
    StandardAttribute("DEM_NAME", ogr.OFTString, 254, 0)
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

    if src_fp.lower().endswith((".shp", ".gdb")):
        _src_fp = src_fp
        src_lyr = os.path.splitext(os.path.basename(src_fp))[0]
    elif ".gdb" in src_fp.lower() and not src_fp.lower().endswith(".gdb"):
        _src_fp, src_lyr = re.split(r"(?<=\.gdb)/", src_fp, re.I)
    else:
        msg = "The source {} does not appear to be a shapefile or File GDB -- quitting".format(src_fp)
        raise RuntimeError(msg)

    return _src_fp, src_lyr


def get_source_names2(src_str):
    """Get the source data format type, dataset connection str, and layer name"""

    src_str_abs = os.path.abspath(src_str)

    if src_str.lower().endswith(".shp"):
        driver = "ESRI Shapefile"
        src_ds = src_str_abs
        src_lyr = os.path.splitext(os.path.basename(src_str_abs))[0]

    elif ".gdb" in src_str.lower():
        driver = "FileGDB"
        if not src_str_abs.lower().endswith(".gdb"):
            src_ds, src_lyr = re.split(r"(?<=\.gdb)/", src_str_abs, re.I)
        else:
            src_ds = src_str
            src_lyr = os.path.splitext(os.path.basename(src_str))[0]

    elif src_str.lower().startswith("pg:"):
        driver = "PostgreSQL"
        pfx, src_ds, src_lyr = src_str.split(":")

    else:
        msg = "The source {} does not appear to be a Shapefile, File GDB, or PostgreSQL connection -- quitting".format(
            src_str)
        raise RuntimeError(msg)

    return driver, src_ds, src_lyr


def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step


def gdalReadAsArraySetsmSceneBand(raster_band, make_nodata_nan=False):
    """Read full GDAL raster band from a SETSM DEM raster into a NumPy array,
    converting data type from scaled integer (Int32) to floating point (Float32)
    if necessary.

    The data type conversion is necessary before working with raw elevation
    values from DEM rasters that are stored in scaled integer format, chiefly
    the `*_dem.tif`, `*_ortho.tif`, and `*_matchtag.tif` rasters from 50cm
    scene DEM results. These rasters are stored in this format with a custom
    LERC & ZSTD compression applied to achive the greatest space savings for
    long term, high data volume storage.

    Rasters that do not have internal 'scale' or 'offset' metadata information
    visible to GDAL will not have their values adjusted, so it should be safe
    to replace all GDAL `ReadAsArray()` calls on SETSM DEM rasters with this
    function.

    Parameters
    ----------
    raster_band : GDALRasterBand
        SETSM DEM raster band to be read.
    make_nodata_nan : boolean, optional
        Convert NoData values in the raster band to NaN in the returned NumPy
        array.

    Returns
    -------
    array_data : numpy.ndarray
        The NumPy array containing adjusted (if necessary) values read from the
        input raster band.
    """
    scale = raster_band.GetScale()
    offset = raster_band.GetOffset()
    if scale is None:
        scale = 1.0
    if offset is None:
        offset = 0.0
    if scale == 1.0 and offset == 0.0:
        array_data = raster_band.ReadAsArray()
        if make_nodata_nan:
            nodata_val = raster_band.GetNoDataValue()
            if nodata_val is not None:
                array_data[array_data == nodata_val] = np.nan
    else:
        if raster_band.DataType != gdalconst.GDT_Int32:
            raise RasterIOError(
                "Expected GDAL raster band with scale!=1.0 or offset!=0.0 to be of Int32 data type"
                " (scaled int LERC_ZSTD-compressed 50cm DEM), but data type is {}".format(
                    gdal.GetDataTypeName(raster_band.DataType)
                )
            )
        if scale == 0.0:
            raise RasterIOError(
                "GDAL raster band has invalid parameters: scale={}, offset={}".format(scale, offset)
            )
        nodata_val = raster_band.GetNoDataValue()
        array_data = raster_band.ReadAsArray(buf_type=gdalconst.GDT_Float32)
        adjust_where = (array_data != nodata_val) if nodata_val is not None else True
        if scale != 1.0:
            np.multiply(array_data, scale, out=array_data, where=adjust_where)
        if offset != 0.0:
            np.add(array_data, offset, out=array_data, where=adjust_where)
        if make_nodata_nan:
            array_nodata = np.logical_not(adjust_where, out=adjust_where)
            array_data[array_nodata] = np.nan
        del adjust_where

    if array_data is None:
        raise RasterIOError("`raster_band.ReadAsArray()` returned None")

    return array_data


def progress(count, total, suffix=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()  # As suggested by Rom Ruben
