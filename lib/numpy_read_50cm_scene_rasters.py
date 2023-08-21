
import numpy as np
from osgeo import gdal, gdalconst


class RasterIOError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)


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
