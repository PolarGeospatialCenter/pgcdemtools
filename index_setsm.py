import os, sys, string, shutil, glob, re, logging
from datetime import *
import gdal, osr, ogr, gdalconst
import argparse
import numpy
from numpy import flatnonzero
from lib import utils, dem

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)



def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="build setsm DEM index"
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory or image")
    parser.add_argument('dst', help="destination index feature class")

    #### Optionsl Arguments
    parser.add_argument('--epsg', type=int, default=4326,
                        help="egsg code for output index projection (default wgs85 geographic epgs:4326)")
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")

    #### Parse Arguments
    args = parser.parse_args()

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)

    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)

    try:
        dst_dir, dst_lyr = utils.get_source_names(dst)
    except RuntimeError, e:
        parser.error(e)

    print (dst_dir,dst_lyr)

    ogr_driver_str = "ESRI Shapefile" if dst.lower().endswith(".shp") else "FileGDB"
    ogrDriver = ogr.GetDriverByName(ogr_driver_str)
    if ogrDriver is None:
        parser.error("GDAL FileGDB driver is not available")

    #### Test epsg
    try:
        spatial_ref = utils.SpatialRef(args.epsg)
    except RuntimeError, e:
        parser.error(e)

    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    if args.log:
        if os.path.isdir(args.log):
            tm = datetime.now()
            logfile = os.path.join(args.log,"index_setsm_{}.log".format(tm.strftime("%Y%m%d%H%M%S")))
        else:
            parser.error('log folder does not exist: {}'.format(args.log))

        lfh = logging.FileHandler(logfile)
        lfh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
        lfh.setFormatter(formatter)
        logger.addHandler(lfh)

    rasters = []

    #### ID rasters
    logger.info('Identifying DEMs')
    if os.path.isfile(src):
        logger.info(src)
        try:
            raster = dem.SetsmDem(os.path.join(src))
        except RuntimeError, e:
            logger.error( e )
        else:
            rasters.append(raster)

    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if f.endswith("dem.tif") and "m_" in f:
                    logger.debug(os.path.join(root,f))
                    try:
                        raster = dem.SetsmDem(os.path.join(root,f))
                    except RuntimeError, e:
                        logger.error( e )
                    else:
                        rasters.append(raster)

    total = len(rasters)

    if total > 0:
        #### Write index
        #### delete old index if shp,  delete layer if GDB?
        if dst.endswith('.shp') and os.path.isfile(dst):
            if args.overwrite:
                logger.info("Removing old index... %s" %os.path.basename(dst))
                ogrDriver.DeleteDataSource(dst)
            else:
                logger.error("Dst shapefile exists.  Use the --overwrite flag to overwrite.")
                sys.exit()

        if dst_dir.endswith('.gdb') and os.path.isdir(dst_dir):
            ds = ogrDriver.Open(dst_dir,1)
            if ds is not None:
                #logger.info(ds.TestCapability(ogr.ODsCDeleteLayer))
                for i in range(ds.GetLayerCount()):
                    lyr = ds.GetLayer(i)
                    if lyr.GetName() == dst_lyr:
                        if args.overwrite:
                            logger.info("Removing old index layer: {}".format(dst_lyr))
                            del lyr
                            ds.DeleteLayer(i)
                            break
                        else:
                            logger.error("Dst GDB layer exists.  Use the --overwrite flag to overwrite.")
                            sys.exit()
                ds = None

        if dst.endswith('.shp'):
            if not os.path.isfile(dst):
                ds = ogrDriver.CreateDataSource(dst)
            else:
                ds = None

        else:
            if os.path.isdir(dst_dir):
                ds = ogrDriver.Open(dst_dir,1)
            else:
                ds = ogrDriver.CreateDataSource(dst_dir)

        if ds is not None:

            logger.info("Building index...")
            #### build new index
            tgt_srs = osr.SpatialReference()
            tgt_srs.ImportFromEPSG(args.epsg)

            layer = ds.CreateLayer(dst_lyr, tgt_srs, ogr.wkbPolygon)

            if layer is not None:

                for field_def in utils.DEM_ATTRIBUTE_DEFINITIONS:

                    field = ogr.FieldDefn(field_def.fname, field_def.ftype)
                    field.SetWidth(field_def.fwidth)
                    field.SetPrecision(field_def.fprecision)
                    layer.CreateField(field)

                #### loop through rasters and add features
                i=0
                for raster in rasters:
                    i+=1
                    progress(i,total,"features written")
                    #print raster.stripid
                    try:
                        raster.get_dem_info()
                        raster.get_geocell()
                    except RuntimeError, e:
                        logger.error( e )
                    else:
                        feat = ogr.Feature(layer.GetLayerDefn())

                        ## Set fields
                        feat.SetField("DEM_ID",raster.stripid)
                        feat.SetField("PAIRNAME",raster.pairname)
                        feat.SetField("SENSOR1",raster.sensor1)
                        feat.SetField("SENSOR2",raster.sensor2)
                        feat.SetField("ACQDATE1",raster.acqdate1.strftime("%Y-%m-%d"))
                        feat.SetField("ACQDATE2",raster.acqdate2.strftime("%Y-%m-%d"))
                        feat.SetField("CATALOGID1",raster.catid1)
                        feat.SetField("CATALOGID2",raster.catid2)
                        feat.SetField("ND_VALUE",raster.ndv)
                        feat.SetField("DEM_NAME",raster.srcfn)
                        feat.SetField("FILEPATH",raster.srcdir)
                        feat.SetField("ALGM_VER",raster.algm_version)
                        feat.SetField("IS_LSF",int(raster.is_lsf))
                        if raster.version:
                            feat.SetField("REL_VER",raster.version)
                        if raster.srcdir.startswith(r'/mnt/pgc'):
                            winpath = raster.srcdir.replace(r'/mnt/pgc',r'V:/pgc')
                            feat.SetField("WIN_PATH",winpath)
                        res = (raster.xres + raster.yres) / 2.0
                        feat.SetField("DEM_RES",res)
                        feat.SetField("GEOCELL",raster.geocell)
                        feat.SetField("FILE_SZ_DEM",raster.filesz_dem)
                        feat.SetField("FILE_SZ_MT",raster.filesz_mt)
                        feat.SetField("FILE_SZ_OR",raster.filesz_or)

                        if raster.density is None:
                            density = -9999
                        else:
                            density = raster.density
                        feat.SetField("DENSITY",density)

                        if len(raster.reginfo_list) > 0:
                            for reginfo in raster.reginfo_list:
                                if reginfo.name == 'ICESat':
                                    feat.SetField("DX",reginfo.dx)
                                    feat.SetField("DY",reginfo.dy)
                                    feat.SetField("DZ",reginfo.dz)
                                    feat.SetField("REG_SRC",'ICESat')
                                    feat.SetField("NUM_GCPS",reginfo.num_gcps)
                                    feat.SetField("MEANRESZ",reginfo.mean_resid_z)

                        #### Set fields if populated (will not be populated if metadata file is not found)
                        if raster.creation_date:
                            feat.SetField("CR_DATE",raster.creation_date.strftime("%Y-%m-%d"))

                        ## transfrom and write geom
                        feat.SetField("PROJ4",raster.proj4)
                        feat.SetField("EPSG",raster.epsg)

                        src_srs = osr.SpatialReference()
                        src_srs.ImportFromWkt(raster.proj)

                        if raster.exact_geom:
                            geom = raster.exact_geom.Clone()
                            transform = osr.CoordinateTransformation(src_srs,tgt_srs)
                            geom.Transform(transform)

                            centroid = geom.Centroid()
                            feat.SetField("CENT_LAT",centroid.GetY())
                            feat.SetField("CENT_LON",centroid.GetX())

                            ## if srs is geographic and geom crosses 180, split geom into 2 parts
                            if tgt_srs.IsGeographic:

                                #### Get Lat and Lon coords in arrays
                                lons = []
                                lats = []
                                #print extent_geom.GetGeometryCount()
                                ring  = geom.GetGeometryRef(0)  #### assumes a 1 part polygon
                                for j in range(0, ring.GetPointCount()):
                                    pt = ring.GetPoint(j)
                                    lons.append(pt[0])
                                    lats.append(pt[1])

                                #### Test if image crosses 180
                                if max(lons) - min(lons) > 180:
                                    split_geom = wrap_180(geom)
                                    feat.SetGeometry(split_geom)
                                else:
                                    feat.SetGeometry(geom)

                            else:
                                feat.SetGeometry(geom)
                            #### add new feature to layer
                            layer.CreateFeature(feat)

                        else:
                            logger.error('No valid geom found, feature skipped: {}'.format(raster.srcfp))


            else:
                logger.error('Cannot create layer: {}'.format(dst_lyr))

            ds = None

        else:
            logger.info("Cannot open/create dataset: %s" %dst)

    else:
        logger.info("No valid rasters found")

    logger.info("Done")




def progress(count, total, suffix=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()  # As suggested by Rom Ruben


def wrap_180(src_geom):

    ## create 2 point lists for west component and east component
    west_points = []
    east_points = []

    ## for each point in geom except final point
    ring  = src_geom.GetGeometryRef(0)  #### assumes a 1 part polygon
    for i in range(0, ring.GetPointCount()-1):
        pt1 = ring.GetPoint(i)
        pt2 = ring.GetPoint(i+1)

        ## test if point is > or < 0 and add to correct bin
        if pt1[0] < 0:
            west_points.append(pt1)
        else:
            east_points.append(pt1)

        ## test if segment to next point crosses 180 (x is opposite sign)
        if cmp(pt1[0],0) <> cmp(pt2[0],0):

            ## if segment crosses,calculate interesection point y value
            pt3_y = calc_y_intersection_180(pt1, pt2)

            ## add intersection point to both bins (make sureot change 180 to -180 for western list)
            pt3_west = ( -180, pt3_y )
            pt3_east = ( 180, pt3_y )

            west_points.append(pt3_west)
            east_points.append(pt3_east)


    #print "west", len(west_points)
    #for pt in west_points:
    #    print pt[0], pt[1]
    #
    #print "east", len(east_points)
    #for pt in east_points:
    #    print pt[0], pt[1]

    ## cat point lists to make multipolygon(remember to add 1st point to the end)
    geom_multipoly = ogr.Geometry(ogr.wkbMultiPolygon)

    for ring_points in west_points, east_points:
        if len(ring_points) > 0:
            poly = ogr.Geometry(ogr.wkbPolygon)
            ring = ogr.Geometry(ogr.wkbLinearRing)

            for pt in ring_points:
                ring.AddPoint(pt[0],pt[1])

            ring.AddPoint(ring_points[0][0],ring_points[0][1])

            poly.AddGeometry(ring)
            geom_multipoly.AddGeometry(poly)
            del poly
            del ring

    #print geom_multipoly
    return geom_multipoly


def calc_y_intersection_180(pt1, pt2):

    #### add 360 to all x coords < 0
    if pt1[0] < 0:
        pt1_x = pt1[0] + 360
    else:
        pt1_x = pt1[0]

    if pt2[0] < 0:
        pt2_x = pt2[0] + 360
    else:
        pt2_x = pt2[0]

    rise = pt2[1] - pt1[1]
    run = pt2_x - pt1_x
    run_prime = 180.0 - pt1_x

    pt3_y = ((run_prime * rise) / run) + pt1[1]
    #print "pt1",pt1
    #print "pt2",pt2
    #print "pt1_x", pt1_x
    #print "pt2_x", pt2_x
    #print "rise",rise
    #print "run", run
    #print "run_prime", run_prime
    #print "y_intersect", pt3_y

    return pt3_y



if __name__ == '__main__':
    main()
