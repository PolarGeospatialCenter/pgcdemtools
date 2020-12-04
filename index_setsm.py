import os, sys, string, shutil, glob, re, logging, json, pickle
import datetime
import gdal, osr, ogr, gdalconst
import argparse
import numpy
from numpy import flatnonzero
from lib import utils, dem
try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

class GdalErrorHandler(object):
    def __init__(self):
        self.err_level=gdal.CE_None
        self.err_no=0
        self.err_msg=''

    def handler(self, err_level, err_no, err_msg):
        self.err_level=err_level
        self.err_no=err_no
        self.err_msg=err_msg

err=GdalErrorHandler()
handler=err.handler # Note don't pass class method directly or python segfaults
gdal.PushErrorHandler(handler)
gdal.UseExceptions() #Exceptions will get raised on anything >= gdal.CE_Failure

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

FORMAT_OPTIONS = {
    'SHP':'ESRI Shapefile',
    'GDB':'ESRI Geodatabase',
    'PG':'PostgreSQL Database (PG:<config.ini section with connection info>:<layer name>}',
}
FORMAT_HELP = ['{}:{},'.format(k,v) for k, v in FORMAT_OPTIONS.items()]

PROJECTS = {
    'arcticdem': 'ArcticDEM',
    'rema': 'REMA',
    'earthdem': 'EarthDEM',
}

mask_strip_suffixes = (
    '_dem_water-masked.tif',
    '_dem_cloud-masked.tif',
    '_dem_cloud-water-masked.tif',
    '_dem_masked.tif'
)

MODES = {
    ## mode : (class, suffix, groupid_fld, field_def)
    'scene': (dem.SetsmScene, '_meta.txt', 'stripdemid',
               utils.SCENE_ATTRIBUTE_DEFINITIONS, utils.SCENE_ATTRIBUTE_DEFINITIONS_REGISTRATION),
    'strip': (dem.SetsmDem, '_dem.tif', 'stripdemid',
               utils.DEM_ATTRIBUTE_DEFINITIONS, utils.DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION),
    'tile':  (dem.SetsmTile, '_dem.tif', 'supertile_id',
               utils.TILE_DEM_ATTRIBUTE_DEFINITIONS, utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION),
}

id_flds = ['SCENEDEMID', 'STRIPDEMID', 'DEM_ID', 'TILE', 'LOCATION', 'INDEX_DATE']
recordid_map = {
    'scene': '{SCENEDEMID}|{STRIPDEMID}|{LOCATION}|{INDEX_DATE}',
    'strip': '{DEM_ID}|{STRIPDEMID}|{LOCATION}|{INDEX_DATE}',
    'tile':  '{DEM_ID}|{TILE}|{LOCATION}|{INDEX_DATE}',
}

BP_PATH_PREFIX = 'https://blackpearl-data2.pgc.umn.edu/dems/setsm'
TNVA_PATH_PREFIX = '/mnt/pgc/data/elev/dem/setsm'


def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="build setsm DEM index"
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory or image")
    parser.add_argument('dst', help="destination index dataset (use PG:<config.ini section name>:<layer name> for a postgresql DB")

    #### Optional Arguments
    parser.add_argument('--mode', choices=MODES.keys(), default='scene',
                        help="type of items to index {} default=scene".format(MODES.keys()))
    parser.add_argument('--config', default=os.path.join(os.path.dirname(sys.argv[0]),'config.ini'),
                        help="config file (default is config.ini in script dir")
    parser.add_argument('--epsg', type=int, default=4326,
                        help="egsg code for output index projection (default wgs85 geographic epsg:4326)")
    parser.add_argument('--dsp-original-res', action='store_true', default=False,
                        help='write index of downsampled product (dsp) scenes as original resolution (mode=scene only)')
    parser.add_argument('--status', help='custom value for status field')
    parser.add_argument('--include-registration', action='store_true', default=False,
                        help='include registration info if present (mode=strip and tile only)')
    parser.add_argument('--search-masked', action='store_true', default=False,
                        help='search for masked and unmasked DEMs (mode=strip only)')
    parser.add_argument('--read-json', action='store_true', default=False,
                        help='search for json files instead of images to populate the index')
    parser.add_argument('--write-json', action='store_true', default=False,
                        help='write results to json files in dst folder')
    parser.add_argument('--log', help="directory for log output")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument('--append', action='store_true', default=False,
                        help="append records to existing index")
    parser.add_argument('--check', action='store_true', default=False,
                        help='verify new records exist in target index (not compatible with --write-json)')
    parser.add_argument('--skip-region-lookup', action='store_true', default=False,
                        help="skip region lookup on danco (used for testing)")
    parser.add_argument("--write-pickle", help="store region lookup in a pickle file. skipped if --write-json is used")
    parser.add_argument("--read-pickle", help='read region lookup from a pickle file. skipped if --write-json is used')
    parser.add_argument("--bp-paths", action='store_true', default=False, help='Use BlackPearl path schema')
    parser.add_argument("--tnva-paths", action='store_true', default=False, help='Use Terranova path schema')
    parser.add_argument('--project', choices=PROJECTS.keys(), help='project name (required when writing tiles)')
    parser.add_argument('--dryrun', action='store_true', default=False, help='run script without inserting records')


    #### Parse Arguments
    args = parser.parse_args()

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)

    #src = os.path.abspath(args.src)
    src = args.src
    dst = args.dst

    if args.overwrite and args.append:
        parser.error('--append and --overwrite are mutually exclusive')

    if args.write_json and args.append:
        parser.error('--append cannot be used with the --write-json option')

    if (args.write_json or args.read_json) and args.search_masked:
        parser.error('--search-masked cannot be used with the --write-json or --read-json options')

    if args.mode != 'strip' and args.search_masked:
        parser.error('--search-masked applies only to mode=strip')

    if args.write_json and args.check:
        parser.error('--check cannot be used with the --write-json option')

    ## Check project
    if args.mode == 'tile' and not args.project:
        parser.error("--project option is required if when mode=tile")

    if args.write_pickle:
        if not os.path.isdir(os.path.dirname(args.write_pickle)):
            parser.error("Pickle file must be in an existing directory")
    if args.read_pickle:
        if not os.path.isfile(args.read_pickle):
            parser.error("Pickle file must be an existing file")

    if args.status and args.bp_paths:
        parser.error("--bp-paths sets status field to 'tape' and cannot be used with --status")

    if args.tnva_paths and args.bp_paths:
        parser.error("--bp-paths and --tnva-paths are incompatible options")

    if args.mode != 'scene' and args.dsp_original_res:
        parser.error("--dsp-original-res is applicable only when mode = scene")

    if args.bp_paths:
        db_path_prefix = BP_PATH_PREFIX
    elif args.tnva_paths:
        db_path_prefix = TNVA_PATH_PREFIX
    else:
        db_path_prefix = None

    #### Set up loggers
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    if args.log:
        if os.path.isdir(args.log):
            tm = datetime.datetime.now()
            logfile = os.path.join(args.log,"index_setsm_{}.log".format(tm.strftime("%Y%m%d%H%M%S")))
        else:
            parser.error('log folder does not exist: {}'.format(args.log))

        lfh = logging.FileHandler(logfile)
        lfh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
        lfh.setFormatter(formatter)
        logger.addHandler(lfh)

    ## Check path if --write-json is invoked
    if args.write_json:
        if not os.path.isdir(dst):
            parser.error("Destination must be an existing directory with --write-json option")
        ogr_driver_str = None

        if args.epsg:
            logger.warning('--epsg and --dsp-original-res will be ignored with the --write-json option')

    ## If not writing to JSON, get OGR driver, ds name, and layer name
    else:
        try:
            ogr_driver_str, dst_dsp, dst_lyr = utils.get_source_names2(dst)
        except RuntimeError as e:
            parser.error(e)

        ogrDriver = ogr.GetDriverByName(ogr_driver_str)
        if ogrDriver is None:
            parser.error("Driver is not available: {}".format(ogr_driver_str))

        #### Get Config file contents
        try:
            config = ConfigParser.SafeConfigParser()
        except NameError:
            config = ConfigParser.ConfigParser()  # ConfigParser() replaces SafeConfigParser() in Python >=3.2
        config.read(args.config)

        #### Get output DB connection if specified
        if ogr_driver_str in ("PostgreSQL"):

            section = dst_dsp
            if section in config.sections():
                conn_info = {
                    'host':config.get(section,'host'),
                    'port':config.getint(section,'port'),
                    'name':config.get(section,'name'),
                    'schema':config.get(section,'schema'),
                    'user':config.get(section,'user'),
                    'pw':config.get(section,'pw'),
                }
                dst_ds = "PG:host={host} port={port} dbname={name} user={user} password={pw} active_schema={schema}".format(**conn_info)

            else:
                logger.error('Config.ini file must contain credentials to connect to {}'.format(section))
                sys.exit(-1)

        #### Set dataset path is SHP or GDB
        elif ogr_driver_str in ("ESRI Shapefile","FileGDB"):
            dst_ds = dst_dsp

        else:
            logger.error("Format {} is not supported".format(ogr_driver_str))

        ## Get pairname-region dict
        if args.skip_region_lookup or args.mode == 'tile':
            pairs = {}
        else:
            if args.read_pickle:
                logger.info("Fetching region lookup from pickle file")
                pairs = pickle.load(open(args.read_pickle, "rb"))

            else:
                #### Get Danco connection if available
                section = 'danco'
                if section in config.sections():
                    danco_conn_info = {
                        'host':config.get(section,'host'),
                        'port':config.getint(section,'port'),
                        'name':config.get(section,'name'),
                        'schema':config.get(section,'schema'),
                        'user':config.get(section,'user'),
                        'pw':config.get(section,'pw'),
                    }

                    logger.info("Fetching region lookup from Danco")
                    pairs = get_pair_region_dict(danco_conn_info)
                else:
                    logger.warning('Config file does not contain credentials to connect to Danco. Region cannot be determined.')
                    pairs = {}

            if len(pairs) == 0:
                logger.warning("Cannot get region-pair lookup")

                if args.tnva_paths:
                    logger.error("Region-pair lookup required for --tnva-paths option")
                    sys.exit()

        ## Save pickle if selected
        if args.write_pickle:
            logger.info("Pickling region lookup")
            pickle.dump(pairs, open(args.write_pickle, "wb"))

        #### Test epsg
        try:
            spatial_ref = utils.SpatialRef(args.epsg)
        except RuntimeError as e:
            parser.error(e)

        #### Test if dst table exists
        if ogr_driver_str == 'ESRI Shapefile' and os.path.isfile(dst_ds):
            if args.overwrite:
                logger.info("Removing old index... %s" %os.path.basename(dst_ds))
                if not args.dryrun:
                    ogrDriver.DeleteDataSource(dst_ds)
            elif not args.append:
                logger.error("Dst shapefile exists.  Use the --overwrite or --append options.")
                sys.exit(-1)

        if ogr_driver_str == 'FileGDB' and os.path.isdir(dst_ds):
            ds = ogrDriver.Open(dst_ds,1)
            if ds:
                for i in range(ds.GetLayerCount()):
                    lyr = ds.GetLayer(i)
                    if lyr.GetName() == dst_lyr:
                        if args.overwrite:
                            logger.info("Removing old index layer: {}".format(dst_lyr))
                            del lyr
                            ds.DeleteLayer(i)
                            break
                        elif not args.append:
                            logger.error("Dst GDB layer exists.  Use the --overwrite or --append options.")
                            sys.exit(-1)
                ds = None

        ## Postgres check - do not overwrite
        if ogr_driver_str == 'PostgreSQL':
            ds = ogrDriver.Open(dst_ds,1)
            if ds:
                for i in range(ds.GetLayerCount()):
                    lyr = ds.GetLayer(i)
                    if lyr.GetName() == dst_lyr:
                        if args.overwrite:
                            logger.info("Removing old index layer: {}".format(dst_lyr))
                            del lyr
                            ds.DeleteLayer(i)
                            break
                        elif not args.append:
                            logger.error("Dst DB layer exists.  Use the --overwrite or --append options.")
                            sys.exit(-1)
                ds = None


    #### ID records
    dem_class, suffix, groupid_fld, fld_defs_base, reg_fld_defs = MODES[args.mode]
    if args.mode == 'strip' and args.search_masked:
        suffix = mask_strip_suffixes + tuple([suffix])
    fld_defs = fld_defs_base + reg_fld_defs if args.include_registration else fld_defs_base
    src_fps = []
    records = []
    logger.info('Identifying DEMs')
    if os.path.isfile(src):
        logger.info(src)
        src_fps.append(src)
    else:
        for root,dirs,files in os.walk(src):
            for f in files:
                if (f.endswith('.json') and args.read_json) or (f.endswith(suffix)):
                    logger.debug(os.path.join(root,f))
                    src_fps.append(os.path.join(root,f))

    for src_fp in src_fps:
        if args.read_json:
            temp_records = read_json(os.path.join(src_fp),args.mode)
            records.extend(temp_records)
        else:
            try:
                record = dem_class(src_fp)
                record.get_dem_info()
            except RuntimeError as e:
                logger.error( e )
            else:
                if args.mode == 'scene' and not os.path.isfile(record.dspinfo) and args.dsp_original_res:
                    logger.error("Record {} has no Dsp downsample info file: {}, skipping".format(record.id,record.dspinfo))
                else:
                    records.append(record)

    total = len(records)

    if total == 0:
        logger.info("No valid records found")
    else:
        logger.info("{} records found".format(total))
        ## Group into strips or tiles for json writing
        groups = {}
        for record in records:
            groupid = getattr(record,groupid_fld)
            if groupid in groups:
                groups[groupid].append(record)
            else:
                groups[groupid] = [record]

        #### Write index
        if args.write_json:
            write_to_json(dst, groups, total, args)
        else:
            write_result = write_to_ogr_dataset(ogr_driver_str, ogrDriver, dst_ds, dst_lyr, groups,
                                                pairs, total, db_path_prefix, fld_defs, args)
            if write_result:
                sys.exit(0)
            else:
                sys.exit(-1)


def write_to_ogr_dataset(ogr_driver_str, ogrDriver, dst_ds, dst_lyr, groups, pairs, total, db_path_prefix, fld_defs, args):

    ## Create dataset if it does not exist
    if ogr_driver_str == 'ESRI Shapefile':
        max_fld_width = 254
        if os.path.isfile(dst_ds):
            ds = ogrDriver.Open(dst_ds,1)
        else:
            ds = ogrDriver.CreateDataSource(dst_ds)

    elif ogr_driver_str == 'FileGDB':
        max_fld_width = 1024
        if os.path.isdir(dst_ds):
            ds = ogrDriver.Open(dst_ds,1)
        else:
            ds = ogrDriver.CreateDataSource(dst_ds)

    elif ogr_driver_str == 'PostgreSQL':
        max_fld_width = 1024
        # DB must already exist
        ds = ogrDriver.Open(dst_ds,1)

    else:
        logger.error("Format {} is not supported".format(ogr_driver_str))

    if args.status:
        status = args.status
    elif args.bp_paths:
        status = 'tape'
    else:
        status = 'online'

    if ds is not None:

        ## Create table if it does not exist
        layer = ds.GetLayerByName(dst_lyr)
        fld_list = [f.fname for f in fld_defs]

        err.err_level = gdal.CE_None
        tgt_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
        tgt_srs.ImportFromEPSG(args.epsg)
        if err.err_level >= gdal.CE_Warning:
            raise RuntimeError(err.err_level, err.err_no, err.err_msg)

        if not layer:
            logger.info("Creating table...")

            layer = ds.CreateLayer(dst_lyr, tgt_srs, ogr.wkbMultiPolygon)
            if layer:
                for field_def in fld_defs:
                    field = ogr.FieldDefn(field_def.fname, field_def.ftype)
                    field.SetWidth(min(max_fld_width, field_def.fwidth))
                    field.SetPrecision(field_def.fprecision)
                    layer.CreateField(field)

        ## Append Records
        if layer:
            # Get field widths
            lyr_def = layer.GetLayerDefn()
            fwidths = {lyr_def.GetFieldDefn(i).GetName().upper(): lyr_def.GetFieldDefn(i).GetWidth() for i in range(lyr_def.GetFieldCount())}

            logger.info("Appending records...")
            #### loop through records and add features
            i=0
            recordids = []

            for groupid in groups:
                for record in groups[groupid]:
                    i+=1
                    progress(i,total,"features written")
                    feat = ogr.Feature(layer.GetLayerDefn())
                    valid_record = True

                    ## Set attributes
                    ## Fields for scene DEM
                    if args.mode == 'scene':

                        attrib_map = {
                            'SCENEDEMID': record.dsp_sceneid if (args.dsp_original_res and record.is_dsp) else record.sceneid,
                            'STRIPDEMID': record.dsp_stripdemid if (args.dsp_original_res and record.is_dsp) else record.stripdemid,
                            'STATUS': status,
                            'PAIRNAME': record.pairname,
                            'SENSOR1': record.sensor1,
                            'SENSOR2': record.sensor2,
                            'ACQDATE1': record.acqdate1.strftime('%Y-%m-%d'),
                            'ACQDATE2': record.acqdate2.strftime('%Y-%m-%d'),
                            'CATALOGID1': record.catid1,
                            'CATALOGID2': record.catid2,
                            'HAS_LSF': int(os.path.isfile(record.lsf_dem)),
                            'HAS_NONLSF': int(os.path.isfile(record.dem)),
                            'IS_XTRACK': int(record.is_xtrack),
                            'IS_DSP': 0 if args.dsp_original_res else int(record.is_dsp),
                            'ALGM_VER': record.algm_version,
                            'PROJ4': record.proj4,
                            'EPSG': record.epsg,
                        }

                        attr_pfx = 'dsp_' if args.dsp_original_res else ''
                        for k in record.filesz_attrib_map:
                            attrib_map[k.upper()] = getattr(record,'{}{}'.format(attr_pfx,k))

                        # Set region
                        try:
                            region = pairs[record.pairname]
                        except KeyError as e:
                            region = None
                        else:
                            attrib_map['REGION'] = region

                        if db_path_prefix:
                            if args.bp_paths:
                                # https://blackpearl-data2.pgc.umn.edu/dem/setsm/scene/WV02/2015/05/
                                # WV02_20150506_1030010041510B00_1030010043050B00_50cm_v040002.tar
                                custom_path = "{}/{}/{}/{}/{}.tar".format(
                                    args.mode,               # mode (scene, strip, tile)
                                    record.pairname[:4],     # sensor
                                    record.pairname[5:9],    # year
                                    record.pairname[9:11],   # month
                                    groupid                  # mode-specific group ID
                                )

                            elif args.tnva_paths:
                                # /mnt/pgc/data/elev/dem/setsm/ArcticDEM/region/arcticdem_01_iceland/scenes/
                                # 2m/WV01_20200630_10200100991E2C00_102001009A862700_2m_v040204/
                                # WV01_20200630_10200100991E2C00_102001009A862700_504471479080_01_P001_504471481090_01_P001_2_meta.txt

                                if not region:
                                    logger.error("Pairname not found in region lookup {}, cannot built custom path".format(record.pairname))
                                    valid_record = False

                                else:
                                    pretty_project = PROJECTS[region.split('_')[0]]
                                    res_dir = record.res_str + '_dsp' if record.is_dsp else record.res_str

                                    custom_path = "{}/{}/region/{}/scenes/{}/{}/{}".format(
                                        db_path_prefix,
                                        pretty_project,         # project (e.g. ArcticDEM)
                                        region,                 # region
                                        res_dir,                # e.g. 2m, 50cm, 2m_dsp
                                        groupid,                # strip ID
                                        record.srcfn            # file name (meta.txt)
                                    )
                            else:
                                logger.error("Mode {} does not support the specified custom path option, skipping record".format(args.mode))
                                valid_record = False

                    ## Fields for strip DEM
                    if args.mode == 'strip':
                        attrib_map = {
                            'DEM_ID': record.stripid,
                            'STRIPDEMID': record.stripdemid,
                            'PAIRNAME': record.pairname,
                            'SENSOR1': record.sensor1,
                            'SENSOR2': record.sensor2,
                            'ACQDATE1': record.acqdate1.strftime('%Y-%m-%d'),
                            'ACQDATE2': record.acqdate2.strftime('%Y-%m-%d'),
                            'CATALOGID1': record.catid1,
                            'CATALOGID2': record.catid2,
                            'IS_LSF': int(record.is_lsf),
                            'IS_XTRACK': int(record.is_xtrack),
                            'EDGEMASK': int(record.mask_tuple[0]),
                            'WATERMASK': int(record.mask_tuple[1]),
                            'CLOUDMASK': int(record.mask_tuple[2]),
                            'ALGM_VER': record.algm_version,
                            'FILESZ_DEM': record.filesz_dem,
                            'FILESZ_MT': record.filesz_mt,
                            'FILESZ_OR': record.filesz_or,
                            'FILESZ_OR2': record.filesz_or2,
                            'PROJ4': record.proj4,
                            'EPSG': record.epsg,
                            'GEOCELL': record.geocell,
                        }

                        ## Set region
                        try:
                            region = pairs[record.pairname]
                        except KeyError as e:
                            pass
                        else:
                            attrib_map['REGION'] = region

                        if record.version:
                            attrib_map['REL_VER'] = record.version
                        if record.density:
                            attrib_map['DENSITY'] = record.density
                        else:
                            attrib_map['DENSITY'] = -9999

                        ## If registration info exists
                        if args.include_registration:
                            if len(record.reginfo_list) > 0:
                                for reginfo in record.reginfo_list:
                                    if reginfo.name == 'ICESat':
                                        attrib_map["DX"] = reginfo.dx
                                        attrib_map["DY"] = reginfo.dy
                                        attrib_map["DZ"] = reginfo.dz
                                        attrib_map["REG_SRC"] = 'ICESat'
                                        attrib_map["NUM_GCPS"] = reginfo.num_gcps
                                        attrib_map["MEANRESZ"] = reginfo.mean_resid_z

                        ## Set path folders for use if db_path_prefix specified
                        if db_path_prefix:
                            if args.bp_paths:
                               custom_path = "{}/{}/{}/{}/{}/{}.tar".format(
                                    db_path_prefix,
                                    args.mode,               # mode (scene, strip, tile)
                                    record.pairname[:4],     # sensor
                                    record.pairname[5:9],    # year
                                    record.pairname[9:11],   # month
                                    groupid                  # mode-specific group ID
                                )
                            else:
                                logger.error("Mode {} does not support the specified custom path option, skipping record".format(args.mode))
                                valid_record = False

                    ## Fields for tile DEM
                    if args.mode == 'tile':
                        attrib_map = {
                            'DEM_ID': record.tileid,
                            'TILE': record.tilename,
                            'NUM_COMP': record.num_components,
                            'FILESZ_DEM': record.filesz_dem,
                        }

                        ## Optional attributes
                        if record.version:
                            attrib_map['REL_VER'] = record.version
                            version = record.version
                        else:
                            version = 'novers'
                        if record.density:
                            attrib_map['DENSITY'] = record.density
                        else:
                            attrib_map['DENSITY'] = -9999

                        if args.include_registration:
                            if record.reg_src:
                                attrib_map["REG_SRC"] = record.reg_src
                                attrib_map["NUM_GCPS"] = record.num_gcps
                            if record.mean_resid_z:
                                attrib_map["MEANRESZ"] = record.mean_resid_z

                        ## Set path folders for use if db_path_prefix specified
                        if db_path_prefix:
                            if args.bp_paths:
                                custom_path = "{}/{}/{}/{}/{}/{}.tar".format(
                                    db_path_prefix,
                                    record.mode,               # mode (scene, strip, tile)
                                    args.project.lower(),    # project
                                    record.res,              # resolution
                                    version,                 # version
                                    groupid                  # mode-specific group ID
                                )
                            else:
                                logger.error("Mode {} does not support the specified custom path option, skipping record".format(args.mode))
                                valid_record = False

                    ## Common fields
                    if valid_record:
                        ## Common Attributes across all modes
                        attrib_map['INDEX_DATE'] = datetime.datetime.today().strftime('%Y-%m-%d')
                        attrib_map['CR_DATE'] = record.creation_date.strftime('%Y-%m-%d')
                        attrib_map['ND_VALUE'] = record.ndv
                        if args.dsp_original_res:
                            res = record.dsp_dem_res
                        else:
                            res = (record.xres + record.yres) / 2.0
                        attrib_map['DEM_RES'] = res

                        ## Set location
                        if db_path_prefix:
                            location = custom_path
                        else:
                            location = record.srcfp
                        attrib_map['LOCATION'] = location

                        ## Transform and write geom
                        src_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
                        src_srs.ImportFromWkt(record.proj)

                        if not record.geom:
                            logger.error('No valid geom found, feature skipped: {}'.format(record.sceneid))
                            valid_record = False
                        else:
                            temp_geom = record.geom.Clone()
                            transform = osr.CoordinateTransformation(src_srs,tgt_srs)
                            try:
                                temp_geom.Transform(transform)
                            except TypeError as e:
                                logger.error('Geom transformation failed, feature skipped: {} {}'.format(e, record.sceneid))
                                valid_record = False
                            else:

                                ## Get centroid coordinates
                                centroid = temp_geom.Centroid()
                                if 'CENT_LAT' in fld_list:
                                    attrib_map['CENT_LAT'] = centroid.GetY()
                                    attrib_map['CENT_LON'] = centroid.GetX()

                                ## If srs is geographic and geom crosses 180, split geom into 2 parts
                                if tgt_srs.IsGeographic:

                                    ## Get Lat and Lon coords in arrays
                                    lons = []
                                    lats = []
                                    ring = temp_geom.GetGeometryRef(0)  #### assumes a 1 part polygon
                                    for j in range(0, ring.GetPointCount()):
                                        pt = ring.GetPoint(j)
                                        lons.append(pt[0])
                                        lats.append(pt[1])

                                    ## Test if image crosses 180
                                    if max(lons) - min(lons) > 180:
                                        split_geom = wrap_180(temp_geom)
                                        feat_geom = split_geom
                                    else:
                                        mp_geom = ogr.ForceToMultiPolygon(temp_geom)
                                        feat_geom = mp_geom

                                else:
                                    mp_geom = ogr.ForceToMultiPolygon(temp_geom)
                                    feat_geom = mp_geom

                    ## Write feature
                    if valid_record:
                        for fld,val in attrib_map.items():
                            if fld in fwidths:
                                if isinstance(val, str) and len(val) > fwidths[fld]:
                                    logger.warning("Attribute value {} is too long for field {} (width={}). Feature skipped".format(
                                        val, fld, fwidths[fld]
                                    ))
                                    valid_record = False
                            else:
                                logger.warning("Field {} is not in target table. Feature skipped".format(fld))
                                valid_record = False
                            feat.SetField(  # force unicode to str for a bug in GDAL's SetField. Revisit in Python3
                                fld.encode('utf-8'),
                                val if not isinstance(val, unicode) else val.encode('utf-8')
                            )
                        feat.SetGeometry(feat_geom)

                        ## Add new feature to layer
                        if valid_record:
                            if not args.dryrun:
                                # Record record identifiers for later checking
                                if args.check:
                                    recordids.append(recordid_map[args.mode].format(**attrib_map))

                                # Append record
                                err.err_level = gdal.CE_None
                                try:
                                    if ogr_driver_str in ('PostgreSQL'):
                                        layer.StartTransaction()
                                        layer.CreateFeature(feat)
                                        layer.CommitTransaction()
                                    else:
                                        layer.CreateFeature(feat)
                                except Exception as e:
                                    raise e
                                else:
                                    if err.err_level >= gdal.CE_Warning:
                                        raise RuntimeError(err.err_level, err.err_no, err.err_msg)
                                finally:
                                    gdal.PopErrorHandler()

            # Check contents of layer for all records
            if args.check and not args.dryrun:
                if len(recordids) == 0:
                    logger.info("No valid records found. No checks to perform")
                else:
                    layer.ResetReading()
                    attrib_maps = [{id_fld: feat.GetField(id_fld) for id_fld in id_flds if id_fld in fld_list} for feat in layer]
                    layer_recordids = [recordid_map[args.mode].format(**attrib_map) for attrib_map in attrib_maps]
                    layer_recordids = set(layer_recordids)

                    err_cnt = 0
                    for recordid in recordids:
                        if recordid not in layer_recordids:
                            err_cnt += 1
                            logger.error("Record not found in target layer: {}".format(recordid))

                    if err_cnt > 0:
                        sys.exit(-1)

        else:
            logger.error('Cannot open layer: {}'.format(dst_lyr))
            ds = None
            sys.exit(-1)

        ds = None

    else:
        logger.info("Cannot open dataset: {}".format(dst_ds))
        sys.exit(-1)

    if args.dryrun:
        logger.info("Done (dryrun)")
    else:
        logger.info("Done")
    sys.exit(0)


def read_json(json_fp, mode):

    json_fh = open(json_fp,'r')
    records = []
    try:
        md = json.load(json_fh, object_hook=decode_json)
    except ValueError as e:
        logger.error("Cannot decode json in {}: {}".format(json_fp,e))
    else:
        dem_class = MODES[mode][0]

        for k in md:
            try:
                record = dem_class(k,md[k])
            except RuntimeError as e:
                logger.error("Record {}: {}".format(k,e))
            else:
                records.append(record)

    return records


def write_to_json(json_fd, groups, total, args):

    i=0
    for groupid, items in groups.items():

        md = {}
        if args.mode == 'tile':
            json_fn = "{}_{}".format(args.project,groupid)
            json_fp = "{}.json".format(os.path.join(json_fd,json_fn))
        else:
            json_fp = "{}.json".format(os.path.join(json_fd,groupid))

        if os.path.isfile(json_fp) and args.overwrite:
            os.remove(json_fp)

        if not os.path.isfile(json_fp):

            if not args.dryrun:
                # open json
                json_fh = open(json_fp,'w')

            for item in items:
                i+=1
                progress(i,total,"records written")

                # organize scene obj into dict and write to json
                md[item.id] = item.__dict__

            json_txt = json.dumps(md, default=encode_json)
            #print json_txt

            if not args.dryrun:
                json_fh.write(json_txt)
                json_fh.close()

        else:
            logger.info("Json file already exists {}. Use --overwrite to overwrite".format(json_fp))


def get_pair_region_dict(conn_info):
    """Fetches a pairname-region lookup dictionary from Danco's footprint DB
    pairnames_with_earthdem_region table
    """

    pairs = {}
    conn_str = "PG:host={host} port={port} dbname={name} user={user} password={pw} active_schema={schema}".format(**conn_info)
    stereo_ds = ogr.Open(conn_str)

    if stereo_ds is None:
        logger.warning("Could not connect to footprint db")
    else:
        stereo_lyr = stereo_ds.GetLayer("public.pairname_with_earthdem_region")
        if stereo_lyr is None:
            logger.warning("Could not obtain public.pairname_with_earthdem_region layer")
            stereo_ds = None
        else:
            pairs = {f["pairname"]:f["region_id"] for f in stereo_lyr}

    return pairs


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
        if (pt1[0] > 0) - (pt1[0] < 0) != (pt2[0] > 0) - (pt2[0] < 0):

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


def encode_json(o):
    if isinstance(o,datetime.datetime):
        return {
            '__datetime__': True,
            'value': o.__repr__(),
        }
    if isinstance(o,ogr.Geometry):
        return {
            '__geometry__': True,
            'value': o.__str__(),
        }
    if isinstance(o,osr.SpatialReference):
        return {
            '__srs__': True,
            'value': o.__str__(),
        }


def decode_json(d):
    if '__datetime__' in d:
        return eval(d['value'])
    if '__geometry__' in d:
        return ogr.CreateGeometryFromWkt(d['value'])
    if '__srs__' in d:
        srs = osr.SpatialReference()
        srs.ImportFromWkt(d['value'])
        return srs
    return d


if __name__ == '__main__':
    main()
