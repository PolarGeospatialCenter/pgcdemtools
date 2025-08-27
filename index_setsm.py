import argparse
import configparser
import datetime
import json
import logging
import os
import pickle
import re
import sys

from osgeo import gdal, osr, ogr

from lib import utils, dem, walk
from lib import VERSION, SHORT_VERSION

logger = utils.get_logger()
utils.setup_gdal_error_handler()
gdal.UseExceptions()

# Script paths and execution
SCRIPT_FILE = os.path.abspath(os.path.realpath(__file__))
SCRIPT_FNAME = os.path.basename(SCRIPT_FILE)
SCRIPT_NAME, SCRIPT_EXT = os.path.splitext(SCRIPT_FNAME)
SCRIPT_DIR = os.path.dirname(SCRIPT_FILE)

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
    'strip': (dem.SetsmDem, '_dem.tif', 'stripdirname',
               utils.DEM_ATTRIBUTE_DEFINITIONS, utils.DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION),
    'tile':  (dem.SetsmTile, '_dem.tif', 'supertile_id',
               utils.TILE_DEM_ATTRIBUTE_DEFINITIONS, utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_REGISTRATION),
}

id_flds = ['SCENEDEMID', 'STRIPDEMID', 'DEM_ID', 'TILE', 'LOCATION', 'INDEX_DATE', 'IS_DSP']
recordid_map = {
    'scene': '{SCENEDEMID}|{STRIPDEMID}|{IS_DSP}|{LOCATION}|{INDEX_DATE}',
    'strip': '{DEM_ID}|{STRIPDEMID}|{LOCATION}|{INDEX_DATE}',
    'strip_release': '{DEM_ID}|{STRIPDEMID}|{FILEURL}|{CR_DATE}',
    'tile':  '{DEM_ID}|{TILE}|{LOCATION}|{INDEX_DATE}',
    'tile_release': '{DEM_ID}|{TILE}|{FILEURL}|{CR_DATE}',
}

BP_PATH_PREFIX = 'https://blackpearl-data2.pgc.umn.edu'
PGC_PATH_PREFIX = '/mnt/pgc/data/elev/dem/setsm'
BW_PATH_PREFIX = '/scratch/sciteam/GS_bazu/elev/dem/setsm'
CSS_PATH_PREFIX = '/css/nga-dems/setsm'

custom_path_prefixes = {
    'BP': BP_PATH_PREFIX,
    'PGC': PGC_PATH_PREFIX,
    'BW': BW_PATH_PREFIX,
    'CSS': CSS_PATH_PREFIX
}

DSP_OPTIONS = {
    'dsp': 'record using current downsample product DEM res',
    'orig': 'record using original pre-DSP DEM res',
    'both': 'write a record for each'
}

dem_type_folder_lookup = {
    'strip': 'strips',
    'tile': 'mosaics',
}

DEFAULT_DSP_OPTION = 'dsp'

# handle unicode in Python 3
try:
    unicode('')
except NameError:
    unicode = str


class RawTextArgumentDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter): pass


def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        formatter_class=RawTextArgumentDefaultsHelpFormatter,
        description="build setsm DEM index"
        )

    #### Positional Arguments
    parser.add_argument('src', help="source directory or image")
    parser.add_argument('dst', help="destination index dataset (use PG:<config.ini section name>:<layer name> for a postgresql DB")

    #### Optional Arguments
    parser.add_argument('--mode', choices=MODES.keys(), default='scene',
                        help="type of items to index {} default=scene".format(MODES.keys()))
    parser.add_argument('--config', default=os.path.join(SCRIPT_DIR, 'config.ini'),
                        help="config file (default is config.ini in script dir, or fallback to ~/.pg_service.conf)")
    parser.add_argument('--epsg', type=int, default=4326,
                        help="egsg code for output index projection (default wgs85 geographic epsg:4326)")
    parser.add_argument('--dsp-record-mode', choices=DSP_OPTIONS.keys(), default=DEFAULT_DSP_OPTION,
                        help='resolution mode for downsampled product (dsp) record (mode=scene only):\n{}'.format(
                            '\n'.join([k+': '+v for k,v in DSP_OPTIONS.items()])
                        ))
    parser.add_argument('--status', help='custom value for status field')
    parser.add_argument('--status-dsp-record-mode-orig', help='custom value for status field when dsp-record-mode is set to "orig"')
    # DEPRECATED
    # parser.add_argument('--include-registration', action='store_true', default=False,
    #                     help='include registration info if present (mode=strip and tile only)')
    parser.add_argument('--use-release-fields', action='store_true', default=False,
                        help="use field definitions for tile release indices (mode=tile only)")
    parser.add_argument('--long-fieldnames', action='store_true', default=False,
                        help="use long format (>10 chars) version of fieldnames")
    parser.add_argument('--lowercase-fieldnames', action='store_true', default=False,
                        help="make fieldnames lowercase when writing to new destination index")
    parser.add_argument('--search-masked', action='store_true', default=False,
                        help='search for masked and unmasked DEMs (mode=strip only)')
    parser.add_argument('--read-json', action='store_true', default=False,
                        help='search for json files instead of images to populate the index')
    parser.add_argument('--write-json', action='store_true', default=False,
                        help='write results to json files in dst folder')
    parser.add_argument('--maxdepth', type=float, default=float('inf'),
                        help='maximum depth into source directory to be searched')
    parser.add_argument('--log', help="directory for log output (debug messages written here)")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help="overwrite existing index")
    parser.add_argument('--append', action='store_true', default=False,
                        help="append records to existing index")
    parser.add_argument('--check', action='store_true', default=False,
                        help='verify new records exist in target index (not compatible with --write-json or --dryrun)')
    parser.add_argument('--skip-region-lookup', action='store_true', default=False,
                        help="skip region lookup on danco")
    parser.add_argument('--skip-records-missing-dsp-original-info', action='store_true', default=False,
                        help="skip adding records where the file info on the source DEM for a dsp product is missing"
                             " (valid only if --dsp-record-mode is orig or both)")
    parser.add_argument("--write-pickle", help="store region lookup in a pickle file. skipped if --write-json is used")
    parser.add_argument("--read-pickle", help='read region lookup from a pickle file. skipped if --write-json is used')
    parser.add_argument("--custom-paths", choices=custom_path_prefixes.keys(), help='Use custom path schema')
    parser.add_argument('--project', choices=utils.PROJECTS.keys(), help='project name (required when writing tiles)')
    parser.add_argument('--debug', action='store_true', default=False, help='print DEBUG level logger messages to terminal')
    parser.add_argument('--dryrun', action='store_true', default=False, help='run script without inserting records')
    parser.add_argument('--np', action='store_true', default=False, help='do not print progress bar')
    parser.add_argument('--version', action='version', version=f"Current version: {SHORT_VERSION}",
                        help='print version and exit')
    parser.add_argument('--release-fileurl', type=str, default="https://data.pgc.umn.edu/elev/dem/setsm/<project>/<type>/<version>/<resolution>/<group>/<dem_id>.tar.gz",
                        help="template for release field 'fileurl' (--use-release-fields only)")
    parser.add_argument('--release-s3url', type=str, default="https://polargeospatialcenter.github.io/stac-browser/#/external/pgc-opendata-dems.s3.us-west-2.amazonaws.com/<project>/<type>/<version>/<resolution>/<group>/<dem_id>.json",
                        help="template for release field 's3url' (--use-release-fields only)")
    #### Parse Arguments
    args = parser.parse_args()

    if args.debug:
        utils.logger_streamhandler_debug()

    #### Verify Arguments
    if not os.path.isdir(args.src) and not os.path.isfile(args.src):
        parser.error("Source directory or file does not exist: %s" %args.src)

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

    if args.mode == 'strip' and args.use_release_fields and not args.project:
        parser.error("--project option is required when mode=strip using --use-release-fields")

    if args.mode == 'scene' and args.use_release_fields:
        parser.error("--use-release-fields option is not applicable to mode=scene")

    ## Todo add Bp region lookup via API instead of Danco?
    if args.skip_region_lookup and (args.custom_paths == 'PGC' or args.custom_paths == 'BP'):
        parser.error('--skip-region-lookup is not compatible with --custom-paths = PGC or BP')

    if args.write_pickle:
        if not os.path.isdir(os.path.dirname(args.write_pickle)):
            parser.error("Pickle file must be in an existing directory")
    if args.read_pickle:
        if not os.path.isfile(args.read_pickle):
            parser.error("Pickle file must be an existing file")

    if args.status and args.custom_paths == 'BP':
        parser.error("--custom_paths BP sets status field to 'tape' and cannot be used with --status.  For dsp-record-mode=orig custom status, use --status-dsp-record-mode-orig")

    path_prefix = custom_path_prefixes[args.custom_paths] if args.custom_paths else None

    if args.log:
        if os.path.isdir(args.log):
            tm = datetime.datetime.now()
            logfile = os.path.join(args.log,"index_setsm_{}.log".format(tm.strftime("%Y%m%d%H%M%S")))
        else:
            parser.error('log folder does not exist: {}'.format(args.log))

        lfh = logging.FileHandler(logfile)
        lfh.setLevel(logging.DEBUG)
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

    logger.info("Current repo version: %s", VERSION)
    rc = 0

    if args.write_json:
        logger.info("Forcing indexer to use absolute paths for writing JSONs")
        src = os.path.abspath(args.src)

    ## If not writing to JSON, get OGR driver, ds name, and layer name
    else:
        try:
            ogr_driver_list, dst_dsp, dst_lyr = utils.get_source_names2(dst)
        except RuntimeError as e:
            parser.error(e)

        ogrDriver = None
        for ogr_driver_str in ogr_driver_list:
            ogrDriver = ogr.GetDriverByName(ogr_driver_str)
            if ogrDriver is not None:
                break
        if ogrDriver is None:
            parser.error("Driver(s) not available: {}".format(', '.join(ogr_driver_list)))
        else:
            logger.info("Driver selected: {}".format(ogr_driver_str))

        #### Get Config file contents
        config = configparser.ConfigParser()
        config.read(args.config)

        pg_config_file = os.path.expanduser("~/.pg_service.conf")
        pg_config = configparser.ConfigParser()
        pg_config.read(pg_config_file)

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
                conn_str_redacted = re.sub(r"password=\S+", "password=PASS", dst_ds)
                logger.info(f"Derived dst dataset PG connection string from {args.config}: '{conn_str_redacted}'")

            elif section in pg_config.sections():
                dst_ds = f"PG:service={section}"
                logger.info(f"Derived dst dataset PG connection string from {pg_config_file}: '{dst_ds}'")

            else:
                logger.error(f"--config file or ~/.pg_service.conf must contain credentials for service name '{section}'")
                rc = -1

        #### Set dataset path is SHP or GDB
        elif ogr_driver_str in ("ESRI Shapefile", "FileGDB", "OpenFileGDB", "GPKG"):
            dst_ds = dst_dsp
            if not os.path.isdir(os.path.dirname(dst_dsp)):
                parser.error('--dst must be within an existing directory')

        else:
            logger.error("Format {} is not supported".format(ogr_driver_str))
            rc = -1

        ## Get pairname-region dict
        if args.skip_region_lookup or args.mode == 'tile':
            pairs = {}
        else:
            if args.read_pickle:
                logger.info("Fetching region lookup from pickle file")
                pairs = pickle.load(open(args.read_pickle, "rb"))

            else:
                #### Get Danco connection if available
                section_depr = 'danco'
                section = 'pgc_danco_footprint'
                conn_str = None
                if section not in config.sections() and section_depr in config.sections():
                    logger.warning(f"Config section name '{section_depr}' is deprecated and should be changed to '{section}'")
                    section = section_depr
                if section in config.sections():
                    danco_conn_info = {
                        'host':config.get(section,'host'),
                        'port':config.getint(section,'port'),
                        'name':config.get(section,'name'),
                        'schema':config.get(section,'schema'),
                        'user':config.get(section,'user'),
                        'pw':config.get(section,'pw'),
                    }
                    conn_str = "PG:host={host} port={port} dbname={name} user={user} password={pw} active_schema={schema}".format(**danco_conn_info)
                    conn_str_redacted = re.sub(r"password=\S+", "password=PASS", conn_str)
                    logger.info(f"Derived Danco connection string from {args.config}: '{conn_str_redacted}'")
                elif section in pg_config.sections():
                    conn_str = f"PG:service={section} active_schema=public"
                    logger.info(f"Derived Danco connection string from {pg_config_file}: '{conn_str}'")
                if conn_str:
                    logger.info("Fetching region lookup from Danco")
                    pairs = get_pair_region_dict(conn_str)
                else:
                    logger.warning(f"--config file or ~/.pg_service.conf do not contain credentials for service name '{section}'. Region cannot be determined.")
                    pairs = {}

            if len(pairs) == 0:
                logger.warning("Cannot get region-pair lookup")

                if args.custom_paths == 'PGC' or args.custom_paths == 'BP':
                    logger.error("Region-pair lookup required for --custom_paths PGC or BP option")
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
        if ogr_driver_str == 'ESRI Shapefile':
            if os.path.isfile(dst_ds):
                if args.overwrite:
                    logger.info("Removing old index... %s" %os.path.basename(dst_ds))
                    if not args.dryrun:
                        ogrDriver.DeleteDataSource(dst_ds)
                elif not args.append:
                    logger.error("Dst shapefile exists.  Use the --overwrite or --append options.")
                    rc = -1

        elif ogr_driver_str in ('FileGDB', 'OpenFileGDB', 'GPKG'):
            if os.path.isdir(dst_ds):
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
                                rc = -1
                    ds = None

        ## Postgres check - do not overwrite
        elif ogr_driver_str == 'PostgreSQL':
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
                            rc = -1
                ds = None

        else:
            logger.error("Format {} not handled in dst table existence check".format(ogr_driver_str))
            rc = -1

    if rc == 0:
        #### ID records
        dem_class, suffix, groupid_fld, fld_defs_base, reg_fld_defs = MODES[args.mode]
        if args.mode == 'tile' and args.use_release_fields:
            fld_defs_base = utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_RELEASE
        if args.mode == 'strip' and args.use_release_fields:
            fld_defs_base = utils.DEM_ATTRIBUTE_DEFINITIONS_RELEASE
        if args.mode == 'strip' and args.search_masked:
            suffix = mask_strip_suffixes + tuple([suffix])
        # fld_defs = fld_defs_base + reg_fld_defs if args.include_registration else fld_defs_base - DEPRECATED
        fld_defs = fld_defs_base
        src_fps = []
        records = []
        logger.info('Source: {}'.format(src))
        logger.info('Identifying DEMs')

        if os.path.isfile(src):
            logger.info(src)
            src_fps.append(src)
        else:
            for root, dirs, files in walk.walk(src, maxdepth=args.maxdepth):
                for f in files:
                    if (f.endswith('.json') and args.read_json) or (f.endswith(suffix) and not args.read_json):
                        logger.debug(os.path.join(root,f))
                        src_fps.append(os.path.join(root,f))

        total = len(src_fps)
        i=0
        for src_fp in src_fps:
            i+=1
            if not args.np:
                utils.progress(i, total, "DEMs identified")
            if args.read_json:
                temp_records = read_json(os.path.join(src_fp),args.mode)
                records.extend(temp_records)
            else:
                record = None
                try:
                    record = dem_class(src_fp)
                    record.get_dem_info()
                except Exception as e:
                    logger.error(e)
                    if record is not None and hasattr(record, 'srcfp'):
                        logger.error("Error encountered on DEM record: {}".format(record.srcfp))
                else:
                    ## Check if DEM is a DSP DEM, dsp-record mode includes 'orig', and the original DEM data is unavailable
                    if args.mode == 'scene' and record.is_dsp and not os.path.isfile(record.dspinfo) \
                            and args.dsp_record_mode in ['orig', 'both']:
                        logger.error("DEM {} has no Dsp downsample info file: {}, skipping".format(record.id,record.dspinfo))
                    else:
                        records.append(record)
        if not args.np:
            print('')

        total = len(records)

        if total == 0:
            logger.info("No valid records found")
        else:
            logger.info("{} records found".format(total))
            ## Group into strips or tiles for json writing
            groups = {}
            for record in records:
                groupid = getattr(record, groupid_fld)
                if groupid in groups:
                    groups[groupid].append(record)
                else:
                    groups[groupid] = [record]

            #### Write index
            if args.write_json:
                write_to_json(dst, groups, total, args)
            elif not args.dryrun:
                rc = write_to_ogr_dataset(ogr_driver_str, ogrDriver, dst_ds, dst_lyr, groups,
                                                    pairs, total, path_prefix, fld_defs, args)
            else:
                logger.info("Exiting dryrun")

    sys.exit(rc)


def write_to_ogr_dataset(ogr_driver_str, ogrDriver, dst_ds, dst_lyr, groups, pairs, total, path_prefix, fld_defs, args):

    ds = None
    rc = 0

    ## Create dataset if it does not exist
    if ogr_driver_str == 'ESRI Shapefile':
        max_fld_width = 254
        if os.path.isfile(dst_ds):
            ds = ogrDriver.Open(dst_ds,1)
        else:
            ds = ogrDriver.CreateDataSource(dst_ds)

    elif ogr_driver_str in ['FileGDB', 'OpenFileGDB', 'GPKG']:
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
    elif args.custom_paths == 'BP':
        status = 'tape'
    else:
        status = 'online'

    dsp_orig_status = args.status_dsp_record_mode_orig if args.status_dsp_record_mode_orig else status

    fld_def_location_fwidth_gdb = None
    for f in fld_defs:
        if f.fname.upper() == 'LOCATION':
            fld_def_location_fwidth_gdb = min(f.fwidth, 1024)
            break

    fld_def_short_to_long_dict = {
        field_def.fname: (field_def.fname_long if field_def.fname_long else field_def.fname) for field_def in fld_defs
    }

    if ds is None:
        logger.info("Cannot open dataset: {}".format(dst_ds))
        rc = -1
    else:

        ## Create table if it does not exist
        layer = ds.GetLayerByName(dst_lyr)
        fld_list = [f.fname for f in fld_defs]

        tgt_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
        tgt_srs.ImportFromEPSG(args.epsg)

        if not layer:
            logger.info("Creating table...")

            # FileGDB will throw a warning when inserting datetimes without this
            if ogr_driver_str in ['FileGDB', 'OpenFileGDB']:
                co = ['TIME_IN_UTC=NO']
            else:
                co = []

            layer = ds.CreateLayer(dst_lyr, tgt_srs, ogr.wkbMultiPolygon, options=co)
            if layer:
                for field_def in fld_defs:
                    fname = fld_def_short_to_long_dict[field_def.fname] if args.long_fieldnames else field_def.fname
                    if args.lowercase_fieldnames:
                        fname = fname.lower()
                    fstype = None
                    if field_def.ftype == ogr.OFTDateTime and ogr_driver_str in ['ESRI Shapefile']:
                        ftype = ogr.OFTString
                        fwidth = 28
                    elif field_def.ftype == ogr.OFSTBoolean:
                        ftype = ogr.OFTInteger
                        fstype = field_def.ftype
                        fwidth = field_def.fwidth
                    else:
                        ftype = field_def.ftype
                        fwidth = field_def.fwidth
                    field = ogr.FieldDefn(fname, ftype)
                    if fstype:
                        field.SetSubType(fstype)
                    field.SetWidth(min(max_fld_width, fwidth))
                    field.SetPrecision(field_def.fprecision)
                    layer.CreateField(field)

                # When creating a new dataset/layer schema with GDAL, something about the schema
                # is kept in cache tied to the dataset connection instance.
                # If after writing to the new layer, the layer records are read back in with GDAL
                # using the same connection instance (with the --check option for example),
                # GDAL may fail to read certain field data types properly (such as boolean fields).
                # This is likely a bug in GDAL!
                # To avoid this, manually close and reopen the connection.
                ds = None
                layer = None
                ds = ogrDriver.Open(dst_ds, 1)
                layer = ds.GetLayerByName(dst_lyr)

        ## Append Records
        if layer:
            # Get field widths
            lyr_def = layer.GetLayerDefn()
            fwidths = {lyr_def.GetFieldDefn(i).GetName():
                           (lyr_def.GetFieldDefn(i).GetWidth(), lyr_def.GetFieldDefn(i).GetType())
                            for i in range(lyr_def.GetFieldCount())
                       }
            fnameupper_fnamelayer_dict = {k.upper(): k for k, v in fwidths.items()}

            logger.info("Appending records...")
            #### loop through records and add features
            i=0
            recordids = []
            invalid_record_cnt = 0
            duplicate_record_cnt = 0

            dsp_modes = ['orig','dsp'] if args.dsp_record_mode == 'both' else [args.dsp_record_mode]

            for groupid in groups:
                for record in groups[groupid]:
                    for dsp_mode in dsp_modes:

                        region = None
                        bp_region = None

                        # skip writing a second "orig" record if the DEM is not a DSP DEM sene
                        if args.mode == 'scene':
                            if not record.is_dsp and dsp_mode == 'orig':
                                continue

                        i+=1
                        if not args.np:
                            utils.progress(i, total * len(dsp_modes), "features written")
                        feat = ogr.Feature(layer.GetLayerDefn())
                        valid_record = True

                        ## Set attributes
                        ## Fields for scene DEM
                        if args.mode == 'scene':
                            logger.debug(f"Processing scene: {record.sceneid} - mode {dsp_mode}")

                            attrib_map = {
                                'SCENEDEMID': record.dsp_sceneid if (dsp_mode == 'orig') else record.sceneid,
                                'STRIPDEMID': record.dsp_stripdemid if (dsp_mode == 'orig') else record.stripdemid,
                                'STATUS': dsp_orig_status if (dsp_mode == 'orig') else status,
                                'PAIRNAME': record.pairname,
                                'SENSOR1': record.sensor1,
                                'SENSOR2': record.sensor2,
                                'ACQDATE1': record.acqdate1.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                'ACQDATE2': record.acqdate2.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                'CATALOGID1': record.catid1,
                                'CATALOGID2': record.catid2,
                                'SCENE1': record.scene1,
                                'SCENE2': record.scene2,
                                'GEN_TIME1': record.gentime1.strftime('%Y-%m-%dT%H:%M:%SZ') if record.gentime1 else None,
                                'GEN_TIME2': record.gentime2.strftime('%Y-%m-%dT%H:%M:%SZ') if record.gentime2 else None,
                                'HAS_LSF': record.has_lsf,
                                'HAS_NONLSF': record.has_nonlsf,
                                'IS_XTRACK': record.is_xtrack,
                                'IS_DSP': False if dsp_mode == 'orig' else record.is_dsp,
                                'ALGM_VER': record.algm_version,
                                'PROD_VER': record.prod_version,
                                'PROJ4': record.proj4,
                                'EPSG': record.epsg,
                            }

                            attr_pfx = 'dsp_' if dsp_mode == 'orig' else ''
                            for k in record.filesz_attrib_map:
                                attrib_map[k.upper()] = getattr(record,'{}{}'.format(attr_pfx,k))

                            # TODO revisit after all incorrect 50cminfo.txt files are ingested
                            # Overwrite original res dsp filesz values will Null
                            if dsp_mode == 'orig':
                                for k in record.filesz_attrib_map:
                                    attrib_map[k.upper()] = None

                            # Test if filesz attr is valid for dsp original res records
                            if dsp_mode == 'orig':
                                if attrib_map['FILESZ_DEM'] is None:
                                    if not args.skip_records_missing_dsp_original_info:
                                        logger.debug(
                                            "Original res filesz_dem is empty for {}. Record will still be written".format(
                                                record.sceneid))
                                    else:
                                        logger.error(
                                            "Original res filesz_dem is empty for {}. Record skipped".format(record.sceneid))
                                        valid_record = False
                                elif attrib_map['FILESZ_DEM'] == 0:
                                    logger.warning(
                                        "Original res filesz_dem is 0 for {}. Record will still be written".format(record.sceneid))

                            # Test if filesz attr is valid for normal records
                            elif not attrib_map['FILESZ_DEM'] and not attrib_map['FILESZ_LSF']:
                                logger.warning(
                                    "DEM and LSF DEM file size is zero or null for {}. Record skipped".format(record.sceneid))
                                valid_record = False

                            # Set region
                            try:
                                pair_items = pairs[record.pairname]
                            except KeyError as e:
                                region = None
                                bp_region = None
                            else:
                                if isinstance(pair_items, str):
                                    region = pair_items
                                elif isinstance(pair_items, tuple):
                                    region, bp_region = pair_items
                                else:
                                    logger.error("Pairname-region lookup value cannot be parsed for pairname {}: {}".format(
                                        record.pairname, pair_items))
                                attrib_map['REGION'] = region

                            if path_prefix:
                                res_dir = record.res_str + '_dsp' if record.is_dsp else record.res_str

                                if args.custom_paths == 'BP':
                                    # https://blackpearl-data2.pgc.umn.edu/dem-scenes-2m-arceua/2m/WV02/2015/05/
                                    # WV02_20150506_1030010041510B00_1030010043050B00_50cm_v040002.tar

                                    if not region:
                                        logger.error("Pairname not found in region lookup {}, cannot build custom path".format(
                                            record.pairname))
                                        valid_record = False

                                    else:
                                        bucket = 'dem-{}s-{}-{}'.format(
                                            args.mode, record.res_str, bp_region.split('-')[0])
                                        custom_path = '/'.join([
                                            path_prefix,
                                            bucket,
                                            res_dir,                 # e.g. 2m, 50cm, 2m_dsp
                                            record.pairname[:4],     # sensor
                                            record.pairname[5:9],    # year
                                            record.pairname[9:11],   # month
                                            groupid+'.tar'           # mode-specific group ID
                                        ])

                                elif args.custom_paths in ('PGC', 'BW'):
                                    # /mnt/pgc/data/elev/dem/setsm/ArcticDEM/region/arcticdem_01_iceland/scenes/
                                    # 2m/WV01_20200630_10200100991E2C00_102001009A862700_2m_v040204/
                                    # WV01_20200630_10200100991E2C00_102001009A862700_
                                    # 504471479080_01_P001_504471481090_01_P001_2_meta.txt

                                    if not region:
                                        logger.error("Pairname not found in region lookup {}, cannot build custom path".format(
                                            record.pairname))
                                        valid_record = False

                                    else:
                                        pretty_project = utils.PROJECTS[region.split('_')[0]]

                                        custom_path = '/'.join([
                                            path_prefix,
                                            pretty_project,         # project (e.g. ArcticDEM)
                                            'region',
                                            region,                 # region
                                            'scenes',
                                            res_dir,                # e.g. 2m, 50cm, 2m_dsp
                                            groupid,                # strip ID
                                            record.srcfn            # file name (meta.txt)
                                        ])

                                elif args.custom_paths == 'CSS':
                                    # /css/nga-dems/setsm/scene/2m/2021/04/21/
                                    # W2W2_20161025_103001005E00BD00_103001005E89F900_2m_v040306
                                    custom_path = '/'.join([
                                        path_prefix,
                                        args.mode,  # mode (scene, strip, tile)
                                        res_dir,  # e.g. 2m, 50cm, 2m_dsp
                                        record.pairname[:4],  # sensor
                                        record.pairname[5:9],  # year
                                        record.pairname[9:11],  # month
                                        groupid,  # mode-specific group ID
                                        record.srcfn  # file name (meta.txt)
                                    ])

                                else:
                                    logger.error("Mode {} does not support the specified custom path option,\
                                     skipping record".format(args.mode))
                                    valid_record = False

                        ## Fields for strip DEM
                        if args.mode == 'strip':
                            logger.debug(f"Processing strip: {record.stripid}")
                            attrib_map = {
                                'DEM_ID': record.stripid,
                                'STRIPDEMID': record.stripdemid,
                                'PAIRNAME': record.pairname,
                                'SENSOR1': record.sensor1,
                                'SENSOR2': record.sensor2,
                                'ACQDATE1': record.acqdate1.strftime('%Y-%m-%d'),
                                'ACQDATE2': record.acqdate2.strftime('%Y-%m-%d'),
                                'AVGACQTM1': record.avg_acqtime1.strftime("%Y-%m-%d %H:%M:%S") if record.avg_acqtime1 is not None else None,
                                'AVGACQTM2': record.avg_acqtime2.strftime("%Y-%m-%d %H:%M:%S") if record.avg_acqtime2 is not None else None,
                                'CATALOGID1': record.catid1,
                                'CATALOGID2': record.catid2,
                                'IS_LSF': record.is_lsf,
                                'IS_XTRACK': record.is_xtrack,
                                'EDGEMASK': record.mask_tuple[0],
                                'WATERMASK': record.mask_tuple[1],
                                'CLOUDMASK': record.mask_tuple[2],
                                'ALGM_VER': record.algm_version,
                                'S2S_VER': record.s2s_version,
                                'RMSE': record.rmse,
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
                                pair_items = pairs[record.pairname]
                            except KeyError as e:
                                region = None
                                bp_region = None
                            else:
                                if isinstance(pair_items, str):
                                    region = pair_items
                                elif isinstance(pair_items, tuple):
                                    region, bp_region = pair_items
                                else:
                                    logger.error("Pairname-region lookup value cannot be parsed for pairname {}: {}".format(
                                        record.pairname, pair_items))
                                attrib_map['REGION'] = region

                            if record.release_version and 'REL_VER' in fld_list:
                                attrib_map['REL_VER'] = record.release_version

                            for f, a in utils.field_attrib_map.items():
                                val = getattr(record, a)
                                attrib_map[f] = round(val, 6) if val is not None else -9999

                            ## If registration info exists - DEPRECATED
                            # if args.include_registration:
                            #     if len(record.reginfo_list) > 0:
                            #         for reginfo in record.reginfo_list:
                            #             if reginfo.name == 'ICESat':
                            #                 attrib_map["DX"] = reginfo.dx
                            #                 attrib_map["DY"] = reginfo.dy
                            #                 attrib_map["DZ"] = reginfo.dz
                            #                 attrib_map["REG_SRC"] = 'ICESat'
                            #                 attrib_map["NUM_GCPS"] = reginfo.num_gcps
                            #                 attrib_map["MEANRESZ"] = reginfo.mean_resid_z

                            ## Set path folders for use if path_prefix specified
                            if path_prefix:
                                res_dir = record.res_str + '_dsp' if record.is_dsp else record.res_str

                                if args.custom_paths == 'BP':
                                    # https://blackpearl-data2.pgc.umn.edu/dem-strips-arc/2m/WV02/2015/05/
                                    # WV02_20150506_1030010041510B00_1030010043050B00_50cm_v040002_s2s042.tar
                                    if not region:
                                        logger.error("Pairname not found in region lookup {}, cannot build custom path".format(
                                                record.pairname))
                                        valid_record = False

                                    else:
                                        # bp_region is like anteas-05, arcgeu-08, nplnam-02, ...
                                        # bucket names are dem-strips-{ant,arc,npl}
                                        # (scenes use bp_region.split('-')[0] ... like anteas)
                                        bucket = f"dem-strips-{bp_region[0:3]}"
                                        custom_path = '/'.join([
                                            path_prefix,
                                            bucket,
                                            res_dir,  # e.g. 2m, 50cm, 2m_dsp
                                            record.pairname[:4],  # sensor
                                            record.pairname[5:9],  # year
                                            record.pairname[9:11],  # month
                                            groupid + '.tar'  # mode-specific group ID
                                        ])

                                elif args.custom_paths in ('PGC', 'BW'):
                                    # /mnt/pgc/data/elev/dem/setsm/ArcticDEM/region/arcticdem_01_iceland/s2s042/
                                    # 2m/WV01_20200630_10200100991E2C00_102001009A862700_2m_v040204_s2s042/
                                    # WV01_20200630_10200100991E2C00_102001009A862700_seg1_etc

                                    if not region:
                                        logger.error("Pairname not found in region lookup {}, cannot build custom path".format(
                                            record.pairname))
                                        valid_record = False

                                    else:
                                        pretty_project = utils.PROJECTS[region.split('_')[0]]

                                        custom_path = '/'.join([
                                            path_prefix,
                                            pretty_project,         # project (e.g. ArcticDEM)
                                            'region',
                                            region,                 # region
                                            record.release_version, # e.g. s2s042, previously 'strips_v{}'.format(record.s2s_version)
                                            res_dir,                # e.g. 2m, 50cm, 2m_dsp
                                            groupid,                # strip ID
                                            record.srcfn            # file name (meta.txt)
                                        ])

                                elif args.custom_paths == 'CSS':
                                    # /css/nga-dems/setsm/strip/strips_v3/2m/2021/04/21/
                                    # W2W2_20161025_103001005E00BD00_103001005E89F900_2m_v040306
                                    custom_path = '/'.join([
                                        path_prefix,
                                        args.mode,  # mode (scene, strip, tile)
                                        'strips_v{}'.format(record.s2s_version),
                                        res_dir,  # e.g. 2m, 50cm, 2m_dsp
                                        record.pairname[:4],  # sensor
                                        record.pairname[5:9],  # year
                                        record.pairname[9:11],  # month
                                        groupid,  # mode-specific group ID
                                        record.srcfn  # file name (meta.txt)
                                    ])

                                else:
                                    logger.error("Mode {} does not support the specified custom path option,\
                                     skipping record".format(args.mode))
                                    valid_record = False

                        ## Fields for tile DEM
                        if args.mode == 'tile':
                            logger.debug(f"Processing tile: {record.tileid}")
                            attrib_map = {
                                'DEM_ID': record.tileid,
                                'TILE': record.tile_id_no_res,
                                'SUPERTILE': record.supertile_id_no_res,
                                'NUM_COMP': record.num_components,
                                'FILESZ_DEM': record.filesz_dem,
                                'EPSG': record.epsg,
                            }

                            ## Optional attributes
                            if record.release_version and ('REL_VER' in fld_list or 'RELEASEVER' in fld_list):
                                attrib_map['REL_VER'] = record.release_version
                                version = record.release_version
                            else:
                                version = 'novers'

                            attrib_map['DENSITY'] = record.density if record.density is not None else -9999

                            # if args.include_registration: --DEPRECATED
                            #     if record.reg_src:
                            #         attrib_map["REG_SRC"] = record.reg_src
                            #         attrib_map["NUM_GCPS"] = record.num_gcps
                            #     if record.mean_resid_z:
                            #         attrib_map["MEANRESZ"] = record.mean_resid_z

                            ## Set path folders for use if db_path_prefix specified
                            if path_prefix:
                                if args.custom_paths == 'BP':
                                    custom_path = '.'.join([
                                        path_prefix,
                                        record.mode,               # mode (scene, strip, tile)
                                        args.project.lower(),    # project
                                        record.res,              # resolution
                                        version,                 # version
                                        groupid+'.tar'                  # mode-specific group ID
                                    ])
                                else:
                                    logger.error("Mode {} does not support the specified custom path option,\
                                     skipping record".format(args.mode))
                                    valid_record = False

                        ## Common fields
                        if valid_record:
                            ## Common Attributes across all modes
                            attrib_map['INDEX_DATE'] = datetime.datetime.today().strftime('%Y-%m-%d')
                            attrib_map['CR_DATE'] = record.creation_date.strftime('%Y-%m-%d')
                            attrib_map['ND_VALUE'] = record.ndv
                            if dsp_mode == 'orig':
                                res = record.dsp_dem_res
                            else:
                                res = (record.xres + record.yres) / 2.0
                            attrib_map['DEM_RES'] = res

                            ## Set location
                            if path_prefix:
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
                                            split_geom = utils.getWrappedGeometry(temp_geom)
                                            feat_geom = split_geom
                                        else:
                                            mp_geom = ogr.ForceToMultiPolygon(temp_geom)
                                            feat_geom = mp_geom

                                    else:
                                        mp_geom = ogr.ForceToMultiPolygon(temp_geom)
                                        feat_geom = mp_geom

                            del src_srs  # clean up memory

                            ## Convert fields for tile and strip DEM to release format
                            if args.use_release_fields:
                                tile_to_general_attrib_name = {
                                    'GSD': 'DEM_RES',
                                    'RELEASEVER': 'REL_VER',
                                    'DATA_PERC': 'DENSITY',
                                    'ACQDATE1': 'AVGACQTM1',
                                    'ACQDATE2': 'AVGACQTM2',
                                    'SETSM_VER': 'ALGM_VER',
                                }

                                if args.mode == 'tile':
                                    fdefs1 = utils.TILE_DEM_ATTRIBUTE_DEFINITIONS
                                    fdefs2 = utils.TILE_DEM_ATTRIBUTE_DEFINITIONS_RELEASE
                                    version = f'v{record.release_version}'
                                    group = record.supertile_id_no_res
                                elif args.mode == 'strip':
                                    fdefs1 = utils.DEM_ATTRIBUTE_DEFINITIONS
                                    fdefs2 = utils.DEM_ATTRIBUTE_DEFINITIONS_RELEASE
                                    version = record.release_version
                                    group = record.geocell
                                else:
                                    msg = '--use-release-fields used with an incompatible mode'
                                    raise RuntimeError(msg)

                                remove_attrib_names = sorted(list(set.difference(
                                    set([attr.fname for attr in fdefs1]),
                                    set([attr.fname for attr in fdefs2]),
                                )))

                                for tname, gname in tile_to_general_attrib_name.items():
                                    if gname in attrib_map:
                                        attrib_map[tname] = attrib_map[gname]
                                        del attrib_map[gname]
                                for fname in remove_attrib_names:
                                    if fname in attrib_map:
                                        del attrib_map[fname]

                                if args.release_fileurl:
                                    filurl = args.release_fileurl
                                    pretty_project = utils.PROJECTS[args.project]
                                    filurl = filurl.replace('<project>', pretty_project)
                                    filurl = filurl.replace('<type>', dem_type_folder_lookup[args.mode])
                                    filurl = filurl.replace('<version>', version)
                                    filurl = filurl.replace('<resolution>', record.res_str)
                                    filurl = filurl.replace('<group>', group)
                                    filurl = filurl.replace('<dem_id>', record.id)
                                    attrib_map['FILEURL'] = filurl

                                if args.release_s3url:
                                    s3url = args.release_s3url
                                    s3url = s3url.replace('<project>', args.project)
                                    s3url = s3url.replace('<type>', dem_type_folder_lookup[args.mode])
                                    s3url = s3url.replace('<version>', version)
                                    s3url = s3url.replace('<resolution>', record.res_str)
                                    s3url = s3url.replace('<group>', group)
                                    s3url = s3url.replace('<dem_id>', record.id)
                                    attrib_map['S3URL'] = s3url

                        ## Write feature
                        if valid_record:
                            for fld, val in attrib_map.items():
                                fld_schema = fld_def_short_to_long_dict[fld] if args.long_fieldnames else fld
                                if fld_schema.upper() in fnameupper_fnamelayer_dict:
                                    fld = fnameupper_fnamelayer_dict[fld_schema.upper()]
                                    fwidth, ftype = fwidths[fld]
                                    # Check if attribute length is too long for the field width. Note that the varchar
                                    # type in postgres returns a width of 0 if no max width is specified in the table
                                    # creation
                                    if isinstance(val, str) and ftype == ogr.OFTString and 0 < fwidth < len(val):
                                        logger.error("Attribute value {} is too long for field {} (width={}). "
                                                     "Feature skipped".format(val, fld, fwidth))
                                        valid_record = False
                                        if fld.upper() == 'LOCATION' and ogr_driver_str == 'ESRI Shapefile':
                                            if fld_def_location_fwidth_gdb is not None \
                                                    and fld_def_location_fwidth_gdb > fwidth:
                                                logger.warning("Tip: LOCATION field values can be longer (width={}) \
                                                    if you write to a non-Shapefile index such as FileGDB or PostgreSQL table".format(
                                                    fld_def_location_fwidth_gdb
                                                ))
                                else:
                                    logger.error("Field {} is not in target table. Feature skipped".format(fld_schema))
                                    valid_record = False

                                if sys.version_info[0] < 3:  # force unicode to str for a bug in Python2 GDAL's SetField.
                                    fld = fld.encode('utf-8')
                                    val = val if not isinstance(val, unicode) else val.encode('utf-8')

                                if valid_record:
                                    feat.SetField(fld, val)
                                else:
                                    break
                            if valid_record:
                                feat.SetGeometry(feat_geom)

                            ## Add new feature to layer
                            if not valid_record:
                                invalid_record_cnt += 1
                            else:
                                if not args.dryrun:
                                    # Store record identifiers for later checking
                                    recordid_mode = args.mode + '_release' if args.use_release_fields else args.mode
                                    recordids.append(recordid_map[recordid_mode].format(**attrib_map))

                                    # Append record
                                    if ogr_driver_str in ('PostgreSQL'):
                                        layer.StartTransaction()
                                        utils.GDAL_ERROR_HANDLER.reset_error_state()
                                        try:
                                            layer.CreateFeature(feat)
                                        except Exception as e:
                                            if utils.GDAL_ERROR_HANDLER.errored:
                                                gdal_errmsg = utils.GDAL_ERROR_HANDLER.err_msg
                                                if "duplicate key value violates unique constraint" in gdal_errmsg:
                                                    duplicate_record_cnt += 1
                                                    log_errmsg = "Skipping duplicate record error in OGR CreateFeature call:\n{}".format(gdal_errmsg)
                                                    if duplicate_record_cnt <= 30:
                                                        logger.error(log_errmsg)
                                                        if duplicate_record_cnt == 30:
                                                            logger.warning("Maximum 'duplicate record' error messages printed to terminal,"
                                                                           " further messages will be printed to debug")
                                                    else:
                                                        logger.debug(log_errmsg)
                                                else:
                                                    raise
                                            else:
                                                raise
                                        layer.CommitTransaction()

                                    else:
                                        layer.CreateFeature(feat)
            if not args.np:
                print('')

            if invalid_record_cnt > 0:
                logger.error("{} invalid records skipped".format(invalid_record_cnt))

            if duplicate_record_cnt > 0:
                logger.warning("{} duplicate records skipped".format(duplicate_record_cnt))

            if len(recordids) == 0 and not args.dryrun:
                logger.error("No valid records found")
                rc = -1

            # Check contents of layer for all records
            if args.check and not args.dryrun:
                logger.info("Checking for new records in target table")
                layer.ResetReading()
                attrib_maps = [{id_fld: convert_value(id_fld, feat.GetField(id_fld)) for id_fld in id_flds if id_fld in fld_list} for feat in layer]
                layer_recordids = [recordid_map[args.mode].format(**attrib_map) for attrib_map in attrib_maps]
                layer_recordids = set(layer_recordids)

                err_cnt = 0
                for recordid in recordids:
                    if recordid not in layer_recordids:
                        err_cnt += 1
                        if err_cnt == 1 and layer_recordids:
                            logger.error("Example record already existing in target layer: {}".format(next(iter(layer_recordids))))
                        logger.error("New record not found in target layer: {}".format(recordid))
                if err_cnt > 1:
                    logger.error("Example record already existing in target layer: {}".format(next(iter(layer_recordids))))

                if err_cnt > 0:
                    rc = -1

        else:
            logger.error('Cannot open layer: {}'.format(dst_lyr))
            rc = -1

        ds = None
        del tgt_srs  # clean up memory

    if args.dryrun:
        logger.info("Done (dryrun)")
    else:
        logger.info("Done")

    return rc


def convert_value(fld, val):
    # Convert target layer field value that was read with GDAL to expected value.

    # Convert date to expected string.
    # Reading a datetime field can return a string in several possible timestamp formats,
    # depending on the table format (Shapefile vs FileGDB vs Postgres, etc).
    if fld == 'INDEX_DATE' and type(val) is str:
        dt = None
        for case in range(2):
            try:
                if case == 0:
                    dt = datetime.datetime.strptime(val[:10], "%Y/%m/%d")
                # elif case == 1:
                #     dt = datetime.datetime.strptime(val[:10], "%Y-%m-%d")
            except ValueError:
                pass
            else:
                if dt is not None:
                    break
        if dt is not None:
            return dt.strftime("%Y-%m-%d")

    # Convert integer to expected boolean
    elif fld == 'IS_DSP':
        if type(val) is int and val in (0, 1):
            return bool(val)
        elif type(val) is str and val in ('0', '1'):
            return bool(int(val))

    return val


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
            # TODO: this seems like a hack
            # remove _s2s042 if at end of groupid to match stripdirname as returned from setsm.
            # This is what batch_check_setsm.py -vqc and shelve_setsm_dems.py expect.
            # - batch_check_setsm.py expects the json to be named based on the folder as produced
            #   by setsm-prod-v1.
            # - shelve_setsm_dems.py expects the json to be named based on part of the filenames
            # both of those happen to be the same and don't have the _s2s042 part.
            json_fn = re.sub(r"_s2s\d{3}$","", groupid) # remove _s2s042 if at end of groupid
            json_fp = "{}.json".format(os.path.join(json_fd,json_fn))

        if os.path.isfile(json_fp) and args.overwrite:
            os.remove(json_fp)

        if not os.path.isfile(json_fp):

            if not args.dryrun:
                # open json
                json_fh = open(json_fp,'w')

            for item in items:
                i+=1
                if not args.np:
                    utils.progress(i,total,"records written")

                # organize scene obj into dict and write to json
                md[item.id] = item.__dict__

            json_txt = json.dumps(md, default=encode_json)
            #print json_txt

            if not args.dryrun:
                json_fh.write(json_txt)
                json_fh.close()

        else:
            logger.info("Json file already exists {}. Use --overwrite to overwrite".format(json_fp))
    if not args.np:
        print('')


def get_pair_region_dict(conn_str):
    """Fetches a pairname-region lookup dictionary from Danco's footprint DB
    pairnames_with_earthdem_region table
    """

    pairs = {}
    stereo_ds = ogr.Open(conn_str)

    if stereo_ds is None:
        logger.warning("Could not connect to footprint db")
    else:
        stereo_lyr = stereo_ds.GetLayer("public.pairname_with_earthdem_region")
        if stereo_lyr is None:
            logger.warning("Could not obtain public.pairname_with_earthdem_region layer")
            stereo_ds = None
        else:
            pairs = {f["pairname"]:(f["region_id"],f["bp_region"]) for f in stereo_lyr}

    return pairs


def encode_json(o):
    if isinstance(o, datetime.datetime):
        return {
            '__datetime__': True,
            'value': o.__repr__(),
        }
    if isinstance(o, ogr.Geometry):
        return {
            '__geometry__': True,
            'value': o.__str__(),
        }
    if isinstance(o, osr.SpatialReference):
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
