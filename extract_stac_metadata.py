import argparse
import json
import logging
import pathlib
from dataclasses import dataclass
from typing import NamedTuple

import psycopg2

from lib import dem, stac

# Create a module logger into scope so that functions can use it
# Configure in main()
logger = logging.getLogger(__name__)


@dataclass
class ScriptArgs:
    src: pathlib.Path
    domain: str
    dsn: str
    upsert: bool
    batch_size: int
    verbose: bool
    dryrun: bool

    @classmethod
    def parse(cls):
        #### Set Up Arguments
        parser = argparse.ArgumentParser(
            description="Populate sandwich tables with additional data from source assets used to construct STAC items."
        )

        #### Positional Arguments
        parser.add_argument(
            'src',
            type=pathlib.Path,
            help="Source directory, text file of file paths, or dem",
        )

        #### Always required flags
        domain_choices = ("arcticdem", "earthdem", "rema")
        parser.add_argument(
            "--domain",
            help="PGC domain [required]",
            required=True,
            choices=domain_choices,
        )
        parser.add_argument("--dsn", required=True, help="Postgres DSN [required]")

        #### Options Arguments
        parser.add_argument(
            "--upsert",
            help="On primary key conflict, update row (DO UPDATE SET ...). Otherwise, insert will be ignored (DO NOTHING).",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--batch-size",
            default=100,
            type=int,
            help="Number of DB records to generate before performing insert. (default=%(default)s)"
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            default=False,
            help="Verbose output"
        )
        parser.add_argument(
            "--dryrun",
            action="store_true",
            default=False,
            help="Preform record generation without executing insert"
        )

        args = parser.parse_args()
        return cls(**vars(args))

    def validate(self):
        if not self.src.exists():
            raise FileNotFoundError(f"src does not exist: {self.src}")

        if self.src.is_file() and self.src.suffix not in (".tif", ".txt"):
            raise ValueError("src must be a directory, text file, or tif")
        return self


def main() -> None:
    # Configure default script logging
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(levelname)s- %(message)s",
                          datefmt="%m-%d-%Y %H:%M:%S")
    )
    logger.addHandler(handler)

    # Parse and verify arguments
    try:
        args = ScriptArgs.parse().validate()
    except (FileNotFoundError, ValueError) as e:
        logger.error(e)
        exit(1)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug(f"Parsed arguments: {args}")

    extract_stac_metadata(args=args)


def extract_stac_metadata(args: ScriptArgs) -> None:
    # Gather input dems
    src = args.src
    if src.is_dir():
        logger.info(f"{src} is a directory")
        logger.info("Recursively searching for files ending with '_dem.tif'")
        files = list(src.rglob("*_dem.tif"))
    elif src.suffix == ".tif":
        logger.info("{src} is a tif")
        files = [args.src]
    else:
        logger.info("{src} is a text file")
        logger.info("Reading paths from text file")
        with open(src, "r") as f:
            files = [pathlib.Path(line.strip()) for line in f.readlines()]

    logger.info(f"Found {len(files)} DEM(s)")

    # Generate the database records from the setsm objects in a loop
    # When the batch_size is reached, insert the records and clear the record lists
    # Continue until all inputs are processed
    asset_info_records = []
    mosaic_pairname_ids_records = []

    logger.info("Generating DB records from DEM(s)")
    for file in files:
        logger.info(f"Working on {file}")
        if file.name.startswith("SETSM_"):
            setsm = dem.SetsmDem(f"{file}")
        else:
            setsm = dem.SetsmTile(f"{file}")
        setsm.get_dem_info()

        if isinstance(setsm, dem.SetsmDem):
            collection = f'{args.domain}-strips-{setsm.release_version}-{setsm.res_str}'
        else:
            collection = f'{args.domain}-mosaics-v{setsm.release_version}-{setsm.res}'

        if collection not in stac.COLLECTIONS:
            logger.error(f"Invalid collection: {collection} from {setsm.srcfp}")
            logger.error("Skipping file")
            continue

        # Generate stac_raster_asset_info records
        records = get_stac_raster_asset_info_records(collection, setsm)
        logger.debug(f"Generated record(s): {records}")
        asset_info_records.extend(records)

        if "mosaics" in collection:
            # Generate stac_moscaic_pairname_ids record
            record = StacMosaicInfoRecord.from_tile(collection, setsm)
            logger.debug(f"Generated record: {record}")
            mosaic_pairname_ids_records.append(record)

        # When the batch_size is reached, insert the records and clear the record list
        record_count = len(asset_info_records) + len(mosaic_pairname_ids_records)
        if record_count >= args.batch_size:
            logger.info("Record batch size reached.")
            logger.info(
                f"Inserting {len(asset_info_records)} records into {StacRasterAssetInfoRecord.table_identifier()}"
            )
            if not args.dryrun:
                insert_records(dsn=args.dsn, records=asset_info_records, upsert=args.upsert)
            asset_info_records.clear()

            logger.info(
                f"Inserting {len(mosaic_pairname_ids_records)} records into {StacMosaicInfoRecord.table_identifier()}"
            )
            if not args.dryrun:
                insert_records(dsn=args.dsn, records=mosaic_pairname_ids_records, upsert=args.upsert)
            mosaic_pairname_ids_records.clear()

    # Insert any remaining records
    logger.info(
        f"Inserting {len(asset_info_records)} records into {StacRasterAssetInfoRecord.table_identifier()}"
    )
    if not args.dryrun:
        insert_records(dsn=args.dsn, records=asset_info_records, upsert=args.upsert)
    asset_info_records.clear()

    logger.info(
        f"Inserting {len(mosaic_pairname_ids_records)} records into {StacMosaicInfoRecord.table_identifier()}"
    )
    if not args.dryrun:
        insert_records(dsn=args.dsn, records=mosaic_pairname_ids_records, upsert=args.upsert)
    mosaic_pairname_ids_records.clear()


def _generate_insert_statement(
        record_class: NamedTuple,
        table_identifier: str,
        conflict_fields: list[str],
        upsert: bool,
) -> str:
    """
    Generate an SQL INSERT IGNORE statement (INSERT ... ON CONFLICT ... DO NOTHING)
    based on the field order in the NamedTuple. If upsert is True, generate an UPSERT
    statement (INSERT ... ON CONFLICT ... DO UPDATE).

    Args:
        record_class: Class representing a record as a NamedTuple
        table_identifier: Name of the database table, optionally including schema
        conflict_fields: List of fields forming the primary key or unique constraint

    Returns:
        SQL statement with placeholders
    """
    field_names = record_class._fields

    columns = ", ".join(field_names)
    placeholders = ", ".join(["%s"] * len(field_names))
    conflict_target = ", ".join(conflict_fields)
    update_fields = [f for f in field_names if f not in conflict_fields]
    update_clause = ", ".join(
        [f"{field} = EXCLUDED.{field}" for field in update_fields])

    do_clause = "DO NOTHING" if not upsert else f"DO UPDATE SET {update_clause}"

    sql = f"""
    INSERT INTO {table_identifier} ({columns}) 
    VALUES ({placeholders})
    ON CONFLICT ({conflict_target})
    {do_clause}"""

    return sql


JsonStr = str
Iso8601Str = str


class StacRasterAssetInfoRecord(NamedTuple):
    collection: str
    item_id: str
    asset_key: str
    gsd: float
    proj_code: str
    proj_shape: JsonStr
    proj_transform: JsonStr
    proj_bbox: JsonStr
    proj_geometry: JsonStr
    proj_centroid: JsonStr

    @classmethod
    def table_identifier(cls) -> str:
        return "dem.stac_raster_asset_info"

    @classmethod
    def get_insert_statement(cls, upsert: bool) -> str:
        primary_key = ["collection", "item_id", "asset_key"]
        return _generate_insert_statement(
            record_class=cls,
            table_identifier=cls.table_identifier(),
            conflict_fields=primary_key,
            upsert=upsert,
        )

    @classmethod
    def from_raster(cls, collection: str, item_id: str, asset_key: str, filepath: str):
        info = stac.RasterAssetInfo.from_raster(filepath)
        return cls(
            collection=collection,
            item_id=item_id,
            asset_key=asset_key,
            gsd=info.gsd,
            proj_code=info.proj_code,
            proj_shape=json.dumps(info.proj_shape),
            proj_transform=json.dumps(info.proj_transform),
            proj_bbox=json.dumps(info.proj_bbox),
            proj_geometry=json.dumps(info.proj_geojson),
            proj_centroid=json.dumps(info.proj_centroid),
        )


def get_stac_raster_asset_info_records(
        collection: str,
        strip_or_tile: dem.SetsmDem | dem.SetsmTile
) -> list[StacRasterAssetInfoRecord]:
    item_id = strip_or_tile.stripid if "strips" in collection else strip_or_tile.tileid

    records = [
        StacRasterAssetInfoRecord.from_raster(
            collection=collection,
            item_id=item_id,
            asset_key="dem",
            filepath=strip_or_tile.srcfp,
        )
    ]

    collections_with_downsampled_assets = {
        "arcticdem-mosaics-v4.1-2m",
        "arcticdem-strips-s2s041-2m",
        "earthdem-strips-s2s041-2m",
        "rema-mosaics-v2.0-2m",
        "rema-strips-s2s041-2m",
    }

    if collection in collections_with_downsampled_assets:
        records.append(
            StacRasterAssetInfoRecord.from_raster(
                collection=collection,
                item_id=item_id,
                asset_key="browse" if "v3.0" in collection else "hillshade",
                filepath=strip_or_tile.browse,
            )
        )

    return records


class StacMosaicInfoRecord(NamedTuple):
    collection: str
    item_id: str
    pairname_ids: JsonStr
    start_datetime: Iso8601Str
    end_datetime: Iso8601Str

    @classmethod
    def table_identifier(cls) -> str:
        return "dem.stac_mosaic_info"

    @classmethod
    def get_insert_statement(cls, upsert: bool) -> str:
        primary_key = ["collection", "item_id"]
        return _generate_insert_statement(
            record_class=cls,
            table_identifier=cls.table_identifier(),
            conflict_fields=primary_key,
            upsert=upsert,
        )

    @classmethod
    def from_tile(cls, collection: str, tile: dem.SetsmTile):
        return cls(
            collection=collection,
            item_id=tile.tileid,
            pairname_ids=json.dumps(tile.pairname_ids),
            start_datetime=tile.acqdate_min.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_datetime=tile.acqdate_max.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )


def insert_records(
        dsn: str,
        records: list[StacRasterAssetInfoRecord] | list[StacMosaicInfoRecord],
        upsert: bool,
) -> None:
    if not records:
        return

    query = records[0].get_insert_statement(upsert)

    conn = psycopg2.connect(dsn)
    try:
        curs = conn.cursor()
        curs.executemany(query, records)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
