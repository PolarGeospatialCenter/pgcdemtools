import argparse
import json
import logging
import pathlib
from dataclasses import dataclass
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection
from psycopg2 import sql

from lib import stac, utils

# Create a module logger into scope so that functions can use it
# Configure in main()
logger = logging.getLogger(__name__)


@dataclass
class ScriptArgs:
    dsn: str
    stac_base_dir: pathlib.Path
    collection: str
    item_id: str
    text_file: pathlib.Path
    all_items: bool
    ndjson: bool
    overwrite: bool
    verbose: bool
    dryrun: bool

    @classmethod
    def parse(cls):
        parser = argparse.ArgumentParser(
            description="Write STAC Item(s) to the file system as individual files \
            (.json) into a directory structure mirroring the AWS pgc-opendata-dems \
            bucket, or as a single newline-delimited file (.ndjson)"
        )

        #### Always required flags
        parser.add_argument(
            "--dsn",
            required=True,
            help="Postgres DSN"
        )
        parser.add_argument(
            '--stac-base-dir',
            required=True,
            type=pathlib.Path,
            help="Base directory to write STAC JSON files"
        )
        parser.add_argument(
            "--collection",
            help="STAC collection id",
            required=True,
        )

        #### Mutually exclusive flags for controlling which items to export
        parser.add_argument(
            "--item-id",
            help="Extract a single STAC Item. Mutually exclusive with --text-file and \
            --all-items",
            default=None,
        )
        parser.add_argument(
            "--text-file",
            help="Extract STAC Item for all items IDs in a newline-delimited text file.\
             Mutually exclusive with --item-id and --all-items",
            type=pathlib.Path,
            default=None,
        )
        parser.add_argument(
            "--all-items",
            help="Extract all STAC Items in a collection. Mutually exclusive with \
            --item-id and --text-file",
            action="store_true",
            default=False,
        )

        #### Optional output mode
        parser.add_argument(
            "--ndjson",
            help="Write all extracted items a newline delimited json (.ndjson) file \
            rather than one file per item.",
            action="store_true",
            default=False,
        )

        #### Extra CLI stuff
        parser.add_argument(
            "--overwrite",
            help="Replace existing files when encountered",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            help="Verbose output",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--dryrun",
            action="store_true",
            default=False,
            help="Preform export process without making any filesystem changes",
        )

        args = parser.parse_args()
        return cls(**vars(args))

    def validate(self):
        if self.collection not in stac.COLLECTIONS:
            raise ValueError(
                f"Invalid collection: {self.collection}. Must be one of: {stac.COLLECTIONS}"
            )

        input_count = 0
        input_count += 1 if self.item_id else 0
        input_count += 1 if self.text_file else 0
        input_count += 1 if self.all_items else 0
        if input_count != 1:
            raise ValueError(
                "Select one and only one of --item-id, --text-file, or --all-items"
            )

        if not self.stac_base_dir.exists():
            raise FileNotFoundError(
                f"The path provided for --stac-base-dir does not exist: {self.stac_base_dir}"
            )
        if not self.stac_base_dir.is_dir():
            raise NotADirectoryError(
                f"The path provided for --stac-base-dir must be a directory: {self.stac_base_dir}"
            )
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
    except (FileNotFoundError, NotADirectoryError, ValueError) as e:
        logger.error(e)
        exit(1)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug(f"Parsed arguments: {args}")

    extract_stac_items_from_sandwich(args)


def extract_stac_items_from_sandwich(args: ScriptArgs):
    conn = None
    try:
        logger.info("Connecting to database")
        conn = psycopg2.connect(args.dsn)

        logger.info("Fetching STAC Items")
        if args.item_id:
            total = 1
            items = [fetchone_item(conn, args.collection, args.item_id)]
        elif args.text_file:
            with open(args.text_file, "r") as f:
                item_ids = [line.strip() for line in f.readlines()]
            total = len(item_ids)
            items = fetchmany_items(conn, args.collection, item_ids)
        elif args.all_items:
            total = fetchall_count(conn, args.collection)
            items = fetchall_items(conn, args.collection)
        else:
            logger.error("None of --item-id, --text-file, or --all-items set.")
            return

        logger.info(f"Count of STAC Items selected: {total}")

        if args.ndjson:
            file = args.stac_base_dir / f"{args.collection}.ndjson"

            if not args.overwrite and file.exists():
                logger.error(
                    f"Output file already exists and --overwrite not provided. {file}"
                )
                return

            logger.info(f"Writing STAC Items as newline-delimited JSON to: {file}")
            with open(file, "w") as f:
                f.writelines((f"{json.dumps(item, indent=None)}\n" for item in items))

        else:
            logger.info("Writing STAC Items as mirror of AWS bucket")
            for idx, item in enumerate(items):
                utils.progress(idx, total, "STAC Items processed")
                file = get_mirror_path(args.stac_base_dir, item)

                if not args.overwrite and file.exists():
                    logger.warning(
                        f"File already exists and --overwrite not provided. Skipping {file}"
                    )

                if not args.dryrun:
                    file.parent.mkdir(exist_ok=True, parents=True)
                    with open(file, "w") as f:
                        f.write(json.dumps(item, indent=2))

    finally:
        conn.close() if conn else None


def get_mirror_path(stac_base_dir: pathlib.Path, item: dict) -> pathlib.Path:
    self_link = [link for link in item["links"] if link["rel"] == "self"][0]
    self_href = self_link["href"]
    s3_key = self_href.replace("https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/",
                               "")
    return stac_base_dir / s3_key


def fetchone_item(db_connection: connection, collection: str, item_id: str) -> dict:
    query = sql.SQL("""
    SELECT content
    FROM dem.stac_static_item
    WHERE collection = %s AND item_id = %s
    """)

    with db_connection.cursor() as cur:
        cur.execute(query, (collection, item_id))
        row = cur.fetchone()
        return row[0]


def fetchmany_items(
        db_connection: connection, collection: str, item_ids: list[str]
) -> Iterator[dict]:
    create_temp_table = sql.SQL("""
    CREATE TEMP TABLE temp_item_ids (collection VARCHAR, item_id VARCHAR)
    """)

    insert_values = sql.SQL("INSERT INTO temp_item_ids VALUES (%s, %s)")
    values = [(collection, item_id) for item_id in item_ids]

    query = sql.SQL("""
    SELECT 
        stac.content
    FROM temp_item_ids
    LEFT JOIN dem.stac_static_item AS stac
        USING (collection, item_id)
    """)

    with db_connection.cursor() as cur:
        cur.execute(create_temp_table)
        cur.executemany(insert_values, values)
        cur.execute(query)

        for row in cur:
            yield row[0]


def fetchall_items(db_connection: connection, collection: str) -> Iterator[dict]:
    query = sql.SQL("""
    SELECT content
    FROM dem.stac_static_item
    WHERE collection = %s
    """)

    with db_connection.cursor() as cur:
        cur.execute(query, (collection,))

        for row in cur:
            yield row[0]


def fetchall_count(db_connection: connection, collection: str) -> int:
    query = sql.SQL("""
    SELECT count(*) 
    FROM dem.stac_static_item
    WHERE collection = %s
    """)

    with db_connection.cursor() as cur:
        cur.execute(query, (collection,))
        row = cur.fetchone()
        return row[0]


if __name__ == "__main__":
    main()
