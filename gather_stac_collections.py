import argparse
import json
import logging
import pathlib
from dataclasses import dataclass

from lib import stac

# Create a module logger into scope so that functions can use it
# Configure in main()
logger = logging.getLogger(__name__)


@dataclass
class ScriptArgs:
    aws_mirror_dir: pathlib.Path
    output: pathlib.Path
    overwrite: bool
    verbose: bool
    dryrun: bool

    @classmethod
    def parse(cls):
        parser = argparse.ArgumentParser(
            description="Gather STAC Collections from the AWS STAC tree mirror and \
            write an pgstac-ingest-ready NDJSON file."
        )

        #### Always required flags
        parser.add_argument(
            '--aws-mirror-dir',
            required=True,
            type=pathlib.Path,
            help="Directory containing the root of the aws mirror of the STAC tree"
        )
        parser.add_argument(
            '--output',
            required=True,
            type=pathlib.Path,
            help="Path to write NDJSON result"
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
        if not self.aws_mirror_dir.exists():
            raise FileNotFoundError(
                f"The path provided for --aws-mirror-dir does not exist: {self.aws_mirror_dir}"
            )
        if not self.aws_mirror_dir.is_dir():
            raise NotADirectoryError(
                f"The path provided for --aws-mirror-dir must be a directory: {self.aws_mirror_dir}"
            )

        if not self.overwrite and self.output.exists():
            raise FileExistsError(
                "Output file already exists. Provide --overwrite flag to replace it."
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

    gather_stac_collections(args)


def gather_stac_collections(args: ScriptArgs):
    logger.info("Building paths to collection files")
    json_paths = set()
    for collection_id in stac.COLLECTIONS:
        domain, kind, version, resolution = collection_id.split("-")
        json_paths.add(
            args.aws_mirror_dir / domain / kind / version / f"{resolution}.json"
        )

    for path in json_paths:
        if not path.exists():
            raise FileNotFoundError(f"{path} does not exist.")
        logger.debug(f"Found {path}")
    logger.info(f"Found all {len(json_paths)} expected files")

    logger.info("Parsing json and stripping 'links' section")
    collections = []
    for path in json_paths:
        with open(path, "r") as f:
            collection = json.load(f)
            collection["links"] = []
            collections.append(collection)

    logger.info(f"Writing {args.output}")
    with open(args.output, "w") as f:
        for collection in collections:
            f.write(f"{json.dumps(collection, indent=None)}\n")


if __name__ == "__main__":
    main()
