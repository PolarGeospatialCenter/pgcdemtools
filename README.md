# pgcdemtools
Tools for indexing, shelving, copying, and modifying SETSM and ASP dems

## Download
Version `1.2` released 2023-03-10.

#### [Download Latest](https://github.com/PolarGeospatialCenter/pgcdemtools/releases)

## Config
Scripts that support writing to an output Postgres database (such as index_setsm) have a `--config` argument path to a 
config file containing database access information. See the example [config](./config.ini.example) for reference. Alternatively, the 
information in this config file may be sourced from the Postgres standard pair of [~/.pgpass](https://www.postgresql.org/docs/current/libpq-pgpass.html) 
and [~/.pg_service.conf](https://www.postgresql.org/docs/current/libpq-pgservice.html) files.
> **Note:** The `~/.pg_service.conf` file does not support definition of an active schema in the config service 
> information. When leveraging a service from the `~/.pg_service.conf` file in a script argument for `pgcdemtools` 
> scripts, always indicate the schema name of the target layer. For example, `"PG:service_name:schema_name.layer_name"`.
> If the schema name is not provided and `~/.pg_service.conf` is used, the default schema name (usually 'public') will 
> be assumed.

The STAC tools that interact Postgres have a `--dsn` option that will accept any valid [PostgreSQL connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).
The tools use predefined schema and table names, so the note above is not applicable to these tools. Use of service 
definitions via `~/.pgpass` and `~/.pg_service.conf` is recommended.

## Tools
### Rename
rename_setsm_add_version

### Index
index_setsm - Build a gdb or shapefile index of a single SETSM DEM scene, strip, or tile, or a directory of DEMs of 
these types.

### Package
package_setsm - Build an index of a SETSM DEM or directory of DEMs and package all auxiliary files into tar.gz archives.

package_setsm_tiles - Build an index of a SETSM DEM mosaic tiles or directory of tiles and package all auxiliary files
into tar.gz archives.

### Shelve
shelve_setsm_by_date - Move or copy a SETSM DEM or a directory of DEMs into folders based on acquisition date.

shelve_setsm_by_geocell - Move or copy a SETSM DEM or a directory of DEMs into folders based on the geocell that 
intersects with the DEM centroid, as identified by the lower left geocell corner.

shelve_setsm_by_shp - Move or copy a SETSM DEM or a directory of DEMs into folders based on a custom shapefile index.

### Retrieve
copy_dems - Copy DEMs using a subset of the DEM index built using index_setsm.py.

### Misc
apply_setsm_registration - If GCP registration information is included with a SETSM DEM, apply the offset and output a 
new raster.

resample_setsm - resample SETSM DEMs to a lower resolution.

divide_setsm_tiles - Divide SETSM DEM mosaic tiles into subtiles.


## SpatioTemporal Asset Catalog (STAC) Building Tools

### Tool Descriptions
build_stac_items - Scans a directory for DEMs and creates a STAC Item for each DEM.

build_stac_tree - Scans a directory tree for STAC Items and builds a PGC STAC open data catalog.

extract_stac_metadata - Scans a directory for DEMs and extracts additional raster metadata needed to construct STAC 
Items from Sandwich tables.

extract_stac_items_from_sandwich - Pulls STAC Items from Sandwich tables to the file system as a mirror of the AWS 
public bucket or NDJSON.

gather_stac_collections - Scans a directory tree for STAC Collections and gathers them into an NDJSON file for updating the dynamic STAC API.


### Example - Build STAC Tree from source DEMs:

```
STAC_DIR: output base directory for STAC files
DEM_DIR: A STAC item is created for each *_dem.tif found (recursively) under this directory
DOMAIN: arcticdem, earthdem, or rema.  Must match the domain for the DEMs in DEM_DIR

# Build STAC items in parallel
find "${DEM_DIR} \
	-maxdepth 1 -type d -print0 | \
		xargs -0 -n 1 -I'{}' -P8 \
			./build_stac_items.py --overwrite --stac-base-dir="${STAC_DIR}" --domain=${DOMAIN} '{}'

# (re-)Build STAC Catalogs/Collections tree from STAC Items
./build_stac_tree.py --overwrite "${STAC_DIR}"
```

### Example - Build STAC Tree from Sandwich:

```shell
STAC_DIR: output base directory for STAC files
DEM_DIR: A STAC item is created for each *_dem.tif found (recursively) under this directory
DOMAIN: arcticdem, earthdem, or rema.  Must match the domain for the DEMs in DEM_DIR
NDJSON_DIR: Output directory to write NDJSON files for updating the dynamic STAC API

# Insert additional raster metadata into Sandwich
python extract_stac_items_from_sandwich.py --dsn "service=..." --domain "${DOMAIN}"

# Refresh the STAC Item materialized view
psql "service=..." -c "REFRESH MATERIALIZED VIEW dem.stac_static_item; VACUUM ANALYZE dem.stac_static_items;"

# Write STAC Items to AWS public bucket mirror 
# Run once per collection that that has been updated
python extract_stac_items_from_sandwich.py \
    --dsn "service=..." --stac-base-dir "${STAC_DIR}" --collection "..." --all-items --overwrite
    
# Rebuild the STAC Tree
python build_stac_tree.py --overwrite $"{STAC_DIR}"

# Write STAC Items as NDJSON for updating the dynamic STAC API 
# Run once per collection that that has been updated
python extract_stac_items_from_sandwich.py \
    --dsn "service=..." --stac-base-dir "${NDJSON_DIR}" --collection "..." --all-items --overwrite --ndjson
    
# Write STAC Collections as NDJSON for updating the dynamic STAC API
python gather_stac_collections.py --aws-mirror-dir "${STAC_DIR}" --output "${NDJSON}" --overwrite
```

See the pgc/stac-service repository for additional steps to update the dynamic STAC API

## Usage notes
Some of the tools are designed to be run either in a serial, parallel, or with a PBS or SLURM scheduler.  If a 
scheduler is present, the --scheduler option can be used to submit the jobs to PBS or SLURM.  The pbs_* and slurm_* 
scripts can be modified or used as a template for job submission scripts. The --parallel-processes option, if available,
allows the given tool to operate on several tasks at once.


## Running Tests

### Setup Test Data Directory
pgcdemtools uses pytest for running tests. Some use large files that are not included in the git repo, but are available
upon request

On Linux systems, make a symlink to the test data location:
```sh
# first time only
ln -s <test_data_location>/tests/testdata tests/
```

On Windows, you have to use the full network path and not a mounted drive letter path:
```sh
# first time only
# may need to use execute in a shell with elevated privileges (i.e. 'Run as administrator')
mklink /d tests\testdata <\\server.school.edu\test_data_location>\tests\testdata
```

### Install Additional Libraries
Some STAC tests utilize the pystac (version 1.12.0 or greater) and jsonschema (to preform schema validation) libraries. 
If these dependencies are not available in your environment, the tests that require them will be skipped. Add these
libraries to a clone of your primary environment with the following commands.

```shell
# Clone the primary environment 
conda create --name pgc-with-pystac --clone pgc
# Activate the new environment
conda activate pgc-with-pystac
# Add the additional libraries
conda install pystac jsonschema
```

### Configure Environment Variables

Tests that interact with the PostgreSQL require a DSN to configure the connection. Tests that assert the content of 
database tables/views allow for configuring the sampling strategy. The environment variables and their accepted values
are described below.

```shell
export SANDWICH_DSN="..."           # PostgreSQL connection string 
export SAMPLING_STRATEGY="static"   # 1% random sample (repeatable) [DEFAULT IF NOT SET]
export SAMPLING_STRATEGY="random"   # 1% random sample (non-repeatable)
export SAMPLING_STRATEGY="full"     # Full dataset scan
```

### Execute Tests

The full test suite can take more than 15 minutes to complete. Pytest provides [several options](https://docs.pytest.org/en/stable/how-to/usage.html) 
for selecting subsets of tests to run. Some examples are shown below.

```shell
# run all the tests
pytest

# run tests in a specific file
pytest tests/test_view_stac_static_item.py

# run the tests with names that contain a specified string
pytest -k stac

# report the selected tests without running
pytest -k stac --collect-only
```

## Contact
To report any questions or issues, please open a GitHub issue or contact the Polar Geospatial Center: 
pgc-support@umn.edu
