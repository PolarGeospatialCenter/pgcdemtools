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

### Build STAC Items
build_stac_items - scans a directory for DEMs and creates a STAC Item for each DEM.

### Build STAC Tree
build_stac_tree - scans a directory tree for STAC Items and builds a PGC STAC open data catalog


### Example:

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

## Usage notes
Some of the tools are designed to be run either in a serial, parallel, or with a PBS or SLURM scheduler.  If a 
scheduler is present, the --scheduler option can be used to submit the jobs to PBS or SLURM.  The pbs_* and slurm_* 
scripts can be modified or used as a template for job submission scripts. The --parallel-processes option, if available,
allows the given tool to operate on several tasks at once.


## Running Tests
pgcdemtools uses pytest for running tests. Some use large files that are not included in the git repo, but are available
upon request

On Linux systems, make a symlink to the test data location:
```sh
# first time only
ln -s <test_data_location>/tests/testdata tests/

# run the tests
pytest
```

On Windows, you have to use the full network path and not a mounted drive letter path:
```sh
# first time only
mklink /d tests\testdata <\\server.school.edu\test_data_location>\tests\testdata

# run the tests
pytest
```

The STAC creation tests were written with pytest and utilize the pystac (version 1.12.0 or greater) and 
jsonschema (to preform schema validation) libraries. If these dependencies are not available in your 
environment, the tests that require them will be skipped.

On Linux systems:
```shell
# link the testdata directory as shown in the previous section
ln -s <test_data_location>/tests/testdata tests/
# or set the environment variable TESTDATA_DIR
export TESTDATA_DIR=<test_data_location>

# run all the tests
pytest -vv

# run only the tests matching a grep-like pattern
pytest -vv -k arcticdem_mosaics_v4_1
```

## Contact
To report any questions or issues, please open a GitHub issue or contact the Polar Geospatial Center: 
pgc-support@umn.edu
