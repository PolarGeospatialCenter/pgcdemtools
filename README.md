# pgcdemtools
Tools for indexing, shelving, copying, and modifying SETSM and ASP dems

## Download
Version `1.2` released 2023-03-10.

#### [Download Latest](https://github.com/PolarGeospatialCenter/pgcdemtools/releases)

## Tools
### Rename
rename_setsm_add_version

### Index
index_setsm - Build a gdb or shapefile index of a single SETSM DEM scene, strip, or tile, or a directory of DEMs of these types.

### Package
package_setsm - Build an index of a SETSM DEM or directory of DEMs and package all auxilary files into tar.gz archives.

package_setsm_tiles - Build an index of a SETSM DEM mosaic tiles or directory of tiles and package all auxilary files into tar.gz archives.

### Shelve
shelve_setsm_by_date - Move or copy a SETSM DEM or a directory of DEMs into folders based on acquisition date.

shelve_setsm_by_geocell - Move or copy a SETSM DEM or a directory of DEMs into folders based on the geocell that intersects with the DEM cetroid, as identified by the lower left geocell corner.

shelve_setsm_by_shp - Move or copy a SETSM DEM or a directory of DEMs into folders based on a custon shapefile index.

### Retrieve
copy_dems - Copy DEMs using a subset of the DEM index bult using index_setsm.py.

### Misc
apply_setsm_registration - If GCP registration information is included with a SETSM DEM, apply the offset and output a new raster.

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
Some of the tools are designed to be run either in a serial, parallel, or with a PBS scheduler.  If a PBS scheduler is present, the --pbs option can often be used to submit the jobs to PBS.  The qsub* scripts can be modified or used as a template for job submission scripts. The --parallel-processes option, if available, allows the given tool to operate on several tasks at once.

## Contact
Claire Porter

[Polar Geospatial Center](//www.pgc.umn.edu)

Email: <porte254@umn.edu>
