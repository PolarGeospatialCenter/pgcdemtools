History
=======

1.4.0 (2025-08-28)
------------------
* Fixup `index_setsm.py` dryrun actions
* Add database view definitions
* Index setsm bugfix
* Rebuild strip DEMs scene metadata from .mdf files
* Update Slurm Options and Handle Old Scene DEM Jsons
* Fix package_setsm.py rasterproxy logic bug
* add bitmask as component option to resample_setsm.py
* Update for python 3.12
* Update shelve_setsm_strips to use mdf.txt
* Adapt unittest tests to be run by pytest
* Migrate to STAC spec v1.1.0 and generate items from Sandwich
* Add gdal_wgs84_to_egm08_working.sh script.
* Change Strip DEM view definitions to manage strips on tape
* Folder structure update for v4.2 strips.
* Remove 50cm DEMs at CSDA from dem.scene_dem_all view definition
* Switch region lookup from danco to sandwich
* Make project arg required only when absolutely necessary for index_setsm.py

1.3.0 (2023-08-28)
------------------
* More complete check of `SetsmScene` source file existence
* Add new '*_datamask.tif' DEM mosaic tile component raster to scripts
* New toggle for writing "tile release" format index fields
* Support for pg_service.conf postgres connection settings
* Limit rasterproxy creation to relevant tifs in packaging
* Improve handling of errors in SETSM product packaging scripts
* Fix security issue in logging, change expected Danco service name in PG config
* Add python script with function to read LERC_ZSTD compressed 50cm Scene DEM rasters
* Strip DEM index changes to use correct field tupes

1.2.1 (2023-05-01)
------------------
* STAC: Add support for ArcticDEM v3.0 mosaics
* Skip over unique constraint duplicate record errors, adjust error logging in index_setsm.py 
* Add --version options to all scripts and update slurm submission scripts 
* Change scheduler CLI options with back-compatability to old style

1.2 (2023-03-10)
------------------
* Added ability to convert Strip DEMs to COG upon packaging and build raster proxy files
* Added scene DEM index fields and correct improper field types
* Added ability to creat STAC items and catalogs
* Updated packagingot handle s2s 4.x strip DEM items and names
* Converted to Python3, GDAL3, PROJ6
* Added exit codes
* Added --check option to indexer
