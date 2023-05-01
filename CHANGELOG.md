History
=======

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
