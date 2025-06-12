#!/bin/bash
set -euo pipefail

# This script tests the current environment to verify that GDAL is properly
# applying the vertical datum transformation between WGS84 and EGM08 based
# on an (arbitrarily selected) sample pixel from the pgc-opendata-dems S3
# bucket.
#
# If the grid transform files are not available to PROJ, PROJ (and thus GDAL)
# will silently apply the null transform instead.
#
# The first test checks that the source data is as expected,
# the second test checks the vdatum transform works.
#
# Test successful:
#         Testing if Proj Datum Grids are installed correctly...
#         Test WGS84 elevation...
#         Expected: -53.859375
#         Got: -53.859375
#
#         Test EGM08 elevation...
#         Expected: 1.05549550056458
#         Got: 1.05549550056458
#
# Test failed (and exits 1):
#         Testing if Proj Datum Grids are installed correctly...
#         Test WGS84 elevation...
#         Expected: -53.859375
#         Got: -53.859375
#
#         Test EGM08 elevation...
#         Expected: 1.05549550056458
#         Got: -53.859375
#         Failed

# Set PROJ_NETWORK=ON or OFF
export PROJ_NETWORK=OFF

# There isn't any particular significance to the DEM or point chosen.
DEM="/vsicurl/https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/rema/strips/s2s041/2m/s77e166/SETSM_s2s041_WV01_20180915_1020010079996900_1020010078866100_2m_lsf_seg1_dem.tif"
LOC="324249.000 -1380735.000"

expect () {
	# params: test_value, expected_value, tolerance
	echo Expected: $2
	echo Got: $1
	python3 -c '
import sys
t=abs(float(sys.argv[1]) - float(sys.argv[2])) >= float(sys.argv[3])
if t:
	print("Failed")
sys.exit(t)
' $1 $2 $3
}


echo "Testing if Proj Datum Grids are installed correctly..."

# Test WGS84 elevation.  Expect -53.8594...
echo "Test WGS84 elevation..."
R=$(gdallocationinfo -geoloc -valonly \
	"${DEM}" \
	${LOC}
)
expect "$R" -53.859375 0.0001

# Transform to EGM08.  Expect 1.0555...
echo
echo "Test EGM08 elevation..."
TEMP_VRT=$(mktemp --suffix=.vrt --tmpdir="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}")
trap "rm -f ${TEMP_VRT}" EXIT
R=$(gdalwarp -of VRT \
	-t_srs EPSG:3031+3855 \
	"${DEM}" \
	"${TEMP_VRT}"
)
R=$(gdallocationinfo -geoloc -valonly \
	"${TEMP_VRT}" \
	${LOC}
)
expect "$R" 1.05549550056458 0.0001
