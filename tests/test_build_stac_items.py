from dataclasses import dataclass
import os
import sys
from pathlib import Path

from psycopg2 import sql
import pytest
import requests
import shapely

repo = Path(__file__).parent.parent
sys.path.append(f"{repo}")

from lib import dem
from build_stac_items import (build_strip_stac_item,
                              build_mosaic_stac_item,
                              build_mosaic_v3_stac_item)

PYSTAC_REQUIRED_REASON = "requires pystac >= 1.12.0"
try:
    import pystac

    # Check the version of pystac installed is at least 1.12.0, which is when the STAC
    # spec version 1.1 was first supported.
    from importlib.metadata import version
    from packaging.version import Version

    installed = Version(version("pystac"))
    required = Version("1.12.0")
    PYSTAC_UNAVAILABLE = installed < required
except ImportError:
    PYSTAC_UNAVAILABLE = True

JSONSCHEMA_REQUIRED_REASON = "requires pystac >= 1.12.0 and jsonschema"
try:
    import jsonschema

    JSONSCHEMA_UNAVAILABLE = False
except ImportError:
    JSONSCHEMA_UNAVAILABLE = True


def get_testdata_dir() -> Path:
    if os.getenv("TESTDATA_DIR"):
        return Path(os.getenv("TESTDATA_DIR"))

    # Assume that a directory named 'testdata' has been linked into the tests directory
    # as described in the README
    test_dir = Path(__file__).parent
    if not test_dir.exists():
        raise FileNotFoundError(
            "testdata dir not found. Set environment variable TESTDATA_DIR or see README for linking instructions")
    return test_dir / "testdata"


########################################################################################
# STAC Item dict generation
# Pytest fixtures that generate a stac-item-like dict from testdata
########################################################################################


BASE_URL = "https://pgc-opendata-dems.s3.us-west-2.amazonaws.com"


@pytest.fixture()
def arcticdem_strips_s2s041_2m() -> dict:
    """Based on item id: SETSM_s2s041_W2W3_20231231_10300100F3CE3A00_104001008FBB9800_2m_seg1

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/strips/s2s041/2m/n60e030/SETSM_s2s041_W2W3_20231231_10300100F3CE3A00_104001008FBB9800_2m_seg1.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-strips-s2s041-2m/items/SETSM_s2s041_W2W3_20231231_10300100F3CE3A00_104001008FBB9800_2m_seg1
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_strips_s2s041_2m" / "SETSM_s2s041_W2W3_20231231_10300100F3CE3A00_104001008FBB9800_2m_seg1_dem.tif"
    raster = dem.SetsmDem(f"{filepath}")
    raster.get_dem_info()
    return build_strip_stac_item(base_url=BASE_URL, domain="arcticdem", raster=raster)


@pytest.fixture()
def earthdem_strips_s2s041_2m() -> dict:
    """Based on item id: SETSM_s2s041_WV01_20211104_10200100B9450600_10200100BA406F00_2m_lsf_seg1

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/earthdem/strips/s2s041/2m/n46w090/SETSM_s2s041_WV01_20211104_10200100B9450600_10200100BA406F00_2m_lsf_seg1.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/earthdem-strips-s2s041-2m/items/SETSM_s2s041_WV01_20211104_10200100B9450600_10200100BA406F00_2m_lsf_seg1
    """
    filepath = get_testdata_dir() / "stac_item" / "earthdem_strips_s2s041_2m" / "SETSM_s2s041_WV01_20211104_10200100B9450600_10200100BA406F00_2m_lsf_seg1_dem.tif"
    raster = dem.SetsmDem(f"{filepath}")
    raster.get_dem_info()
    return build_strip_stac_item(base_url=BASE_URL, domain="earthdem", raster=raster)


@pytest.fixture()
def rema_strips_s2s041_2m() -> dict:
    """Based on item id: SETSM_s2s041_WV02_20231231_10300100F3190400_10300100F4CB6200_2m_seg1

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/rema/strips/s2s041/2m/s73e121/SETSM_s2s041_WV02_20231231_10300100F3190400_10300100F4CB6200_2m_seg1.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/rema-strips-s2s041-2m/items/SETSM_s2s041_WV02_20231231_10300100F3190400_10300100F4CB6200_2m_seg1
    """
    filepath = get_testdata_dir() / "stac_item" / "rema_strips_s2s041_2m" / "SETSM_s2s041_WV02_20231231_10300100F3190400_10300100F4CB6200_2m_seg1_dem.tif"
    raster = dem.SetsmDem(f"{filepath}")
    raster.get_dem_info()
    return build_strip_stac_item(base_url=BASE_URL, domain="rema", raster=raster)


@pytest.fixture()
def arcticdem_mosaics_v4_1_2m() -> dict:
    """Based on item id: 44_74_1_2_2m_v4.1

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/mosaics/v4.1/2m/44_74/44_74_1_2_2m_v4.1.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-mosaics-v4.1-2m/items/44_74_1_2_2m_v4.1
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_mosaics_v4.1_2m" / "44_74_1_2_2m_v4.1_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "4.1"
    return build_mosaic_stac_item(base_url=BASE_URL, domain="arcticdem", tile=tile)


@pytest.fixture()
def arcticdem_mosaics_v4_1_10m() -> dict:
    """Based on item id: 58_05_10m_v4.1

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/mosaics/v4.1/10m/58_05/58_05_10m_v4.1.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-mosaics-v4.1-10m/items/58_05_10m_v4.1
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_mosaics_v4.1_10m" / "58_05_10m_v4.1_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "4.1"
    return build_mosaic_stac_item(base_url=BASE_URL, domain="arcticdem", tile=tile)


@pytest.fixture()
def arcticdem_mosaics_v4_1_32m() -> dict:
    """Based on item id: 58_05_32m_v4.1

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/mosaics/v4.1/32m/58_05/58_05_32m_v4.1.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-mosaics-v4.1-32m/items/58_05_32m_v4.1
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_mosaics_v4.1_32m" / "58_05_32m_v4.1_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "4.1"
    return build_mosaic_stac_item(base_url=BASE_URL, domain="arcticdem", tile=tile)


@pytest.fixture()
def rema_mosaics_v2_0_2m() -> dict:
    """Based on item id: 26_12_1_2_2m_v2.0

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/rema/mosaics/v2.0/2m/26_12/26_12_1_2_2m_v2.0.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/rema-mosaics-v2.0-2m/items/26_12_1_2_2m_v2.0
    """
    filepath = get_testdata_dir() / "stac_item" / "rema_mosaics_v2.0_2m" / "26_12_1_2_2m_v2.0_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "2.0"
    return build_mosaic_stac_item(base_url=BASE_URL, domain="rema", tile=tile)


@pytest.fixture()
def rema_mosaics_v2_0_10m() -> dict:
    """Based on item id: 29_30_10m_v2.0

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/rema/mosaics/v2.0/10m/29_30/29_30_10m_v2.0.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/rema-mosaics-v2.0-10m/items/29_30_10m_v2.0
    """
    filepath = get_testdata_dir() / "stac_item" / "rema_mosaics_v2.0_10m" / "29_30_10m_v2.0_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "2.0"
    return build_mosaic_stac_item(base_url=BASE_URL, domain="rema", tile=tile)


@pytest.fixture()
def rema_mosaics_v2_0_32m() -> dict:
    """Based on item id: 29_30_32m_v2.0

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/rema/mosaics/v2.0/32m/29_30/29_30_32m_v2.0.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/rema-mosaics-v2.0-32m/items/29_30_32m_v2.0
    """
    filepath = get_testdata_dir() / "stac_item" / "rema_mosaics_v2.0_32m" / "29_30_32m_v2.0_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "2.0"
    return build_mosaic_stac_item(base_url=BASE_URL, domain="rema", tile=tile)


@pytest.fixture()
def arcticdem_mosaics_v3_0_2m() -> dict:
    """Based on item id: 50_49_2_2_2m_v3.0

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/mosaics/v3.0/2m/50_49/50_49_2_2_2m_v3.0.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-mosaics-v3.0-2m/items/50_49_2_2_2m_v3.0
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_mosaics_v3.0_2m" / "50_49_2_2_2m_v3.0_reg_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "3.0"
    return build_mosaic_v3_stac_item(base_url=BASE_URL, domain="arcticdem", tile=tile)


@pytest.fixture()
def arcticdem_mosaics_v3_0_10m() -> dict:
    """Based on item id: 50_47_10m_v3.0

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/mosaics/v3.0/10m/50_47/50_47_10m_v3.0.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-mosaics-v3.0-10m/items/50_47_10m_v3.0
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_mosaics_v3.0_10m" / "50_47_10m_v3.0_reg_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "3.0"
    return build_mosaic_v3_stac_item(base_url=BASE_URL, domain="arcticdem", tile=tile)


@pytest.fixture()
def arcticdem_mosaics_v3_0_32m() -> dict:
    """Based on item id: 50_47_32m_v3.0

    Existing static item: https://pgc-opendata-dems.s3.us-west-2.amazonaws.com/arcticdem/mosaics/v3.0/32m/50_47/50_47_32m_v3.0.json

    Existing dynamic item: https://stac.pgc.umn.edu/api/v1/collections/arcticdem-mosaics-v3.0-32m/items/50_47_32m_v3.0
    """
    filepath = get_testdata_dir() / "stac_item" / "arcticdem_mosaics_v3.0_32m" / "50_47_32m_v3.0_reg_dem.tif"
    tile = dem.SetsmTile(f"{filepath}")
    tile.get_dem_info()
    assert tile.release_version == "3.0"
    return build_mosaic_v3_stac_item(base_url=BASE_URL, domain="arcticdem", tile=tile)


########################################################################################
# Schema validation
# Uses pystac's validation methods to check items against core and extension
# json-schemas. Requires jsonschema library.
########################################################################################

def assert_item_is_valid(stac_item: dict):
    expected_schemas = {
        'https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/item.json',
        'https://stac-extensions.github.io/projection/v2.0.0/schema.json',
        'https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json',
    }

    valid_schemas = set(pystac.validation.validate_dict(stac_item))
    assert valid_schemas == expected_schemas


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_strips_s2s041_2m(arcticdem_strips_s2s041_2m):
    assert_item_is_valid(arcticdem_strips_s2s041_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_earthdem_strips_s2s041_2m(earthdem_strips_s2s041_2m):
    assert_item_is_valid(earthdem_strips_s2s041_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_rema_strips_s2s041_2m(rema_strips_s2s041_2m):
    assert_item_is_valid(rema_strips_s2s041_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_mosaics_v4_1_2m(arcticdem_mosaics_v4_1_2m):
    assert_item_is_valid(arcticdem_mosaics_v4_1_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_mosaics_v4_1_10m(arcticdem_mosaics_v4_1_10m):
    assert_item_is_valid(arcticdem_mosaics_v4_1_10m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_mosaics_v4_1_32m(arcticdem_mosaics_v4_1_32m):
    assert_item_is_valid(arcticdem_mosaics_v4_1_32m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_rema_mosaics_v2_0_2m(rema_mosaics_v2_0_2m):
    assert_item_is_valid(rema_mosaics_v2_0_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_rema_mosaics_v2_0_10m(rema_mosaics_v2_0_10m):
    assert_item_is_valid(rema_mosaics_v2_0_10m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_rema_mosaics_v2_0_32m(rema_mosaics_v2_0_32m):
    assert_item_is_valid(rema_mosaics_v2_0_32m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_mosaics_v3_0_2m(arcticdem_mosaics_v3_0_2m):
    assert_item_is_valid(arcticdem_mosaics_v3_0_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_mosaics_v3_0_10m(arcticdem_mosaics_v3_0_10m):
    assert_item_is_valid(arcticdem_mosaics_v3_0_10m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE or JSONSCHEMA_UNAVAILABLE,
                    reason=JSONSCHEMA_REQUIRED_REASON)
def test_schema_arcticdem_mosaics_v3_0_32m(arcticdem_mosaics_v3_0_32m):
    assert_item_is_valid(arcticdem_mosaics_v3_0_32m)


########################################################################################
# Href validation
# Makes a HEAD request to all https hrefs in an item and checks that the status code of
# the response is 200. Only works for previously published items.
########################################################################################

def assert_https_hrefs(stac_item: dict):
    item = pystac.Item.from_dict(stac_item)
    link_hrefs = {link.href for link in item.links}
    asset_hrefs = {asset.href for asset in item.assets.values()}

    for href in link_hrefs | asset_hrefs:
        response = requests.head(href, timeout=5)
        # Include href in comparison so that failures report the unreachable href value
        assert (href, response.status_code) == (href, 200)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_strips_s2s041_2m(arcticdem_strips_s2s041_2m):
    assert_https_hrefs(arcticdem_strips_s2s041_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_earthdem_strips_s2s041_2m(earthdem_strips_s2s041_2m):
    assert_https_hrefs(earthdem_strips_s2s041_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_rema_strips_s2s041_2m(rema_strips_s2s041_2m):
    assert_https_hrefs(rema_strips_s2s041_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_mosaics_v4_1_2m(arcticdem_mosaics_v4_1_2m):
    assert_https_hrefs(arcticdem_mosaics_v4_1_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_mosaics_v4_1_10m(arcticdem_mosaics_v4_1_10m):
    assert_https_hrefs(arcticdem_mosaics_v4_1_10m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_mosaics_v4_1_32m(arcticdem_mosaics_v4_1_32m):
    assert_https_hrefs(arcticdem_mosaics_v4_1_32m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_rema_mosaics_v2_0_2m(rema_mosaics_v2_0_2m):
    assert_https_hrefs(rema_mosaics_v2_0_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_rema_mosaics_v2_0_10m(rema_mosaics_v2_0_10m):
    assert_https_hrefs(rema_mosaics_v2_0_10m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_rema_mosaics_v2_0_32m(rema_mosaics_v2_0_32m):
    assert_https_hrefs(rema_mosaics_v2_0_32m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_mosaics_v3_0_2m(arcticdem_mosaics_v3_0_2m):
    assert_https_hrefs(arcticdem_mosaics_v3_0_2m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_mosaics_v3_0_10m(arcticdem_mosaics_v3_0_10m):
    assert_https_hrefs(arcticdem_mosaics_v3_0_10m)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_hrefs_arcticdem_mosaics_v3_0_32m(arcticdem_mosaics_v3_0_32m):
    assert_https_hrefs(arcticdem_mosaics_v3_0_32m)


########################################################################################
# Content validation
# Misc item and asset property checks
########################################################################################

@dataclass
class AssetParams:
    asset: str
    nodata: int | float
    data_type: str
    unit: str | None


def assert_asset_params(item: pystac.Item, asset_params: list[AssetParams]):
    for expected in asset_params:
        actual = AssetParams(
            asset=expected.asset,
            nodata=item.assets[expected.asset].extra_fields.get("nodata"),
            data_type=item.assets[expected.asset].extra_fields.get("data_type"),
            unit=item.assets[expected.asset].extra_fields.get("unit"),
        )
        assert actual == expected


def asset_has_proj_keys(asset: pystac.Asset) -> bool:
    proj_keys = {"gsd", "proj:code", "proj:shape", "proj:transform", "proj:geometry"}
    asset_keys = set(asset.to_dict().keys())
    return proj_keys.issubset(asset_keys)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_strips_s2s041_2m(arcticdem_strips_s2s041_2m):
    item = pystac.Item.from_dict(arcticdem_strips_s2s041_2m)

    assert item.id == "SETSM_s2s041_W2W3_20231231_10300100F3CE3A00_104001008FBB9800_2m_seg1"
    assert item.collection_id == "arcticdem-strips-s2s041-2m"
    assert item.common_metadata.gsd == 2
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime
    assert item.properties["pgc:rmse"] != -9999

    assert asset_has_proj_keys(item.assets["hillshade"])
    assert asset_has_proj_keys(item.assets["hillshade_masked"])
    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="hillshade_masked", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="mask", nodata=1, data_type="uint8", unit=None),
        AssetParams(asset="matchtag", nodata=0, data_type="uint8", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_earthdem_strips_s2s041_2m(earthdem_strips_s2s041_2m):
    item = pystac.Item.from_dict(earthdem_strips_s2s041_2m)

    assert item.collection_id == "earthdem-strips-s2s041-2m"
    assert item.common_metadata.gsd == 2
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime
    assert item.properties["pgc:rmse"] != -9999

    assert asset_has_proj_keys(item.assets["hillshade"])
    assert asset_has_proj_keys(item.assets["hillshade_masked"])
    assert item.ext.proj.code == "EPSG:32616"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="hillshade_masked", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="mask", nodata=1, data_type="uint8", unit=None),
        AssetParams(asset="matchtag", nodata=0, data_type="uint8", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_rema_strips_s2s041_2m(rema_strips_s2s041_2m):
    item = pystac.Item.from_dict(rema_strips_s2s041_2m)

    assert item.collection_id == "rema-strips-s2s041-2m"
    assert item.common_metadata.gsd == 2
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime
    assert item.properties["pgc:rmse"] != -9999

    assert asset_has_proj_keys(item.assets["hillshade"])
    assert asset_has_proj_keys(item.assets["hillshade_masked"])
    assert item.ext.proj.code == "EPSG:3031"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="hillshade_masked", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="mask", nodata=1, data_type="uint8", unit=None),
        AssetParams(asset="matchtag", nodata=0, data_type="uint8", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_mosaics_v4_1_2m(arcticdem_mosaics_v4_1_2m):
    item = pystac.Item.from_dict(arcticdem_mosaics_v4_1_2m)

    assert item.collection_id == "arcticdem-mosaics-v4.1-2m"
    assert item.common_metadata.gsd == 2
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert asset_has_proj_keys(item.assets["hillshade"])
    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="count", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="mad", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="maxdate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="mindate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="datamask", nodata=0, data_type="uint8", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_mosaics_v4_1_10m(arcticdem_mosaics_v4_1_10m):
    item = pystac.Item.from_dict(arcticdem_mosaics_v4_1_10m)

    assert item.collection_id == "arcticdem-mosaics-v4.1-10m"
    assert item.common_metadata.gsd == 10
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert not asset_has_proj_keys(item.assets["hillshade"])
    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="count", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="mad", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="maxdate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="mindate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="datamask", nodata=0, data_type="uint8", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_mosaics_v4_1_32m(arcticdem_mosaics_v4_1_32m):
    item = pystac.Item.from_dict(arcticdem_mosaics_v4_1_32m)

    assert item.collection_id == "arcticdem-mosaics-v4.1-32m"
    assert item.common_metadata.gsd == 32
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert not asset_has_proj_keys(item.assets["hillshade"])
    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="count", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="mad", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="maxdate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="mindate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="datamask", nodata=0, data_type="uint8", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_rema_mosaics_v2_0_2m(rema_mosaics_v2_0_2m):
    item = pystac.Item.from_dict(rema_mosaics_v2_0_2m)

    assert item.collection_id == "rema-mosaics-v2.0-2m"
    assert item.common_metadata.gsd == 2
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert asset_has_proj_keys(item.assets["hillshade"])
    assert item.ext.proj.code == "EPSG:3031"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="count", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="mad", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="maxdate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="mindate", nodata=0, data_type="int16", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_rema_mosaics_v2_0_10m(rema_mosaics_v2_0_10m):
    item = pystac.Item.from_dict(rema_mosaics_v2_0_10m)

    assert item.collection_id == "rema-mosaics-v2.0-10m"
    assert item.common_metadata.gsd == 10
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert not asset_has_proj_keys(item.assets["hillshade"])
    assert item.ext.proj.code == "EPSG:3031"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="count", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="mad", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="maxdate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="mindate", nodata=0, data_type="int16", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_rema_mosaics_v2_0_32m(rema_mosaics_v2_0_32m):
    item = pystac.Item.from_dict(rema_mosaics_v2_0_32m)

    assert item.collection_id == "rema-mosaics-v2.0-32m"
    assert item.common_metadata.gsd == 32
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert not asset_has_proj_keys(item.assets["hillshade"])
    assert item.ext.proj.code == "EPSG:3031"

    asset_params = [
        AssetParams(asset="hillshade", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="count", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="mad", nodata=-9999.0, data_type="float32", unit="meter"),
        AssetParams(asset="maxdate", nodata=0, data_type="int16", unit=None),
        AssetParams(asset="mindate", nodata=0, data_type="int16", unit=None),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_mosaics_v3_0_2m(arcticdem_mosaics_v3_0_2m):
    item = pystac.Item.from_dict(arcticdem_mosaics_v3_0_2m)

    assert item.collection_id == "arcticdem-mosaics-v3.0-2m"
    assert item.common_metadata.gsd == 2
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert asset_has_proj_keys(item.assets["browse"])
    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="browse", nodata=0, data_type="uint8", unit=None),
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_mosaics_v3_0_10m(arcticdem_mosaics_v3_0_10m):
    item = pystac.Item.from_dict(arcticdem_mosaics_v3_0_10m)

    assert item.collection_id == "arcticdem-mosaics-v3.0-10m"
    assert item.common_metadata.gsd == 10
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
    ]
    assert_asset_params(item, asset_params)


@pytest.mark.skipif(PYSTAC_UNAVAILABLE, reason=PYSTAC_REQUIRED_REASON)
def test_content_arcticdem_mosaics_v3_0_32m(arcticdem_mosaics_v3_0_32m):
    item = pystac.Item.from_dict(arcticdem_mosaics_v3_0_32m)

    assert item.collection_id == "arcticdem-mosaics-v3.0-32m"
    assert item.common_metadata.gsd == 32
    assert item.common_metadata.start_datetime <= item.common_metadata.end_datetime

    assert item.ext.proj.code == "EPSG:3413"

    asset_params = [
        AssetParams(asset="dem", nodata=-9999.0, data_type="float32", unit="meter"),
    ]
    assert_asset_params(item, asset_params)


########################################################################################
# Sandwich synchronization
# The STAC Items produced by build_stac_items.py should be the same as those contained
# in dem.stac_static_item
########################################################################################


def get_same_item_from_sandwich(db_connection, item_dict):
    view_name = "stac_strip_item" \
        if "strips" in item_dict["collection"] \
        else "stac_mosaic_item"

    query = sql.SQL("""
    SELECT content
    FROM {view}
    WHERE collection = {collection}
        AND item_id = {item_id}
    """).format(
        view=sql.Identifier("dem", view_name),
        collection=sql.Literal(item_dict["collection"]),
        item_id=sql.Literal(item_dict["id"]),
    )

    with db_connection.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        return row[0]


def extract_structure(d, path=""):
    """
    Recursively extract the structure of a dictionary.
    Returns a set of dot-separated key paths.
    """
    structure = set()

    if isinstance(d, dict):
        # If there are no keys, add the current path as a leaf
        if not d:
            structure.add(path)

        # Process each key
        for key, value in d.items():
            new_path = f"{path}.{key}" if path else key

            # If value is a dict, recurse
            if isinstance(value, dict):
                structure.update(extract_structure(value, new_path))
            # If value is a list, process each item with its index
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        structure.update(extract_structure(item, f"{new_path}[{i}]"))
                    else:
                        structure.add(f"{new_path}[{i}]")
            # Other values are leaves
            else:
                structure.add(new_path)

    return structure


def assert_items_are_equivalent(from_raster: dict, from_sandwich: dict):
    raster_structure = extract_structure(from_raster)
    sandwhich_structure = extract_structure(from_sandwich)
    assert raster_structure == sandwhich_structure

    # The "published" property will never align (datetime.utcnow() vs sandwich value),
    # but that's okay.
    from_raster_props = {k: v for k, v in from_raster["properties"].items()
                         if k != "published"}
    from_sandwich_props = {k: v for k, v in from_sandwich["properties"].items()
                           if k != "published"}
    assert from_raster_props == from_sandwich_props

    assert from_raster["assets"] == from_sandwich["assets"]

    from_raster_polygon = shapely.MultiPolygon(
        from_raster["geometry"]["coordinates"]
    ).normalize()
    from_sandwich_polygon = shapely.MultiPolygon(
        from_sandwich["geometry"]["coordinates"]
    ).normalize()
    diff = from_raster_polygon.difference(from_sandwich_polygon)
    assert diff.area < 0.1


def test_sync_arcticdem_strips_s2s041_2m(db_connection, arcticdem_strips_s2s041_2m):
    from_raster = arcticdem_strips_s2s041_2m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_earthdem_strips_s2s041_2m(db_connection, earthdem_strips_s2s041_2m):
    from_raster = earthdem_strips_s2s041_2m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_rema_strips_s2s041_2m(db_connection, rema_strips_s2s041_2m):
    from_raster = rema_strips_s2s041_2m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_arcticdem_mosaics_v4_1_2m(db_connection, arcticdem_mosaics_v4_1_2m):
    from_raster = arcticdem_mosaics_v4_1_2m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_arcticdem_mosaics_v4_1_10m(db_connection, arcticdem_mosaics_v4_1_10m):
    from_raster = arcticdem_mosaics_v4_1_10m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_arcticdem_mosaics_v4_1_32m(db_connection, arcticdem_mosaics_v4_1_32m):
    from_raster = arcticdem_mosaics_v4_1_32m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_rema_mosaics_v2_0_2m(db_connection, rema_mosaics_v2_0_2m):
    from_raster = rema_mosaics_v2_0_2m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_rema_mosaics_v2_0_10m(db_connection, rema_mosaics_v2_0_10m):
    from_raster = rema_mosaics_v2_0_10m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_rema_mosaics_v2_0_32m(db_connection, rema_mosaics_v2_0_32m):
    from_raster = rema_mosaics_v2_0_32m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_arcticdem_mosaics_v3_0_2m(db_connection, arcticdem_mosaics_v3_0_2m):
    from_raster = arcticdem_mosaics_v3_0_2m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    # The source files to calculate this property from the raster no longer exists, so
    # the raster and sandwich versions will never match
    del from_raster["properties"]["pgc:data_perc"]
    del from_sandwich["properties"]["pgc:data_perc"]

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_arcticdem_mosaics_v3_0_10m(db_connection, arcticdem_mosaics_v3_0_10m):
    from_raster = arcticdem_mosaics_v3_0_10m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    # The source files to calculate this property from the raster no longer exists, so
    # the raster and sandwich versions will never match
    del from_raster["properties"]["pgc:data_perc"]
    del from_sandwich["properties"]["pgc:data_perc"]

    # These properties do not match between the existing metadata files and the indexes
    # that are the source for the sandwich table. TODO: Investigate which is correct.
    del from_raster["properties"]["pgc:num_components"]
    del from_sandwich["properties"]["pgc:num_components"]

    assert_items_are_equivalent(from_raster, from_sandwich)


def test_sync_arcticdem_mosaics_v3_0_32m(db_connection, arcticdem_mosaics_v3_0_32m):
    from_raster = arcticdem_mosaics_v3_0_32m
    from_sandwich = get_same_item_from_sandwich(db_connection, from_raster)

    # The source files to calculate this property from the raster no longer exists, so
    # the raster and sandwich versions will never match
    del from_raster["properties"]["pgc:data_perc"]
    del from_sandwich["properties"]["pgc:data_perc"]

    # These properties do not match between the existing metadata files and the indexes
    # that are the source for the sandwich table. TODO: Investigate which is correct.
    del from_raster["properties"]["pgc:num_components"]
    del from_sandwich["properties"]["pgc:num_components"]

    assert_items_are_equivalent(from_raster, from_sandwich)
