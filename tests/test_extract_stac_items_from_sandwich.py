import pathlib
import sys

from psycopg2 import sql

repo = pathlib.Path(__file__).parent.parent
sys.path.append(f"{repo}")

from extract_stac_items_from_sandwich import get_mirror_path


def test_get_mirror_path(db_connection):
    query = sql.SQL("""
    SELECT content
    FROM dem.stac_static_item
    WHERE collection = %s AND item_id = %s
    """)

    stac_base_dir = pathlib.Path("/path/to/stac-mirror")
    test_values = [
        (
            "arcticdem-mosaics-v3.0-10m",
            "58_07_10m_v3.0",
            "/path/to/stac-mirror/arcticdem/mosaics/v3.0/10m/58_07/58_07_10m_v3.0.json"
        ),
        (
            "arcticdem-mosaics-v3.0-2m",
            "30_24_2_1_2m_v3.0",
            "/path/to/stac-mirror/arcticdem/mosaics/v3.0/2m/30_24/30_24_2_1_2m_v3.0.json"
        ),
        (
            "arcticdem-mosaics-v3.0-32m",
            "73_40_32m_v3.0",
            "/path/to/stac-mirror/arcticdem/mosaics/v3.0/32m/73_40/73_40_32m_v3.0.json"
        ),
        (
            "arcticdem-mosaics-v4.1-10m",
            "29_29_10m_v4.1",
            "/path/to/stac-mirror/arcticdem/mosaics/v4.1/10m/29_29/29_29_10m_v4.1.json"
        ),
        (
            "arcticdem-mosaics-v4.1-2m",
            "30_32_2_1_2m_v4.1",
            "/path/to/stac-mirror/arcticdem/mosaics/v4.1/2m/30_32/30_32_2_1_2m_v4.1.json"
        ),
        (
            "arcticdem-mosaics-v4.1-32m",
            "80_24_32m_v4.1",
            "/path/to/stac-mirror/arcticdem/mosaics/v4.1/32m/80_24/80_24_32m_v4.1.json"
        ),
        (
            "rema-mosaics-v2.0-10m",
            "06_37_10m_v2.0",
            "/path/to/stac-mirror/rema/mosaics/v2.0/10m/06_37/06_37_10m_v2.0.json"
        ),
        (
            "rema-mosaics-v2.0-2m",
            "06_37_2_1_2m_v2.0",
            "/path/to/stac-mirror/rema/mosaics/v2.0/2m/06_37/06_37_2_1_2m_v2.0.json"
        ),
        (
            "rema-mosaics-v2.0-32m",
            "06_37_32m_v2.0",
            "/path/to/stac-mirror/rema/mosaics/v2.0/32m/06_37/06_37_32m_v2.0.json"
        ),
        (
            "arcticdem-strips-s2s041-2m",
            "SETSM_s2s041_W1W1_20150504_102001003D18EC00_102001003D98D300_2m_lsf_seg1",
            "/path/to/stac-mirror/arcticdem/strips/s2s041/2m/n82w029/SETSM_s2s041_W1W1_20150504_102001003D18EC00_102001003D98D300_2m_lsf_seg1.json"
        ),
        (
            "earthdem-strips-s2s041-2m",
            "SETSM_s2s041_WV03_20211018_104001006E0CCE00_104001006F3D6200_2m_lsf_seg1",
            "/path/to/stac-mirror/earthdem/strips/s2s041/2m/n46w093/SETSM_s2s041_WV03_20211018_104001006E0CCE00_104001006F3D6200_2m_lsf_seg1.json"
        ),
        (
            "rema-strips-s2s041-2m",
            "SETSM_s2s041_W1W1_20200227_102001009078A500_1020010092303400_2m_lsf_seg1",
            "/path/to/stac-mirror/rema/strips/s2s041/2m/s80e060/SETSM_s2s041_W1W1_20200227_102001009078A500_1020010092303400_2m_lsf_seg1.json"
        ),
    ]

    with db_connection.cursor() as cur:
        for collection, item_id, path_str in test_values:
            cur.execute(query, (collection, item_id))
            item = cur.fetchone()[0]
            result = get_mirror_path(stac_base_dir, item)

            assert result == pathlib.Path(path_str)


def test_timezones_are_not_changed(db_connection):
    query = sql.SQL("""
    SELECT content
    FROM dem.stac_static_item
    WHERE collection = %s AND item_id = %s
    """)

    test_values = [
        ('arcticdem-mosaics-v3.0-10m', '58_07_10m_v3.0', '2013-04-26T00:00:00Z'),
        ('arcticdem-mosaics-v3.0-2m', '30_24_2_1_2m_v3.0', '2012-04-18T00:00:00Z'),
        ('arcticdem-mosaics-v3.0-32m', '73_40_32m_v3.0', '2012-11-25T00:00:00Z'),
        ('arcticdem-mosaics-v4.1-10m', '29_29_10m_v4.1', '2009-08-01T00:00:00Z'),
        ('arcticdem-mosaics-v4.1-2m', '30_32_2_1_2m_v4.1', '2011-06-03T00:00:00Z'),
        ('arcticdem-mosaics-v4.1-32m', '80_24_32m_v4.1', '2010-05-12T00:00:00Z'),
        ('rema-mosaics-v2.0-10m', '06_37_10m_v2.0', '2010-11-11T00:00:00Z'),
        ('rema-mosaics-v2.0-2m', '06_37_2_1_2m_v2.0', '2011-02-18T00:00:00Z'),
        ('rema-mosaics-v2.0-32m', '06_37_32m_v2.0', '2010-11-11T00:00:00Z'),
        (
            'arcticdem-strips-s2s041-2m',
            'SETSM_s2s041_W1W1_20150504_102001003D18EC00_102001003D98D300_2m_lsf_seg1',
            '2015-05-04T18:28:30Z'
        ),
        (
            'earthdem-strips-s2s041-2m',
            'SETSM_s2s041_WV03_20211018_104001006E0CCE00_104001006F3D6200_2m_lsf_seg1',
            '2021-10-18T17:19:40Z'
        ),
        (
            'rema-strips-s2s041-2m',
            'SETSM_s2s041_W1W1_20200227_102001009078A500_1020010092303400_2m_lsf_seg1',
            '2020-02-27T06:53:34Z'
        ),
    ]

    with db_connection.cursor() as cur:
        for collection, item_id, datetime_str in test_values:
            cur.execute(query, (collection, item_id))
            item = cur.fetchone()[0]

            assert item["properties"]["datetime"] == datetime_str
