import os
import pytest

# The materialized view being tested contains more than 1 million records, so tests that
# access jsonb content may take a few minutes to complete when run against all rows.
# Set this environment variable to control sampling strategy. Strategies and default are
# defined in the following function.
SAMPLING_STRATEGY_ENV_VARIABLE = "SAMPLING_STRATEGY"


@pytest.fixture(scope='module')
def sampling_strategy() -> tuple[str, str]:
    strategies = {
        "random": {
            "clause": "TABLESAMPLE BERNOULLI(1)",
            "description": "1% random sample (non-repeatable)"
        },
        "static": {
            "clause": "TABLESAMPLE BERNOULLI(1) REPEATABLE(42)",
            "description": "1% static random sample (repeatable)"
        },
        "full": {
            "clause": "",
            "description": "full dataset scan"
        }
    }

    sample_strategy = os.environ.get(SAMPLING_STRATEGY_ENV_VARIABLE, "static")

    if sample_strategy not in strategies.keys():
        accepted_values = ", ".join(strategies.keys())
        pytest.exit(
            f"Environment variable {SAMPLING_STRATEGY_ENV_VARIABLE} set to {sample_strategy}. Accepted values are: {accepted_values}"
        )

    sampling_clause = strategies[sample_strategy]["clause"]
    sampling_description = strategies[sample_strategy]["description"]

    return sampling_clause, sampling_description


def test_unique_item_ids(db_connection):
    query = """
    SELECT collection, item_id, count(*)
    FROM dem.stac_static_item
    GROUP BY collection, item_id 
    HAVING count(*) > 1
    LIMIT 10
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, f"Found duplicate item_ids (LIMIT 10): {rows}"


def test_datetime_values(db_connection, sampling_strategy):
    sampling_clause, sampling_description = sampling_strategy

    query = f"""
    SELECT * 
    FROM (
        SELECT 
            collection, 
            item_id, 
            (content->'properties'->>'datetime')::TIMESTAMPTZ AS datetime,
            (content->'properties'->>'start_datetime')::TIMESTAMPTZ AS start_datetime,
            (content->'properties'->>'end_datetime')::TIMESTAMPTZ AS end_datetime
        FROM dem.stac_static_item {sampling_clause}
    ) AS subquery
    WHERE datetime != start_datetime
        OR start_datetime > end_datetime
    LIMIT 10
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, \
            f"Found invalid datetime values using {sampling_description} (LIMIT 10): {rows}"


def test_no_null_property_values(db_connection, sampling_strategy):
    sampling_clause, sampling_description = sampling_strategy

    query = f"""
    SELECT
        collection,
        item_id,
        key,
        value
    FROM dem.stac_static_item {sampling_clause}, jsonb_each(content->'properties')
    WHERE value = 'null'::jsonb
    LIMIT 10
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, \
            f"Found nulls using {sampling_description} (LIMIT 10): {rows}"


def test_no_null_asset_properties(db_connection, sampling_strategy):
    sampling_clause, sampling_description = sampling_strategy

    query = f"""
    SELECT
        collection,
        item_id
    FROM dem.stac_static_item {sampling_clause}
    WHERE jsonb_path_exists(content->'assets', '$.** ? (@==null)')
    LIMIT 10
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, \
            f"Found nulls using {sampling_description} (LIMIT 10): {rows}"


def test_strip_rmse_property(db_connection, sampling_strategy):
    sampling_clause, sampling_description = sampling_strategy

    query = f"""
    SELECT
        collection,
        item_id
    FROM dem.stac_static_item {sampling_clause}
    WHERE collection LIKE '%strips%'
        AND content->'properties'->'pgc:rmse' = to_jsonb(-9999)
    LIMIT 10
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, \
            f"Found invalid rmse values using {sampling_description} (LIMIT 10): {rows}"


def test_mat_view_in_sync_with_strip_dem_release(db_connection):
    query = """
    SELECT
        sd_release.project,
        sd_release.dem_id,
        stac.collection,
        stac.item_id
    FROM dem.strip_dem_release AS sd_release
    FULL OUTER JOIN dem.stac_static_item AS stac
        ON sd_release.dem_id = stac.item_id
    WHERE sd_release.license = 'public'
        AND (stac.item_id IS NULL OR sd_release.dem_id IS NULL)
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, \
            f"Materialized view may be out of sync with dem.strip_dem_release (LIMIT 10): {rows}"


def test_mat_view_in_sync_with_mosaic_dem_release(db_connection):
    query = """
    SELECT
        md_release.project,
        md_release.dem_id,
        stac.collection,
        stac.item_id
    FROM dem.mosaic_dem_release AS md_release
    FULL OUTER JOIN dem.stac_static_item AS stac
        ON (
            md_release.dem_id = stac.item_id
            AND md_release.project = split_part(stac.collection, '-', 1)
        )
    WHERE md_release.license = 'public'
        AND (md_release.dem_id IS NULL OR stac.item_id IS NULL)
    """

    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 0, \
            f"Materialized view may be out of sync with dem.mosaic_dem_release (LIMIT 10): {rows}"


def test_asset_keys(db_connection, sampling_strategy):
    sampling_clause, sampling_description = sampling_strategy

    query = f"""
    WITH actual AS (
        SELECT
            collection,
            item_id,
            array_agg(key) AS asset_keys
        FROM dem.stac_static_item {sampling_clause},
            jsonb_object_keys(content->'assets') AS key
        WHERE collection = %s
        GROUP BY collection, item_id
    ),

    expected AS (
        SELECT %s::TEXT[] AS asset_keys
    ),

    compare AS (
        SELECT
            collection,
            item_id,
            array_to_json(actual.asset_keys)::jsonb - expected.asset_keys AS extra_keys,
            array_to_json(expected.asset_keys)::jsonb - actual.asset_keys AS missing_keys
        FROM actual, expected
    )

    SELECT *
    FROM compare
    WHERE extra_keys != '[]'::jsonb
        OR missing_keys != '[]'::jsonb
    LIMIT 10
    """

    strip_assets = ["hillshade", "hillshade_masked", "dem", "mask", "matchtag",
                    "metadata", "readme"]

    arcticdem_mosaics_v4_1_assets = ["hillshade", "dem", "count", "mad", "maxdate",
                                     "mindate", "datamask", "metadata"]

    rema_mosaics_v2_0_assets = ["hillshade", "dem", "count", "mad", "maxdate",
                                "mindate", "metadata"]

    expected = {
        "arcticdem-strips-s2s041-2m": strip_assets,
        "earthdem-strips-s2s041-2m": strip_assets,
        "rema-strips-s2s041-2m": strip_assets,
        "arcticdem-mosaics-v4.1-2m": arcticdem_mosaics_v4_1_assets,
        "arcticdem-mosaics-v4.1-10m": arcticdem_mosaics_v4_1_assets,
        "arcticdem-mosaics-v4.1-32m": arcticdem_mosaics_v4_1_assets,
        "rema-mosaics-v2.0-2m": rema_mosaics_v2_0_assets,
        "rema-mosaics-v2.0-10m": rema_mosaics_v2_0_assets,
        "rema-mosaics-v2.0-32m": rema_mosaics_v2_0_assets,
        "arcticdem-mosaics-v3.0-2m": ["browse", "dem", "metadata"],
        "arcticdem-mosaics-v3.0-10m": ["dem", "metadata"],
        "arcticdem-mosaics-v3.0-32m": ["dem", "metadata"],
    }

    with db_connection.cursor() as cur:
        for collection, expected_asset_keys in expected.items():
            cur.execute(query, (collection, expected_asset_keys))
            rows = cur.fetchall()
            assert len(rows) == 0, \
                f"Found strips with missing or extra keys using {sampling_description} (LIMIT 10): {rows}"
