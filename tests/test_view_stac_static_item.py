import os
import pytest
import psycopg2

# All the tests in this module run against data in Sandwich. Set the environment
# variable defined below to a valid PostgeSQL DSN.
# If the variable is not set, the tests will be skipped.
# If an invalid PostgreSQL DSN is provided, the tests will fast-fail with a connection
# error.
DSN_ENV_VARIABLE = "SANDWICH_DSN"


def get_dsn() -> str | None:
    return os.environ.get(DSN_ENV_VARIABLE)


@pytest.fixture(scope='module')
def db_connection():
    dsn = get_dsn()
    if not dsn:
        pytest.skip(f"No DSN provided. Set {DSN_ENV_VARIABLE} environment variable")

    conn = None
    try:
        conn = psycopg2.connect(dsn)
        yield conn
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")
    finally:
        if conn is not None:
            conn.close()


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
        -- TODO: Set dem.strip_dem_release.release_date for 2025 strip dem release, then remove the following clause
        AND key != 'published' 
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
