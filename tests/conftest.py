import os
import pytest
import psycopg2

# Some tests in this suite run against data in Sandwich. Set the environment variable
# defined below to a valid PostgeSQL DSN.
# If the variable is not set, the tests that require a db connection will be skipped.
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

