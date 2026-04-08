"""
Database access layer using mysql.connector.

Responsibilities:
- Create and manage DB connection
- List available tests
- Load test metadata
- Load all telemetry tables for a given testID
"""

import mysql.connector
import pandas as pd

# -------------------------------------------------------------------
# CONFIG – update these for your environment
# -------------------------------------------------------------------

DB_CONFIG = {
    "user": "lsr_remote",
    "password": "LSR2026!",  # consider moving to env var later
    "host": "100.108.209.100",
    "port": 3306,
    "database": "lsr_testing_database",
}


# -------------------------------------------------------------------
# CONNECTION HELPERS
# -------------------------------------------------------------------

def get_connection():
    """Return a new MySQL connection."""
    return mysql.connector.connect(**DB_CONFIG)


# -------------------------------------------------------------------
# TEST METADATA
# -------------------------------------------------------------------

def list_tests(limit=100):
    """
    Return a DataFrame of available tests (basic metadata).
    """
    query = """
        SELECT
            testID,
            date_attr AS date,
            datetime AS datetime_attr,
            duration,
            riderID,
            vehicleID,
            bikekerID,
            notes
        FROM test
        ORDER BY datetime_attr DESC
        LIMIT %s
    """
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=[limit])
    finally:
        conn.close()
    return df


def load_test_metadata(test_id: int) -> pd.DataFrame:
    """
    Load metadata for a single testID from the test table.
    """
    query = """
        SELECT
            testID,
            date_attr AS date,
            datetime AS datetime_attr,
            duration,
            riderID,
            vehicleID,
            bikekerID,
            notes
        FROM test
        WHERE testID = %s
    """
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=[test_id])
    finally:
        conn.close()
    return df


# -------------------------------------------------------------------
# TELEMETRY LOADING – ALL TABLES FOR A GIVEN testID
# -------------------------------------------------------------------

_TELEMETRY_TABLES = {
    "mychron": "mychron3_data",
    "dataq_mychron": "dataq_mychron3_data",
    "dataq": "dataq_data",
    "kestrel": "kestrel_data",
}


def _load_table_if_exists(conn, table_name: str, test_id: int) -> pd.DataFrame:
    """
    Try to load a telemetry table for a given testID.
    Returns empty DataFrame if no rows exist.
    """
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE testID = %s
        ORDER BY 1
    """
    df = pd.read_sql(query, conn, params=[test_id])
    return df


def load_all_telemetry(test_id: int) -> dict:
    """
    Load all telemetry tables for a given testID.

    Returns:
        {
            "mychron": df_mychron,
            "dataq_mychron": df_dataq_mychron,
            "dataq": df_dataq,
            "kestrel": df_kestrel
        }
    DataFrames may be empty if no rows exist for that testID.
    """
    conn = get_connection()
    try:
        result = {}
        for key, table in _TELEMETRY_TABLES.items():
            df = _load_table_if_exists(conn, table, test_id)
            result[key] = df
        return result
    finally:
        conn.close()