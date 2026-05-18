"""
Database access layer using mysql.connector.

Enhancements:
- Detects unreachable database host
- Raises clean, explicit exceptions for GUI to catch
- Prevents silent hangs when host machine is offline
"""

import mysql.connector
import pandas as pd
from mysql.connector import errors as mysql_errors


# -----------------------------
# Custom Exceptions
# -----------------------------

class DatabaseConnectionError(Exception):
    """Raised when the database host is unreachable or MySQL is not running."""
    pass


# -----------------------------
# Database Configuration
# -----------------------------

DB_CONFIG = {
    "user": "lsr_remote",
    "password": "LSR2026!",  # consider moving to env var later
    "host": "100.108.209.100",
    "port": 3306,
    "database": "lsr_testing_database",
    "connection_timeout": 5,   # prevents GUI from hanging forever
}


# -----------------------------
# Connection Helper
# -----------------------------

def get_connection():
    """Return a new MySQL connection with strict error handling."""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except (mysql_errors.InterfaceError,
            mysql_errors.OperationalError,
            mysql_errors.DatabaseError) as e:
        raise DatabaseConnectionError(
            "Unable to connect to the LSR database. "
            "The host computer may be offline or MySQL is not running."
        ) from e


# -----------------------------
# Query Helpers
# -----------------------------

def list_tests(limit=100):
    """
    Return a DataFrame of available tests (basic metadata).
    """
    query = """
        SELECT
            testID,
            date_attr AS date,
            starttime AS datetime_attr,
            duration,
            riderID,
            vehicleID,
            notes
        FROM test
        ORDER BY starttime DESC
        LIMIT %s
    """

    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=[limit])
        conn.close()
        return df

    except DatabaseConnectionError:
        raise
    except Exception as e:
        raise DatabaseConnectionError(
            f"Failed to query test list: {e}"
        ) from e


def load_test_metadata(test_id: str) -> pd.DataFrame:
    """
    Load metadata for a single testID from the test table.
    """
    query = """
        SELECT
            testID,
            date_attr AS date,
            starttime AS datetime_attr,
            duration,
            riderID,
            vehicleID,
            notes
        FROM test
        WHERE testID = %s
    """

    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=[test_id])
        conn.close()
        return df

    except DatabaseConnectionError:
        raise
    except Exception as e:
        raise DatabaseConnectionError(
            f"Failed to load metadata for TestID {test_id}: {e}"
        ) from e


def load_dataq_mychron(test_id: str) -> pd.DataFrame:
    """
    Load all required DataQ/MyChron columns, including all temperature channels.
    """
    query = """
        SELECT
            t_s,
            my_mph,
            samplingrate,
            segment,
            my_rpm,
            my_h2o_f,
            my_egt_f,
            dq_cht_f,
            dq_in_air_f,
            dq_tank_f,
            dq_pitot_kpa
        FROM dataq_mychron3_data
        WHERE testID = %s
        ORDER BY t_s
    """

    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=[test_id])
        conn.close()
        return df

    except DatabaseConnectionError:
        raise
    except Exception as e:
        raise DatabaseConnectionError(
            f"Failed to load DataQ/MyChron data for TestID {test_id}: {e}"
        ) from e


def load_kestrel(test_id: str) -> pd.DataFrame:
    """
    Load useful environmental columns from kestrel_data.
    """
    query = """
        SELECT
            datetime_attr,
            temperature,
            windspeed,
            barometricpressure
        FROM kestrel_data
        WHERE testID = %s
        ORDER BY datetime_attr
    """

    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=[test_id])
        conn.close()
        return df

    except DatabaseConnectionError:
        raise
    except Exception as e:
        raise DatabaseConnectionError(
            f"Failed to load Kestrel data for TestID {test_id}: {e}"
        ) from e


def load_all_telemetry(test_id: str) -> dict:
    """
    Unified loader used by the GUI.

    Returns:
        {
            "dataq_mychron": df_dqmc,
            "kestrel": df_kestrel
        }
    """
    try:
        return {
            "dataq_mychron": load_dataq_mychron(test_id),
            "kestrel": load_kestrel(test_id),
        }
    except DatabaseConnectionError:
        raise
