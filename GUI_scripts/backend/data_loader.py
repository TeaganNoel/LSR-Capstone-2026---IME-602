"""
Data loading and normalization helpers.

Takes raw DataFrames from database.py and:
- Ensures datetime columns are parsed
- Normalizes column names where helpful
"""

import pandas as pd


def _ensure_datetime(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name])
    return df


def normalize_mychron(df: pd.DataFrame) -> pd.DataFrame:
    """
    mychron3_data:
        time_attr, sampleRate, lapbutton, speed, temp1, temp2, tach,
        configuration, deviceID, testID
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df = _ensure_datetime(df, "time_attr")
    df = df.sort_values("time_attr")
    return df


def normalize_dataq_mychron(df: pd.DataFrame) -> pd.DataFrame:
    """
    dataq_mychron3_data:
        time_attr, sampleRate, segment, dyn_ax, dyn_ay, dyn_az,
        my_eq, my_ef, my_eg, dc_tank_p, dc_oil_p, dc_oil_t,
        dc_lambda, dc_brake_p, deviceID, testID
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df = _ensure_datetime(df, "time_attr")
    df = df.sort_values("time_attr")
    return df


def normalize_dataq(df: pd.DataFrame) -> pd.DataFrame:
    """
    dataq_data:
        time_attr, sampleRate, lapbutton, temp1, temp2, temp3, temp4,
        potlapseal, analogchannel, dc_refvolts, dc_refcurrent, testID
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df = _ensure_datetime(df, "time_attr")
    df = df.sort_values("time_attr")
    return df


def normalize_kestrel(df: pd.DataFrame) -> pd.DataFrame:
    """
    kestrel_data:
        datetime_attr, sampleRate, ..., temperature, ..., testID
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df = _ensure_datetime(df, "datetime_attr")
    df = df.sort_values("datetime_attr")
    return df
