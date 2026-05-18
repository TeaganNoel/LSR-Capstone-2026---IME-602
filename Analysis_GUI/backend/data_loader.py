"""
Data loading and normalization helpers.

This module prepares raw DataFrames from database.py for:
- strict-mode validation
- metrics computation
- plotting

Normalization goals:
- Ensure numeric columns are numeric
- Ensure time columns are sorted
- Ensure Kestrel timestamps are parsed
"""

import pandas as pd


def normalize_dataq_mychron(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataQ/MyChron telemetry.

    Expected columns:
        t_s (FLOAT)
        my_mph
        samplingrate
        segment
        my_rpm
        my_h2o_f
        my_egt_f
        dq_cht_f
        dq_air_f
        dq_tank_f
        dq_pitot_kpa
        testID
    """
    if df is None or df.empty:
        return df.copy()

    df = df.copy()

    # --- Normalize timestamps ---
    if "t_s" in df.columns:
        df["t_s"] = pd.to_numeric(df["t_s"], errors="coerce")
        df = df.sort_values("t_s")

    # --- Normalize speed ---
    if "my_mph" in df.columns:
        df["my_mph"] = pd.to_numeric(df["my_mph"], errors="coerce")

    # --- Normalize RPM ---
    if "my_rpm" in df.columns:
        df["my_rpm"] = pd.to_numeric(df["my_rpm"], errors="coerce")

    # ------------------------------------------------------------
    # Normalize all temperature channels (explicit, fixed list)
    # ------------------------------------------------------------
    temp_cols = [
        "my_h2o_f",
        "my_egt_f",
        "dq_cht_f",
        "dq_air_f",
        "dq_tank_f",
    ]

    for col in temp_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Normalize pitot ---
    if "dq_pitot_kpa" in df.columns:
        df["dq_pitot_kpa"] = pd.to_numeric(df["dq_pitot_kpa"], errors="coerce")

    return df

# ------------------------------------------------------------
# Kestrel Normalization
# ------------------------------------------------------------

def normalize_kestrel(df):
    """
    Normalize Kestrel environmental data.
    Converts barometric pressure from inHg → kPa.
    """

    if df is None or df.empty:
        return df

    df = df.copy()

    # Standardize column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Convert pressure units if present
    if "barometricpressure" in df.columns:
        try:
            p_inhg = df["barometricpressure"].astype(float)
            df["barometricpressure"] = p_inhg * 3.38639  # inHg → kPa
        except Exception:
            pass  # validation will catch non-numeric values

    return df
