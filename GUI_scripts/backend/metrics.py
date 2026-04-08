"""
Calculated metrics for LSR analysis.

Uses:
- mychron3_data for speed (mph) and time
- dataq_mychron3_data for accelerations, pressures, etc.
- kestrel_data for environment (optional)

Assumes:
- time_attr is already aligned across tables (within reason).
"""

import numpy as np
import pandas as pd

from .units import mph_to_mps


def compute_speed_series(df_mychron: pd.DataFrame) -> pd.Series:
    """
    Return speed in m/s as a pandas Series indexed by time_attr.
    """
    if df_mychron.empty or "speed" not in df_mychron.columns:
        return pd.Series(dtype=float)

    s = df_mychron.copy()
    s = s.set_index("time_attr")["speed"].astype(float)
    return mph_to_mps(s)


def compute_acceleration_from_speed(speed_mps: pd.Series) -> pd.Series:
    """
    Compute longitudinal acceleration from speed via finite difference.
    """
    if speed_mps.empty:
        return pd.Series(dtype=float)

    # assume uniform-ish sampling; use time delta in seconds
    dt = speed_mps.index.to_series().diff().dt.total_seconds()
    dv = speed_mps.diff()
    a = dv / dt
    return a


def compute_distance_from_speed(speed_mps: pd.Series) -> pd.Series:
    """
    Integrate speed over time to get distance (m).
    """
    if speed_mps.empty:
        return pd.Series(dtype=float)

    dt = speed_mps.index.to_series().diff().dt.total_seconds().fillna(0.0)
    distance = (speed_mps * dt).cumsum()
    return distance


def compute_basic_run_metrics(df_mychron: pd.DataFrame) -> dict:
    """
    Compute basic run-level metrics from MyChron data:
    - v_max (mph)
    - time_to_vmax (s)
    - distance_to_vmax (m)
    """
    if df_mychron.empty or "speed" not in df_mychron.columns:
        return {}

    df = df_mychron.copy()
    df = df.sort_values("time_attr")

    speed_mps = mph_to_mps(df["speed"].astype(float))
    time_index = pd.to_datetime(df["time_attr"])

    speed_mps.index = time_index
    distance_m = compute_distance_from_speed(speed_mps)

    vmax_mps = speed_mps.max()
    vmax_idx = speed_mps.idxmax()

    t0 = time_index.min()
    time_to_vmax = (vmax_idx - t0).total_seconds()

    distance_to_vmax = distance_m.loc[vmax_idx]

    return {
        "v_max_mps": float(vmax_mps),
        "v_max_mph": float(vmax_mps / 0.44704),
        "time_to_vmax_s": float(time_to_vmax),
        "distance_to_vmax_m": float(distance_to_vmax),
    }


def compute_all_metrics(context: dict) -> dict:
    """
    High-level entry point.

    context:
        {
            "dfs": {
                "mychron": df_mychron,
                "dataq_mychron": df_dataq_mychron,
                "dataq": df_dataq,
                "kestrel": df_kestrel
            },
            "t0": earliest_timestamp,
            "sanity": {...}
        }
    """
    dfs = context["dfs"]
    mychron = dfs["mychron"]

    basic = compute_basic_run_metrics(mychron)

    # placeholder for future: drag, power, env corrections, etc.
    metrics = {
        **basic,
        "sanity_warnings": context["sanity"]["warnings"],
    }

    return metrics
