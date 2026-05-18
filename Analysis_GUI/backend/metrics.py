"""
Calculated metrics for LSR analysis.

This module computes:
- Speed (mph, m/s)
- Acceleration (m/s²)
- Distance (m)
- Air density (kg/m³)
- Pitot-based drag force (N)
- Basic run-level metrics

Strict-mode compatible:
- Missing pitot → allowed (returns None)
- Invalid pitot → caught earlier in validation
"""

import pandas as pd
import numpy as np

R_AIR = 287.058  # J/(kg*K)
DEFAULT_CDA = 0.25  # m² (placeholder for drag estimate)


# ------------------------------------------------------------
# Unit Conversion
# ------------------------------------------------------------

def mph_to_mps(mph_series: pd.Series) -> pd.Series:
    """Convert mph to m/s."""
    return mph_series * 0.44704


# ------------------------------------------------------------
# Core Time-Series Computations
# ------------------------------------------------------------

def compute_speed_series(df_dqmc: pd.DataFrame) -> pd.Series:
    """
    Return speed in m/s as a pandas Series indexed by t_s.
    """
    if df_dqmc.empty:
        return pd.Series(dtype=float)

    df = df_dqmc.sort_values("t_s")
    speed_mps = mph_to_mps(df["my_mph"].astype(float))
    speed_mps.index = df["t_s"].astype(float)
    speed_mps.index.name = "t_s"
    return speed_mps


def compute_acceleration_from_speed(speed_mps: pd.Series) -> pd.Series:
    """
    Compute longitudinal acceleration from speed via finite difference.
    """
    if speed_mps.empty:
        return pd.Series(dtype=float)

    t = speed_mps.index.to_series().astype(float)
    dt = t.diff()
    dv = speed_mps.diff()

    accel = dv / dt
    accel.name = "accel_mps2"
    return accel


def compute_distance_from_speed(speed_mps: pd.Series) -> pd.Series:
    """
    Integrate speed over time to get distance (m).
    """
    if speed_mps.empty:
        return pd.Series(dtype=float)

    t = speed_mps.index.to_series().astype(float)
    dt = t.diff().fillna(0.0)

    distance = (speed_mps * dt).cumsum()
    distance.name = "distance_m"
    return distance


# ------------------------------------------------------------
# Environmental Computations
# ------------------------------------------------------------

def compute_air_density(df_kestrel: pd.DataFrame) -> float | None:
    """
    Estimate mean air density from Kestrel data.

    Assumes:
    - temperature in degC
    - barometricpressure in kPa
    """
    if df_kestrel is None or df_kestrel.empty:
        return None

    if "temperature" not in df_kestrel.columns or "barometricpressure" not in df_kestrel.columns:
        return None

    temp_c = df_kestrel["temperature"].astype(float)
    p_kpa = df_kestrel["barometricpressure"].astype(float)

    T_K = temp_c.mean() + 273.15
    p_Pa = p_kpa.mean() * 1000.0

    if T_K <= 0:
        return None

    rho = p_Pa / (R_AIR * T_K)
    return float(rho)


# ------------------------------------------------------------
# Pitot-Based Drag Estimation
# ------------------------------------------------------------

def compute_pitot_dynamic_pressure(df_dqmc: pd.DataFrame) -> pd.Series | None:
    """
    Convert pitot pressure from kPa → Pa.
    Returns None if pitot missing (allowed in strict mode).
    """
    if "dq_pitot_kpa" not in df_dqmc.columns:
        return None

    df = df_dqmc.sort_values("t_s")
    q_pa = df["dq_pitot_kpa"].astype(float) * 1000.0
    q_pa.index = df["t_s"].astype(float)
    q_pa.index.name = "t_s"
    q_pa.name = "q_pa"
    return q_pa


def estimate_drag_force_from_pitot(df_dqmc: pd.DataFrame,
                                   df_kestrel: pd.DataFrame) -> pd.Series | None:
    """
    Estimate drag force using pitot dynamic pressure and assumed CdA.

    F_drag = q * CdA

    Returns None if pitot missing (allowed).
    """
    q_pa = compute_pitot_dynamic_pressure(df_dqmc)
    if q_pa is None:
        return None

    f_drag = q_pa * DEFAULT_CDA
    f_drag.name = "drag_force_N"
    return f_drag


# ------------------------------------------------------------
# Run-Level Metrics
# ------------------------------------------------------------

def compute_basic_run_metrics(df_dqmc: pd.DataFrame) -> dict:
    """
    Compute basic run-level metrics:
    - v_max (mph, m/s)
    - time_to_vmax (s)
    - distance_to_vmax (m)
    """
    if df_dqmc.empty:
        return {}

    df = df_dqmc.sort_values("t_s")
    speed_mps = mph_to_mps(df["my_mph"].astype(float))
    t_s = df["t_s"].astype(float)

    speed_mps.index = t_s
    distance_m = compute_distance_from_speed(speed_mps)

    vmax_mps = float(speed_mps.max())
    vmax_idx = float(speed_mps.idxmax())

    t0 = float(t_s.min())
    time_to_vmax = vmax_idx - t0
    distance_to_vmax = float(distance_m.loc[vmax_idx])

    return {
        "v_max_mps": vmax_mps,
        "v_max_mph": vmax_mps / 0.44704,
        "time_to_vmax_s": time_to_vmax,
        "distance_to_vmax_m": distance_to_vmax,
    }


# ------------------------------------------------------------
# High-Level Entry Point
# ------------------------------------------------------------

def compute_all_metrics(context: dict) -> dict:
    """
    High-level entry point.

    context:
        {
            "dfs": {
                "dataq_mychron": df_dqmc,
                "kestrel": df_kestrel
            },
            "t0": earliest_timestamp,
            "sanity": { "warnings": [...] }
        }
    """
    dfs = context["dfs"]
    dqmc = dfs["dataq_mychron"]
    kestrel = dfs["kestrel"]

    basic = compute_basic_run_metrics(dqmc)
    rho = compute_air_density(kestrel)

    metrics = {
        **basic,
        "air_density_kg_m3": rho,
        "sanity_warnings": context["sanity"]["warnings"],
    }

    return metrics


# ------------------------------------------------------------
# Multi-Run Metrics
# ------------------------------------------------------------

def compute_basic_run_metrics_for_many(dfs_by_id: dict) -> dict:
    """
    Compute basic metrics for multiple runs.

    dfs_by_id: {test_id: {"dataq_mychron": df_dqmc, "kestrel": df_kestrel}, ...}
    Returns: {test_id: metrics_dict}
    """
    out = {}
    for tid, dfs in dfs_by_id.items():
        out[tid] = compute_basic_run_metrics(dfs["dataq_mychron"])
    return out
