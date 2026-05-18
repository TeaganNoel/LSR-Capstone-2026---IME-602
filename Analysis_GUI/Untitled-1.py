"""
Unified Run Viewer + Performance Metrics for LSR
"""

import argparse
import getpass  
from datetime import timedelta

import mysql.connector
import numpy as np
import pandas as pd
import plotly.graph_objects as go

# ----------------------------------------------------------------------
# DB CONNECTION
# ----------------------------------------------------------------------

server_connect = mysql.connector.connect(
    user='root',
    password='Bikegofast!777',
    host='localhost',
    database='lsr_testing_database'
)

# ----------------------------------------------------------------------
# USER CONFIGURATION (IMPERIAL DISPLAY, SI INTERNAL)
# ----------------------------------------------------------------------

# Default rider and bike weights (lb) if DB metadata missing
DEFAULT_RIDER_WEIGHT_LB = 150.0
DEFAULT_BIKE_WEIGHT_LB = 110.0

# Convert lb → kg for internal physics
def lb_to_kg(lb: float) -> float:
    return lb * 0.453592

# Wheel circumference (inches) → meters (for future use)
WHEEL_CIRCUMFERENCE_IN = 70.0
WHEEL_CIRCUMFERENCE_M = WHEEL_CIRCUMFERENCE_IN * 0.0254

# AFR and EGT lag settings
AFR_LAG_SECONDS = 0.5
EGT_LAG_TAU = 0.3

# Resample frequency (Hz)
RESAMPLE_HZ = 50

# ----------------------------------------------------------------------
# UNIT CONVERSION HELPERS (IMPERIAL DISPLAY ONLY)
# ----------------------------------------------------------------------

def convert_speed_ms_to_mph(v_ms: float) -> float:
    return v_ms * 2.23694  # m/s → mph

def convert_accel_ms2_to_fts2(a_ms2: float) -> float:
    return a_ms2 * 3.28084  # m/s² → ft/s²

# ----------------------------------------------------------------------
# DATA LOADERS (USING PANDAS + mysql.connector)
# ----------------------------------------------------------------------

def load_aim(test_id: int) -> pd.DataFrame:
    query = """
        SELECT time_attr, sampleRate, lapbutton, speed, temp1, temp2,
               tach, configureattr, deviceID, testID
        FROM mychron3_data
        WHERE testID = %s
        ORDER BY time_attr ASC
    """
    df = pd.read_sql(query, server_connect, params=[test_id])
    df = df.rename(columns={"time_attr": "time"})
    df["time"] = pd.to_datetime(df["time"])
    return df.set_index("time").sort_index()

def load_dataq(test_id: int) -> pd.DataFrame:
    query = """
        SELECT time_attr, sampleRate, lapbutton, temp1, temp2, temp3,
               ketprod, potaspeed, analogvalue, effectbrakesensor,
               wheelattr, testID
        FROM dataq_data
        WHERE testID = %s
        ORDER BY time_attr ASC
    """
    df = pd.read_sql(query, server_connect, params=[test_id])
    df = df.rename(columns={"time_attr": "time"})
    df["time"] = pd.to_datetime(df["time"])
    return df.set_index("time").sort_index()

def load_kestrel(test_id: int) -> pd.DataFrame:
    query = """
        SELECT datetime_attr, windspeed, relativehumidity,
               heatindex, dewpointtemp, stationpressure,
               barometricpressure, altitude, densityaltitude,
               crosswind, headwind, compassmagdirection,
               datetype, starttime, locationdescription,
               locationcoordinates, notes, testID
        FROM kestrel_data
        WHERE testID = %s
        ORDER BY datetime_attr ASC
    """
    df = pd.read_sql(query, server_connect, params=[test_id])
    df = df.rename(columns={"datetime_attr": "time"})
    df["time"] = pd.to_datetime(df["time"])
    return df.set_index("time").sort_index()

def load_metadata(test_id: int) -> dict:
    query = """
        SELECT
            t.testID,
            t.date_attr,
            t.datetime,
            t.duration,
            t.notes,
            l.name_attr AS location_name,
            l.latitude,
            l.longitude,
            r.name_attr AS rider_name,
            r.weight AS rider_weight_lb,
            v.name_attr AS vehicle_name,
            v.condition_attr AS vehicle_condition,
            v.gasratio,
            v.orialeo
        FROM test t
        LEFT JOIN location l ON t.locID = l.locID
        LEFT JOIN rider r   ON t.riderID = r.riderID
        LEFT JOIN vehicle v ON t.vehicleID = v.vehicleID
        WHERE t.testID = %s
    """
    df = pd.read_sql(query, server_connect, params=[test_id])
    return df.iloc[0].to_dict() if not df.empty else {}

# ----------------------------------------------------------------------
# SYNC & CLEANING
# ----------------------------------------------------------------------

def find_first_edge(series: pd.Series, threshold: float = 0.5):
    if series is None or series.empty:
        return None
    s = series.astype(float).fillna(0)
    edges = s.diff()
    hits = edges[edges > threshold]
    return hits.index[0] if not hits.empty else None

def align_by_lapbutton(df_aim: pd.DataFrame, df_dq: pd.DataFrame):
    t_aim = find_first_edge(df_aim.get("lapbutton"))
    t_dq = find_first_edge(df_dq.get("lapbutton"))

    if t_aim is None or t_dq is None:
        return df_aim, df_dq

    offset = t_aim - t_dq
    df_dq = df_dq.copy()
    df_dq.index = df_dq.index + offset
    return df_aim, df_dq

def apply_afr_lag(df: pd.DataFrame, col: str = "exhaustsensor", lag: float = AFR_LAG_SECONDS):
    if col not in df.columns:
        return df
    df = df.copy()
    dt = df.index.to_series().diff().dt.total_seconds().median()
    if dt and dt > 0:
        shift = int(round(lag / dt))
        df[col] = df[col].shift(-shift)
    return df

def apply_egt_lag(df: pd.DataFrame, col: str = "temp2", tau: float = EGT_LAG_TAU):
    if col not in df.columns:
        return df
    df = df.copy()
    dt = df.index.to_series().diff().dt.total_seconds().median()
    if dt and dt > 0:
        dTdt = df[col].diff() / dt
        df[col + "_corr"] = df[col] + tau * dTdt
    return df

def resample_merge(df_aim: pd.DataFrame,
                   df_dq: pd.DataFrame,
                   df_kes: pd.DataFrame,
                   hz: int = RESAMPLE_HZ) -> pd.DataFrame:
    t_min = min(df_aim.index.min(), df_dq.index.min())
    t_max = max(df_aim.index.max(), df_dq.index.max())
    if not df_kes.empty:
        t_min = min(t_min, df_kes.index.min())
        t_max = max(t_max, df_kes.index.max())

    dt_ms = int(1000 / hz)
    idx = pd.date_range(t_min, t_max, freq=f"{dt_ms}L")

    def rs(df: pd.DataFrame):
        if df.empty:
            return pd.DataFrame(index=idx)
        df = df.reindex(df.index.union(idx)).sort_index()
        df = df.interpolate("time").reindex(idx)
        return df

    aim = rs(df_aim).add_prefix("aim_")
    dq = rs(df_dq).add_prefix("dq_")
    kes = rs(df_kes).add_prefix("kes_")

    return pd.concat([aim, dq, kes], axis=1)

# ----------------------------------------------------------------------
# DERIVED CHANNELS (SI INTERNAL → IMPERIAL DISPLAY)
# ----------------------------------------------------------------------

def derive_channels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Speed (AiM speed in km/h → m/s)
    if "aim_speed" in df.columns:
        df["wheel_speed_ms"] = df["aim_speed"] * (1000.0 / 3600.0)

    # Acceleration from speed (m/s²)
    if "wheel_speed_ms" in df.columns:
        dt = df.index.to_series().diff().dt.total_seconds().median()
        if dt and dt > 0:
            df["accel_ms2"] = df["wheel_speed_ms"].diff() / dt

    # Gear estimation from RPM / speed ratio
    if "aim_tach" in df.columns and "wheel_speed_ms" in df.columns:
        ratio = df["aim_tach"] / df["wheel_speed_ms"].replace(0, np.nan)
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        if len(ratio) > 10:
            bins = np.quantile(ratio, [0, 0.2, 0.4, 0.6, 0.8, 1.0])
            df["gear"] = pd.cut(
                df["aim_tach"] / df["wheel_speed_ms"].replace(0, np.nan),
                bins=bins,
                labels=False
            ) + 1

    # Slip from potaspeed vs wheel speed
    if "dq_potaspeed" in df.columns and "wheel_speed_ms" in df.columns:
        v_w = df["wheel_speed_ms"]
        v_p = df["dq_potaspeed"].replace(0, np.nan)
        df["slip"] = (v_w - v_p) / v_p

    # Air density from Kestrel (stationpressure kPa, heatindex °C)
    if "kes_stationpressure" in df.columns and "kes_heatindex" in df.columns:
        p = df["kes_stationpressure"] * 100.0  # kPa → Pa
        T = df["kes_heatindex"] + 273.15       # °C → K
        df["air_density"] = p / (287.05 * T)

    # Dynamic pressure q = 0.5 * rho * v^2 (v from potaspeed in m/s)
    if "dq_potaspeed" in df.columns and "air_density" in df.columns:
        v = df["dq_potaspeed"]
        rho = df["air_density"]
        df["q_dynamic"] = 0.5 * rho * v**2

    # Imperial display channels
    if "wheel_speed_ms" in df.columns:
        df["speed_mph"] = df["wheel_speed_ms"].apply(convert_speed_ms_to_mph)
    if "accel_ms2" in df.columns:
        df["accel_fts2"] = df["accel_ms2"].apply(convert_accel_ms2_to_fts2)

    return df

# ----------------------------------------------------------------------
# PERFORMANCE METRICS (IMPERIAL DISPLAY)
# ----------------------------------------------------------------------

def compute_performance_metrics(df: pd.DataFrame, meta: dict) -> dict:
    results: dict = {}

    if "wheel_speed_ms" not in df.columns:
        return {"error": "wheel_speed_ms not available"}

    df = df.copy()
    df["speed_mph"] = df["wheel_speed_ms"].apply(convert_speed_ms_to_mph)
    if "accel_ms2" in df.columns:
        df["accel_fts2"] = df["accel_ms2"].apply(convert_accel_ms2_to_fts2)

    # Time-to-speed milestones (mph)
    def time_to_speed(target_speed_mph: float):
        hits = df[df["speed_mph"] >= target_speed_mph]
        if hits.empty:
            return None
        t0 = df.index[0]
        return (hits.index[0] - t0).total_seconds()

    results["t_0_20_mph"] = time_to_speed(20.0)
    results["t_20_40_mph"] = time_to_speed(40.0)
    results["t_40_60_mph"] = time_to_speed(60.0)

    # Peak acceleration
    if "accel_ms2" in df.columns:
        idx_peak = df["accel_ms2"].idxmax()
        results["peak_accel_fts2"] = df.loc[idx_peak, "accel_fts2"]
        results["peak_accel_speed_mph"] = df.loc[idx_peak, "speed_mph"]
    else:
        results["peak_accel_fts2"] = None
        results["peak_accel_speed_mph"] = None

    # Mean acceleration in speed bands (mph)
    def accel_in_band(vmin: float, vmax: float):
        if "accel_fts2" not in df.columns:
            return None
        mask = (df["speed_mph"] >= vmin) & (df["speed_mph"] < vmax)
        if mask.sum() == 0:
            return None
        return df.loc[mask, "accel_fts2"].mean()

    results["accel_0_20_fts2"] = accel_in_band(0.0, 20.0)
    results["accel_20_40_fts2"] = accel_in_band(20.0, 40.0)
    results["accel_40_60_fts2"] = accel_in_band(40.0, 60.0)

    # Power estimation (rear-wheel, SI internally)
    try:
        rider_weight_lb = meta.get("rider_weight_lb", DEFAULT_RIDER_WEIGHT_LB)
        rider_mass_kg = lb_to_kg(float(rider_weight_lb))
        bike_mass_kg = lb_to_kg(DEFAULT_BIKE_WEIGHT_LB)
        total_mass_kg = rider_mass_kg + bike_mass_kg

        if "accel_ms2" in df.columns:
            df["power_watts"] = total_mass_kg * df["accel_ms2"] * df["wheel_speed_ms"]
            idx_p = df["power_watts"].idxmax()
            results["peak_power_watts"] = df.loc[idx_p, "power_watts"]
            results["peak_power_speed_mph"] = df.loc[idx_p, "speed_mph"]
        else:
            results["peak_power_watts"] = None
            results["peak_power_speed_mph"] = None
    except Exception:
        results["peak_power_watts"] = None
        results["peak_power_speed_mph"] = None

    # Terminal-speed behavior (last 2 seconds)
    try:
        t_end = df.index[-1]
        t_start = t_end - timedelta(seconds=2)
        tail = df[df.index >= t_start]

        results["terminal_speed_mph"] = tail["speed_mph"].mean()
        if "accel_fts2" in df.columns:
            results["terminal_accel_fts2"] = tail["accel_fts2"].mean()
        else:
            results["terminal_accel_fts2"] = None
    except Exception:
        results["terminal_speed_mph"] = None
        results["terminal_accel_fts2"] = None

    return results

# ----------------------------------------------------------------------
# PIPELINE
# ----------------------------------------------------------------------

def build_unified_run(test_id: int) -> tuple[pd.DataFrame, dict]:
    aim = load_aim(test_id)
    dq = load_dataq(test_id)
    kes = load_kestrel(test_id)
    meta = load_metadata(test_id)

    aim, dq = align_by_lapbutton(aim, dq)
    dq = apply_afr_lag(dq)          # AFR lag if exhaustsensor present
    aim = apply_egt_lag(aim)        # EGT lag on temp2

    df = resample_merge(aim, dq, kes)
    df = derive_channels(df)

    return df, meta

# ----------------------------------------------------------------------
# VIEWER
# ----------------------------------------------------------------------

def plot_unified_run(df: pd.DataFrame, meta: dict | None = None):
    fig = go.Figure()
    t = df.index

    if "speed_mph" in df.columns:
        fig.add_trace(go.Scatter(
            x=t, y=df["speed_mph"],
            name="Speed (mph)"
        ))

    if "aim_tach" in df.columns:
        fig.add_trace(go.Scatter(
            x=t, y=df["aim_tach"],
            name="RPM",
            yaxis="y2"
        ))

    if "dq_analogvalue" in df.columns:
        fig.add_trace(go.Scatter(
            x=t, y=df["dq_analogvalue"],
            name="Analog (DataQ)"
        ))

    if "gear" in df.columns:
        fig.add_trace(go.Scatter(
            x=t, y=df["gear"],
            name="Gear"
        ))

    if "accel_fts2" in df.columns:
        fig.add_trace(go.Scatter(
            x=t, y=df["accel_fts2"],
            name="Accel (ft/s²)"
        ))

    title_id = meta.get("testID", "") if meta else ""
    fig.update_layout(
        title=f"Unified Run Viewer — Test {title_id}",
        xaxis_title="Time",
        yaxis_title="Speed / Accel / Misc",
        yaxis2=dict(
            title="RPM",
            overlaying="y",
            side="right"
        ),
        height=800
    )

    fig.show()

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Unified LSR run analysis + performance metrics (Imperial, mysql.connector)"
    )
    parser.add_argument("test_id", type=int, help="testID from the MySQL database")
    args = parser.parse_args()

    df, meta = build_unified_run(args.test_id)
    print("Unified run loaded:", df.shape)

    metrics = compute_performance_metrics(df, meta)
    print("\nPerformance Metrics (mph, ft/s², watts):")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    plot_unified_run(df, meta)
