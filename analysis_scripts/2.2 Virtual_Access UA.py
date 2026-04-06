"""
LSR Unified Run Viewer — Version 2 (Remote Access)
Connects to Mason's MySQL server over the network.
Requires remote access to be enabled on Mason's machine.
"""

import argparse
import mysql.connector
import pandas as pd
import numpy as np
from datetime import timedelta
import plotly.graph_objects as go

# ------------------------------------------------------------
# REMOTE CONNECTION (YOU MUST INSERT MASON'S IP)
# ------------------------------------------------------------

server_connect = mysql.connector.connect(
    user='lsr_remote',
    password='LSR2026!',
    host='REPLACE_WITH_MASONS_IP',   # Example: "10.249.92.110"
    port=3306,
    database='lsr_testing_database'
)

# ------------------------------------------------------------
# CONSTANTS & UNIT CONVERSIONS
# ------------------------------------------------------------

DEFAULT_RIDER_WEIGHT_LB = 150.0
DEFAULT_BIKE_WEIGHT_LB = 110.0

def lb_to_kg(lb):
    return lb * 0.453592

def convert_speed_ms_to_mph(v_ms):
    return v_ms * 2.23694

def convert_accel_ms2_to_fts2(a_ms2):
    return a_ms2 * 3.28084

# ------------------------------------------------------------
# DATA LOADERS
# ------------------------------------------------------------

def load_aim(test_id):
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

def load_dataq(test_id):
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

def load_kestrel(test_id):
    query = """
        SELECT datetime_attr, windspeed, relativehumidity,
               heatindex, stationpressure, densityaltitude,
               crosswind, headwind, compassmagdirection, testID
        FROM kestrel_data
        WHERE testID = %s
        ORDER BY datetime_attr ASC
    """
    df = pd.read_sql(query, server_connect, params=[test_id])
    df = df.rename(columns={"datetime_attr": "time"})
    df["time"] = pd.to_datetime(df["time"])
    return df.set_index("time").sort_index()

def load_metadata(test_id):
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

# ------------------------------------------------------------
# ALIGNMENT & MERGING
# ------------------------------------------------------------

def find_first_edge(series, threshold=0.5):
    if series is None or series.empty:
        return None
    s = series.astype(float).fillna(0)
    edges = s.diff()
    hits = edges[edges > threshold]
    return hits.index[0] if not hits.empty else None

def align_by_lapbutton(df_aim, df_dq):
    t_aim = find_first_edge(df_aim.get("lapbutton"))
    t_dq = find_first_edge(df_dq.get("lapbutton"))
    if t_aim is None or t_dq is None:
        return df_aim, df_dq
    offset = t_aim - t_dq
    df_dq = df_dq.copy()
    df_dq.index = df_dq.index + offset
    return df_aim, df_dq

def resample_merge(df_aim, df_dq, df_kes, hz=50):
    t_min = min(df_aim.index.min(), df_dq.index.min())
    t_max = max(df_aim.index.max(), df_dq.index.max())
    if not df_kes.empty:
        t_min = min(t_min, df_kes.index.min())
        t_max = max(t_max, df_kes.index.max())
    idx = pd.date_range(t_min, t_max, freq=f"{int(1000/hz)}L")

    def rs(df):
        if df.empty:
            return pd.DataFrame(index=idx)
        df = df.reindex(df.index.union(idx)).sort_index()
        df = df.interpolate("time").reindex(idx)
        return df

    aim = rs(df_aim).add_prefix("aim_")
    dq = rs(df_dq).add_prefix("dq_")
    kes = rs(df_kes).add_prefix("kes_")
    return pd.concat([aim, dq, kes], axis=1)

# ------------------------------------------------------------
# DERIVED CHANNELS
# ------------------------------------------------------------

def derive_channels(df):
    df = df.copy()

    if "aim_speed" in df.columns:
        df["wheel_speed_ms"] = df["aim_speed"] * (1000/3600)

    if "wheel_speed_ms" in df.columns:
        dt = df.index.to_series().diff().dt.total_seconds().median()
        if dt and dt > 0:
            df["accel_ms2"] = df["wheel_speed_ms"].diff() / dt

    if "aim_tach" in df.columns and "wheel_speed_ms" in df.columns:
        ratio = df["aim_tach"] / df["wheel_speed_ms"].replace(0, np.nan)
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        if len(ratio) > 10:
            bins = np.quantile(ratio, [0, .2, .4, .6, .8, 1])
            df["gear"] = pd.cut(
                df["aim_tach"] / df["wheel_speed_ms"].replace(0, np.nan),
                bins=bins,
                labels=False
            ) + 1

    if "dq_potaspeed" in df.columns and "wheel_speed_ms" in df.columns:
        v_w = df["wheel_speed_ms"]
        v_p = df["dq_potaspeed"].replace(0, np.nan)
        df["slip"] = (v_w - v_p) / v_p

    if "kes_stationpressure" in df.columns and "kes_heatindex" in df.columns:
        p = df["kes_stationpressure"] * 100
        T = df["kes_heatindex"] + 273.15
        df["air_density"] = p / (287.05 * T)

    if "dq_potaspeed" in df.columns and "air_density" in df.columns:
        df["q_dynamic"] = 0.5 * df["air_density"] * df["dq_potaspeed"]**2

    if "wheel_speed_ms" in df.columns:
        df["speed_mph"] = df["wheel_speed_ms"].apply(convert_speed_ms_to_mph)

    if "accel_ms2" in df.columns:
        df["accel_fts2"] = df["accel_ms2"].apply(convert_accel_ms2_to_fts2)

    return df

# ------------------------------------------------------------
# PERFORMANCE METRICS
# ------------------------------------------------------------

def compute_performance_metrics(df, meta):
    results = {}
    if "wheel_speed_ms" not in df.columns:
        return {"error": "wheel_speed_ms missing"}

    df = df.copy()
    df["speed_mph"] = df["wheel_speed_ms"].apply(convert_speed_ms_to_mph)
    if "accel_ms2" in df.columns:
        df["accel_fts2"] = df["accel_ms2"].apply(convert_accel_ms2_to_fts2)

    def time_to_speed(target):
        hits = df[df["speed_mph"] >= target]
        if hits.empty:
            return None
        return (hits.index[0] - df.index[0]).total_seconds()

    results["t_0_20_mph"] = time_to_speed(20)
    results["t_20_40_mph"] = time_to_speed(40)
    results["t_40_60_mph"] = time_to_speed(60)

    if "accel_ms2" in df.columns:
        idx = df["accel_ms2"].idxmax()
        results["peak_accel_fts2"] = df.loc[idx, "accel_fts2"]
        results["peak_accel_speed_mph"] = df.loc[idx, "speed_mph"]

    rider_mass = lb_to_kg(meta.get("rider_weight_lb", DEFAULT_RIDER_WEIGHT_LB))
    bike_mass = lb_to_kg(DEFAULT_BIKE_WEIGHT_LB)
    total_mass = rider_mass + bike_mass

    if "accel_ms2" in df.columns:
        df["power_watts"] = total_mass * df["accel_ms2"] * df["wheel_speed_ms"]
        idx = df["power_watts"].idxmax()
        results["peak_power_watts"] = df.loc[idx, "power_watts"]
        results["peak_power_speed_mph"] = df.loc[idx, "speed_mph"]

    tail = df[df.index >= df.index[-1] - timedelta(seconds=2)]
    results["terminal_speed_mph"] = tail["speed_mph"].mean()
    results["terminal_accel_fts2"] = tail["accel_fts2"].mean() if "accel_fts2" in df.columns else None

    return results

# ------------------------------------------------------------
# PLOTTING
# ------------------------------------------------------------

def plot_unified_run(df, meta):
    fig = go.Figure()
    t = df.index

    if "speed_mph" in df.columns:
        fig.add_trace(go.Scatter(x=t, y=df["speed_mph"], name="Speed (mph)"))

    if "aim_tach" in df.columns:
        fig.add_trace(go.Scatter(x=t, y=df["aim_tach"], name="RPM", yaxis="y2"))

    if "accel_fts2" in df.columns:
        fig.add_trace(go.Scatter(x=t, y=df["accel_fts2"], name="Accel (ft/s²)"))

    fig.update_layout(
        title=f"Unified Run Viewer — Test {meta.get('testID', '')}",
        xaxis_title="Time",
        yaxis_title="Speed / Accel",
        yaxis2=dict(title="RPM", overlaying="y", side="right"),
        height=800
    )

    fig.show()

# ------------------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LSR Unified Run Viewer — Remote Access")
    parser.add_argument("test_id", type=int)
    args = parser.parse_args()

    aim = load_aim(args.test_id)
    dq = load_dataq(args.test_id)
    kes = load_kestrel(args.test_id)
    meta = load_metadata(args.test_id)

    print("Connected to Mason's database remotely.")
