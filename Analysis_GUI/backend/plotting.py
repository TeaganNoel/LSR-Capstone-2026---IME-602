"""
Plotting utilities using Plotly.

This module:
- Creates Plotly figures for browser-based rendering
- Returns only the Figure object (no HTML, no PNG)
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .metrics import (
    compute_speed_series,
    compute_acceleration_from_speed,
    estimate_drag_force_from_pitot,
)

# ------------------------------------------------------------
# Metric Units (for GUI table)
# ------------------------------------------------------------

METRIC_UNITS = {
    "v_max_mps": "m/s",
    "v_max_mph": "mph",
    "time_to_vmax_s": "s",
    "distance_to_vmax_m": "mi",  # now miles (converted in GUI)
    "air_density_kg_m3": "kg/m³",
    "peak_drag_coefficient": "-",  # dimensionless
}


# ------------------------------------------------------------
# Helper: detect pass boundaries (speed < 10 mph)
# ------------------------------------------------------------

def _find_pass_boundaries(speed_mph: pd.Series, threshold: float = 10.0):
    """
    Return a list of times where speed drops below `threshold` mph
    and marks the start of a "slow" segment (pass boundary).
    """
    if speed_mph.empty:
        return []

    below = speed_mph < threshold
    # Rising edges of "below" segments
    edges = (below & ~below.shift(1, fill_value=False))
    return list(speed_mph.index[edges])


# ------------------------------------------------------------
# Single Run Plot
# ------------------------------------------------------------

def plot_single_run(dfs: dict):
    """
    Plot a single run with:
    - Left: Speed (mph), RPM, acceleration, drag force vs time
    - Right: EGT and coolant temperature vs time
    """

    df_dqmc = dfs["dataq_mychron"]

    # -----------------------------
    # Core time-series
    # -----------------------------
    speed_mps = compute_speed_series(df_dqmc)
    speed_mph = speed_mps / 0.44704
    t_s = speed_mps.index

    accel_mps2 = compute_acceleration_from_speed(speed_mps)

    rpm = None
    if "my_rpm" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        rpm = pd.Series(
            df_sorted["my_rpm"].astype(float).values,
            index=df_sorted["t_s"].astype(float),
            name="rpm"
        )

    # -----------------------------
    # Drag force
    # -----------------------------
    drag_force = estimate_drag_force_from_pitot(df_dqmc, dfs.get("kestrel"))
    drag_available = drag_force is not None and not drag_force.empty

    # -----------------------------
    # Temperature signals
    # -----------------------------
    egt = None
    if "my_egt_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        egt = pd.Series(
            df_sorted["my_egt_f"].astype(float).values,
            index=df_sorted["t_s"].astype(float),
            name="EGT (°F)",
        )

    coolant = None
    if "my_h2o_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        coolant = pd.Series(
            df_sorted["my_h2o_f"].astype(float).values,
            index=df_sorted["t_s"].astype(float),
            name="Coolant (°F)",
        )

    # -----------------------------
    # Pass boundaries
    # -----------------------------
    pass_times = _find_pass_boundaries(speed_mph, threshold=10.0)

    # -----------------------------
    # Create subplot layout
    # -----------------------------
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Speed / RPM / Acceleration / Drag vs Time",
            "EGT and Coolant Temperature vs Time",
        ),
        specs=[[{"secondary_y": True}, {"secondary_y": True}]],
        horizontal_spacing=0.12,
    )

    # ------------------------------------------------------------
    # LEFT PLOT — Speed, RPM, Acceleration, Drag
    # ------------------------------------------------------------

    # Speed
    fig.add_trace(
        go.Scatter(
            x=t_s,
            y=speed_mph,
            mode="lines",
            name="Speed (mph)",
            line=dict(color="royalblue"),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    # RPM
    if rpm is not None:
        fig.add_trace(
            go.Scatter(
                x=rpm.index,
                y=rpm,
                mode="lines",
                name="RPM",
                line=dict(color="firebrick"),
            ),
            row=1,
            col=1,
            secondary_y=True,
        )

    # Acceleration (overlay)
    if not accel_mps2.empty:
        fig.add_trace(
            go.Scatter(
                x=accel_mps2.index,
                y=accel_mps2,
                mode="lines",
                name="Acceleration (m/s²)",
                yaxis="y3",
                line=dict(color="green"),
            ),
            row=1,
            col=1,
        )

    # Drag force (overlay)
    if drag_available:
        fig.add_trace(
            go.Scatter(
                x=drag_force.index,
                y=drag_force,
                mode="lines",
                name="Drag Force (N)",
                yaxis="y4",
                line=dict(color="orange"),
            ),
            row=1,
            col=1,
        )

    # Axes for left plot
    fig.update_xaxes(title_text="Time (s)", row=1, col=1)
    fig.update_yaxes(title_text="Speed (mph)", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="RPM", row=1, col=1, secondary_y=True)

    # Extra overlay axes for accel + drag
    fig.update_layout(
        yaxis3=dict(
            title="Acceleration (m/s²)",
            overlaying="y",
            side="right",
            position=0.98,
            showgrid=False,
        ),
        yaxis4=dict(
            title="Drag Force (N)",
            overlaying="y",
            side="left",
            position=0.0,
            showgrid=False,
        ),
    )

    # ------------------------------------------------------------
    # RIGHT PLOT — All Temperature Channels
    # ------------------------------------------------------------

    temp_traces = []

    # Water temp
    if "my_h2o_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        temp_traces.append((
            df_sorted["t_s"].astype(float),
            df_sorted["my_h2o_f"].astype(float),
            "Water Temp (°F)",
            "deepskyblue"
        ))

    # Exhaust gas temp
    if "my_egt_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        temp_traces.append((
            df_sorted["t_s"].astype(float),
            df_sorted["my_egt_f"].astype(float),
            "EGT (°F)",
            "purple"
        ))

    # Cylinder head temp
    if "dq_cht_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        temp_traces.append((
            df_sorted["t_s"].astype(float),
            df_sorted["dq_cht_f"].astype(float),
            "CHT (°F)",
            "orange"
        ))

    # Air temp
    if "dq_in_air_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        temp_traces.append((
            df_sorted["t_s"].astype(float),
            df_sorted["dq_in_air_f"].astype(float),
            "Air Temp (°F)",
            "green"
        ))

    # Tank temp
    if "dq_tank_f" in df_dqmc.columns:
        df_sorted = df_dqmc.sort_values("t_s")
        temp_traces.append((
            df_sorted["t_s"].astype(float),
            df_sorted["dq_tank_f"].astype(float),
            "Tank Temp (°F)",
            "red"
        ))

    if temp_traces:
        # First trace uses primary y-axis, others use secondary
        first = True
        for t, y, label, color in temp_traces:
            fig.add_trace(
                go.Scatter(
                    x=t,
                    y=y,
                    mode="lines",
                    name=label,
                    line=dict(color=color),
                ),
                row=1,
                col=2,
                secondary_y=not first,
            )
            first = False

        fig.update_yaxes(title_text="Primary Temps (°F)", row=1, col=2, secondary_y=False)
        fig.update_yaxes(title_text="Secondary Temps (°F)", row=1, col=2, secondary_y=True)

    else:
        fig.add_annotation(
            text="Temperature data unavailable.",
            xref="x2",
            yref="y2",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=14),
        )

    fig.update_xaxes(title_text="Time (s)", row=1, col=2)


    # ------------------------------------------------------------
    # Pass boundary lines (both plots)
    # ------------------------------------------------------------

    for t in pass_times:
        fig.add_vline(
            x=t,
            line_width=1,
            line_color="lightgray",
            opacity=0.2,
            row=1,
            col=1,
        )
        fig.add_vline(
            x=t,
            line_width=1,
            line_color="lightgray",
            opacity=0.2,
            row=1,
            col=2,
        )

    # ------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------

    fig.update_layout(
        title="Run Overview",
        template="plotly_white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
    )

    return fig


# ------------------------------------------------------------
# Multi-Run Plot
# ------------------------------------------------------------

def plot_multi_run(dfs_by_id: dict):
    fig = go.Figure()

    for tid, dfs in dfs_by_id.items():
        df_dqmc = dfs["dataq_mychron"]
        speed_mps = compute_speed_series(df_dqmc)
        speed_mph = speed_mps / 0.44704

        fig.add_trace(
            go.Scatter(
                x=speed_mph.index,
                y=speed_mph,
                mode="lines",
                name=f"Run {tid}",
            )
        )

    fig.update_layout(
        title="Multi‑Run Speed vs Time",
        xaxis_title="Time (s)",
        yaxis_title="Speed (mph)",
        template="plotly_white",
    )

    return fig


# ------------------------------------------------------------
# Metrics Table Helper
# ------------------------------------------------------------

def metrics_to_table(metrics: dict) -> pd.DataFrame:
    rows = []
    for k, v in metrics.items():
        if k == "sanity_warnings":
            continue
        rows.append(
            {
                "Metric": k,
                "Value": v,
                "Units": METRIC_UNITS.get(k, ""),
            }
        )
    return pd.DataFrame(rows, columns=["Metric", "Value", "Units"])
