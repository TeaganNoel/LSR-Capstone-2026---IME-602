"""
Plotting utilities using Plotly.

Designed to be GUI-safe:
- No .show()
- Just return figures and DataFrames for the GUI to render/export.
"""

import pandas as pd
import plotly.graph_objects as go

from .metrics import compute_speed_series, compute_distance_from_speed


def plot_single_run(dfs: dict) -> go.Figure:
    """
    Plot speed vs time and speed vs distance for a single run.

    dfs:
        {
            "mychron": df_mychron,
            "dataq_mychron": df_dataq_mychron,
            "dataq": df_dataq,
            "kestrel": df_kestrel
        }
    """
    df_mychron = dfs["mychron"]
    if df_mychron.empty:
        fig = go.Figure()
        fig.update_layout(title="No MyChron data available for this test.")
        return fig

    speed_mps = compute_speed_series(df_mychron)
    distance_m = compute_distance_from_speed(speed_mps)

    fig = make_speed_distance_figure(speed_mps, distance_m)
    return fig


def make_speed_distance_figure(speed_mps: pd.Series,
                               distance_m: pd.Series) -> go.Figure:
    """
    Build a dual-subplot figure: speed vs time, speed vs distance.
    """
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Speed vs Time", "Speed vs Distance"),
        horizontal_spacing=0.15,
    )

    # Speed vs Time
    fig.add_trace(
        go.Scatter(
            x=speed_mps.index,
            y=speed_mps * 3.6,  # m/s -> km/h for nicer scale, or use mph if you prefer
            mode="lines",
            name="Speed (km/h)",
        ),
        row=1,
        col=1,
    )

    # Speed vs Distance
    fig.add_trace(
        go.Scatter(
            x=distance_m,
            y=speed_mps * 3.6,
            mode="lines",
            name="Speed (km/h)",
            showlegend=False,
        ),
        row=1,
        col=2,
    )

    fig.update_xaxes(title_text="Time", row=1, col=1)
    fig.update_yaxes(title_text="Speed (km/h)", row=1, col=1)

    fig.update_xaxes(title_text="Distance (m)", row=1, col=2)
    fig.update_yaxes(title_text="Speed (km/h)", row=1, col=2)

    fig.update_layout(
        title="Run Overview",
        template="plotly_white",
    )

    return fig


def metrics_to_table(metrics: dict) -> pd.DataFrame:
    """
    Convert metrics dict to a simple 2-column DataFrame for display in a Treeview.
    """
    if not metrics:
        return pd.DataFrame(columns=["Metric", "Value"])

    rows = []
    for k, v in metrics.items():
        if k == "sanity_warnings":
            continue
        rows.append({"Metric": k, "Value": v})

    return pd.DataFrame(rows)
