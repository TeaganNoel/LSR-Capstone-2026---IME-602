"""
Preprocessing and basic sanity checks.

Assumptions:
- Data is nominally time-aligned before entering DB.
- We still:
  - Sort by time
  - Check for obvious time issues
  - Optionally create a unified time reference.
"""

import pandas as pd


def _get_earliest_timestamp(dfs: dict) -> pd.Timestamp | None:
    """
    Find earliest timestamp across all telemetry DataFrames.
    """
    candidates = []

    if not dfs["mychron"].empty:
        candidates.append(dfs["mychron"]["time_attr"].min())
    if not dfs["dataq_mychron"].empty:
        candidates.append(dfs["dataq_mychron"]["time_attr"].min())
    if not dfs["dataq"].empty:
        candidates.append(dfs["dataq"]["time_attr"].min())
    if not dfs["kestrel"].empty:
        candidates.append(dfs["kestrel"]["datetime_attr"].min())

    if not candidates:
        return None
    return min(candidates)


def basic_sanity_checks(dfs: dict) -> dict:
    """
    Perform simple checks:
    - Ensure time columns are monotonic
    - Flag if any obvious issues
    Returns a dict of warnings (string list).
    """
    warnings = []

    def check_monotonic(df, col, label):
        if df.empty or col not in df.columns:
            return
        if not df[col].is_monotonic_increasing:
            warnings.append(f"{label}: {col} is not monotonic increasing.")

    check_monotonic(dfs["mychron"], "time_attr", "mychron")
    check_monotonic(dfs["dataq_mychron"], "time_attr", "dataq_mychron")
    check_monotonic(dfs["dataq"], "time_attr", "dataq")
    check_monotonic(dfs["kestrel"], "datetime_attr", "kestrel")

    return {"warnings": warnings}


def create_unified_context(dfs: dict) -> dict:
    """
    Create a simple unified context object for downstream metrics/plotting.

    Returns:
        {
            "dfs": original dfs,
            "t0": earliest_timestamp (or None),
            "sanity": { "warnings": [...] }
        }
    """
    t0 = _get_earliest_timestamp(dfs)
    sanity = basic_sanity_checks(dfs)
    return {
        "dfs": dfs,
        "t0": t0,
        "sanity": sanity,
    }
