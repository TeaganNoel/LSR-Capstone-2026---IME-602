"""
Preprocessing and basic sanity checks.

Assumptions:
- t_s is relative time in seconds since start.
- Kestrel timestamps are absolute.
"""

import pandas as pd


def _get_earliest_timestamp(dfs):
    """
    Return the earliest timestamp across all telemetry sources.
    Converts all timestamps to float seconds for comparison.
    """

    candidates = []

    # DataQ/MyChron timestamps (already float seconds)
    dqmc = dfs.get("dataq_mychron")
    if dqmc is not None and not dqmc.empty:
        if "t_s" in dqmc.columns:
            candidates.append(float(dqmc["t_s"].min()))

    # Kestrel timestamps (datetime → float seconds)
    kestrel = dfs.get("kestrel")
    if kestrel is not None and not kestrel.empty:
        if "datetime_attr" in kestrel.columns:
            try:
                dt = pd.to_datetime(kestrel["datetime_attr"])
                # convert to seconds since epoch
                ts = dt.astype("int64") / 1e9
                candidates.append(float(ts.min()))
            except Exception:
                pass  # ignore bad timestamps

    if not candidates:
        return 0.0

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
        if df is None or df.empty or col not in df.columns:
            return
        if not df[col].is_monotonic_increasing:
            warnings.append(f"{label}: {col} is not monotonic increasing.")

    check_monotonic(dfs.get("dataq_mychron"), "t_s", "dataq_mychron")
    check_monotonic(dfs.get("kestrel"), "datetime_attr", "kestrel")

    return {"warnings": warnings}


"""
Preprocessing module for unifying telemetry sources.
"""

import pandas as pd


def create_unified_context(dfs: dict) -> dict:
    """
    Create a unified context dictionary for metrics and plotting.

    dfs:
        {
            "dataq_mychron": df_dqmc,
            "kestrel": df_kestrel
        }
    """
    dqmc = dfs["dataq_mychron"]

    if dqmc.empty:
        t0 = 0.0
    else:
        t0 = float(dqmc["t_s"].min())

    context = {
        "dfs": dfs,
        "t0": t0,
        "sanity": {"warnings": []},
    }

    return context
