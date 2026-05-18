"""
Validation module for strict-mode telemetry checking.
"""

import pandas as pd


class ValidationError(Exception):
    pass


class ValidationWarning(Exception):
    pass


# ------------------------------------------------------------
# Pitot Validation (Option A)
# ------------------------------------------------------------

def validate_pitot(df: pd.DataFrame):
    """
    Pitot validation (Option A):
    - Missing pitot = WARNING
    - Small negative values allowed (normal sensor offset)
    - Large negative values (< -0.5 kPa) = ERROR
    - Constant values = WARNING
    """
    if "dq_pitot_kpa" not in df.columns:
        raise ValidationWarning(
            "Pitot data missing — aerodynamic analysis disabled for this run."
        )

    q = df["dq_pitot_kpa"]

    if q.isna().all():
        raise ValidationWarning(
            "Pitot data present but empty — aerodynamic analysis disabled."
        )

    try:
        q = q.astype(float)
    except Exception:
        raise ValidationError("Pitot data contains non-numeric values.")

    if q.min() < -0.5:
        raise ValidationError(
            f"Pitot data contains large negative values ({q.min():.2f} kPa). "
            "Sensor may be reversed or malfunctioning."
        )

    if q.nunique() == 1:
        raise ValidationWarning(
            "Pitot data constant — sensor may have been unplugged. Aero analysis disabled."
        )


# ------------------------------------------------------------
# DataQ/MyChron Validation
# ------------------------------------------------------------

def validate_dataq_mychron(df: pd.DataFrame):
    if df.empty:
        raise ValidationError("DataQ/MyChron data is empty.")

    required = ["t_s", "my_mph"]
    for col in required:
        if col not in df.columns:
            raise ValidationError(f"Missing required column: {col}")


# ------------------------------------------------------------
# Kestrel Validation
# ------------------------------------------------------------

def validate_kestrel(df: pd.DataFrame):
    """
    Kestrel validation (final version):
    - Kestrel is OPTIONAL
    - All issues produce WARNINGS, never ERRORS
    - GUI should always continue even if Kestrel is bad
    """

    if df is None or df.empty:
        raise ValidationWarning("Kestrel data missing — air density unavailable.")

    if "barometricpressure" not in df.columns:
        raise ValidationWarning("Kestrel pressure missing — air density unavailable.")

    try:
        p = df["barometricpressure"].astype(float)
    except Exception:
        raise ValidationWarning("Kestrel pressure contains non-numeric values.")

    p_min = p.min()
    p_max = p.max()

    # Acceptable station pressure range (after inHg → kPa conversion)
    if p_min < 70 or p_max > 110:
        raise ValidationWarning(
            f"Kestrel pressure values out of expected range (70–110 kPa). "
            f"Observed range: {p_min:.2f}–{p_max:.2f} kPa. "
            "Air density may be inaccurate."
        )



# ------------------------------------------------------------
# High-Level Validation
# ------------------------------------------------------------

def validate_all(dfs: dict):
    dqmc = dfs["dataq_mychron"]
    kestrel = dfs["kestrel"]

    validate_dataq_mychron(dqmc)
    validate_kestrel(kestrel)
    validate_pitot(dqmc)
