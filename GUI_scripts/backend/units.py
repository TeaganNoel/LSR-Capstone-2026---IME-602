"""
Unit conversion utilities.
"""

MPH_TO_MPS = 0.44704
FT_TO_M = 0.3048
LB_TO_N = 4.4482216152605


def mph_to_mps(v_mph):
    return v_mph * MPH_TO_MPS


def mps_to_mph(v_mps):
    return v_mps / MPH_TO_MPS


def ft_to_m(x_ft):
    return x_ft * FT_TO_M


def m_to_ft(x_m):
    return x_m / FT_TO_M


def lb_to_n(f_lb):
    return f_lb * LB_TO_N


def n_to_lb(f_n):
    return f_n / LB_TO_N


def f_to_c(temp_f):
    return (temp_f - 32.0) * 5.0 / 9.0


def c_to_f(temp_c):
    return temp_c * 9.0 / 5.0 + 32.0
