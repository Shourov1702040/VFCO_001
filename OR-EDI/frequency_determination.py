# frequency_determination.py
import numpy as np
import pandas as pd
from pathlib import Path

EPS = 1e-9


# A) Load static parameters _____________________________________________________________________

def load_static(es_static_csv: str, es_id: str) -> dict:
    df = pd.read_csv(es_static_csv)
    row = df.loc[df["es_id"] == es_id]
    if row.empty:
        raise ValueError(f"es_id={es_id} not found in {es_static_csv}")
    r = row.iloc[0].to_dict()
    return {
        "C_nominal": float(r["C_nominal"]),
        "f_min": float(r["f_min"]),
        "f_max": float(r["f_max"]),
        # "T": float(r["T"]),
    }


# B) Load latest metrics row_____________________________________________________________________

def load_last_metrics(es_metrics_csv: str) -> dict:
    df = pd.read_csv(es_metrics_csv)
    if df.empty:
        raise ValueError(f"No rows in {es_metrics_csv}")
    last = df.iloc[-1].to_dict()

    # Required columns (as you specified)
    needed = [
        "TP","TN","u_avg","a_avg","D_avg_s",
        "tau_cpu_avg_s","b_net_avg_bytes","U_cpu_avg","U_net_avg"
    ]
    for k in needed:
        if k not in last:
            raise ValueError(f"Missing column {k} in {es_metrics_csv}")

    TP = float(last["TP"])
    TN = float(last["TN"])
    rho = TN / (TP + TN + EPS)

    return {
        "TP": TP,
        "TN": TN,
        "rho": float(np.clip(rho, 0.0, 1.0)),
        "u_avg": float(last["u_avg"]),
        "a_avg": float(last["a_avg"]),
        "D_avg_s": float(last["D_avg_s"]),
        "tau_cpu_avg_s": float(last["tau_cpu_avg_s"]),
        "b_net_avg_bytes": float(last["b_net_avg_bytes"]),
        "U_cpu_avg": float(last["U_cpu_avg"]),
        "U_net_avg": float(last["U_net_avg"]),
        # optional bookkeeping (not required)
        "f_applied": float(last.get("f_applied", np.nan)),
        "window_id": last.get("window_id", None),
    }


# C) Objectives (new manuscript)_____________________________________________________________________

def risk_R(f: float, rho: float, T: float, a_avg: float = 1.0, importance: float = 1.0) -> float:
    """Expected corrupted-request exposure under persistent, uniformly timed corruption.

    For m=ceil(fT) privately jittered challenges, use mean residual wait T/(2m)
    as a stochastic estimator of Eq. (4), not the detection probability of Eq. (3).
    This approximation is intended for online selection; measured attack times are
    used for experimental CST where available.
    """
    m = max(1, int(np.ceil(f * T - 1e-12)))
    return float(np.clip(rho, 0, 1)) * max(0.0, a_avg) * max(0.0, importance) * T / (2.0 * m)


def service_degradation_S(f: float, C_nominal: float, u_avg: float, a_avg: float, D_avg_s: float) -> float:
    # C_eff(f) = C(1 - f D)
    C_eff = C_nominal * (1.0 - f * D_avg_s)
    if C_eff <= 0:
        return np.inf  # infeasible
    alpha = a_avg / (C_eff + EPS)
    if alpha >= 1.0:
        return np.inf  # unstable / infeasible
    Gamma = 1.0 / (1.0 - alpha + EPS)
    # S(f) = u_avg * (Gamma - 1)
    return u_avg * (Gamma - 1.0)

def resource_consumption_Cres(f: float, tau_cpu_avg_s: float, b_net_avg_bytes: float, U_cpu_avg: float, U_net_avg: float) -> float:
    # C(f) = f*tau/(1-U_cpu) + f*b/(1-U_net)
    denom_cpu = max(1.0 - U_cpu_avg, EPS)
    denom_net = max(1.0 - U_net_avg, EPS)
    return (f * tau_cpu_avg_s) / denom_cpu + (f * b_net_avg_bytes) / denom_net


# D) Normalization _____________________________________________________________________

def minmax_norm(arr: np.ndarray) -> np.ndarray:
    a_min = float(np.min(arr))
    a_max = float(np.max(arr))
    if abs(a_max - a_min) < 1e-12:
        return np.zeros_like(arr, dtype=float)
    return (arr - a_min) / (a_max - a_min + EPS)




# E) Choose f_next (per ES)_____________________________________________________________________

def choose_f_next(
    es_static_csv: str,
    es_metrics_csv: str,
    es_id: str,
    T_control:float,
    weights=(0.4, 0.3, 0.3),
    df_step: float | None = None,
    freq_levels: list[float] | None = None,
    weight_refs: dict | None = None,       # legacy optional arguments (unused)
    weight_exponents: dict | None = None,  # AHP priorities remain fixed
    max_gap_s: float | None = None,
) -> tuple[float, float]:
    """Return (rho, f_next, ceil(f_next*T), f_min, f_max, previous_frequency).

    Candidate costs follow the manuscript's weighted, normalized exposure,
    service-degradation and resource models; infeasible candidates are rejected.
    """
    T = T_control
    st = load_static(es_static_csv, es_id)
    met = load_last_metrics(es_metrics_csv)

    # Normalize base weights (in case AHP output slightly off)
    w1, w2, w3 = map(float, weights)
    s0 = w1 + w2 + w3
    if s0 <= 0:
        w1, w2, w3 = (1/3, 1/3, 1/3)
    else:
        w1, w2, w3 = (w1/s0, w2/s0, w3/s0)

    # AHP weights encode application preferences, not measured workload.
    # Recent observations influence the three objectives instead.

    f_min, f_max,  = st["f_min"], st["f_max"]
    f_applied_prev = met["f_applied"]

    # Candidate frequencies
    if freq_levels is not None and len(freq_levels) > 0:
        F = np.array(sorted(freq_levels), dtype=float)
        F = F[(F >= f_min) & (F <= f_max)]
    else:
        if df_step is None:
            df_step = (f_max - f_min) / 20.0 if f_max > f_min else max(f_min, 1e-3)
        if df_step <= 0:
            df_step = max(f_min, 1e-3)
        F = np.arange(f_min, f_max + df_step/2.0, df_step, dtype=float)

    # Evaluate objectives
    R_vals, S_vals, C_vals, F_ok = [], [], [], []

    for f in F:
        m = int(np.ceil(float(f) * T - 1e-12))
        if f <= 0 or m < 1:
            continue
        # Eq. (22) requires a nonzero randomization range for this gap.
        if max_gap_s is not None and max_gap_s <= T / m:
            continue
        R  = risk_R(f, met["rho"], T, met["a_avg"])
        S  = service_degradation_S(f, st["C_nominal"], met["u_avg"], met["a_avg"], met["D_avg_s"])  # higher f => higher S
        Cc = resource_consumption_Cres(f, met["tau_cpu_avg_s"], met["b_net_avg_bytes"], met["U_cpu_avg"], met["U_net_avg"])  # higher f => higher C

        if not np.isfinite(S):
            continue

        F_ok.append(f)
        R_vals.append(R)
        S_vals.append(S)
        C_vals.append(Cc)

    if len(F_ok) == 0:
        raise ValueError(f"No VFCO-feasible frequency for {es_id}; check capacity, delay and maximum gap")

    F_ok = np.array(F_ok, dtype=float)
    R_vals = np.array(R_vals, dtype=float)
    S_vals = np.array(S_vals, dtype=float)
    C_vals = np.array(C_vals, dtype=float)

    # Normalize per objective (still OK because weights now carry parameter influence)
    Rn = minmax_norm(R_vals)
    Sn = minmax_norm(S_vals)
    Cn = minmax_norm(C_vals)

    cost = w1 * Rn + w2 * Sn + w3 * Cn
    best_idx = int(np.argmin(cost))
    f_next = float(F_ok[best_idx])

    return float(met["rho"]), f_next, int(np.ceil(f_next * T - 1e-12)), f_min, f_max, f_applied_prev

# E for event triggered frequency determination 
def event_triggered_frequency(f_min, f_max, f_applied_prev, rho, theta=0, clean_epochs=1):
    """Section 5.1.3: increase after tampering, decrease after k clean epochs.

    Pass the observed tamper count (rather than rho) and the tracked streak
    for exact k-epoch experiments. The 1-epoch default preserves compatibility.
    """
    if rho > theta:
        return min(1.5 * f_applied_prev, f_max)
    if clean_epochs >= 1:
        return max((2.0 / 3.0) * f_applied_prev, f_min)
    return f_applied_prev

# F) Write back f_next to CSV _____________________________________________________________________
def write_f_next(es_metrics_csv: str, f_next: float) -> None:
    df = pd.read_csv(es_metrics_csv)
    if df.empty:
        raise ValueError(f"No rows in {es_metrics_csv}")
    df.loc[df.index[-1], "f_next"] = f_next
    df.to_csv(es_metrics_csv, index=False)
